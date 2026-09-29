use std::{
    collections::BTreeMap,
    future::Future,
    io::{Error, ErrorKind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, Semaphore, broadcast, mpsc},
};

use crate::{
    FileFuture, FileHandle, FileHandleId, FileOperation, FileResponse, FileServiceError,
    FileSessionService, IncomingMessage, MAX_FILE_READ_LENGTH, MAX_FILESYSTEM_SYNC_FRAME_SIZE,
    OpenRequest, ScanPage, SessionRequest, SyncMessage, Transport,
};

const IROH_FILE_PROTOCOL_VERSION: u8 = 1;
const MAX_ACTIVE_REQUESTS: usize = 64;
const MAX_READ_RESPONSE_FRAMES: usize = 512;
const MAX_READ_RESPONSE_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IrohResourceDescriptor {
    pub owner_subject: String,
    pub application_id: String,
    pub filesystem_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(bound(serialize = "P: Serialize", deserialize = "P: Ord + Deserialize<'de>"))]
enum WireMessage<P> {
    Hello {
        version: u8,
        resource: IrohResourceDescriptor,
    },
    Metadata(SyncMessage<P>),
    Request {
        id: u64,
        operation: FileOperation,
    },
    Response {
        id: u64,
        response: FileResponse<P>,
    },
}

type FrameAuthorizer = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

struct Inner {
    peer: EndpointId,
    authorize_frame: Option<FrameAuthorizer>,
    outgoing: mpsc::Sender<Vec<u8>>,
    metadata: broadcast::Sender<IncomingMessage<EndpointId>>,
    sessions: broadcast::Sender<SessionRequest<EndpointId>>,
    pending: Mutex<BTreeMap<u64, mpsc::UnboundedSender<FileResponse<EndpointId>>>>,
    request_slots: Arc<Semaphore>,
    next_id: AtomicU64,
}

#[derive(Clone)]
pub struct IrohFileTransport {
    inner: Arc<Inner>,
}

impl IrohFileTransport {
    pub async fn open(
        connection: &Connection,
        resource: IrohResourceDescriptor,
    ) -> Result<Self, Error> {
        let (send, recv) = connection.open_bi().await.map_err(Error::other)?;
        let transport = Self::new(connection.remote_id(), send, recv, true, None);
        transport
            .enqueue(WireMessage::<EndpointId>::Hello {
                version: IROH_FILE_PROTOCOL_VERSION,
                resource,
            })
            .await?;
        Ok(transport)
    }

    pub async fn open_authorized<F, Fut>(
        connection: &Connection,
        resource: IrohResourceDescriptor,
        authorize: F,
    ) -> Result<Self, Error>
    where
        F: Fn(IrohResourceDescriptor) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        if !authorize(resource.clone()).await {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "filesystem resource is not authorized for sync",
            ));
        }
        let (send, recv) = connection.open_bi().await.map_err(Error::other)?;
        let authorize = Arc::new(authorize);
        let authorized_resource = resource.clone();
        let authorize_frame: FrameAuthorizer = Arc::new(move || {
            let authorize = Arc::clone(&authorize);
            let resource = authorized_resource.clone();
            Box::pin(async move { authorize(resource).await })
        });
        let transport = Self::new(
            connection.remote_id(),
            send,
            recv,
            true,
            Some(authorize_frame),
        );
        transport
            .enqueue(WireMessage::<EndpointId>::Hello {
                version: IROH_FILE_PROTOCOL_VERSION,
                resource,
            })
            .await?;
        Ok(transport)
    }

    pub async fn accept_authorized<F, Fut>(
        connection: &Connection,
        send: SendStream,
        mut recv: RecvStream,
        authorize: F,
    ) -> Result<(Self, IrohResourceDescriptor), Error>
    where
        F: Fn(IrohResourceDescriptor) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        let resource = match read_message::<EndpointId>(&mut recv).await? {
            WireMessage::Hello { version, resource } if version == IROH_FILE_PROTOCOL_VERSION => {
                resource
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "invalid filesystem stream handshake",
                ));
            }
        };
        if !authorize(resource.clone()).await {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "filesystem resource is not authorized for sync",
            ));
        }
        let authorize = Arc::new(authorize);
        let authorized_resource = resource.clone();
        let authorize_frame: FrameAuthorizer = Arc::new(move || {
            let authorize = Arc::clone(&authorize);
            let resource = authorized_resource.clone();
            Box::pin(async move { authorize(resource).await })
        });
        let transport = Self::new(
            connection.remote_id(),
            send,
            recv,
            false,
            Some(authorize_frame),
        );
        transport
            .enqueue(WireMessage::<EndpointId>::Hello {
                version: IROH_FILE_PROTOCOL_VERSION,
                resource: resource.clone(),
            })
            .await?;
        Ok((transport, resource))
    }

    fn new(
        peer: EndpointId,
        send: SendStream,
        recv: RecvStream,
        expect_hello: bool,
        authorize_frame: Option<FrameAuthorizer>,
    ) -> Self {
        let (outgoing, mut outgoing_rx) = mpsc::channel::<Vec<u8>>(64);
        let (metadata, _) = broadcast::channel(64);
        let (sessions, _) = broadcast::channel(64);
        let inner = Arc::new(Inner {
            peer,
            authorize_frame,
            outgoing,
            metadata,
            sessions,
            pending: Mutex::new(BTreeMap::new()),
            request_slots: Arc::new(Semaphore::new(MAX_ACTIVE_REQUESTS)),
            next_id: AtomicU64::new(1),
        });
        let writer_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            let mut send = send;
            while let Some(frame) = outgoing_rx.recv().await {
                if write_frame(&mut send, &frame).await.is_err() {
                    break;
                }
            }
            let _ = send.finish();
            fail_pending(&writer_inner).await;
        });
        tokio::spawn(read_loop(Arc::clone(&inner), recv, expect_hello));
        Self { inner }
    }

    async fn request(
        &self,
        operation: FileOperation,
    ) -> Result<(u64, mpsc::UnboundedReceiver<FileResponse<EndpointId>>), Error> {
        if matches!(&operation, FileOperation::Write { data, .. } if data.len() > MAX_FILESYSTEM_SYNC_FRAME_SIZE - 128)
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "filesystem write exceeds frame size limit",
            ));
        }
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode(&WireMessage::<EndpointId>::Request { id, operation })?;
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.pending.lock().await.insert(id, tx);
        if let Err(error) = self.inner.outgoing.try_send(frame) {
            self.inner.pending.lock().await.remove(&id);
            return Err(Error::other(error));
        }
        Ok((id, rx))
    }

    async fn rpc(&self, operation: FileOperation) -> Result<Vec<FileResponse<EndpointId>>, Error> {
        let read_limit = match &operation {
            FileOperation::Read { length, .. } => {
                Some((*length).min(MAX_FILE_READ_LENGTH) as usize)
            }
            _ => None,
        };
        let (id, mut rx) = self.request(operation).await?;
        let mut responses = Vec::new();
        let mut read_bytes = 0_usize;
        while let Some(response) = rx.recv().await {
            if let (Some(limit), FileResponse::ReadChunk(bytes)) = (read_limit, &response) {
                let Some(total) = read_bytes.checked_add(bytes.len()) else {
                    self.inner.pending.lock().await.remove(&id);
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "read response exceeds requested length",
                    ));
                };
                read_bytes = total;
                if read_bytes > limit || responses.len() >= MAX_READ_RESPONSE_FRAMES {
                    self.inner.pending.lock().await.remove(&id);
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "read response exceeds requested bounds",
                    ));
                }
            }
            let done = !matches!(response, FileResponse::ReadChunk(_));
            responses.push(response);
            if done {
                break;
            }
        }
        self.inner.pending.lock().await.remove(&id);
        if responses.is_empty() {
            return Err(Error::new(
                ErrorKind::BrokenPipe,
                "Iroh file session closed",
            ));
        }
        Ok(responses)
    }
}

impl Transport<EndpointId> for IrohFileTransport {
    type Error = Error;

    fn peers(&self) -> Vec<EndpointId> {
        vec![self.inner.peer]
    }

    async fn send(&self, peer: EndpointId, message: SyncMessage<EndpointId>) -> Result<(), Error> {
        self.check_peer(peer)?;
        self.enqueue(WireMessage::Metadata(message)).await
    }

    async fn broadcast(&self, message: SyncMessage<EndpointId>) -> Result<(), Error> {
        self.enqueue(WireMessage::Metadata(message)).await
    }

    fn subscribe(&self) -> broadcast::Receiver<IncomingMessage<EndpointId>> {
        self.inner.metadata.subscribe()
    }
}

impl FileSessionService<EndpointId> for IrohFileTransport {
    type Handle = IrohFileHandle;

    async fn open(&self, peer: EndpointId, request: OpenRequest) -> Result<Self::Handle, Error> {
        self.check_peer(peer)?;
        let responses = self.rpc(FileOperation::Open(request)).await?;
        match responses.into_iter().next() {
            Some(FileResponse::Opened {
                handle, revision, ..
            }) => Ok(IrohFileHandle {
                transport: self.clone(),
                handle,
                revision,
            }),
            Some(FileResponse::Error(error)) => Err(service_error(error)),
            _ => Err(Error::new(ErrorKind::InvalidData, "invalid open response")),
        }
    }

    fn subscribe_file_sessions(&self) -> broadcast::Receiver<SessionRequest<EndpointId>> {
        self.inner.sessions.subscribe()
    }
}

impl IrohFileTransport {
    fn check_peer(&self, peer: EndpointId) -> Result<(), Error> {
        if peer != self.inner.peer {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "request peer does not match connection peer",
            ));
        }
        Ok(())
    }

    async fn enqueue<P: Serialize>(&self, message: WireMessage<P>) -> Result<(), Error> {
        if let Some(authorize_frame) = &self.inner.authorize_frame
            && !authorize_frame().await
        {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "filesystem sync access revoked",
            ));
        }
        let frame = encode(&message)?;
        self.inner.outgoing.send(frame).await.map_err(Error::other)
    }
}

pub struct IrohFileHandle {
    transport: IrohFileTransport,
    handle: FileHandleId,
    revision: uuid::Uuid,
}

impl FileHandle<EndpointId> for IrohFileHandle {
    type Error = Error;

    fn read<'a>(
        &'a mut self,
        offset: u64,
        length: u32,
    ) -> FileFuture<'a, Result<bytes::Bytes, Error>> {
        Box::pin(async move {
            let responses = self
                .transport
                .rpc(FileOperation::Read {
                    handle: self.handle.clone(),
                    offset,
                    length,
                })
                .await?;
            let mut data = Vec::new();
            for response in responses {
                match response {
                    FileResponse::ReadChunk(bytes) => data.extend_from_slice(&bytes),
                    FileResponse::ReadEnd => break,
                    FileResponse::Error(error) => return Err(service_error(error)),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "invalid read response")),
                }
            }
            Ok(bytes::Bytes::from(data))
        })
    }

    fn write<'a>(
        &'a mut self,
        offset: u64,
        data: bytes::Bytes,
    ) -> FileFuture<'a, Result<u32, Error>> {
        Box::pin(async move {
            match self
                .transport
                .rpc(FileOperation::Write {
                    handle: self.handle.clone(),
                    expected_revision: self.revision,
                    offset,
                    data,
                })
                .await?
                .into_iter()
                .next()
            {
                Some(FileResponse::Written { count, revision }) => {
                    self.revision = revision;
                    Ok(count)
                }
                Some(FileResponse::Error(error)) => Err(service_error(error)),
                _ => Err(Error::new(ErrorKind::InvalidData, "invalid write response")),
            }
        })
    }

    fn scan<'a>(
        &'a mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> FileFuture<'a, Result<ScanPage<EndpointId>, Error>> {
        Box::pin(async move {
            match self
                .transport
                .rpc(FileOperation::Scan {
                    handle: self.handle.clone(),
                    cursor,
                    limit,
                })
                .await?
                .into_iter()
                .next()
            {
                Some(FileResponse::Page(page)) => Ok(page),
                Some(FileResponse::Error(error)) => Err(service_error(error)),
                _ => Err(Error::new(ErrorKind::InvalidData, "invalid scan response")),
            }
        })
    }

    fn close(self: Box<Self>) -> FileFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            match self
                .transport
                .rpc(FileOperation::Close {
                    handle: self.handle,
                })
                .await?
                .into_iter()
                .next()
            {
                Some(FileResponse::Closed) => Ok(()),
                Some(FileResponse::Error(error)) => Err(service_error(error)),
                _ => Err(Error::new(ErrorKind::InvalidData, "invalid close response")),
            }
        })
    }
}

async fn read_loop(inner: Arc<Inner>, mut recv: RecvStream, expect_hello: bool) {
    if expect_hello
        && !matches!(
            read_message::<EndpointId>(&mut recv).await,
            Ok(WireMessage::Hello { version, .. }) if version == IROH_FILE_PROTOCOL_VERSION
        )
    {
        fail_pending(&inner).await;
        return;
    }
    loop {
        let message = match read_message::<EndpointId>(&mut recv).await {
            Ok(message) => message,
            Err(_) => break,
        };
        if let Some(authorize_frame) = &inner.authorize_frame
            && !authorize_frame().await
        {
            break;
        }
        match message {
            WireMessage::Hello { .. } => break,
            WireMessage::Metadata(message) => {
                let _ = inner.metadata.send((inner.peer, message));
            }
            WireMessage::Response { id, response } => {
                if let Some(sender) = inner.pending.lock().await.get(&id) {
                    let _ = sender.send(response);
                }
            }
            WireMessage::Request { id, operation } => {
                let Ok(permit) = Arc::clone(&inner.request_slots).try_acquire_owned() else {
                    let _ = send_response(
                        &inner.outgoing,
                        id,
                        FileResponse::Error(FileServiceError::Io),
                    )
                    .await;
                    continue;
                };
                let (tx, mut rx) = mpsc::channel(16);
                if inner.sessions.send((inner.peer, operation, tx)).is_err() {
                    let _ = send_response(
                        &inner.outgoing,
                        id,
                        FileResponse::Error(FileServiceError::Io),
                    )
                    .await;
                    continue;
                }
                let outgoing = inner.outgoing.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    while let Some(response) = rx.recv().await {
                        let done = !matches!(response, FileResponse::ReadChunk(_));
                        if send_response(&outgoing, id, response).await.is_err() || done {
                            break;
                        }
                    }
                });
            }
        }
    }
    fail_pending(&inner).await;
}

async fn fail_pending(inner: &Inner) {
    inner.pending.lock().await.clear();
}

async fn send_response(
    outgoing: &mpsc::Sender<Vec<u8>>,
    id: u64,
    response: FileResponse<EndpointId>,
) -> Result<(), Error> {
    match response {
        FileResponse::ReadChunk(bytes) => {
            for chunk in bytes.chunks(MAX_READ_RESPONSE_CHUNK_BYTES) {
                send_wire(
                    outgoing,
                    WireMessage::<EndpointId>::Response {
                        id,
                        response: FileResponse::ReadChunk(bytes::Bytes::copy_from_slice(chunk)),
                    },
                )
                .await?;
            }
            Ok(())
        }
        response => {
            send_wire(
                outgoing,
                WireMessage::<EndpointId>::Response { id, response },
            )
            .await
        }
    }
}

async fn send_wire<P: Serialize>(
    outgoing: &mpsc::Sender<Vec<u8>>,
    message: WireMessage<P>,
) -> Result<(), Error> {
    outgoing.send(encode(&message)?).await.map_err(Error::other)
}

fn encode<P: Serialize>(message: &WireMessage<P>) -> Result<Vec<u8>, Error> {
    let frame = postcard::to_allocvec(message).map_err(Error::other)?;
    if frame.len() > MAX_FILESYSTEM_SYNC_FRAME_SIZE {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "filesystem frame exceeds size limit",
        ));
    }
    Ok(frame)
}

async fn read_message<P: DeserializeOwned + Ord>(
    recv: &mut RecvStream,
) -> Result<WireMessage<P>, Error> {
    let mut prefix = [0; 4];
    recv.read_exact(&mut prefix).await.map_err(Error::other)?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len > MAX_FILESYSTEM_SYNC_FRAME_SIZE {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "filesystem frame exceeds size limit",
        ));
    }
    let mut frame = vec![0; len];
    recv.read_exact(&mut frame).await.map_err(Error::other)?;
    postcard::from_bytes(&frame).map_err(Error::other)
}

async fn write_frame(send: &mut SendStream, frame: &[u8]) -> Result<(), Error> {
    if frame.len() > MAX_FILESYSTEM_SYNC_FRAME_SIZE {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "filesystem frame exceeds size limit",
        ));
    }
    send.write_all(&(frame.len() as u32).to_be_bytes())
        .await
        .map_err(Error::other)?;
    send.write_all(frame).await.map_err(Error::other)?;
    send.flush().await.map_err(Error::other)
}

fn service_error(error: FileServiceError) -> Error {
    Error::other(format!("remote file service error: {error:?}"))
}
