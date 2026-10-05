use std::{
    collections::BTreeMap,
    future::Future,
    io::{Error, ErrorKind},
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, watch},
};

use crate::{
    FileFuture, FileHandle, FileHandleId, FileOperation, FileResponse, FileServiceError,
    FileSessionService, IncomingMessage, MAX_FILE_READ_LENGTH, MAX_FILESYSTEM_SYNC_FRAME_SIZE,
    OpenRequest, ScanPage, SessionRequest, SyncMessage, Transport,
};

pub const FILESYSTEM_STREAM_KIND: u8 = 2;

const IROH_FILE_PROTOCOL_VERSION: u8 = 2;
const MAX_ACTIVE_REQUESTS: usize = 64;
const MAX_PENDING_RESPONSES: usize = 1;
const MAX_READ_RESPONSE_FRAMES: usize = 512;
const MAX_READ_RESPONSE_CHUNK_BYTES: usize = MAX_FILESYSTEM_SYNC_FRAME_SIZE - 1024;

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
        deleted: bool,
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
    outgoing: StdMutex<Option<mpsc::Sender<Vec<u8>>>>,
    metadata: StdMutex<Option<broadcast::Sender<IncomingMessage<EndpointId>>>>,
    sessions: StdMutex<Option<broadcast::Sender<SessionRequest<EndpointId>>>>,
    pending: Mutex<BTreeMap<u64, mpsc::Sender<FileResponse<EndpointId>>>>,
    request_slots: Arc<Semaphore>,
    next_id: AtomicU64,
    transport_handles: AtomicUsize,
    shutdown: watch::Sender<bool>,
}

pub struct IrohFileTransport {
    inner: Arc<Inner>,
}

impl Clone for IrohFileTransport {
    fn clone(&self) -> Self {
        self.inner.transport_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for IrohFileTransport {
    fn drop(&mut self) {
        if self.inner.transport_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            close_stream(&self.inner);
        }
    }
}

impl IrohFileTransport {
    pub fn close(&self) {
        close_stream(&self.inner);
    }

    pub async fn open(
        connection: &Connection,
        resource: IrohResourceDescriptor,
    ) -> Result<Self, Error> {
        let (mut send, recv) = connection.open_bi().await.map_err(Error::other)?;
        send.write_all(&[FILESYSTEM_STREAM_KIND]).await?;
        let transport = Self::new(connection.remote_id(), send, recv, true, None);
        transport
            .enqueue(WireMessage::<EndpointId>::Hello {
                version: IROH_FILE_PROTOCOL_VERSION,
                resource,
                deleted: false,
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
        Self::open_authorized_with_deletion(connection, resource, false, authorize).await
    }

    pub async fn open_tombstone_authorized<F, Fut>(
        connection: &Connection,
        resource: IrohResourceDescriptor,
        authorize: F,
    ) -> Result<Self, Error>
    where
        F: Fn(IrohResourceDescriptor) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        Self::open_authorized_with_deletion(connection, resource, true, authorize).await
    }

    async fn open_authorized_with_deletion<F, Fut>(
        connection: &Connection,
        resource: IrohResourceDescriptor,
        deleted: bool,
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
        let (mut send, recv) = connection.open_bi().await.map_err(Error::other)?;
        send.write_all(&[FILESYSTEM_STREAM_KIND]).await?;
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
                deleted,
            })
            .await?;
        Ok(transport)
    }

    pub async fn accept_authorized<F, Fut>(
        connection: &Connection,
        send: SendStream,
        recv: RecvStream,
        authorize: F,
    ) -> Result<(Self, IrohResourceDescriptor, bool), Error>
    where
        F: Fn(IrohResourceDescriptor) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        let mut recv = recv;
        let mut kind = [0; 1];
        recv.read_exact(&mut kind).await.map_err(Error::other)?;
        if kind[0] != FILESYSTEM_STREAM_KIND {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid filesystem stream kind",
            ));
        }
        Self::accept_authorized_after_marker(connection, send, recv, authorize).await
    }

    pub async fn accept_authorized_after_marker<F, Fut>(
        connection: &Connection,
        send: SendStream,
        mut recv: RecvStream,
        authorize: F,
    ) -> Result<(Self, IrohResourceDescriptor, bool), Error>
    where
        F: Fn(IrohResourceDescriptor) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        let (resource, deleted) = match read_message::<EndpointId>(&mut recv).await? {
            WireMessage::Hello {
                version,
                resource,
                deleted,
            } if version == IROH_FILE_PROTOCOL_VERSION => (resource, deleted),
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
                deleted: false,
            })
            .await?;
        Ok((transport, resource, deleted))
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
        let (shutdown, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            peer,
            authorize_frame,
            outgoing: StdMutex::new(Some(outgoing)),
            metadata: StdMutex::new(Some(metadata)),
            sessions: StdMutex::new(Some(sessions)),
            pending: Mutex::new(BTreeMap::new()),
            request_slots: Arc::new(Semaphore::new(MAX_ACTIVE_REQUESTS)),
            next_id: AtomicU64::new(1),
            transport_handles: AtomicUsize::new(1),
            shutdown,
        });
        let writer_inner = Arc::clone(&inner);
        let mut writer_shutdown = inner.shutdown.subscribe();
        tokio::spawn(async move {
            let mut send = send;
            loop {
                let frame = tokio::select! {
                    _ = writer_shutdown.changed() => break,
                    frame = outgoing_rx.recv() => match frame {
                        Some(frame) => frame,
                        None => break,
                    },
                };
                if write_frame(&mut send, &frame).await.is_err() {
                    break;
                }
            }
            let _ = send.finish();
            close_stream(&writer_inner);
            fail_pending(&writer_inner).await;
        });
        tokio::spawn(read_loop(
            Arc::clone(&inner),
            recv,
            expect_hello,
            inner.shutdown.subscribe(),
        ));
        if let Some(authorize) = inner.authorize_frame.clone() {
            let authorization_inner = Arc::clone(&inner);
            let mut authorization_shutdown = inner.shutdown.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = authorization_shutdown.changed() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    }
                    if *authorization_shutdown.borrow()
                        || authorization_inner
                            .outgoing
                            .lock()
                            .expect("Iroh outgoing lock poisoned")
                            .is_none()
                    {
                        break;
                    }
                    if !authorize().await {
                        close_stream(&authorization_inner);
                        fail_pending(&authorization_inner).await;
                        break;
                    }
                }
            });
        }
        Self { inner }
    }

    async fn request(
        &self,
        operation: FileOperation,
    ) -> Result<
        (
            u64,
            mpsc::Receiver<FileResponse<EndpointId>>,
            OwnedSemaphorePermit,
        ),
        Error,
    > {
        if matches!(&operation, FileOperation::Write { data, .. } if data.len() > MAX_FILESYSTEM_SYNC_FRAME_SIZE - 128)
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "filesystem write exceeds frame size limit",
            ));
        }
        let permit = Arc::clone(&self.inner.request_slots)
            .acquire_owned()
            .await
            .map_err(|_| Error::new(ErrorKind::BrokenPipe, "Iroh file session is closed"))?;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode(&WireMessage::<EndpointId>::Request { id, operation })?;
        let outgoing = self
            .inner
            .outgoing
            .lock()
            .expect("Iroh outgoing lock poisoned")
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "Iroh file session is closed"))?;
        let (tx, rx) = mpsc::channel(MAX_PENDING_RESPONSES);
        self.inner.pending.lock().await.insert(id, tx);
        if let Err(error) = outgoing.try_send(frame) {
            self.inner.pending.lock().await.remove(&id);
            return Err(Error::other(error));
        }
        Ok((id, rx, permit))
    }

    async fn rpc(&self, operation: FileOperation) -> Result<Vec<FileResponse<EndpointId>>, Error> {
        let read_limit = match &operation {
            FileOperation::Read { length, .. } => {
                Some((*length).min(MAX_FILE_READ_LENGTH) as usize)
            }
            _ => None,
        };
        let (id, mut rx, _permit) = self.request(operation).await?;
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
        subscribe(&self.inner.metadata)
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
        subscribe(&self.inner.sessions)
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
        let outgoing = self
            .inner
            .outgoing
            .lock()
            .expect("Iroh outgoing lock poisoned")
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "Iroh file session is closed"))?;
        outgoing.send(frame).await.map_err(Error::other)
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

async fn read_loop(
    inner: Arc<Inner>,
    mut recv: RecvStream,
    expect_hello: bool,
    mut shutdown: watch::Receiver<bool>,
) {
    if expect_hello
        && !matches!(
            read_message_or_shutdown::<EndpointId>(&mut recv, &mut shutdown).await,
            Ok(WireMessage::Hello { version, .. }) if version == IROH_FILE_PROTOCOL_VERSION
        )
    {
        close_stream(&inner);
        fail_pending(&inner).await;
        return;
    }
    loop {
        if authorize_frame(&inner).await.is_err() {
            break;
        }
        let message = match read_message_or_shutdown::<EndpointId>(&mut recv, &mut shutdown).await {
            Ok(message) => message,
            Err(_) => break,
        };
        if authorize_frame(&inner).await.is_err() {
            break;
        }
        match message {
            WireMessage::Hello { .. } => break,
            WireMessage::Metadata(message) => {
                let sender = inner
                    .metadata
                    .lock()
                    .expect("Iroh metadata lock poisoned")
                    .as_ref()
                    .cloned();
                let Some(sender) = sender else {
                    break;
                };
                let _ = sender.send((inner.peer, message));
            }
            WireMessage::Response { id, response } => {
                let sender = inner.pending.lock().await.get(&id).cloned();
                if let Some(sender) = sender
                    && sender.send(response).await.is_err()
                {
                    break;
                }
            }
            WireMessage::Request { id, operation } => {
                if inner
                    .outgoing
                    .lock()
                    .expect("Iroh outgoing lock poisoned")
                    .is_none()
                {
                    break;
                }
                let Ok(permit) = Arc::clone(&inner.request_slots).try_acquire_owned() else {
                    let _ =
                        send_response(&inner, id, FileResponse::Error(FileServiceError::Io)).await;
                    continue;
                };
                let (tx, mut rx) = mpsc::channel(MAX_PENDING_RESPONSES);
                let sessions = inner
                    .sessions
                    .lock()
                    .expect("Iroh sessions lock poisoned")
                    .as_ref()
                    .cloned();
                if sessions.is_none_or(|sender| sender.send((inner.peer, operation, tx)).is_err()) {
                    let _ =
                        send_response(&inner, id, FileResponse::Error(FileServiceError::Io)).await;
                    continue;
                }
                let response_inner = Arc::clone(&inner);
                tokio::spawn(async move {
                    let _permit = permit;
                    while let Some(response) = rx.recv().await {
                        let done = !matches!(response, FileResponse::ReadChunk(_));
                        if send_response(&response_inner, id, response).await.is_err() || done {
                            break;
                        }
                    }
                });
            }
        }
    }
    close_stream(&inner);
    fail_pending(&inner).await;
}

fn close_stream(inner: &Inner) {
    inner.shutdown.send_replace(true);
    inner.request_slots.close();
    inner
        .outgoing
        .lock()
        .expect("Iroh outgoing lock poisoned")
        .take();
    inner
        .metadata
        .lock()
        .expect("Iroh metadata lock poisoned")
        .take();
    inner
        .sessions
        .lock()
        .expect("Iroh sessions lock poisoned")
        .take();
}

fn subscribe<T: Clone>(sender: &StdMutex<Option<broadcast::Sender<T>>>) -> broadcast::Receiver<T> {
    let sender = sender.lock().expect("Iroh broadcast lock poisoned");
    match sender.as_ref() {
        Some(sender) => sender.subscribe(),
        None => {
            let (sender, receiver) = broadcast::channel(1);
            drop(sender);
            receiver
        }
    }
}

async fn fail_pending(inner: &Inner) {
    inner.pending.lock().await.clear();
}

async fn send_response(
    inner: &Arc<Inner>,
    id: u64,
    response: FileResponse<EndpointId>,
) -> Result<(), Error> {
    let outgoing = inner
        .outgoing
        .lock()
        .expect("Iroh outgoing lock poisoned")
        .as_ref()
        .cloned()
        .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "Iroh file session is closed"))?;
    match response {
        FileResponse::ReadChunk(bytes) => {
            for chunk in bytes.chunks(MAX_READ_RESPONSE_CHUNK_BYTES) {
                authorize_frame(inner).await?;
                send_wire(
                    &outgoing,
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
            authorize_frame(inner).await?;
            send_wire(
                &outgoing,
                WireMessage::<EndpointId>::Response { id, response },
            )
            .await
        }
    }
}

async fn authorize_frame(inner: &Inner) -> Result<(), Error> {
    if let Some(authorize) = &inner.authorize_frame
        && !authorize().await
    {
        close_stream(inner);
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "filesystem sync access revoked",
        ));
    }
    Ok(())
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

async fn read_message_or_shutdown<P: DeserializeOwned + Ord>(
    recv: &mut RecvStream,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<WireMessage<P>, Error> {
    if *shutdown.borrow() {
        return Err(Error::new(
            ErrorKind::BrokenPipe,
            "Iroh file session is closed",
        ));
    }
    tokio::select! {
        result = read_message(recv) => result,
        _ = shutdown.changed() => Err(Error::new(ErrorKind::BrokenPipe, "Iroh file session is closed")),
    }
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

#[cfg(test)]
mod authorization_tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, atomic::AtomicU64},
    };

    use tokio::sync::{Mutex, Semaphore, broadcast, mpsc};

    use super::{FrameAuthorizer, Inner, send_response};
    use crate::{FileResponse, FileServiceError};

    #[tokio::test]
    async fn denied_policy_cannot_send_filesystem_response() {
        let (outgoing, mut received) = mpsc::channel(1);
        let (metadata, _) = broadcast::channel(1);
        let (sessions, _) = broadcast::channel(1);
        let authorize: FrameAuthorizer = Arc::new(|| Box::pin(async { false }));
        let peer = iroh::EndpointId::from_bytes(&[0; 32]).expect("valid fixed endpoint ID");
        let inner = Arc::new(Inner {
            peer,
            authorize_frame: Some(authorize),
            outgoing: std::sync::Mutex::new(Some(outgoing)),
            metadata: std::sync::Mutex::new(Some(metadata)),
            sessions: std::sync::Mutex::new(Some(sessions)),
            pending: Mutex::new(BTreeMap::new()),
            request_slots: Arc::new(Semaphore::new(1)),
            next_id: AtomicU64::new(1),
        });

        let error = send_response(&inner, 1, FileResponse::Error(FileServiceError::Io))
            .await
            .expect_err("policy denial must block response send");

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(received.try_recv().is_err());
    }
}

fn service_error(error: FileServiceError) -> Error {
    Error::other(format!("remote file service error: {error:?}"))
}
