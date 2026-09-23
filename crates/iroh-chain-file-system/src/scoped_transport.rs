use std::{
    collections::BTreeMap,
    future::Future,
    io::{Error, ErrorKind},
    sync::{Arc, Mutex},
};

use file_system::{FileOperation, IncomingMessage, SessionRequest, SyncMessage, Transport};
use iroh::{
    EndpointAddr, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_chain::{
    AllowedEndpointId, FILE_TRANSFER_ALPN, METADATA_SYNC_ALPN, RootAuthorizer, RootId, Server,
};
use serde::{Deserialize, Serialize};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{broadcast, mpsc},
};

const INCOMING_CAPACITY: usize = 64;
const MAX_METADATA_MESSAGE: usize = 16 * 1024 * 1024;
const MAX_FILE_FRAME: usize = 16 * 1024 * 1024;

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FileSessionRequestFrame {
    pub(crate) root_id: [u8; 32],
    pub(crate) authorization: Vec<u8>,
    pub(crate) operation: FileOperation,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum FileSessionResponseFrame {
    Response(file_system::FileResponse<EndpointId>),
}

type Message = IncomingMessage<EndpointId>;

pub trait AccessTokenProvider: Send + Sync + 'static {
    fn access_token(
        &self,
        root_id: RootId,
        local_id: EndpointId,
        remote_id: EndpointId,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send;
}

#[derive(Clone)]
pub struct StaticAccessToken(Vec<u8>);

impl StaticAccessToken {
    pub fn new(authorization: Vec<u8>) -> Self {
        Self(authorization)
    }
}

impl AccessTokenProvider for StaticAccessToken {
    async fn access_token(
        &self,
        _: RootId,
        _: EndpointId,
        _: EndpointId,
    ) -> Result<Vec<u8>, Error> {
        Ok(self.0.clone())
    }
}

pub struct RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    pub(crate) inner: Arc<RootIrohTransportInner<A, V, P>>,
}

pub(crate) struct RootIrohTransportInner<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    pub(crate) manager: Server<A, V>,
    pub(crate) root_id: RootId,
    pub(crate) authorization: P,
    pub(crate) peers: Mutex<BTreeMap<EndpointId, EndpointAddr>>,
    pub(crate) incoming: broadcast::Sender<Message>,
    pub(crate) sessions: broadcast::Sender<SessionRequest<EndpointId>>,
    peer_events: broadcast::Sender<EndpointId>,
}

impl<A, V, P> RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    pub fn new(manager: Server<A, V>, root_id: RootId, authorization: P) -> Self {
        let (incoming, _) = broadcast::channel(INCOMING_CAPACITY);
        let (sessions, _) = broadcast::channel(INCOMING_CAPACITY);
        let (peer_events, _) = broadcast::channel(INCOMING_CAPACITY);
        Self {
            inner: Arc::new(RootIrohTransportInner {
                manager,
                root_id,
                authorization,
                peers: Mutex::new(BTreeMap::new()),
                incoming,
                sessions,
                peer_events,
            }),
        }
    }

    pub fn router(&self) -> Router {
        let metadata = MetadataHandler {
            root_id: self.inner.root_id,
            incoming: self.inner.incoming.clone(),
        };
        let files = FileHandler {
            server: self.inner.manager.clone(),
            root_id: self.inner.root_id,
            sessions: self.inner.sessions.clone(),
        };
        self.inner.manager.router(metadata, files)
    }

    pub async fn connect(&self, endpoint: impl Into<EndpointAddr>) -> Result<EndpointId, Error> {
        let endpoint = endpoint.into();
        let remote_id = endpoint.id;
        if !self.inner.manager.is_allowed(remote_id).await {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Iroh peer is not allowed",
            ));
        }
        let authorization = self
            .inner
            .authorization
            .access_token(
                self.inner.root_id,
                self.inner.manager.endpoint().id(),
                remote_id,
            )
            .await?;
        if authorization.len() > 4096 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "authorization is too large",
            ));
        }
        self.inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .insert(remote_id, endpoint);
        let _ = self.inner.peer_events.send(remote_id);
        Ok(remote_id)
    }

    /// Connect through the address-lookup/discovery services configured on the endpoint.
    pub async fn connect_discovered(&self, peer: EndpointId) -> Result<EndpointId, Error> {
        self.connect(EndpointAddr::new(peer)).await
    }

    pub fn subscribe_peers(&self) -> broadcast::Receiver<EndpointId> {
        self.inner.peer_events.subscribe()
    }

    pub fn peers(&self) -> Vec<EndpointId> {
        self.inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .keys()
            .copied()
            .collect()
    }

    pub async fn disconnect(&self, peer_id: EndpointId) -> bool {
        self.inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .remove(&peer_id)
            .is_some()
    }
}

impl<A, V, P> Clone for RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A, V, P> Transport<EndpointId> for RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    type Error = Error;

    fn peers(&self) -> Vec<EndpointId> {
        Self::peers(self)
    }

    async fn send(&self, peer: EndpointId, message: SyncMessage<EndpointId>) -> Result<(), Error> {
        if !self.inner.manager.is_allowed(peer).await {
            self.disconnect(peer).await;
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Iroh peer is not allowed",
            ));
        }
        let address = self
            .inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .get(&peer)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::NotConnected, "Iroh peer is not connected"))?;

        let alpn = METADATA_SYNC_ALPN;
        send_message(
            &self
                .inner
                .manager
                .endpoint()
                .connect(address, alpn)
                .await
                .map_err(Error::other)?,
            self.inner.root_id,
            message,
        )
        .await
    }

    async fn broadcast(&self, message: SyncMessage<EndpointId>) -> Result<(), Error> {
        for peer in self.peers() {
            self.send(peer, message.clone()).await?;
        }
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.inner.incoming.subscribe()
    }
}

#[derive(Clone)]
struct MetadataHandler {
    root_id: RootId,
    incoming: broadcast::Sender<Message>,
}

#[derive(Clone)]
struct FileHandler<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    server: Server<A, V>,
    root_id: RootId,
    sessions: broadcast::Sender<SessionRequest<EndpointId>>,
}

impl std::fmt::Debug for MetadataHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataHandler")
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for MetadataHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();

        let (mut send, mut recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(error) => return Err(error.into()),
        };
        let data = recv
            .read_to_end(MAX_METADATA_MESSAGE)
            .await
            .map_err(|error| AcceptError::from(Error::other(error)))?;

        if data.len() < 32 {
            return Err(AcceptError::from(Error::new(
                ErrorKind::InvalidData,
                "metadata message is missing its root ID",
            )));
        }
        let mut root_bytes = [0_u8; 32];
        root_bytes.copy_from_slice(&data[..32]);
        let root_id = RootId::new(root_bytes);
        let message: SyncMessage<EndpointId> = postcard::from_bytes(&data[32..])
            .map_err(|error| AcceptError::from(Error::new(ErrorKind::InvalidData, error)))?;

        if root_id != self.root_id {
            return Err(AcceptError::from(Error::new(
                ErrorKind::PermissionDenied,
                "root ID mismatch",
            )));
        }
        self.incoming
            .send((remote_id, message))
            .map_err(|_| AcceptError::from(Error::other("metadata receiver closed")))?;

        send.write_all(&[1]).await.map_err(Error::other)?;
        send.flush().await.map_err(Error::other)?;
        send.finish().map_err(Error::other)?;

        connection.closed().await;
        Ok(())
    }
}

impl<A, V> std::fmt::Debug for FileHandler<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileHandler")
            .finish_non_exhaustive()
    }
}

impl<A, V> ProtocolHandler for FileHandler<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        let (mut send, mut recv) = connection.accept_bi().await?;
        loop {
            let Some(frame) = read_frame::<_, FileSessionRequestFrame>(&mut recv)
                .await
                .map_err(AcceptError::from)?
            else {
                break;
            };

            if RootId::new(frame.root_id) != self.root_id {
                return Err(AcceptError::from(Error::new(
                    ErrorKind::PermissionDenied,
                    "root ID mismatch",
                )));
            }
            if !self
                .server
                .authorize(self.root_id, remote_id, &frame.authorization)
                .await
            {
                return Err(AcceptError::from(Error::new(
                    ErrorKind::PermissionDenied,
                    "file session authorization rejected",
                )));
            }
            let close = matches!(frame.operation, FileOperation::Close { .. });
            let (sender, mut receiver) = mpsc::channel(INCOMING_CAPACITY);
            self.sessions
                .send((remote_id, frame.operation, sender))
                .map_err(|_| AcceptError::from(Error::other("file session receiver closed")))?;

            while let Some(response) = receiver.recv().await {
                write_frame(&mut send, &FileSessionResponseFrame::Response(response))
                    .await
                    .map_err(AcceptError::from)?;
            }
            send.flush().await.map_err(Error::other)?;
            if close {
                break;
            }
        }
        send.finish().map_err(Error::other)?;
        connection.closed().await;
        Ok(())
    }
}

pub(crate) async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload =
        postcard::to_allocvec(value).map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
    if payload.len() > MAX_FILE_FRAME {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "file frame exceeds maximum size",
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "file frame is too large"))?;
    writer.write_u32_le(length).await.map_err(Error::other)?;
    writer.write_all(&payload).await.map_err(Error::other)
}

pub(crate) async fn read_frame<R, T>(reader: &mut R) -> Result<Option<T>, Error>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut prefix = [0_u8; 4];
    if reader.read(&mut prefix[..1]).await.map_err(Error::other)? == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut prefix[1..])
        .await
        .map_err(Error::other)?;
    let length = u32::from_le_bytes(prefix) as usize;
    if length > MAX_FILE_FRAME {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "file frame exceeds maximum size",
        ));
    }
    let mut payload = vec![0_u8; length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(Error::other)?;
    postcard::from_bytes(&payload)
        .map(Some)
        .map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

async fn send_message(
    connection: &Connection,
    root_id: RootId,
    message: SyncMessage<EndpointId>,
) -> Result<(), Error> {
    let mut data = root_id.as_bytes().to_vec();
    data.extend(
        postcard::to_allocvec(&message)
            .map_err(|error| Error::new(ErrorKind::InvalidData, error))?,
    );
    let (mut send, mut recv) = connection.open_bi().await.map_err(Error::other)?;

    send.write_all(&data).await.map_err(Error::other)?;
    send.finish().map_err(Error::other)?;

    recv.read_to_end(1).await.map_err(Error::other)?;
    connection.close(0u32.into(), b"done");
    Ok(())
}

const _: &[u8] = FILE_TRANSFER_ALPN;

#[cfg(test)]
mod tests {
    use super::{FileSessionRequestFrame, FileSessionResponseFrame, read_frame, write_frame};
    use file_system::{FileOperation, OpenRequest};
    use tokio::io::duplex;

    #[tokio::test]
    async fn file_session_frames_round_trip_operations() {
        let (mut writer, mut reader) = duplex(1024);
        let request = FileSessionRequestFrame {
            root_id: [7; 32],
            authorization: Vec::new(),
            operation: FileOperation::Open(OpenRequest {
                path: "notes/today.txt".to_owned(),
                revision: None,
            }),
        };
        let response = FileSessionResponseFrame::Response(file_system::FileResponse::ReadEnd);
        let request_to_write = request.clone();
        let response_to_write = response.clone();
        let task = tokio::spawn(async move {
            write_frame(&mut writer, &request_to_write)
                .await
                .expect("write request");
            write_frame(&mut writer, &response_to_write)
                .await
                .expect("write response");
        });

        assert_eq!(
            read_frame(&mut reader).await.expect("read request"),
            Some(request)
        );
        assert_eq!(
            read_frame(&mut reader).await.expect("read response"),
            Some(response)
        );
        task.await.expect("writer task");
    }
}
