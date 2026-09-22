use std::{
    collections::BTreeMap,
    future::Future,
    io::{Error, ErrorKind},
    sync::{Arc, Mutex},
};

use file_system::{
    ByteStream, FileRequest, FileRequestMessage, FileService, IncomingMessage, SyncMessage,
    Transport,
};
use futures_util::stream;
use iroh::{
    EndpointAddr, EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_chain::{
    AllowedEndpointId, FILE_TRANSFER_ALPN, METADATA_SYNC_ALPN, RootAuthorizer, RootId, Server,
};

use tokio::{
    io::AsyncWriteExt,
    sync::{broadcast, mpsc},
};

const INCOMING_CAPACITY: usize = 64;
const MAX_METADATA_MESSAGE: usize = 16 * 1024 * 1024;

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
    inner: Arc<RootIrohTransportInner<A, V, P>>,
}

struct RootIrohTransportInner<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    manager: Server<A, V>,
    root_id: RootId,
    authorization: P,
    peers: Mutex<BTreeMap<EndpointId, EndpointAddr>>,
    incoming: broadcast::Sender<Message>,
    file_requests: broadcast::Sender<file_system::FileRequestMessage<EndpointId>>,
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
        let (file_requests, _) = broadcast::channel(INCOMING_CAPACITY);
        let (peer_events, _) = broadcast::channel(INCOMING_CAPACITY);
        Self {
            inner: Arc::new(RootIrohTransportInner {
                manager,
                root_id,
                authorization,
                peers: Mutex::new(BTreeMap::new()),
                incoming,
                file_requests,
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
            root_id: self.inner.root_id,
            requests: self.inner.file_requests.clone(),
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

impl<A, V, P> FileService<EndpointId> for RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    async fn open_file(
        &self,
        peer: EndpointId,
        request: FileRequest,
    ) -> Result<ByteStream<Self::Error>, Self::Error> {
        if !self.inner.manager.is_allowed(peer).await {
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
        let connection = self
            .inner
            .manager
            .endpoint()
            .connect(address, FILE_TRANSFER_ALPN)
            .await
            .map_err(Error::other)?;
        let (mut send, mut recv) = connection.open_bi().await.map_err(Error::other)?;
        let mut data = self.inner.root_id.as_bytes().to_vec();
        data.extend(
            postcard::to_allocvec(&request)
                .map_err(|error| Error::new(ErrorKind::InvalidData, error))?,
        );
        send.write_all(&data).await.map_err(Error::other)?;
        send.finish().map_err(Error::other)?;
        let (sender, receiver) = mpsc::channel(INCOMING_CAPACITY);
        tokio::spawn(async move {
            let _connection = connection;
            loop {
                match recv.read_chunk(64 * 1024).await {
                    Ok(Some(chunk)) => {
                        if sender.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let _ = sender.send(Err(Error::other(error))).await;
                        break;
                    }
                }
            }
        });
        Ok(Box::pin(stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|chunk| (chunk, receiver))
        })))
    }

    fn subscribe_file_requests(&self) -> broadcast::Receiver<FileRequestMessage<EndpointId>> {
        self.inner.file_requests.subscribe()
    }
}

#[derive(Clone)]
struct MetadataHandler {
    root_id: RootId,
    incoming: broadcast::Sender<Message>,
}

#[derive(Clone)]
struct FileHandler {
    root_id: RootId,
    requests: broadcast::Sender<file_system::FileRequestMessage<EndpointId>>,
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

impl std::fmt::Debug for FileHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileHandler")
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for FileHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        let (mut send, mut recv) = connection.accept_bi().await?;
        let data = recv
            .read_to_end(MAX_METADATA_MESSAGE)
            .await
            .map_err(|error| AcceptError::from(Error::other(error)))?;
        if data.len() < 32 {
            return Err(AcceptError::from(Error::new(
                ErrorKind::InvalidData,
                "file message is missing its root ID",
            )));
        }
        let mut root_bytes = [0_u8; 32];
        root_bytes.copy_from_slice(&data[..32]);
        if RootId::new(root_bytes) != self.root_id {
            return Err(AcceptError::from(Error::new(
                ErrorKind::PermissionDenied,
                "root ID mismatch",
            )));
        }
        let request = postcard::from_bytes(&data[32..])
            .map_err(|error| AcceptError::from(Error::new(ErrorKind::InvalidData, error)))?;
        let (sender, mut receiver) = mpsc::channel(INCOMING_CAPACITY);
        self.requests
            .send((remote_id, request, sender))
            .map_err(|_| AcceptError::from(Error::other("file request receiver closed")))?;
        while let Some(chunk) = receiver.recv().await {
            send.write_all(&chunk).await.map_err(Error::other)?;
        }
        send.finish().map_err(Error::other)?;
        connection.closed().await;
        Ok(())
    }
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
