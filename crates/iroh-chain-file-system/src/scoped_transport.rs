use std::{
    collections::BTreeMap,
    future::Future,
    io::{Error, ErrorKind},
    sync::{Arc, Mutex},
};

use file_system::{IncomingMessage, SyncMessage, Transport};
use iroh::{EndpointAddr, EndpointId};
use iroh_chain::{AllowedEndpointId, Server, Tunnel, TunnelAuthorizer, VaultId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::broadcast,
};

const INCOMING_CAPACITY: usize = 64;
const MAX_FRAME_SIZE: usize = 1024 * 1024;

type Message = IncomingMessage<EndpointId>;

pub trait AccessTokenProvider: Send + Sync + 'static {
    fn access_token(
        &self,
        vault_id: VaultId,
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
        _: VaultId,
        _: EndpointId,
        _: EndpointId,
    ) -> Result<Vec<u8>, Error> {
        Ok(self.0.clone())
    }
}

pub struct ScopedIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
    P: AccessTokenProvider,
{
    inner: Arc<ScopedIrohTransportInner<A, V, P>>,
}

struct ScopedIrohTransportInner<A, V, P>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
    P: AccessTokenProvider,
{
    manager: Server<A, V>,
    vault_id: VaultId,
    authorization: P,
    peers: Mutex<BTreeMap<EndpointId, Tunnel>>,
    incoming: broadcast::Sender<Message>,
    peer_events: broadcast::Sender<EndpointId>,
}

impl<A, V, P> ScopedIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
    P: AccessTokenProvider,
{
    pub fn new(manager: Server<A, V>, vault_id: VaultId, authorization: P) -> Self {
        let (incoming, _) = broadcast::channel(INCOMING_CAPACITY);
        let (peer_events, _) = broadcast::channel(INCOMING_CAPACITY);
        let inner = Arc::new(ScopedIrohTransportInner {
            manager: manager.clone(),
            vault_id,
            authorization,
            peers: Mutex::new(BTreeMap::new()),
            incoming,
            peer_events,
        });
        let task_inner = Arc::clone(&inner);
        let mut events = manager.subscribe();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) if event.vault_id == task_inner.vault_id => {
                        add_tunnel(&task_inner, event.tunnel).await;
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Self { inner }
    }

    pub async fn connect(&self, endpoint: impl Into<EndpointAddr>) -> Result<EndpointId, Error> {
        let endpoint = endpoint.into();
        let authorization = self
            .inner
            .authorization
            .access_token(
                self.inner.vault_id,
                self.inner.manager.endpoint().id(),
                endpoint.id,
            )
            .await?;
        let tunnel = self
            .inner
            .manager
            .connect(self.inner.vault_id, endpoint, &authorization)
            .await?;
        let peer_id = tunnel.remote_id();
        add_tunnel(&self.inner, tunnel).await;
        Ok(peer_id)
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
            .remove(&peer_id);
        self.inner
            .manager
            .close_tunnel(self.inner.vault_id, peer_id)
            .await
    }
}

impl<A, V, P> Clone for ScopedIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
    P: AccessTokenProvider,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A, V, P> Transport<EndpointId> for ScopedIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
    P: AccessTokenProvider,
{
    type Error = Error;
    fn peers(&self) -> Vec<EndpointId> {
        Self::peers(self)
    }

    async fn send(
        &self,
        peer: EndpointId,
        message: SyncMessage<EndpointId>,
    ) -> Result<(), Self::Error> {
        let data = encode(&message)?;
        if !self.inner.manager.is_allowed(peer).await {
            self.disconnect(peer).await;
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Iroh peer is not allowed",
            ));
        }
        let tunnel = self
            .inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .get(&peer)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::NotConnected, "Iroh peer is not connected"))?;
        write_frame(&tunnel, &data).await
    }

    async fn broadcast(&self, message: SyncMessage<EndpointId>) -> Result<(), Self::Error> {
        let data = encode(&message)?;
        let peers = self
            .inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .iter()
            .map(|(peer_id, tunnel)| (*peer_id, tunnel.clone()))
            .collect::<Vec<_>>();
        for (peer_id, tunnel) in peers {
            if !self.inner.manager.is_allowed(peer_id).await {
                self.disconnect(peer_id).await;
                continue;
            }
            write_frame(&tunnel, &data).await?;
        }
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.inner.incoming.subscribe()
    }
}

async fn add_tunnel<A, V, P>(inner: &Arc<ScopedIrohTransportInner<A, V, P>>, tunnel: Tunnel)
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
    P: AccessTokenProvider,
{
    let peer_id = tunnel.remote_id();
    if inner
        .peers
        .lock()
        .expect("peer lock poisoned")
        .insert(peer_id, tunnel.clone())
        .is_some()
    {
        tunnel.close().await;
        return;
    }
    let _ = inner.peer_events.send(peer_id);
    let incoming = inner.incoming.clone();
    let task_inner = Arc::clone(inner);
    tokio::spawn(async move {
        let mut reader = tunnel.reader().await;
        loop {
            let mut length = [0_u8; 4];
            if reader.read_exact(&mut length).await.is_err() {
                break;
            }
            let length = usize::try_from(u32::from_be_bytes(length)).expect("u32 fits usize");
            if length > MAX_FRAME_SIZE {
                break;
            }
            let mut data = vec![0; length];
            if reader.read_exact(&mut data).await.is_err() {
                break;
            }
            if !task_inner.manager.is_allowed(peer_id).await {
                break;
            }
            let message = match postcard::from_bytes(&data) {
                Ok(message) => message,
                Err(_) => break,
            };
            let _ = incoming.send((peer_id, message));
        }
        drop(reader);
        task_inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .remove(&peer_id);
        task_inner
            .manager
            .close_tunnel(task_inner.vault_id, peer_id)
            .await;
    });
}

fn encode(message: &SyncMessage<EndpointId>) -> Result<Vec<u8>, Error> {
    let data = postcard::to_allocvec(message)
        .map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
    if data.len() > MAX_FRAME_SIZE {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "file-system message is too large",
        ));
    }
    Ok(data)
}

async fn write_frame(tunnel: &Tunnel, data: &[u8]) -> Result<(), Error> {
    let length = u32::try_from(data.len())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "frame exceeds u32"))?;
    let mut writer = tunnel.writer().await;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await
}
