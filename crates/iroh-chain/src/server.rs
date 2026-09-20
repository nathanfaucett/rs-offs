use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind},
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::Connection};
use noq::{RecvStream, SendStream, VarInt};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::{Mutex, MutexGuard, broadcast},
};

use crate::{AllowedEndpointId, PairingOffer, pairing::validate_payload};

pub const PAIRING_ALPN: &[u8] = b"idp-pairing/1";
pub const TUNNEL_ALPN: &[u8] = b"idp-tunnel/1";
const PROTOCOL_VERSION: u8 = 1;
const MAX_AUTHORIZATION_LENGTH: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct VaultId([u8; 32]);

impl VaultId {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn hash(&self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    pub fn from_application(user_sub: &str, application_id: i64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"idp-vault-id/v1");
        hasher.update(&(user_sub.len() as u64).to_be_bytes());
        hasher.update(user_sub.as_bytes());
        hasher.update(&application_id.to_be_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    #[must_use]
    pub fn global_identity() -> Self {
        Self(*blake3::hash(b"idp-global-identity-v1").as_bytes())
    }
}

pub trait TunnelAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        vault_id: VaultId,
        initiating_id: EndpointId,
        accepting_id: EndpointId,
        authorization: &[u8],
    ) -> impl Future<Output = bool> + Send;
}

#[derive(Clone, Debug)]
pub struct Tunnel {
    inner: Arc<TunnelInner>,
}

#[derive(Debug)]
struct TunnelInner {
    remote_id: EndpointId,
    send: Mutex<SendStream>,
    recv: Mutex<RecvStream>,
}

impl Tunnel {
    fn new(remote_id: EndpointId, send: SendStream, recv: RecvStream) -> Self {
        Self {
            inner: Arc::new(TunnelInner {
                remote_id,
                send: Mutex::new(send),
                recv: Mutex::new(recv),
            }),
        }
    }

    pub fn remote_id(&self) -> EndpointId {
        self.inner.remote_id
    }

    pub async fn reader(&self) -> TunnelReader<'_> {
        TunnelReader {
            guard: self.inner.recv.lock().await,
        }
    }

    pub async fn writer(&self) -> TunnelWriter<'_> {
        TunnelWriter {
            guard: self.inner.send.lock().await,
        }
    }

    pub async fn close(&self) {
        let _ = self.inner.send.lock().await.reset(VarInt::from_u32(1));
        let _ = self.inner.recv.lock().await.stop(VarInt::from_u32(1));
    }
}

pub struct TunnelReader<'a> {
    guard: MutexGuard<'a, RecvStream>,
}

impl AsyncRead for TunnelReader<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), Error>> {
        Pin::new(&mut *self.guard).poll_read(context, buffer)
    }
}

pub struct TunnelWriter<'a> {
    guard: MutexGuard<'a, SendStream>,
}

impl AsyncWrite for TunnelWriter<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut *self.guard)
            .poll_write(context, buffer)
            .map_err(Error::other)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut *self.guard).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        Pin::new(&mut *self.guard).poll_shutdown(context)
    }
}

#[derive(Clone, Debug)]
pub struct TunnelEvent {
    pub vault_id: VaultId,
    pub tunnel: Tunnel,
}

struct ServerInner<A, V>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
{
    endpoint: Endpoint,
    allowed: A,
    authorizer: V,
    tunnels: Mutex<BTreeMap<(EndpointId, VaultId), Tunnel>>,
    events: broadcast::Sender<TunnelEvent>,
    pairing_offers: broadcast::Sender<PairingOffer>,
    pairing_enabled: AtomicBool,
    pairing_generation: AtomicU64,
    pairing_lock: StdMutex<()>,
}

pub struct Server<A, V>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
{
    inner: Arc<ServerInner<A, V>>,
}

impl<A, V> Clone for Server<A, V>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A, V> Server<A, V>
where
    A: AllowedEndpointId,
    V: TunnelAuthorizer,
{
    pub fn new(endpoint: Endpoint, allowed: A, authorizer: V) -> Self {
        endpoint.set_alpns(vec![TUNNEL_ALPN.to_vec(), PAIRING_ALPN.to_vec()]);
        let (events, _) = broadcast::channel(64);
        let (pairing_offers, _) = broadcast::channel(64);
        Self {
            inner: Arc::new(ServerInner {
                endpoint,
                allowed,
                authorizer,
                tunnels: Mutex::new(BTreeMap::new()),
                events,
                pairing_offers,
                pairing_enabled: AtomicBool::new(false),
                pairing_generation: AtomicU64::new(0),
                pairing_lock: StdMutex::new(()),
            }),
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TunnelEvent> {
        self.inner.events.subscribe()
    }

    pub fn subscribe_pairing_offers(&self) -> broadcast::Receiver<PairingOffer> {
        self.inner.pairing_offers.subscribe()
    }

    pub fn set_pairing_enabled(&self, enabled: bool) {
        let _lock = self
            .inner
            .pairing_lock
            .lock()
            .expect("pairing lock is poisoned");
        self.inner.pairing_generation.fetch_add(1, Ordering::AcqRel);
        self.inner.pairing_enabled.store(enabled, Ordering::Release);
    }

    pub fn set_pairing_enabled_for(&self, duration: Duration) {
        let generation = {
            let _lock = self
                .inner
                .pairing_lock
                .lock()
                .expect("pairing lock is poisoned");
            let generation = self.inner.pairing_generation.fetch_add(1, Ordering::AcqRel) + 1;
            self.inner.pairing_enabled.store(true, Ordering::Release);
            generation
        };
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            let _lock = inner.pairing_lock.lock().expect("pairing lock is poisoned");
            if inner.pairing_generation.load(Ordering::Acquire) == generation {
                inner.pairing_enabled.store(false, Ordering::Release);
            }
        });
    }

    pub fn pairing_enabled(&self) -> bool {
        self.inner.pairing_enabled.load(Ordering::Acquire)
    }

    pub async fn is_allowed(&self, endpoint_id: EndpointId) -> bool {
        self.inner.allowed.allowed(endpoint_id).await
    }

    pub async fn send_pairing_offer(
        &self,
        endpoint: impl Into<EndpointAddr>,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        validate_payload(payload)?;
        let connection = self
            .inner
            .endpoint
            .connect(endpoint, PAIRING_ALPN)
            .await
            .map_err(Error::other)?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(payload).await.map_err(Error::other)?;
        send.finish().map_err(Error::other)?;
        recv.read_to_end(crate::MAX_PAIRING_PAYLOAD_LENGTH)
            .await
            .map_err(Error::other)
    }

    pub async fn connect(
        &self,
        vault_id: VaultId,
        endpoint: impl Into<EndpointAddr>,
        authorization: &[u8],
    ) -> Result<Tunnel, Error> {
        let connection = self
            .inner
            .endpoint
            .connect(endpoint, TUNNEL_ALPN)
            .await
            .map_err(Error::other)?;
        let remote_id = connection.remote_id();
        if !self.inner.allowed.allowed(remote_id).await {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer is not allowed",
            ));
        }
        let (mut send, mut recv) = connection.open_bi().await?;
        write_handshake(&mut send, vault_id, self.inner.endpoint.id(), authorization).await?;
        if recv.read_u8().await? == 0 {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "tunnel was rejected",
            ));
        }
        self.insert(vault_id, Tunnel::new(remote_id, send, recv))
            .await
    }

    pub async fn listen(&self) {
        loop {
            let Some(connecting) = self.inner.endpoint.accept().await else {
                return;
            };
            let manager = self.clone();
            tokio::spawn(async move {
                let Ok(connection) = connecting.await else {
                    return;
                };
                manager.accept_connection(connection).await;
            });
        }
    }

    pub async fn close(&self) {
        self.inner.endpoint.close().await;
        let tunnels = std::mem::take(&mut *self.inner.tunnels.lock().await);
        for tunnel in tunnels.into_values() {
            tunnel.close().await;
        }
    }

    pub async fn close_tunnel(&self, vault_id: VaultId, remote_id: EndpointId) -> bool {
        let tunnel = self
            .inner
            .tunnels
            .lock()
            .await
            .remove(&(remote_id, vault_id));
        if let Some(tunnel) = tunnel {
            tunnel.close().await;
            true
        } else {
            false
        }
    }

    pub async fn close_disallowed(&self) -> usize {
        let tunnels = self
            .inner
            .tunnels
            .lock()
            .await
            .iter()
            .map(|(key, tunnel)| (*key, tunnel.clone()))
            .collect::<Vec<_>>();
        let mut disallowed = Vec::new();
        for ((remote_id, vault_id), tunnel) in tunnels {
            if !self.inner.allowed.allowed(remote_id).await {
                disallowed.push((remote_id, vault_id, tunnel));
            }
        }
        let mut active = self.inner.tunnels.lock().await;
        let mut removed = Vec::new();
        for (remote_id, vault_id, tunnel) in disallowed {
            if active.remove(&(remote_id, vault_id)).is_some() {
                removed.push(tunnel);
            }
        }
        drop(active);
        for tunnel in &removed {
            tunnel.close().await;
        }
        removed.len()
    }

    async fn accept_connection(&self, connection: Connection) {
        match connection.alpn() {
            PAIRING_ALPN => self.accept_pairing_offer(connection).await,
            TUNNEL_ALPN => self.accept_tunnel_connection(connection).await,
            _ => {}
        }
    }

    async fn accept_pairing_offer(&self, connection: Connection) {
        let remote_id = connection.remote_id();
        let Ok((mut send, mut recv)) = connection.accept_bi().await else {
            return;
        };
        let Ok(payload) = recv.read_to_end(crate::MAX_PAIRING_PAYLOAD_LENGTH).await else {
            return;
        };
        if !self.pairing_enabled() {
            let _ = send.finish();
            return;
        }
        let _ = self
            .inner
            .pairing_offers
            .send(PairingOffer::new(remote_id, payload, send, connection));
    }

    async fn accept_tunnel_connection(&self, connection: Connection) {
        let remote_id = connection.remote_id();
        if !self.inner.allowed.allowed(remote_id).await {
            return;
        }
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            if !self.inner.allowed.allowed(remote_id).await {
                let _ = send.write_u8(0).await;
                let _ = send.flush().await;
                continue;
            }
            let Ok((vault_id, initiating_id, authorization)) = read_handshake(&mut recv).await
            else {
                return;
            };
            let authorized = initiating_id == remote_id
                && self
                    .inner
                    .authorizer
                    .authorize(
                        vault_id,
                        remote_id,
                        self.inner.endpoint.id(),
                        &authorization,
                    )
                    .await;
            let tunnel = Tunnel::new(remote_id, send, recv);
            let accepted = authorized && self.insert(vault_id, tunnel.clone()).await.is_ok();
            let mut writer = tunnel.writer().await;
            let acknowledged =
                writer.write_u8(u8::from(accepted)).await.is_ok() && writer.flush().await.is_ok();
            drop(writer);
            if !acknowledged {
                if accepted {
                    self.close_tunnel(vault_id, remote_id).await;
                }
                return;
            }
            if !accepted {
                tunnel.close().await;
                continue;
            }
            let _ = self.inner.events.send(TunnelEvent { vault_id, tunnel });
        }
    }

    async fn insert(&self, vault_id: VaultId, tunnel: Tunnel) -> Result<Tunnel, Error> {
        let key = (tunnel.remote_id(), vault_id);
        let mut tunnels = self.inner.tunnels.lock().await;
        if tunnels.contains_key(&key) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "tunnel already exists",
            ));
        }
        tunnels.insert(key, tunnel.clone());
        Ok(tunnel)
    }
}

async fn write_handshake(
    send: &mut SendStream,
    vault_id: VaultId,
    initiating_id: EndpointId,
    authorization: &[u8],
) -> Result<(), Error> {
    let authorization_length = u16::try_from(authorization.len())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "tunnel authorization is too large"))?;
    if authorization.len() > MAX_AUTHORIZATION_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "tunnel authorization is too large",
        ));
    }
    send.write_u8(PROTOCOL_VERSION).await?;
    send.write_all(vault_id.as_bytes()).await?;
    send.write_all(initiating_id.as_bytes()).await?;
    send.write_u16(authorization_length).await?;
    send.write_all(authorization).await?;
    send.flush().await
}

async fn read_handshake(recv: &mut RecvStream) -> Result<(VaultId, EndpointId, Vec<u8>), Error> {
    if recv.read_u8().await? != PROTOCOL_VERSION {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unsupported tunnel protocol",
        ));
    }
    let mut vault_id = [0_u8; 32];
    recv.read_exact(&mut vault_id).await.map_err(Error::other)?;
    let mut initiating_id = [0_u8; 32];
    recv.read_exact(&mut initiating_id)
        .await
        .map_err(Error::other)?;
    let initiating_id = EndpointId::from_bytes(&initiating_id).map_err(Error::other)?;
    let length = usize::from(recv.read_u16().await?);
    if length > MAX_AUTHORIZATION_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "tunnel authorization is too large",
        ));
    }
    let mut authorization = vec![0; length];
    recv.read_exact(&mut authorization)
        .await
        .map_err(Error::other)?;
    Ok((VaultId::new(vault_id), initiating_id, authorization))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iroh::{Endpoint, EndpointId, RelayMode, endpoint::presets};

    use super::{Server, TunnelAuthorizer, VaultId};
    use crate::InMemoryEndpointIdStore;

    struct DenyTunnel;

    impl TunnelAuthorizer for DenyTunnel {
        async fn authorize(&self, _: VaultId, _: EndpointId, _: EndpointId, _: &[u8]) -> bool {
            false
        }
    }

    #[test]
    fn derives_stable_domain_separated_vault_ids() {
        let application = VaultId::from_application("user", 1);
        let global_identity = VaultId::global_identity();
        assert_eq!(application, VaultId::from_application("user", 1));
        assert_ne!(application, VaultId::from_application("other-user", 1));
        assert_ne!(application, VaultId::from_application("user", 2));
        assert_ne!(
            VaultId::from_application("a", 12),
            VaultId::from_application("ab", 2)
        );
        assert_eq!(global_identity, VaultId::global_identity());
        assert_ne!(application, global_identity);
        assert_eq!(application.hash().len(), 64);
    }

    #[tokio::test]
    async fn pairing_timeout_does_not_disable_a_newer_enable() {
        let server = Server::new(endpoint().await, InMemoryEndpointIdStore::new(), DenyTunnel);
        server.set_pairing_enabled_for(Duration::from_millis(20));
        tokio::time::sleep(Duration::from_millis(10)).await;
        server.set_pairing_enabled_for(Duration::from_millis(40));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(server.pairing_enabled());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!server.pairing_enabled());
        server.close().await;
    }

    #[tokio::test]
    async fn exchanges_pairing_offer_without_access_token() {
        let sender = Server::new(endpoint().await, InMemoryEndpointIdStore::new(), DenyTunnel);
        let receiver = Server::new(endpoint().await, InMemoryEndpointIdStore::new(), DenyTunnel);
        let mut offers = receiver.subscribe_pairing_offers();
        let listener = tokio::spawn({
            let receiver = receiver.clone();
            async move { receiver.listen().await }
        });
        let receiver_addr = receiver.endpoint().addr();
        let sender_id = sender.endpoint().id();
        let request = tokio::spawn(async move {
            sender
                .send_pairing_offer(receiver_addr, b"offer")
                .await
                .unwrap()
        });

        let offer = offers.recv().await.unwrap();
        assert_eq!(offer.remote_id, sender_id);
        assert_eq!(offer.payload, b"offer");
        offer.reply(b"accepted").await.unwrap();
        assert_eq!(request.await.unwrap(), b"accepted");

        receiver.close().await;
        listener.await.unwrap();
    }

    async fn endpoint() -> Endpoint {
        Endpoint::builder(presets::N0)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap()
    }
}
