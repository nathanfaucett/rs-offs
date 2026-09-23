use std::{
    io::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use iroh::{
    Endpoint, EndpointAddr, EndpointId, SecretKey,
    endpoint::{Connection, presets::Preset},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use tokio::sync::broadcast;

use crate::{AllowedEndpointId, PairingOffer, hooks::AllowlistHook, pairing::validate_payload};

pub const PAIRING_ALPN: &[u8] = b"idp-pairing/1";
pub const METADATA_SYNC_ALPN: &[u8] = b"idp-metadata-sync/1";
pub const FILE_TRANSFER_ALPN: &[u8] = b"idp-file-transfer/1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RootId([u8; 32]);

impl RootId {
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
        hasher.update(b"idp-root-id/v1");
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

pub trait RootAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        root_id: RootId,
        initiating_id: EndpointId,
        accepting_id: EndpointId,
        authorization: &[u8],
    ) -> impl Future<Output = bool> + Send;
}

struct ServerInner<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    endpoint: Endpoint,
    allowed: Arc<A>,
    _authorizer: V,
    pairing_offers: broadcast::Sender<PairingOffer>,
    pairing_enabled: Arc<AtomicBool>,
    pairing_generation: AtomicU64,
    pairing_lock: Mutex<()>,
}

pub struct Server<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    inner: Arc<ServerInner<A, V>>,
}

impl<A, V> Clone for Server<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
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
    V: RootAuthorizer,
{
    pub fn new(endpoint: Endpoint, allowed: A, authorizer: V) -> Self {
        Self::from_parts(
            endpoint,
            Arc::new(allowed),
            authorizer,
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub async fn bind<P>(
        preset: P,
        allowed: A,
        authorizer: V,
    ) -> Result<Self, iroh::endpoint::BindError>
    where
        P: Preset,
    {
        Self::bind_with_secret_key(preset, SecretKey::generate(), allowed, authorizer).await
    }

    pub async fn bind_with_secret_key<P>(
        preset: P,
        secret_key: SecretKey,
        allowed: A,
        authorizer: V,
    ) -> Result<Self, iroh::endpoint::BindError>
    where
        P: Preset,
    {
        let allowed = Arc::new(allowed);
        let pairing_enabled = Arc::new(AtomicBool::new(false));
        let endpoint = Endpoint::builder(preset)
            .secret_key(secret_key)
            .hooks(AllowlistHook::new(
                Arc::clone(&allowed),
                Arc::clone(&pairing_enabled),
            ))
            .bind()
            .await?;
        Ok(Self::from_parts(
            endpoint,
            allowed,
            authorizer,
            pairing_enabled,
        ))
    }

    fn from_parts(
        endpoint: Endpoint,
        allowed: Arc<A>,
        authorizer: V,
        pairing_enabled: Arc<AtomicBool>,
    ) -> Self {
        endpoint.set_alpns(vec![
            METADATA_SYNC_ALPN.to_vec(),
            FILE_TRANSFER_ALPN.to_vec(),
            PAIRING_ALPN.to_vec(),
        ]);
        let (pairing_offers, _) = broadcast::channel(64);
        Self {
            inner: Arc::new(ServerInner {
                endpoint,
                allowed,
                _authorizer: authorizer,
                pairing_offers,
                pairing_enabled,
                pairing_generation: AtomicU64::new(0),
                pairing_lock: Mutex::new(()),
            }),
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    pub async fn authorize(
        &self,
        root_id: RootId,
        initiating_id: EndpointId,
        authorization: &[u8],
    ) -> bool {
        self.inner
            ._authorizer
            .authorize(
                root_id,
                initiating_id,
                self.inner.endpoint.id(),
                authorization,
            )
            .await
    }

    pub fn allowlist_hook(&self) -> AllowlistHook<A> {
        AllowlistHook::new(
            Arc::clone(&self.inner.allowed),
            Arc::clone(&self.inner.pairing_enabled),
        )
    }

    pub fn router<M, F>(&self, metadata: M, files: F) -> Router
    where
        M: ProtocolHandler,
        F: ProtocolHandler,
    {
        Router::builder(self.inner.endpoint.clone())
            .accept(
                PAIRING_ALPN,
                PairingHandler {
                    server: self.clone(),
                },
            )
            .accept(METADATA_SYNC_ALPN, metadata)
            .accept(FILE_TRANSFER_ALPN, files)
            .spawn()
    }

    pub fn subscribe_pairing_offers(&self) -> broadcast::Receiver<PairingOffer> {
        self.inner.pairing_offers.subscribe()
    }

    pub fn set_pairing_enabled(&self, enabled: bool) {
        let _lock = self
            .inner
            .pairing_lock
            .lock()
            .expect("pairing lock poisoned");
        self.inner.pairing_generation.fetch_add(1, Ordering::AcqRel);
        self.inner.pairing_enabled.store(enabled, Ordering::Release);
    }

    pub fn set_pairing_enabled_for(&self, duration: Duration) {
        let generation = {
            let _lock = self
                .inner
                .pairing_lock
                .lock()
                .expect("pairing lock poisoned");
            let generation = self.inner.pairing_generation.fetch_add(1, Ordering::AcqRel) + 1;
            self.inner.pairing_enabled.store(true, Ordering::Release);
            generation
        };
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            let _lock = inner.pairing_lock.lock().expect("pairing lock poisoned");
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

    pub async fn close(&self) {
        self.inner.endpoint.close().await;
    }
}

#[derive(Clone)]
struct PairingHandler<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    server: Server<A, V>,
}

impl<A, V> std::fmt::Debug for PairingHandler<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PairingHandler")
            .finish_non_exhaustive()
    }
}

impl<A, V> ProtocolHandler for PairingHandler<A, V>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
{
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote_id = connection.remote_id();
        let (mut send, mut recv) = connection.accept_bi().await?;
        let payload = recv
            .read_to_end(crate::MAX_PAIRING_PAYLOAD_LENGTH)
            .await
            .map_err(Error::other)?;
        if !self.server.pairing_enabled() {
            send.finish().map_err(Error::other)?;
            return Ok(());
        }
        self.server
            .inner
            .pairing_offers
            .send(PairingOffer::new(remote_id, payload, send, connection))
            .map_err(|_| Error::other("pairing receiver closed"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RootId;

    #[test]
    fn derives_stable_domain_separated_root_ids() {
        let application = RootId::from_application("user", 1);
        let global_identity = RootId::global_identity();
        assert_eq!(application, RootId::from_application("user", 1));
        assert_ne!(application, RootId::from_application("other-user", 1));
        assert_ne!(application, RootId::from_application("user", 2));
        assert_ne!(
            RootId::from_application("a", 12),
            RootId::from_application("ab", 2)
        );
        assert_eq!(global_identity, RootId::global_identity());
        assert_ne!(application, global_identity);
        assert_eq!(application.hash().len(), 64);
    }
}
