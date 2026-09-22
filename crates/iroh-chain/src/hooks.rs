use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use iroh::endpoint::{
    AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks, VarInt,
};

use crate::{AllowedEndpointId, PAIRING_ALPN};

pub struct AllowlistHook<A> {
    allowed: Arc<A>,
    pairing_enabled: Arc<AtomicBool>,
}

impl<A> AllowlistHook<A> {
    pub(crate) fn new(allowed: Arc<A>, pairing_enabled: Arc<AtomicBool>) -> Self {
        Self {
            allowed,
            pairing_enabled,
        }
    }
}

impl<A> std::fmt::Debug for AllowlistHook<A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AllowlistHook")
            .finish_non_exhaustive()
    }
}

impl<A> EndpointHooks for AllowlistHook<A>
where
    A: AllowedEndpointId,
{
    async fn before_connect<'a>(
        &'a self,
        remote_addr: &'a iroh::EndpointAddr,
        alpn: &'a [u8],
    ) -> BeforeConnectOutcome {
        if self.allowed.allowed(remote_addr.id).await
            || (alpn == PAIRING_ALPN && self.pairing_enabled.load(Ordering::Acquire))
        {
            BeforeConnectOutcome::Accept
        } else {
            BeforeConnectOutcome::Reject
        }
    }

    async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
        let allowed = self.allowed.allowed(conn.remote_id()).await;
        let pairing = conn.alpn() == PAIRING_ALPN && self.pairing_enabled.load(Ordering::Acquire);
        if allowed || pairing {
            AfterHandshakeOutcome::Accept
        } else {
            AfterHandshakeOutcome::Reject {
                error_code: VarInt::from_u32(0x100),
                reason: b"endpoint is not allowlisted".to_vec(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{Arc, atomic::AtomicBool},
    };

    use iroh::{
        EndpointAddr, SecretKey,
        endpoint::{BeforeConnectOutcome, EndpointHooks},
    };

    use super::AllowlistHook;
    use crate::{DynamicEndpointIdStore, PAIRING_ALPN};

    #[tokio::test]
    async fn allows_only_trusted_or_open_pairing_connections() {
        let allowed = DynamicEndpointIdStore::default();
        let trusted = SecretKey::generate().public();
        let untrusted = SecretKey::generate().public();
        allowed.replace([trusted]).await;
        let pairing_enabled = Arc::new(AtomicBool::new(false));
        let hook = AllowlistHook::new(Arc::new(allowed), Arc::clone(&pairing_enabled));
        let trusted_addr = EndpointAddr {
            id: trusted,
            addrs: BTreeSet::new(),
        };
        let untrusted_addr = EndpointAddr {
            id: untrusted,
            addrs: BTreeSet::new(),
        };

        assert!(matches!(
            hook.before_connect(&trusted_addr, b"metadata-sync/1").await,
            BeforeConnectOutcome::Accept
        ));
        assert!(matches!(
            hook.before_connect(&untrusted_addr, b"metadata-sync/1")
                .await,
            BeforeConnectOutcome::Reject
        ));
        assert!(matches!(
            hook.before_connect(&untrusted_addr, PAIRING_ALPN).await,
            BeforeConnectOutcome::Reject
        ));

        pairing_enabled.store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            hook.before_connect(&untrusted_addr, PAIRING_ALPN).await,
            BeforeConnectOutcome::Accept
        ));
    }
}
