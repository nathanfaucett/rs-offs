use std::{
    env,
    fmt::Display,
    fs,
    io::Error,
    time::{Duration, SystemTime},
};

use file_system::{FileSystem, Residency};
use iroh::{Endpoint, RelayMode, endpoint::presets};
use iroh_chain::{InMemoryEndpointIdStore, Server, TUNNEL_ALPN, TunnelAuthorizer, VaultId};
use iroh_chain_file_system::{ScopedIrohTransport, StaticAccessToken};
use tokio::{spawn, time::timeout};

#[derive(Clone)]
struct TestAuthorizer;

impl TunnelAuthorizer for TestAuthorizer {
    async fn authorize(
        &self,
        _: VaultId,
        _: iroh::EndpointId,
        _: iroh::EndpointId,
        authorization: &[u8],
    ) -> bool {
        authorization == b"authorized"
    }
}

#[tokio::test]
async fn metadata_converges_over_a_scoped_tunnel() -> Result<(), Error> {
    let peers = peers().await?;
    let left_root = root("metadata-left");
    let right_root = root("metadata-right");
    let left = FileSystem::open(&left_root, peers.left.endpoint().id()).map_err(other)?;
    let right = FileSystem::open(&right_root, peers.right.endpoint().id()).map_err(other)?;
    left.set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    left.write("notes/today.txt", b"hello")
        .await
        .map_err(other)?;
    let mut left_sync = left.metadata_sync(peers.left_transport.clone());
    let mut right_sync = right.metadata_sync(peers.right_transport.clone());

    right_sync.announce().await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    left_sync.pump().await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    right_sync.pump().await.map_err(other)?;

    let left_entry = left.entry("notes/today.txt").await.map_err(other)?;
    let right_entry = right.entry("notes/today.txt").await.map_err(other)?;
    assert_eq!(right_entry.meta, left_entry.meta);

    peers.close().await?;
    fs::remove_dir_all(left_root)?;
    fs::remove_dir_all(right_root)?;
    Ok(())
}

#[tokio::test]
async fn fetches_content_over_a_scoped_tunnel() -> Result<(), Error> {
    let peers = peers().await?;
    let left_root = root("content-left");
    let right_root = root("content-right");
    let left = FileSystem::open(&left_root, peers.left.endpoint().id()).map_err(other)?;
    let right = FileSystem::open(&right_root, peers.right.endpoint().id()).map_err(other)?;
    left.set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    right
        .set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    left.write("notes/today.txt", b"hello")
        .await
        .map_err(other)?;
    let mut left_sync = left.metadata_sync(peers.left_transport.clone());
    let mut right_sync = right.metadata_sync(peers.right_transport.clone());

    right_sync.announce().await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    left_sync.pump().await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    right_sync.pump().await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    left_sync.pump().await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    right_sync.pump().await.map_err(other)?;

    assert_eq!(
        right.read("notes/today.txt").await.map_err(other)?,
        b"hello"
    );

    peers.close().await?;
    fs::remove_dir_all(left_root)?;
    fs::remove_dir_all(right_root)?;
    Ok(())
}

struct Peers {
    left: Server<InMemoryEndpointIdStore, TestAuthorizer>,
    right: Server<InMemoryEndpointIdStore, TestAuthorizer>,
    left_transport: ScopedIrohTransport<InMemoryEndpointIdStore, TestAuthorizer, StaticAccessToken>,
    right_transport:
        ScopedIrohTransport<InMemoryEndpointIdStore, TestAuthorizer, StaticAccessToken>,
    left_listener: tokio::task::JoinHandle<()>,
    right_listener: tokio::task::JoinHandle<()>,
}

impl Peers {
    async fn close(self) -> Result<(), Error> {
        self.left.close().await;
        self.right.close().await;
        self.left_listener.await.map_err(other)?;
        self.right_listener.await.map_err(other)?;
        Ok(())
    }
}

async fn peers() -> Result<Peers, Error> {
    let left_endpoint = endpoint().await?;
    let right_endpoint = endpoint().await?;
    let allowed = InMemoryEndpointIdStore::new();
    allowed.add(left_endpoint.id());
    allowed.add(right_endpoint.id());
    let left = Server::new(left_endpoint, allowed.clone(), TestAuthorizer);
    let right = Server::new(right_endpoint, allowed, TestAuthorizer);
    let left_listener = spawn_listener(left.clone());
    let right_listener = spawn_listener(right.clone());
    let vault_id = VaultId::new([7; 32]);
    let left_transport = ScopedIrohTransport::new(
        left.clone(),
        vault_id,
        StaticAccessToken::new(b"authorized".to_vec()),
    );
    let right_transport = ScopedIrohTransport::new(
        right.clone(),
        vault_id,
        StaticAccessToken::new(b"authorized".to_vec()),
    );

    right_transport.connect(left.endpoint().addr()).await?;
    timeout(Duration::from_secs(5), async {
        while left_transport.peers().is_empty() || right_transport.peers().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(other)?;

    Ok(Peers {
        left,
        right,
        left_transport,
        right_transport,
        left_listener,
        right_listener,
    })
}

async fn endpoint() -> Result<Endpoint, Error> {
    Endpoint::builder(presets::N0)
        .alpns(vec![TUNNEL_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .map_err(other)
}

fn spawn_listener(
    manager: Server<InMemoryEndpointIdStore, TestAuthorizer>,
) -> tokio::task::JoinHandle<()> {
    spawn(async move {
        manager.listen().await;
    })
}

fn root(name: &str) -> std::path::PathBuf {
    env::temp_dir().join(format!(
        "iroh-chain-file-system-{name}-{:?}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
    ))
}

fn other(error: impl Display) -> Error {
    Error::other(error.to_string())
}
