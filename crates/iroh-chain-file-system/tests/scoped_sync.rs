use std::{
    env,
    fmt::Display,
    fs,
    io::Error,
    time::{Duration, SystemTime},
};

use file_system::{Error as FileSystemError, FileSystem, Residency};
use futures_util::TryStreamExt;

use iroh_chain::{InMemoryEndpointIdStore, RootAuthorizer, RootId, Server};
use iroh_chain_file_system::{RootIrohTransport, StaticAccessToken};

#[derive(Clone)]
struct TestAuthorizer;

impl RootAuthorizer for TestAuthorizer {
    async fn authorize(
        &self,
        _: RootId,
        _: iroh::EndpointId,
        _: iroh::EndpointId,
        authorization: &[u8],
    ) -> bool {
        authorization == b"authorized"
    }
}

#[tokio::test]
async fn metadata_converges_over_direct_sync() -> Result<(), Error> {
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

    let left_entry = left
        .entry("notes/today.txt")
        .await
        .map_err(|error| other(format!("left entry: {error}")))?;
    let right_entry = right
        .entry("notes/today.txt")
        .await
        .map_err(|error| other(format!("right entry: {error}")))?;
    assert_eq!(right_entry.meta, left_entry.meta);

    peers.close().await?;
    fs::remove_dir_all(left_root)?;
    fs::remove_dir_all(right_root)?;
    Ok(())
}

#[tokio::test]
async fn discovery_still_requires_allowlist_authorization() -> Result<(), Error> {
    let peers = peers().await?;
    let unknown = iroh::SecretKey::generate().public();

    let error = peers
        .left_transport
        .connect_discovered(unknown)
        .await
        .expect_err("discovery must not bypass the allowlist");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);

    peers.close().await
}

#[tokio::test]
async fn streams_passthrough_content_without_persistence() -> Result<(), Error> {
    let peers = peers().await?;
    let left_root = root("content-left");
    let right_root = root("content-right");
    let left = FileSystem::open(&left_root, peers.left.endpoint().id()).map_err(other)?;
    let right = FileSystem::open(&right_root, peers.right.endpoint().id()).map_err(other)?;
    left.set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    right
        .set_residency("", Residency::Passthrough)
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

    let stream = right_sync.stream("notes/today.txt").await.map_err(other)?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    left_sync.pump().await.map_err(other)?;
    assert_eq!(
        stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(other)?
            .concat(),
        b"hello"
    );
    assert!(matches!(
        right.read("notes/today.txt").await,
        Err(FileSystemError::ContentUnavailable)
    ));

    peers.close().await?;
    fs::remove_dir_all(left_root)?;
    fs::remove_dir_all(right_root)?;
    Ok(())
}

struct Peers {
    left: Server<InMemoryEndpointIdStore, TestAuthorizer>,
    right: Server<InMemoryEndpointIdStore, TestAuthorizer>,
    left_transport: RootIrohTransport<InMemoryEndpointIdStore, TestAuthorizer, StaticAccessToken>,
    right_transport: RootIrohTransport<InMemoryEndpointIdStore, TestAuthorizer, StaticAccessToken>,
    left_router: iroh::protocol::Router,
    right_router: iroh::protocol::Router,
}

impl Peers {
    async fn close(self) -> Result<(), Error> {
        self.left_router.shutdown().await.map_err(other)?;
        self.right_router.shutdown().await.map_err(other)?;
        self.left.close().await;
        self.right.close().await;
        Ok(())
    }
}

async fn peers() -> Result<Peers, Error> {
    let left_key = iroh::SecretKey::generate();
    let right_key = iroh::SecretKey::generate();
    let allowed = InMemoryEndpointIdStore::new();
    allowed.add(left_key.public());
    allowed.add(right_key.public());
    let left = Server::bind_with_secret_key(
        iroh::endpoint::presets::N0,
        left_key,
        allowed.clone(),
        TestAuthorizer,
    )
    .await
    .map_err(other)?;
    let right = Server::bind_with_secret_key(
        iroh::endpoint::presets::N0,
        right_key,
        allowed,
        TestAuthorizer,
    )
    .await
    .map_err(other)?;
    let root_id = RootId::new([7; 32]);
    let left_transport = RootIrohTransport::new(
        left.clone(),
        root_id,
        StaticAccessToken::new(b"authorized".to_vec()),
    );
    let right_transport = RootIrohTransport::new(
        right.clone(),
        root_id,
        StaticAccessToken::new(b"authorized".to_vec()),
    );

    let left_router = left_transport.router();
    let right_router = right_transport.router();
    right_transport.connect(left.endpoint().addr()).await?;
    left_transport.connect(right.endpoint().addr()).await?;

    Ok(Peers {
        left,
        right,
        left_transport,
        right_transport,
        left_router,
        right_router,
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
