use std::{
    env,
    fmt::Display,
    fs,
    io::Error,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use file_system::{
    Error as FileSystemError, FileHandle, FileSessionService, FileSystem, OpenRequest, Residency,
};
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
async fn iroh_file_handle_uses_one_authorized_session() -> Result<(), Error> {
    let peers = peers().await?;
    let left_root = root("session-left");
    let right_root = root("session-right");
    let left = FileSystem::open(&left_root, peers.left.endpoint().id()).map_err(other)?;
    let right = FileSystem::open(&right_root, peers.right.endpoint().id()).map_err(other)?;
    left.set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    left.write("notes/today.txt", b"hello world")
        .await
        .map_err(other)?;
    left.create_dir("docs").await.map_err(other)?;
    left.write("docs/a.txt", b"a").await.map_err(other)?;
    left.write("docs/b.txt", b"b").await.map_err(other)?;

    let revision = left.revision("notes/today.txt").await.map_err(other)?;
    let mut left_sync = left.metadata_sync(peers.left_transport.clone());
    let mut right_sync = right.metadata_sync(peers.right_transport.clone());
    let transport = peers.right_transport.clone();
    let left_id = peers.left.endpoint().id();
    let task = tokio::spawn(async move {
        let mut first = transport
            .open(
                left_id,
                OpenRequest {
                    path: "notes/today.txt".to_owned(),
                    revision: Some(revision),
                },
            )
            .await
            .map_err(other)?;
        let mut second = transport
            .open(
                left_id,
                OpenRequest {
                    path: "notes/today.txt".to_owned(),
                    revision: Some(revision),
                },
            )
            .await
            .map_err(other)?;
        assert_eq!(
            first.read(0, 5).await.map_err(other)?,
            Bytes::from_static(b"hello")
        );
        assert_eq!(
            first
                .write(6, Bytes::from_static(b"IROH"))
                .await
                .map_err(other)?,
            4
        );
        let stale = second
            .write(0, Bytes::from_static(b"bad"))
            .await
            .expect_err("stale handle must be rejected");
        assert!(stale.to_string().contains("StaleRevision"));

        let mut directory = transport
            .open(
                left_id,
                OpenRequest {
                    path: "docs".to_owned(),
                    revision: None,
                },
            )
            .await
            .map_err(other)?;
        let first_page = directory.scan(None, 1).await.map_err(other)?;
        assert_eq!(first_page.entries.len(), 1);
        let second_page = directory.scan(first_page.next, 1).await.map_err(other)?;
        assert_eq!(second_page.entries.len(), 1);
        assert!(second_page.next.is_none());
        Box::new(first).close().await.map_err(other)?;
        Box::new(second).close().await.map_err(other)?;
        Box::new(directory).close().await.map_err(other)?;
        Ok::<(), Error>(())
    });
    while !task.is_finished() {
        tokio::time::sleep(Duration::from_millis(10)).await;
        left_sync.pump().await.map_err(other)?;
        right_sync.pump().await.map_err(other)?;
    }
    task.await.map_err(other)??;
    assert_eq!(
        left.read("notes/today.txt").await.map_err(other)?,
        b"hello IROHd"
    );

    peers.close().await?;
    fs::remove_dir_all(left_root)?;
    fs::remove_dir_all(right_root)?;
    Ok(())
}

#[tokio::test]
async fn offline_peer_rejoins_and_converges_metadata_with_allowlist() -> Result<(), Error> {
    let left_key = iroh::SecretKey::generate();
    let right_key = iroh::SecretKey::generate();
    let offline_key = iroh::SecretKey::generate();
    let allowed = InMemoryEndpointIdStore::new();
    allowed.add(left_key.public());
    allowed.add(right_key.public());
    allowed.add(offline_key.public());

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
        allowed.clone(),
        TestAuthorizer,
    )
    .await
    .map_err(other)?;
    let offline = Server::bind_with_secret_key(
        iroh::endpoint::presets::N0,
        offline_key,
        allowed,
        TestAuthorizer,
    )
    .await
    .map_err(other)?;

    let root_id = RootId::new([9; 32]);
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
    let offline_transport = RootIrohTransport::new(
        offline.clone(),
        root_id,
        StaticAccessToken::new(b"authorized".to_vec()),
    );
    let left_router = left_transport.router();
    let right_router = right_transport.router();

    left_transport.connect(right.endpoint().addr()).await?;
    right_transport.connect(left.endpoint().addr()).await?;

    let left_root = root("offline-rejoin-left");
    let right_root = root("offline-rejoin-right");
    let offline_root = root("offline-rejoin-offline");
    let left_fs = FileSystem::open(&left_root, left.endpoint().id()).map_err(other)?;
    let right_fs = FileSystem::open(&right_root, right.endpoint().id()).map_err(other)?;
    let offline_fs = FileSystem::open(&offline_root, offline.endpoint().id()).map_err(other)?;
    left_fs
        .set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    right_fs
        .set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    offline_fs
        .set_residency("", Residency::Full)
        .await
        .map_err(other)?;
    left_fs
        .write("notes/rejoined.txt", b"offline metadata")
        .await
        .map_err(other)?;

    let mut left_sync = left_fs.metadata_sync(left_transport.clone());
    let mut right_sync = right_fs.metadata_sync(right_transport.clone());
    right_sync.announce().await.map_err(other)?;
    for _ in 0..8 {
        left_sync.pump().await.map_err(other)?;
        right_sync.pump().await.map_err(other)?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let left_entry = left_fs.entry("notes/rejoined.txt").await.map_err(other)?;
    let right_entry = right_fs.entry("notes/rejoined.txt").await.map_err(other)?;
    assert_eq!(right_entry.meta, left_entry.meta);
    assert!(offline_transport.peers().is_empty());

    let offline_router = offline_transport.router();
    left_transport.connect(offline.endpoint().addr()).await?;
    offline_transport.connect(left.endpoint().addr()).await?;
    right_transport.connect(offline.endpoint().addr()).await?;
    offline_transport.connect(right.endpoint().addr()).await?;
    let mut offline_sync = offline_fs.metadata_sync(offline_transport.clone());
    offline_sync.announce().await.map_err(other)?;

    for _ in 0..20 {
        left_sync.pump().await.map_err(other)?;
        right_sync.pump().await.map_err(other)?;
        offline_sync.pump().await.map_err(other)?;
        if offline_fs.entry("notes/rejoined.txt").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let offline_entry = offline_fs
        .entry("notes/rejoined.txt")
        .await
        .map_err(other)?;
    assert_eq!(offline_entry.meta, left_entry.meta);
    let unknown = iroh::SecretKey::generate().public();
    let error = left_transport
        .connect_discovered(unknown)
        .await
        .expect_err("discovery must not bypass the allowlist");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);

    offline_router.shutdown().await.map_err(other)?;
    left_router.shutdown().await.map_err(other)?;
    right_router.shutdown().await.map_err(other)?;
    left.close().await;
    right.close().await;
    offline.close().await;
    fs::remove_dir_all(left_root)?;
    fs::remove_dir_all(right_root)?;
    fs::remove_dir_all(offline_root)?;
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

    let revision = right.revision("notes/today.txt").await.map_err(other)?;
    let provider = peers.left.endpoint().id();
    let transport = peers.right_transport.clone();
    let open = tokio::spawn(async move {
        transport
            .open(
                provider,
                OpenRequest {
                    path: "notes/today.txt".to_owned(),
                    revision: Some(revision),
                },
            )
            .await
            .map_err(other)
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    left_sync.pump().await.map_err(other)?;
    let mut handle = open
        .await
        .map_err(|error| other(format!("open task: {error}")))?
        .map_err(|error| other(format!("open: {error}")))?;

    let read = tokio::spawn(async move {
        handle
            .read(0, 1024)
            .await
            .map_err(|error| other(format!("read: {error}")))
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    left_sync.pump().await.map_err(other)?;
    let content = read
        .await
        .map_err(|error| other(format!("read task: {error}")))??;
    assert_eq!(content, b"hello".as_slice());
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
