use std::{env, fs};

use file_system::{Error, FileKind, FileSystem, MemoryNetwork, Residency, SyncMessage, Transport};
use uuid::Uuid;

fn root() -> std::path::PathBuf {
    env::temp_dir().join(format!("file-system-{}", Uuid::now_v7()))
}

async fn full<PeerId>(file_system: &FileSystem<PeerId>)
where
    PeerId: Clone
        + std::fmt::Debug
        + Ord
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    file_system
        .set_residency("", Residency::Full)
        .await
        .unwrap();
}

async fn synchronize(
    left: &mut file_system::MetadataSync<'_, u8, file_system::MemoryTransport<u8>>,
    right: &mut file_system::MetadataSync<'_, u8, file_system::MemoryTransport<u8>>,
) {
    left.announce().await.unwrap();
    right.announce().await.unwrap();
    for _ in 0..3 {
        left.pump().await.unwrap();
        right.pump().await.unwrap();
    }
}

#[tokio::test]
async fn residency_defaults_to_passthrough_and_uses_longest_prefix() {
    let root = root();
    let file_system = FileSystem::open(&root, 7_u8).unwrap();

    assert_eq!(
        file_system.residency("notes/today.txt").unwrap(),
        Residency::Passthrough
    );
    assert!(matches!(
        file_system.write("blocked.txt", b"content").await,
        Err(Error::PassthroughWrite)
    ));
    assert!(matches!(
        file_system.entry("blocked.txt").await,
        Err(Error::NotFound)
    ));

    file_system
        .set_residency("", Residency::Full)
        .await
        .unwrap();
    file_system
        .set_residency("notes", Residency::Passthrough)
        .await
        .unwrap();
    file_system
        .set_residency("notes/today.txt", Residency::Full)
        .await
        .unwrap();
    assert_eq!(file_system.residency("other.txt").unwrap(), Residency::Full);
    assert_eq!(
        file_system.residency("notes/other.txt").unwrap(),
        Residency::Passthrough
    );
    assert_eq!(
        file_system.residency("notes/today.txt").unwrap(),
        Residency::Full
    );

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn passthrough_evicts_content_and_full_requires_materialization() {
    let root = root();
    let file_system = FileSystem::open(&root, 7_u8).unwrap();
    full(&file_system).await;
    let entry = file_system.write("notes.txt", b"content").await.unwrap();

    file_system
        .set_residency("notes.txt", Residency::Passthrough)
        .await
        .unwrap();
    assert!(matches!(
        file_system.read("notes.txt").await,
        Err(Error::ContentUnavailable)
    ));
    assert!(matches!(
        file_system
            .set_residency("notes.txt", Residency::Full)
            .await,
        Err(Error::ContentUnavailable)
    ));
    assert_eq!(
        file_system.entry("notes.txt").await.unwrap().meta.file_id,
        entry.meta.file_id
    );

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn passthrough_fetch_reports_no_peers() {
    let root = root();
    let file_system = FileSystem::open(&root, 7_u8).unwrap();
    full(&file_system).await;
    file_system.write("notes.txt", b"content").await.unwrap();
    file_system
        .set_residency("notes.txt", Residency::Passthrough)
        .await
        .unwrap();
    let sync = file_system.metadata_sync(MemoryNetwork::new(1).transport(7));

    assert!(matches!(sync.fetch("notes.txt").await, Err(Error::NoPeers)));
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn passthrough_fetch_materializes_before_full_residency() {
    let left_root = root();
    let right_root = root();
    let network = MemoryNetwork::new(16);
    let left = FileSystem::open(&left_root, 1_u8).unwrap();
    let right = FileSystem::open(&right_root, 2_u8).unwrap();
    full(&left).await;
    left.write("notes.txt", b"content").await.unwrap();
    let mut left_sync = left.metadata_sync(network.transport(1));
    let mut right_sync = right.metadata_sync(network.transport(2));

    right_sync.announce().await.unwrap();
    left_sync.pump().await.unwrap();
    right_sync.pump().await.unwrap();
    assert!(matches!(
        right.set_residency("notes.txt", Residency::Full).await,
        Err(Error::ContentUnavailable)
    ));

    right_sync.fetch("notes.txt").await.unwrap();
    left_sync.pump().await.unwrap();
    right_sync.pump().await.unwrap();
    right
        .set_residency("notes.txt", Residency::Full)
        .await
        .unwrap();
    assert_eq!(right.read("notes.txt").await.unwrap(), b"content");

    fs::remove_dir_all(left_root).unwrap();
    fs::remove_dir_all(right_root).unwrap();
}

#[tokio::test]
async fn metadata_sync_converges_tombstones_and_late_joiners() {
    let left_root = root();
    let right_root = root();
    let late_root = root();
    let network = MemoryNetwork::new(16);
    let left = FileSystem::open(&left_root, 1_u8).unwrap();
    let right = FileSystem::open(&right_root, 2_u8).unwrap();
    let left_transport = network.transport(1);
    let right_transport = network.transport(2);
    full(&left).await;
    full(&right).await;

    left.write("conflict.txt", b"left").await.unwrap();
    right.write("conflict.txt", b"right").await.unwrap();
    let mut left_sync = left.metadata_sync(left_transport);
    let mut right_sync = right.metadata_sync(right_transport);
    synchronize(&mut left_sync, &mut right_sync).await;

    let left_entry = left.entry("conflict.txt").await.unwrap();
    let right_entry = right.entry("conflict.txt").await.unwrap();
    assert_eq!(left_entry.meta, right_entry.meta);

    left.delete("conflict.txt").await.unwrap();
    synchronize(&mut left_sync, &mut right_sync).await;
    assert!(matches!(
        right.entry("conflict.txt").await,
        Err(file_system::Error::NotFound)
    ));

    let late = FileSystem::open(&late_root, 3_u8).unwrap();
    let late_transport = network.transport(3);
    let mut late_sync = late.metadata_sync(late_transport);
    late_sync.announce().await.unwrap();
    for _ in 0..3 {
        left_sync.pump().await.unwrap();
        right_sync.pump().await.unwrap();
        late_sync.pump().await.unwrap();
    }
    assert!(matches!(
        late.entry("conflict.txt").await,
        Err(file_system::Error::NotFound)
    ));

    fs::remove_dir_all(left_root).unwrap();
    fs::remove_dir_all(right_root).unwrap();
    fs::remove_dir_all(late_root).unwrap();
}

#[tokio::test]
async fn syncs_content_and_rejects_stale_responses() {
    let left_root = root();
    let right_root = root();
    let network = MemoryNetwork::new(16);
    let left = FileSystem::open(&left_root, 1_u8).unwrap();
    let right = FileSystem::open(&right_root, 2_u8).unwrap();
    let left_transport = network.transport(1);
    let left_sender = left_transport.clone();
    let right_transport = network.transport(2);

    full(&left).await;
    full(&right).await;
    let entry = left.write("file.txt", b"old").await.unwrap();
    assert_eq!(
        entry.meta.pointer.as_deref(),
        Some("sha256:cba06b5736faf67e54b07b561eae94395e774c517a7d910a54369e1263ccfbd4")
    );
    let stale_pointer = entry.meta.pointer.unwrap();
    let mut left_sync = left.metadata_sync(left_transport);
    let mut right_sync = right.metadata_sync(right_transport);
    synchronize(&mut left_sync, &mut right_sync).await;
    assert_eq!(right.read("file.txt").await.unwrap(), b"old");

    left.write("file.txt", b"new").await.unwrap();
    synchronize(&mut left_sync, &mut right_sync).await;
    left_sender
        .send(
            2,
            SyncMessage::ContentResponse {
                file_id: entry.meta.file_id,
                pointer: stale_pointer,
                content: b"old".to_vec(),
            },
        )
        .await
        .unwrap();
    right_sync.pump().await.unwrap();

    assert_eq!(right.read("file.txt").await.unwrap(), b"new");
    fs::remove_dir_all(left_root).unwrap();
    fs::remove_dir_all(right_root).unwrap();
}

#[tokio::test]
async fn reports_unavailable_requested_content() {
    let left_root = root();
    let right_root = root();
    let network = MemoryNetwork::new(16);
    let left = FileSystem::open(&left_root, 1_u8).unwrap();
    let right = FileSystem::open(&right_root, 2_u8).unwrap();
    let left_transport = network.transport(1);
    let right_transport = network.transport(2);
    let right_sender = right_transport.clone();
    let mut left_sync = left.metadata_sync(left_transport);
    let _right_sync = right.metadata_sync(right_transport);

    right_sender
        .send(
            1,
            SyncMessage::ContentRequest {
                file_id: Uuid::now_v7(),
                pointer: "sha256:missing".into(),
            },
        )
        .await
        .unwrap();

    assert!(matches!(
        left_sync.pump().await,
        Err(file_system::Error::ContentUnavailable)
    ));
    fs::remove_dir_all(left_root).unwrap();
    fs::remove_dir_all(right_root).unwrap();
}

#[tokio::test]
async fn supports_non_uuid_peer_ids() {
    let root = root();
    let file_system = FileSystem::open(&root, 7_u8).unwrap();

    full(&file_system).await;
    let entry = file_system.write("file.txt", b"content").await.unwrap();

    assert_eq!(entry.meta.providers.into_iter().collect::<Vec<_>>(), [7]);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn preserves_file_identity_across_overwrite_append_and_rename() {
    let root = root();
    let file_system = FileSystem::open(&root, Uuid::now_v7()).unwrap();

    full(&file_system).await;
    let created = file_system
        .write("notes/today.txt", b"hello")
        .await
        .unwrap();
    file_system
        .append("notes/today.txt", b" world")
        .await
        .unwrap();
    let overwritten = file_system
        .write("notes/today.txt", b"updated")
        .await
        .unwrap();
    file_system
        .rename("notes/today.txt", "notes/tomorrow.txt")
        .await
        .unwrap();

    assert_eq!(created.meta.file_id, overwritten.meta.file_id);
    assert_eq!(
        file_system.read("notes/tomorrow.txt").await.unwrap(),
        b"updated"
    );
    assert_eq!(
        file_system
            .entry("notes/today.txt")
            .await
            .unwrap_err()
            .to_string(),
        "path was not found"
    );
    assert_eq!(file_system.list("notes").await.unwrap().len(), 1);

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn directory_semantics_and_boundaries() {
    let root = root();
    let file_system = FileSystem::open(&root, Uuid::now_v7()).unwrap();

    full(&file_system).await;
    file_system.write("a/b.txt", b"b").await.unwrap();
    file_system.create_dir("a/empty").await.unwrap();
    file_system.write("z.txt", b"z").await.unwrap();

    assert_eq!(
        file_system.entry("a").await.unwrap().meta.kind,
        FileKind::Directory
    );
    assert_eq!(
        file_system
            .list("")
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        ["a", "z.txt"]
    );
    assert_eq!(
        file_system
            .list("a")
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        ["a/b.txt", "a/empty"]
    );
    assert!(matches!(
        file_system.list("missing").await,
        Err(file_system::Error::NotFound)
    ));
    assert!(matches!(
        file_system.list("z.txt").await,
        Err(file_system::Error::NotDirectory)
    ));
    assert!(matches!(
        file_system.create_dir("z.txt/child").await,
        Err(file_system::Error::NotDirectory)
    ));
    assert!(matches!(
        file_system.write("a", b"x").await,
        Err(file_system::Error::IsDirectory)
    ));
    assert!(matches!(
        file_system.stream("z.txt", 0).await,
        Err(file_system::Error::InvalidChunkSize)
    ));
    for path in ["", "/a", "a//b", "a/./b", "a/../b", ".data/a"] {
        assert!(file_system.write(path, b"x").await.is_err());
    }

    file_system.delete("z.txt").await.unwrap();
    assert_eq!(fs::read_dir(root.join(".data")).unwrap().count(), 2);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn renames_directories_without_changing_identities() {
    let root = root();
    let file_system = FileSystem::open(&root, Uuid::now_v7()).unwrap();
    full(&file_system).await;
    let directory = file_system.create_dir("a").await.unwrap();
    let file = file_system.write("a/file.txt", b"content").await.unwrap();

    file_system.rename("a", "b").await.unwrap();

    assert_eq!(
        file_system.entry("b").await.unwrap().meta.file_id,
        directory.meta.file_id
    );
    assert_eq!(
        file_system.entry("b/file.txt").await.unwrap().meta.file_id,
        file.meta.file_id
    );
    assert!(matches!(
        file_system.entry("a").await,
        Err(file_system::Error::NotFound)
    ));
    assert!(matches!(
        file_system.entry("a/file.txt").await,
        Err(file_system::Error::NotFound)
    ));
    assert!(matches!(
        file_system.rename("b", "b/file.txt/nested").await,
        Err(file_system::Error::InvalidPath)
    ));
    file_system.create_dir("taken").await.unwrap();
    assert!(matches!(
        file_system.rename("b", "taken").await,
        Err(file_system::Error::AlreadyExists)
    ));

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn persists_metadata_content_and_directory_markers() {
    let root = root();
    let node_id = Uuid::now_v7();
    let file_id = {
        let file_system = FileSystem::open(&root, node_id).unwrap();
        full(&file_system).await;
        file_system.create_dir("empty").await.unwrap();
        file_system
            .write("notes/today.txt", b"hello")
            .await
            .unwrap()
            .meta
            .file_id
    };
    let file_system = FileSystem::open(&root, node_id).unwrap();
    full(&file_system).await;

    assert_eq!(
        file_system.entry("empty").await.unwrap().meta.kind,
        FileKind::Directory
    );
    assert_eq!(
        file_system
            .entry("notes/today.txt")
            .await
            .unwrap()
            .meta
            .file_id,
        file_id
    );
    assert_eq!(file_system.read("notes/today.txt").await.unwrap(), b"hello");

    fs::remove_dir_all(root).unwrap();
}
