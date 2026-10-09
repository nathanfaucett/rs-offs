use std::{env, fs, sync::Arc};

use file_system::{Error, FileHandle, FileSystemCatalog, OpenRequest, Residency};
use futures_util::StreamExt;
use uuid::Uuid;

#[tokio::test]
async fn projected_eviction_revokes_retained_native_handles() {
    let root = env::temp_dir().join(format!("offs-retirement-{}", Uuid::now_v7()));
    let catalog = FileSystemCatalog::open(&root).expect("open catalog");
    let resource = catalog.create(None).expect("create resource");
    catalog.mark_projected(resource.id).expect("mark projected");
    let filesystem = Arc::new(
        catalog
            .open_filesystem(resource.id, 1_u8)
            .expect("open filesystem"),
    );
    let cloned = Arc::clone(&filesystem);
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full");
    filesystem
        .write("file", b"native content")
        .await
        .expect("write");
    let mut handle = cloned
        .open_handle(OpenRequest {
            path: "file".into(),
            revision: None,
        })
        .await
        .expect("open handle");
    let mut stream = filesystem.stream("file", 3).await.expect("open stream");
    assert_eq!(
        stream.next().await.expect("chunk").expect("read").as_ref(),
        b"nat"
    );
    assert!(
        catalog
            .evict_projected_copy(resource.id)
            .await
            .expect("evict")
    );
    assert!(matches!(
        cloned.read("file").await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        handle.read(0, 6).await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        stream.next().await,
        Some(Err(Error::PermissionDenied))
    ));
    assert!(matches!(
        catalog.open_filesystem(resource.id, 1_u8),
        Err(Error::PermissionDenied)
    ));
    drop(handle);
    drop(stream);
    drop(cloned);
    drop(filesystem);
    drop(catalog);
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn passthrough_denies_delete_and_rename_including_descendants_and_destinations() {
    let root = env::temp_dir().join(format!("offs-retirement-policy-{}", Uuid::now_v7()));
    let filesystem = file_system::FileSystem::open(&root, 1_u8).expect("open filesystem");
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full");
    filesystem
        .write("dir/file", b"original")
        .await
        .expect("write");
    let before = filesystem.entry("dir/file").await.expect("entry");
    filesystem
        .set_residency("dir/file", Residency::Passthrough)
        .await
        .expect("passthrough child");
    assert!(matches!(
        filesystem.delete("dir/file").await,
        Err(Error::PassthroughWrite)
    ));
    assert!(matches!(
        filesystem.rename("dir", "moved").await,
        Err(Error::PassthroughWrite)
    ));
    assert_eq!(
        filesystem.entry("dir/file").await.expect("identity intact"),
        before
    );
    assert!(matches!(
        filesystem.entry("moved").await,
        Err(Error::NotFound)
    ));
    filesystem
        .set_residency("destination", Residency::Passthrough)
        .await
        .expect("passthrough destination");
    filesystem.write("source", b"source").await.expect("source");
    assert!(matches!(
        filesystem.rename("source", "destination/file").await,
        Err(Error::PassthroughWrite)
    ));
    filesystem
        .set_residency("moved/file", Residency::Passthrough)
        .await
        .expect("destination child");
    filesystem
        .set_residency("dir/file", Residency::Full)
        .await
        .expect_err("missing content cannot become full");
    filesystem
        .write("other/file", b"other")
        .await
        .expect("other source");
    assert!(matches!(
        filesystem.rename("other", "moved").await,
        Err(Error::PassthroughWrite)
    ));
    filesystem
        .delete("source")
        .await
        .expect("full delete allowed");
    filesystem
        .rename("other", "allowed")
        .await
        .expect("full rename allowed");
    drop(filesystem);
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn created_copy_cleanup_failure_retries_after_native_reopen_without_catalog_mutation() {
    if let Ok(root) = env::var("OFFS_REOPEN_ROOT") {
        let catalog = FileSystemCatalog::open(&root).expect("fresh process catalog reopen");
        let id = file_system::FileSystemId::parse(&env::var("OFFS_REOPEN_ID").expect("retired ID"))
            .expect("parse retired ID");
        let other =
            file_system::FileSystemId::parse(&env::var("OFFS_REOPEN_OTHER").expect("other ID"))
                .expect("parse other ID");
        assert!(matches!(
            catalog.open_filesystem(id, 1_u8),
            Err(Error::PermissionDenied)
        ));
        catalog
            .remove_managed_copy(id)
            .await
            .expect("fresh process retry");
        assert_eq!(catalog.snapshot().expect("durable identities").len(), 3);
        assert_eq!(
            catalog
                .snapshot()
                .expect("durable tombstone")
                .iter()
                .filter(|entry| entry.deleted)
                .count(),
            1
        );
        let retained = catalog
            .open_filesystem(other, 1_u8)
            .expect("fresh native unrelated reopen");
        assert_eq!(
            retained.read("file").await.expect("fresh native content"),
            b"keep"
        );
        return;
    }
    let root = env::temp_dir().join(format!("offs-retirement-retry-{}", Uuid::now_v7()));
    let catalog = FileSystemCatalog::open(&root).expect("open catalog");
    let resource = catalog.create(Some("created".into())).expect("resource");
    let unrelated = catalog.create(Some("unrelated".into())).expect("unrelated");
    let tombstone = catalog.create(None).expect("tombstone");
    catalog
        .delete(tombstone.id)
        .expect("delete unrelated identity");
    let before = catalog.snapshot().expect("catalog snapshot");
    let filesystem = Arc::new(
        catalog
            .open_filesystem(resource.id, 1_u8)
            .expect("open created copy"),
    );
    let clone = Arc::clone(&filesystem);
    let other = catalog
        .open_filesystem(unrelated.id, 1_u8)
        .expect("open unrelated");
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full");
    other
        .set_residency("", Residency::Full)
        .await
        .expect("other full");
    filesystem
        .write("file", b"target")
        .await
        .expect("target content");
    other.write("file", b"keep").await.expect("other content");
    let local_root = root
        .join("filesystems")
        .join(resource.id.as_uuid().to_string());
    fs::write(local_root.join(".data/orphan.tmp"), b"interrupted stage").expect("stage fixture");
    let saved = root.join("saved-copy");
    fs::rename(&local_root, &saved)
        .expect("preserve native copy for deterministic cleanup failure");
    fs::write(&local_root, b"not a directory").expect("cleanup obstruction");
    assert!(matches!(
        catalog.remove_managed_copy(resource.id).await,
        Err(Error::Io(_))
    ));
    assert!(matches!(
        clone.read("file").await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        clone.write("new", b"blocked").await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(clone.residency(""), Err(Error::PermissionDenied)));
    assert_eq!(catalog.snapshot().expect("unchanged catalog"), before);
    assert_eq!(other.read("file").await.expect("unrelated usable"), b"keep");
    fs::remove_file(&local_root).expect("remove obstruction");
    fs::rename(&saved, &local_root).expect("restore native copy");
    // Native exclusivity proves the owner released its metadata handle despite retained filesystem Arcs.
    drop(
        redb::Database::open(local_root.join("metadata.redb"))
            .expect("native metadata handle released"),
    );
    drop(catalog);
    let reopened =
        FileSystemCatalog::open(&root).expect("reopen catalog with old filesystem handles alive");
    assert!(matches!(
        reopened.open_filesystem(resource.id, 1_u8),
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        file_system::FileSystem::open(&local_root, 1_u8),
        Err(Error::PermissionDenied)
    ));
    reopened
        .remove_managed_copy(resource.id)
        .await
        .expect("retry cleanup");
    reopened
        .remove_managed_copy(resource.id)
        .await
        .expect("idempotent cleanup");
    assert!(!local_root.exists());
    assert_eq!(
        reopened
            .snapshot()
            .expect("identities and tombstones preserved"),
        before
    );
    assert_eq!(
        other.read("file").await.expect("other still usable"),
        b"keep"
    );
    drop(clone);
    drop(filesystem);
    drop(other);
    drop(reopened);
    let restarted = FileSystemCatalog::open(&root).expect("restart without retained handles");
    assert!(matches!(
        restarted.open_filesystem(resource.id, 1_u8),
        Err(Error::PermissionDenied)
    ));
    restarted
        .remove_managed_copy(resource.id)
        .await
        .expect("retry after full restart");
    let retained = restarted
        .open_filesystem(unrelated.id, 1_u8)
        .expect("reopen unrelated native store");
    assert_eq!(
        retained
            .read("file")
            .await
            .expect("unrelated content durable"),
        b"keep"
    );
    assert_eq!(
        retained.residency("file").expect("process local policy"),
        Residency::Passthrough
    );
    assert!(matches!(
        retained.delete("file").await,
        Err(Error::PassthroughWrite)
    ));
    drop(retained);
    drop(restarted);
    let child = std::process::Command::new(env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "created_copy_cleanup_failure_retries_after_native_reopen_without_catalog_mutation",
            "--nocapture",
        ])
        .env("OFFS_REOPEN_ROOT", &root)
        .env("OFFS_REOPEN_ID", resource.id.as_uuid().to_string())
        .env("OFFS_REOPEN_OTHER", unrelated.id.as_uuid().to_string())
        .output()
        .expect("fresh process restart probe");
    assert!(
        child.status.success(),
        "fresh process probe failed: {}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn retirement_cancels_pending_sync_admission_and_invalidates_live_sessions() {
    use file_system::{
        FileOperation, FileSessionService, MemoryNetwork, OperationControl, RemoteOperation,
    };
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    let root = env::temp_dir().join(format!("offs-retirement-sync-{}", Uuid::now_v7()));
    let catalog = FileSystemCatalog::open(&root).expect("catalog");
    let resource = catalog.create(None).expect("resource");
    let filesystem = catalog
        .open_filesystem(resource.id, 1_u8)
        .expect("native filesystem");
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full");
    filesystem.write("file", b"original").await.expect("write");
    let network = MemoryNetwork::new(32);
    let transport = network.transport(2);
    let control = OperationControl::new(|_, operation| {
        if matches!(
            operation,
            RemoteOperation::Session {
                operation: FileOperation::Write { .. },
                ..
            }
        ) {
            Box::pin(std::future::pending())
        } else {
            Box::pin(async { true })
        }
    });
    let mut server = filesystem
        .metadata_sync(network.transport(1))
        .with_operation_control(control);
    let mut handle = transport
        .open(
            1,
            OpenRequest {
                path: "file".into(),
                revision: None,
            },
        )
        .await
        .expect("remote open");
    server.pump().await.expect("issue live session handle");
    let mut write = handle.write(0, bytes::Bytes::from_static(b"blocked"));
    assert!(
        write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let mut pending = Box::pin(server.pump());
    assert!(matches!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    catalog
        .remove_managed_copy(resource.id)
        .await
        .expect("retire while admission is pending");
    pending
        .await
        .expect("unadmitted request responds with denial");
    assert!(matches!(
        write.await,
        Err(file_system::MemoryTransportError::Service(
            file_system::FileServiceError::PermissionDenied
        ))
    ));
    assert!(matches!(
        server.announce().await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        server.fetch("file").await,
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        server.set_residency("file", Residency::Full).await,
        Err(Error::PermissionDenied)
    ));
    let read = handle.read(0, 8);
    assert!(matches!(server.pump().await, Err(Error::PermissionDenied)));
    assert!(matches!(
        read.await,
        Err(file_system::MemoryTransportError::SessionClosed)
    ));
    drop(server);
    drop(filesystem);
    drop(catalog);
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn retirement_wakes_idle_sync_run_and_preserves_target_tombstone() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    let root = env::temp_dir().join(format!("offs-retirement-run-{}", Uuid::now_v7()));
    let catalog = FileSystemCatalog::open(&root).expect("catalog");
    let resource = catalog.create(None).expect("resource");
    let filesystem = catalog
        .open_filesystem(resource.id, 1_u8)
        .expect("native filesystem");
    let network = file_system::MemoryNetwork::new(32);
    let mut run = Box::pin(filesystem.metadata_sync(network.transport(1)).run());
    assert!(
        run.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    catalog.delete(resource.id).expect("caller tombstone");
    let before = catalog.snapshot().expect("tombstoned identity");
    catalog
        .remove_managed_copy(resource.id)
        .await
        .expect("retire tombstoned copy");
    assert!(matches!(run.await, Err(Error::PermissionDenied)));
    assert_eq!(
        catalog.snapshot().expect("target tombstone preserved"),
        before
    );
    assert!(matches!(
        filesystem.entry("file").await,
        Err(Error::PermissionDenied)
    ));
    drop(filesystem);
    drop(catalog);
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn retirement_cancels_unpublished_content_stage_without_touching_provider() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };

    let root = env::temp_dir().join(format!("offs-retirement-stage-{}", Uuid::now_v7()));
    let catalog = FileSystemCatalog::open(&root).expect("catalog");
    let resource = catalog.create(None).expect("target resource");
    let target = catalog
        .open_filesystem(resource.id, 2_u8)
        .expect("native target");
    let source =
        file_system::FileSystem::open(root.join("provider"), 1_u8).expect("native provider");
    source
        .set_residency("", Residency::Full)
        .await
        .expect("provider full");
    let entry = source
        .write("file", b"provider content")
        .await
        .expect("provider content");
    let network = file_system::MemoryNetwork::new(32);
    let server = source.metadata_sync(network.transport(1));
    let mut client = target.metadata_sync(network.transport(2));
    server.announce().await.expect("announce provider metadata");
    client.pump().await.expect("import metadata only");
    let mut fetch = Box::pin(client.fetch("file"));
    assert!(
        fetch
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let local_root = root
        .join("filesystems")
        .join(resource.id.as_uuid().to_string());
    assert!(
        fs::read_dir(
            local_root
                .join(".data")
                .join(entry.meta.file_id.to_string())
        )
        .expect("stage directory")
        .any(|entry| entry
            .expect("stage entry")
            .path()
            .extension()
            .is_some_and(|extension| extension == "tmp"))
    );
    let mut retirement = Box::pin(catalog.remove_managed_copy(resource.id));
    assert!(
        retirement
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(matches!(fetch.await, Err(Error::PermissionDenied)));
    retirement.await.expect("stage unwinds and drain completes");
    assert!(!local_root.exists());
    assert_eq!(
        source.read("file").await.expect("provider unaffected"),
        b"provider content"
    );
    drop(client);
    drop(server);
    drop(target);
    drop(source);
    drop(catalog);
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn native_process_lease_blocks_cleanup_until_owner_releases() {
    use std::time::{Duration, Instant};

    fn wait_for(path: &std::path::Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            assert!(
                Instant::now() < deadline,
                "test-only process handshake watchdog: {}",
                path.display()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    if let Ok(root) = env::var("OFFS_LEASE_ROOT") {
        let root = std::path::PathBuf::from(root);
        let id = env::var("OFFS_LEASE_ID").expect("resource ID");
        let filesystem = file_system::FileSystem::open(root.join("filesystems").join(id), 1_u8)
            .expect("child native copy");
        filesystem
            .set_residency("", Residency::Full)
            .await
            .expect("child full");
        filesystem
            .write("file", b"native process")
            .await
            .expect("child native content");
        fs::write(root.join("ready"), b"").expect("signal native ownership");
        wait_for(&root.join("release"));
        assert!(matches!(
            filesystem.read("file").await,
            Err(Error::PermissionDenied)
        ));
        assert!(matches!(
            filesystem.write("new", b"blocked").await,
            Err(Error::PermissionDenied)
        ));
        return;
    }
    let root = env::temp_dir().join(format!("offs-retirement-process-{}", Uuid::now_v7()));
    let catalog = FileSystemCatalog::open(&root).expect("catalog");
    let resource = catalog.create(None).expect("resource");
    let before = catalog.snapshot().expect("identity");
    let child = std::process::Command::new(env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "native_process_lease_blocks_cleanup_until_owner_releases",
            "--nocapture",
        ])
        .env("OFFS_LEASE_ROOT", &root)
        .env("OFFS_LEASE_ID", resource.id.as_uuid().to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("native owner process");
    wait_for(&root.join("ready"));
    assert!(
        matches!(catalog.remove_managed_copy(resource.id).await, Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    let local_root = root
        .join("filesystems")
        .join(resource.id.as_uuid().to_string());
    assert!(
        local_root.join("metadata.redb").exists(),
        "cleanup cannot unlink another process's native copy"
    );
    assert!(matches!(
        catalog.open_filesystem(resource.id, 1_u8),
        Err(Error::PermissionDenied)
    ));
    fs::write(root.join("release"), b"").expect("release child owner");
    let child = child.wait_with_output().expect("child completion");
    assert!(
        child.status.success(),
        "child failed: {}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    catalog
        .remove_managed_copy(resource.id)
        .await
        .expect("retry after native owner releases");
    assert!(!local_root.exists());
    assert_eq!(catalog.snapshot().expect("catalog preserved"), before);
    drop(catalog);
    fs::remove_dir_all(root).expect("cleanup");
}
