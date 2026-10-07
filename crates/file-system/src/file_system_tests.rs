use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use uuid::Uuid;

use crate::{
    Error, FileHandle, FileSessionService, FileSystem, MemoryNetwork, MemoryTransport,
    MetadataSync, OpenRequest, OperationControl, Residency, staged_content::ContentStep,
};

fn root() -> PathBuf {
    std::env::temp_dir().join(format!("offs-content-fault-{}", Uuid::now_v7()))
}

fn files(path: &Path) -> Vec<PathBuf> {
    let mut output = Vec::new();
    for entry in fs::read_dir(path).expect("content directory") {
        let path = entry.expect("content entry").path();
        if path.is_dir() {
            output.extend(files(&path));
        } else {
            output.push(path);
        }
    }
    output.sort();
    output
}

async fn drive<F: Future>(
    future: F,
    server: &mut MetadataSync<'_, u8, MemoryTransport<u8>>,
) -> F::Output {
    tokio::pin!(future);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            tokio::select! {
                biased;
                result = &mut future => return result,
                () = tokio::task::yield_now() => server.pump().await.expect("server pumps"),
            }
        }
    })
    .await
    .expect("test-only exchange watchdog")
}

#[tokio::test]
async fn write_offset_and_append_faults_preserve_content_revision_and_cleanup() {
    for operation in 0..3 {
        for step in [
            ContentStep::Write,
            ContentStep::Sync,
            ContentStep::Publish,
            ContentStep::Metadata,
        ] {
            let root = root();
            let filesystem = FileSystem::open(&root, 1_u8).expect("filesystem opens");
            filesystem
                .set_residency("", Residency::Full)
                .await
                .expect("full residency");
            let previous = filesystem
                .write("file", b"original")
                .await
                .expect("initial content");
            let previous_files = files(&root.join(".data"));
            *filesystem.content_fault.lock().expect("fault lock") = Some(step);
            let result = match operation {
                0 => filesystem.write("file", b"replacement").await.map(|_| ()),
                1 => filesystem
                    .write_at("file", previous.meta.revision, 2, Bytes::from_static(b"XX"))
                    .await
                    .map(|_| ()),
                _ => filesystem.append("file", b"tail").await,
            };
            assert!(matches!(result, Err(Error::Io(_))), "{operation}/{step:?}");
            assert_eq!(
                filesystem.read("file").await.expect("old complete content"),
                b"original"
            );
            assert_eq!(
                filesystem.revision("file").await.expect("old revision"),
                previous.meta.revision
            );
            assert_eq!(
                files(&root.join(".data")),
                previous_files,
                "no staging or uncommitted publication"
            );
            let count = filesystem
                .write_at("file", previous.meta.revision, 2, Bytes::from_static(b"XX"))
                .await
                .expect("offset retry");
            assert_eq!(count, 2);
            filesystem
                .append("file", b"tail")
                .await
                .expect("append retry");
            assert_eq!(
                filesystem
                    .read("file")
                    .await
                    .expect("offset/append content"),
                b"orXXinaltail"
            );
            fs::remove_dir_all(root).expect("cleanup");
        }
    }
}

#[tokio::test]
async fn admitted_session_write_finishes_when_cancelled_at_native_publication() {
    let root = root();
    let filesystem = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let previous = filesystem
        .write("file", b"original")
        .await
        .expect("initial content");
    let network = MemoryNetwork::new(32);
    let transport = network.transport(2);
    let control = OperationControl::default();
    let mut server = filesystem
        .metadata_sync(network.transport(1))
        .with_operation_control(control.clone());
    let mut handle = transport
        .open(
            1,
            OpenRequest {
                path: "file".into(),
                revision: Some(previous.meta.revision),
            },
        )
        .await
        .expect("open queues");
    server.pump().await.expect("open executes");
    let cancel = control.clone();
    *filesystem.content_action.lock().expect("action lock") =
        Some((ContentStep::Publish, Box::new(move || cancel.cancel())));
    let count = drive(
        handle.write(0, Bytes::from_static(b"accepted")),
        &mut server,
    )
    .await
    .expect("admitted commit finishes");
    assert_eq!(count, 8);
    assert!(control.is_cancelled());
    assert_eq!(
        filesystem.read("file").await.expect("complete content"),
        b"accepted"
    );
    assert_ne!(
        filesystem
            .revision("file")
            .await
            .expect("committed revision"),
        previous.meta.revision
    );
    assert!(matches!(server.pump().await, Err(Error::Cancelled)));
    drop(server);
    filesystem
        .append("file", b" offline")
        .await
        .expect("local writes survive sync stop");
    assert_eq!(
        filesystem.read("file").await.expect("local content"),
        b"accepted offline"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn revision_bound_session_reads_reject_provider_changes_between_reads() {
    let root = root();
    let filesystem = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let previous = filesystem
        .write("file", b"original")
        .await
        .expect("initial content");
    let network = MemoryNetwork::new(32);
    let transport = network.transport(2);
    let mut server = filesystem.metadata_sync(network.transport(1));
    let mut handle = transport
        .open(
            1,
            OpenRequest {
                path: "file".into(),
                revision: Some(previous.meta.revision),
            },
        )
        .await
        .expect("open queues");
    server.pump().await.expect("open executes");
    let read = handle.read(0, 4);
    server.pump().await.expect("first read executes");
    assert_eq!(read.await.expect("first read"), b"orig".as_slice());
    filesystem
        .write("file", b"changed content")
        .await
        .expect("provider revision changes");
    let read = handle.read(4, 4);
    server.pump().await.expect("second read executes");
    assert!(matches!(
        read.await,
        Err(crate::MemoryTransportError::Service(
            crate::FileServiceError::StaleRevision
        ))
    ));
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn cancellation_while_queued_for_execution_never_reaches_policy_or_mutation() {
    let root = root();
    let filesystem = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    filesystem
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let previous = filesystem
        .write("file", b"original")
        .await
        .expect("initial content");
    let decisions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let policy = std::sync::Arc::clone(&decisions);
    let control = OperationControl::new(move |_, operation| {
        if matches!(operation, crate::RemoteOperation::Session { .. }) {
            policy.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Box::pin(async { true })
    });
    let network = MemoryNetwork::new(32);
    let transport = network.transport(2);
    let mut server = filesystem
        .metadata_sync(network.transport(1))
        .with_operation_control(control.clone());
    let mut handle = transport
        .open(
            1,
            OpenRequest {
                path: "file".into(),
                revision: Some(previous.meta.revision),
            },
        )
        .await
        .expect("open queues");
    let execution = filesystem.execution.lock().await;
    let mut queued = Box::pin(server.pump());
    std::future::poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    control.cancel();
    queued.await.expect("unstarted request discarded");
    drop(execution);
    assert_eq!(decisions.load(std::sync::atomic::Ordering::SeqCst), 0);
    let read = handle.read(0, 8);
    assert!(matches!(server.pump().await, Err(Error::Cancelled)));
    assert!(matches!(
        read.await,
        Err(crate::MemoryTransportError::SessionClosed)
    ));
    assert_eq!(
        filesystem.read("file").await.expect("content intact"),
        b"original"
    );
    assert_eq!(
        filesystem.revision("file").await.expect("revision intact"),
        previous.meta.revision
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn a_closed_read_response_is_not_successful_transfer_completion() {
    let network = MemoryNetwork::new(32);
    let server = network.transport(1);
    let mut requests = server.subscribe_file_sessions();
    let client = network.transport(2);
    let mut handle = client
        .open(
            1,
            OpenRequest {
                path: "file".into(),
                revision: Some(Uuid::now_v7()),
            },
        )
        .await
        .expect("open queues");
    drop(requests.recv().await.expect("open request"));
    let read = handle.read(0, 32);
    let (_, _, response) = requests.recv().await.expect("read request");
    response
        .send(crate::FileResponse::ReadChunk(Bytes::from_static(
            b"partial",
        )))
        .await
        .expect("partial response");
    drop(response);
    assert!(matches!(
        read.await,
        Err(crate::MemoryTransportError::SessionClosed)
    ));
}

#[tokio::test]
async fn fetch_write_sync_and_publish_faults_keep_metadata_and_earlier_files() {
    for step in [ContentStep::Write, ContentStep::Sync, ContentStep::Publish] {
        let source_root = root();
        let target_root = root();
        let source = FileSystem::open(&source_root, 1_u8).expect("source opens");
        let target = FileSystem::open(&target_root, 2_u8).expect("target opens");
        source
            .set_residency("", Residency::Full)
            .await
            .expect("source full");
        target
            .set_residency("", Residency::Full)
            .await
            .expect("target full");
        let expected = source
            .write("file", b"complete transfer")
            .await
            .expect("source content");
        let network = MemoryNetwork::new(32);
        let mut server = source.metadata_sync(network.transport(1));
        let mut client = target.metadata_sync(network.transport(2));
        server.announce().await.expect("announce");
        client.pump().await.expect("import");
        target
            .write("retained", b"earlier valid content")
            .await
            .expect("earlier commit");
        let previous_files = files(&target_root.join(".data"));
        *target.content_fault.lock().expect("fault lock") = Some(step);
        assert!(matches!(
            drive(client.fetch("file"), &mut server).await,
            Err(Error::Io(_))
        ));
        assert_eq!(files(&target_root.join(".data")), previous_files);
        assert_eq!(
            target.revision("file").await.expect("imported revision"),
            expected.meta.revision
        );
        assert!(matches!(
            target.read("file").await,
            Err(Error::ContentUnavailable)
        ));
        assert_eq!(
            target.read("retained").await.expect("earlier content"),
            b"earlier valid content"
        );
        drive(client.fetch("file"), &mut server)
            .await
            .expect("fetch retries");
        assert_eq!(
            target.read("file").await.expect("complete content"),
            b"complete transfer"
        );
        fs::remove_dir_all(source_root).expect("source cleanup");
        fs::remove_dir_all(target_root).expect("target cleanup");
    }
}
