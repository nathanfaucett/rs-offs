use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use file_system::{Error, FileSystem, MemoryNetwork, OperationControl, Residency};
use file_system::{
    FileOperation, FileResponse, FileServiceError, FileSessionService, IncomingMessage,
    MemoryTransport, OpenRequest, SessionRequest, SyncMessage, Transport,
};
use tokio::sync::{Notify, broadcast, mpsc};
use uuid::Uuid;

#[derive(Clone)]
struct Exchange {
    inner: MemoryTransport<u8>,
    sessions: broadcast::Sender<SessionRequest<u8>>,
}

impl Exchange {
    fn new() -> Self {
        Self {
            inner: MemoryNetwork::new(32).transport(1),
            sessions: broadcast::channel(32).0,
        }
    }

    fn queue(&self, operation: FileOperation) -> mpsc::Receiver<FileResponse<u8>> {
        let (sender, receiver) = mpsc::channel(4);
        self.sessions
            .send((2, operation, sender))
            .expect("executor subscribed");
        receiver
    }
}

impl Transport<u8> for Exchange {
    type Error = file_system::MemoryTransportError;
    fn peers(&self) -> Vec<u8> {
        self.inner.peers()
    }
    async fn send(&self, peer: u8, message: SyncMessage<u8>) -> Result<(), Self::Error> {
        self.inner.send(peer, message).await
    }
    async fn broadcast(&self, message: SyncMessage<u8>) -> Result<(), Self::Error> {
        self.inner.broadcast(message).await
    }
    fn subscribe(&self) -> broadcast::Receiver<IncomingMessage<u8>> {
        self.inner.subscribe()
    }
}

impl FileSessionService<u8> for Exchange {
    type Handle = <MemoryTransport<u8> as FileSessionService<u8>>::Handle;
    async fn open(&self, peer: u8, request: OpenRequest) -> Result<Self::Handle, Self::Error> {
        self.inner.open(peer, request).await
    }
    fn subscribe_file_sessions(&self) -> broadcast::Receiver<SessionRequest<u8>> {
        self.sessions.subscribe()
    }
}

#[tokio::test]
async fn generic_session_codecs_preserve_open_read_write_scan_and_close_semantics() {
    async fn request(
        sync: &mut file_system::MetadataSync<'_, u8, Exchange>,
        exchange: &Exchange,
        operation: FileOperation,
    ) -> Vec<FileResponse<u8>> {
        let encoded = postcard::to_allocvec(&operation).expect("encode generic operation");
        let operation = postcard::from_bytes(&encoded).expect("decode generic operation");
        let mut response = exchange.queue(operation);
        sync.pump().await.expect("session executes");
        let mut responses = Vec::new();
        while let Some(response) = response.recv().await {
            let encoded = postcard::to_allocvec(&response).expect("encode generic response");
            assert_eq!(
                postcard::from_bytes::<FileResponse<u8>>(&encoded)
                    .expect("decode generic response"),
                response
            );
            responses.push(response);
        }
        responses
    }

    let root = std::env::temp_dir().join(format!("offs-session-{}", Uuid::now_v7()));
    let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    files
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let entry = files
        .write("folder/shared.txt", b"before")
        .await
        .expect("initial write");
    let exchange = Exchange::new();
    let mut sync = files.metadata_sync(exchange.clone());
    let opened = request(
        &mut sync,
        &exchange,
        FileOperation::Open(OpenRequest {
            path: "folder/shared.txt".into(),
            revision: None,
        }),
    )
    .await;
    let [
        FileResponse::Opened {
            handle,
            entry: opened_entry,
            revision,
        },
    ] = opened.as_slice()
    else {
        panic!("opened response expected")
    };
    assert_eq!(opened_entry, &entry);
    assert_eq!(*revision, entry.meta.revision);
    let handle = handle.clone();
    assert_eq!(
        request(
            &mut sync,
            &exchange,
            FileOperation::Read {
                handle: handle.clone(),
                offset: 0,
                length: file_system::MAX_FILE_READ_LENGTH + 1
            }
        )
        .await,
        vec![FileResponse::Error(FileServiceError::InvalidReadLength)]
    );
    assert_eq!(
        request(
            &mut sync,
            &exchange,
            FileOperation::Read {
                handle: handle.clone(),
                offset: 0,
                length: 32
            }
        )
        .await,
        vec![
            FileResponse::ReadChunk(bytes::Bytes::from_static(b"before")),
            FileResponse::ReadEnd
        ]
    );
    let written = request(
        &mut sync,
        &exchange,
        FileOperation::Write {
            handle: handle.clone(),
            expected_revision: entry.meta.revision,
            offset: 0,
            data: bytes::Bytes::from_static(b"after!"),
        },
    )
    .await;
    let [FileResponse::Written { count: 6, revision }] = written.as_slice() else {
        panic!("written response expected")
    };
    assert_ne!(*revision, entry.meta.revision);
    assert_eq!(
        files
            .revision("folder/shared.txt")
            .await
            .expect("committed revision"),
        *revision
    );
    assert_eq!(
        files
            .read("folder/shared.txt")
            .await
            .expect("committed bytes"),
        b"after!"
    );
    assert_eq!(
        request(
            &mut sync,
            &exchange,
            FileOperation::Read {
                handle: handle.clone(),
                offset: 0,
                length: 32
            }
        )
        .await,
        vec![
            FileResponse::ReadChunk(bytes::Bytes::from_static(b"after!")),
            FileResponse::ReadEnd
        ]
    );
    let directory = request(
        &mut sync,
        &exchange,
        FileOperation::Open(OpenRequest {
            path: "folder".into(),
            revision: None,
        }),
    )
    .await;
    let [
        FileResponse::Opened {
            handle: directory, ..
        },
    ] = directory.as_slice()
    else {
        panic!("directory opened response expected")
    };
    let scanned = request(
        &mut sync,
        &exchange,
        FileOperation::Scan {
            handle: directory.clone(),
            cursor: None,
            limit: 1,
        },
    )
    .await;
    let [FileResponse::Page(page)] = scanned.as_slice() else {
        panic!("scan page expected")
    };
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].path, "folder/shared.txt");
    assert_eq!(page.next, None);
    for handle in [directory.clone(), handle.clone()] {
        assert_eq!(
            request(&mut sync, &exchange, FileOperation::Close { handle }).await,
            vec![FileResponse::Closed]
        );
    }
    assert_eq!(
        request(
            &mut sync,
            &exchange,
            FileOperation::Read {
                handle,
                offset: 0,
                length: 1
            }
        )
        .await,
        vec![FileResponse::Error(FileServiceError::NotFound)]
    );
    drop(sync);
    files
        .append("folder/shared.txt", b" offline")
        .await
        .expect("local write after session shutdown");
    assert_eq!(
        files
            .read("folder/shared.txt")
            .await
            .expect("offline bytes"),
        b"after! offline"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn generic_metadata_codec_preserves_live_snapshots_and_tombstones() {
    let source_root = std::env::temp_dir().join(format!("offs-codec-{}", Uuid::now_v7()));
    let target_root = std::env::temp_dir().join(format!("offs-codec-{}", Uuid::now_v7()));
    let source = FileSystem::open(&source_root, 1_u8).expect("source opens");
    let target = FileSystem::open(&target_root, 2_u8).expect("target opens");
    source
        .set_residency("", Residency::Full)
        .await
        .expect("source full");
    let live = source.write("live", b"content").await.expect("live file");
    source
        .write("deleted", b"old")
        .await
        .expect("file before deletion");
    source.delete("deleted").await.expect("tombstone");
    let network = MemoryNetwork::new(32);
    let source_transport = network.transport(1);
    let target_transport = network.transport(2);
    let mut capture = target_transport.subscribe();
    source
        .metadata_sync(source_transport.clone())
        .announce()
        .await
        .expect("export snapshots");
    let message = loop {
        let (_, message) = capture.recv().await.expect("metadata message");
        if matches!(message, SyncMessage::SnapshotDelta { .. }) {
            break message;
        }
    };
    let SyncMessage::SnapshotDelta { snapshots } = &message else {
        panic!("snapshot delta expected")
    };
    assert_eq!(snapshots.len(), 2);
    let frame = file_system::encode_sync_message(&message).expect("encode generic snapshot codec");
    assert_eq!(frame[0], file_system::FILESYSTEM_SYNC_PROTOCOL_VERSION);
    let decoded =
        file_system::decode_sync_message::<u8>(&frame).expect("decode generic snapshot codec");
    assert_eq!(decoded, message);
    let mut receiver = target.metadata_sync(target_transport);
    source_transport
        .send(2, decoded)
        .await
        .expect("queue decoded snapshot set");
    receiver.pump().await.expect("import decoded snapshot set");
    assert_eq!(target.entry("live").await.expect("live metadata"), live);
    assert!(matches!(
        target.entry("deleted").await,
        Err(Error::NotFound)
    ));
    fs::remove_dir_all(source_root).expect("source cleanup");
    fs::remove_dir_all(target_root).expect("target cleanup");
}

#[tokio::test]
async fn queued_snapshots_recheck_local_decision_before_import() {
    let left_root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let right_root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let left = FileSystem::open(&left_root, 1_u8).expect("source opens");
    let right = FileSystem::open(&right_root, 2_u8).expect("target opens");
    left.set_residency("", Residency::Full)
        .await
        .expect("source is full");
    left.write("kept", b"valid").await.expect("source writes");
    let network = MemoryNetwork::new(32);
    let source = left.metadata_sync(network.transport(1));
    let allowed = Arc::new(AtomicBool::new(true));
    let decision = Arc::clone(&allowed);
    let control = OperationControl::new(move |_, _| {
        let decision = Arc::clone(&decision);
        Box::pin(async move { decision.load(Ordering::SeqCst) })
    });
    let mut target = right
        .metadata_sync(network.transport(2))
        .with_operation_control(control);
    source.announce().await.expect("snapshots queue");
    allowed.store(false, Ordering::SeqCst);
    target.pump().await.expect("denied messages are discarded");
    assert!(matches!(right.entry("kept").await, Err(Error::NotFound)));
    allowed.store(true, Ordering::SeqCst);
    source.announce().await.expect("retry queues");
    target.pump().await.expect("retry imports");
    assert_eq!(
        right.entry("kept").await.expect("entry imported").path,
        "kept"
    );
    left.delete("kept").await.expect("tombstone created");
    source.announce().await.expect("tombstone queues");
    allowed.store(false, Ordering::SeqCst);
    target.pump().await.expect("tombstone denied");
    assert!(right.entry("kept").await.is_ok());
    allowed.store(true, Ordering::SeqCst);
    source.announce().await.expect("tombstone retry");
    target.pump().await.expect("tombstone admitted");
    assert!(matches!(right.entry("kept").await, Err(Error::NotFound)));
    fs::remove_dir_all(left_root).expect("source cleanup");
    fs::remove_dir_all(right_root).expect("target cleanup");
}

#[tokio::test]
async fn invalid_snapshot_set_rolls_back_and_retains_prior_imports_and_tombstones() {
    let left_root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let right_root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let left = FileSystem::open(&left_root, 1_u8).expect("source opens");
    let right = FileSystem::open(&right_root, 2_u8).expect("target opens");
    left.set_residency("", Residency::Full)
        .await
        .expect("source full");
    left.write("kept", b"committed")
        .await
        .expect("source writes");
    left.write("deleted", b"old").await.expect("source writes");
    let network = MemoryNetwork::new(32);
    let source_transport = network.transport(1);
    let target_transport = network.transport(2);
    let source = left.metadata_sync(source_transport.clone());
    let mut target = right.metadata_sync(target_transport.clone());
    source.announce().await.expect("first set queues");
    target.pump().await.expect("first set commits");
    left.delete("deleted").await.expect("source tombstone");
    left.write("later", b"uncommitted")
        .await
        .expect("later source writes");
    let mut capture = target_transport.subscribe();
    source.announce().await.expect("capture next set");
    // Discard the valid queued set before constructing a separate receiver.
    drop(target);
    target = right.metadata_sync(target_transport);
    let snapshots = loop {
        let (_, message) = capture.recv().await.expect("captured message");
        if let SyncMessage::SnapshotDelta { mut snapshots } = message {
            let mut invalid = snapshots.first().expect("nonempty export").clone();
            invalid.payload = vec![0xff];
            snapshots.push(invalid);
            break snapshots;
        }
    };
    source_transport
        .send(2, SyncMessage::SnapshotDelta { snapshots })
        .await
        .expect("invalid set queues");
    assert!(matches!(target.pump().await, Err(Error::Metadata(_))));
    assert!(right.entry("kept").await.is_ok());
    assert!(right.entry("deleted").await.is_ok());
    assert!(matches!(right.entry("later").await, Err(Error::NotFound)));
    source.announce().await.expect("valid retry");
    target.pump().await.expect("valid set commits");
    assert!(matches!(right.entry("deleted").await, Err(Error::NotFound)));
    assert!(right.entry("later").await.is_ok());
    fs::remove_dir_all(left_root).expect("source cleanup");
    fs::remove_dir_all(right_root).expect("target cleanup");
}

#[tokio::test]
async fn established_handle_rechecks_every_queued_operation() {
    let root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    files
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let entry = files
        .write("file", b"original")
        .await
        .expect("initial write");
    let allowed = Arc::new(AtomicBool::new(true));
    let decision = Arc::clone(&allowed);
    let control = OperationControl::new(move |_, _| {
        let decision = Arc::clone(&decision);
        Box::pin(async move { decision.load(Ordering::SeqCst) })
    });
    let exchange = Exchange::new();
    let mut sync = files
        .metadata_sync(exchange.clone())
        .with_operation_control(control);
    let mut response = exchange.queue(FileOperation::Open(OpenRequest {
        path: "file".into(),
        revision: Some(entry.meta.revision),
    }));
    sync.pump().await.expect("open executes");
    let Some(FileResponse::Opened { handle, .. }) = response.recv().await else {
        panic!("opened response expected")
    };
    let operations = [
        FileOperation::Write {
            handle: handle.clone(),
            expected_revision: entry.meta.revision,
            offset: 0,
            data: bytes::Bytes::from_static(b"denied"),
        },
        FileOperation::Read {
            handle: handle.clone(),
            offset: 0,
            length: 8,
        },
        FileOperation::Scan {
            handle: handle.clone(),
            cursor: None,
            limit: 1,
        },
        FileOperation::Close {
            handle: handle.clone(),
        },
        FileOperation::Open(OpenRequest {
            path: "file".into(),
            revision: None,
        }),
    ];
    let mut responses = operations
        .into_iter()
        .map(|operation| exchange.queue(operation))
        .collect::<Vec<_>>();
    allowed.store(false, Ordering::SeqCst);
    sync.pump().await.expect("denied operations discarded");
    for response in &mut responses {
        assert_eq!(
            response.recv().await,
            Some(FileResponse::Error(FileServiceError::PermissionDenied))
        );
    }
    assert_eq!(
        files.read("file").await.expect("valid content remains"),
        b"original"
    );
    assert_eq!(
        files.revision("file").await.expect("revision remains"),
        entry.meta.revision
    );
    allowed.store(true, Ordering::SeqCst);
    let response = exchange.queue(FileOperation::Write {
        handle: handle.clone(),
        expected_revision: entry.meta.revision,
        offset: 0,
        data: bytes::Bytes::from_static(b"accepted"),
    });
    drop(response);
    sync.pump()
        .await
        .expect("response loss does not undo admitted write");
    assert_eq!(
        files.read("file").await.expect("write retained"),
        b"accepted"
    );
    let mut response = exchange.queue(FileOperation::Close { handle });
    sync.pump()
        .await
        .expect("close executes after re-admission");
    assert_eq!(response.recv().await, Some(FileResponse::Closed));
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn cancellation_drops_pending_policy_and_prevents_queued_mutation() {
    struct PendingPolicy(Arc<AtomicBool>);
    impl Drop for PendingPolicy {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    files
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let entry = files.write("file", b"valid").await.expect("initial write");
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let notify = Arc::clone(&started);
    let policy_dropped = Arc::clone(&dropped);
    let control = OperationControl::new(move |_, _| {
        let notify = Arc::clone(&notify);
        let guard = PendingPolicy(Arc::clone(&policy_dropped));
        Box::pin(async move {
            let _guard = guard;
            notify.notify_one();
            std::future::pending::<bool>().await
        })
    });
    let exchange = Exchange::new();
    let mut sync = files
        .metadata_sync(exchange.clone())
        .with_operation_control(control.clone());
    let mut response = exchange.queue(FileOperation::Open(OpenRequest {
        path: "file".into(),
        revision: None,
    }));
    let (result, ()) = tokio::join!(sync.pump(), async {
        started.notified().await;
        control.cancel();
    });
    result.expect("cancelled request rejected");
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(
        response.recv().await,
        Some(FileResponse::Error(FileServiceError::PermissionDenied))
    );
    assert!(matches!(sync.pump().await, Err(Error::Cancelled)));
    assert_eq!(
        files.revision("file").await.expect("revision intact"),
        entry.meta.revision
    );
    drop(sync);
    files
        .write("offline", b"still local")
        .await
        .expect("local writes survive stopped sync");
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn admitted_write_survives_cancellation_during_response_backpressure() {
    let root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    files
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    let entry = files
        .write("file", b"original")
        .await
        .expect("initial write");
    let admitted = Arc::new(Notify::new());
    let policy_notify = Arc::clone(&admitted);
    let control = OperationControl::new(move |_, operation| {
        let notify = Arc::clone(&policy_notify);
        Box::pin(async move {
            if matches!(
                operation,
                file_system::RemoteOperation::Session {
                    operation: FileOperation::Write { .. },
                    ..
                }
            ) {
                notify.notify_one();
            }
            true
        })
    });
    let exchange = Exchange::new();
    let mut sync = files
        .metadata_sync(exchange.clone())
        .with_operation_control(control.clone());
    let mut opened = exchange.queue(FileOperation::Open(OpenRequest {
        path: "file".into(),
        revision: None,
    }));
    sync.pump().await.expect("open executes");
    let Some(FileResponse::Opened { handle, .. }) = opened.recv().await else {
        panic!("opened response expected")
    };
    let (sender, mut receiver) = mpsc::channel(1);
    sender
        .send(FileResponse::Closed)
        .await
        .expect("fill response channel");
    exchange
        .sessions
        .send((
            2,
            FileOperation::Write {
                handle: handle.clone(),
                expected_revision: entry.meta.revision,
                offset: 0,
                data: bytes::Bytes::from_static(b"accepted"),
            },
            sender,
        ))
        .expect("write queues");
    let mut later = exchange.queue(FileOperation::Write {
        handle,
        expected_revision: entry.meta.revision,
        offset: 0,
        data: bytes::Bytes::from_static(b"denied!!"),
    });
    let (result, ()) = tokio::join!(sync.pump(), async {
        admitted.notified().await;
        assert_eq!(
            files.read("file").await.expect("admitted commit visible"),
            b"accepted"
        );
        control.cancel();
    });
    result.expect("response backpressure released on cancellation");
    assert_eq!(receiver.recv().await, Some(FileResponse::Closed));
    assert_eq!(receiver.recv().await, None);
    assert_eq!(
        later.recv().await,
        Some(FileResponse::Error(FileServiceError::PermissionDenied))
    );
    assert_eq!(
        files.read("file").await.expect("commit retained"),
        b"accepted"
    );
    assert_ne!(
        files.revision("file").await.expect("committed revision"),
        entry.meta.revision
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn cancellation_wins_when_policy_completes_in_the_same_poll() {
    let root = std::env::temp_dir().join(format!("offs-admission-{}", Uuid::now_v7()));
    let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    let holder = Arc::new(std::sync::Mutex::new(None::<OperationControl<u8>>));
    let policy_holder = Arc::clone(&holder);
    let control = OperationControl::new(move |_, _| {
        let holder = Arc::clone(&policy_holder);
        Box::pin(async move {
            holder
                .lock()
                .expect("control lock")
                .as_ref()
                .expect("control installed")
                .cancel();
            true
        })
    });
    *holder.lock().expect("control lock") = Some(control.clone());
    let exchange = Exchange::new();
    let mut sync = files
        .metadata_sync(exchange.clone())
        .with_operation_control(control);
    let mut response = exchange.queue(FileOperation::Open(OpenRequest {
        path: "missing".into(),
        revision: None,
    }));
    sync.pump().await.expect("race rejects request");
    assert_eq!(
        response.recv().await,
        Some(FileResponse::Error(FileServiceError::PermissionDenied))
    );
    holder.lock().expect("control lock").take();
    fs::remove_dir_all(root).expect("cleanup");
}
