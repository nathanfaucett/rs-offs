use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use file_system::{
    Error, FileSessionService, FileSystem, IncomingMessage, MemoryNetwork, MemoryTransport,
    MemoryTransportError, OpenRequest, OperationControl, RemoteOperation, Residency,
    SessionRequest, SyncMessage, Transport,
};
use std::{future::Future, pin::Pin, sync::atomic::AtomicU8, task::Poll};
use tokio::sync::broadcast;
use uuid::Uuid;

fn take_messages(
    receiver: &mut broadcast::Receiver<(u8, SyncMessage<u8>)>,
) -> Vec<SyncMessage<u8>> {
    let mut messages = Vec::new();
    while let Ok((_, message)) = receiver.try_recv() {
        messages.push(message);
    }
    messages
}

#[tokio::test]
async fn initial_and_changed_snapshots_are_authorized_per_destination() {
    let root = std::env::temp_dir().join(format!("offs-outbound-{}", Uuid::now_v7()));
    let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
    files
        .set_residency("", Residency::Full)
        .await
        .expect("full residency");
    files
        .write("initial", b"private metadata")
        .await
        .expect("initial write");
    let network = MemoryNetwork::new(32);
    let source = network.transport(1);
    let denied = network.transport(2);
    let allowed = network.transport(3);
    let mut denied_messages = denied.subscribe();
    let mut allowed_messages = allowed.subscribe();
    let permitted = Arc::new(AtomicBool::new(true));
    let policy = Arc::clone(&permitted);
    let control = OperationControl::new(move |peer, operation| {
        let policy = Arc::clone(&policy);
        Box::pin(async move {
            !matches!(operation, RemoteOperation::ExportSnapshots)
                || (peer == 3 && policy.load(Ordering::SeqCst))
        })
    });
    let mut sync = files.metadata_sync(source).with_operation_control(control);
    sync.announce().await.expect("initial exchange");
    assert!(
        take_messages(&mut denied_messages).is_empty(),
        "denied peer must receive no initial exchange"
    );
    let initial = take_messages(&mut allowed_messages);
    assert!(
        matches!(initial.as_slice(), [SyncMessage::SnapshotRequest { peer: 3 }, SyncMessage::SnapshotDelta { snapshots }] if snapshots.len() == 1)
    );
    files
        .write("changed", b"new metadata")
        .await
        .expect("local change");
    sync.pump().await.expect("changed publication");
    assert!(
        take_messages(&mut denied_messages).is_empty(),
        "denied peer must receive no changes"
    );
    assert!(
        matches!(take_messages(&mut allowed_messages).as_slice(), [SyncMessage::SnapshotDelta { snapshots }] if snapshots.len() == 2)
    );
    denied
        .send(1, SyncMessage::SnapshotRequest { peer: 3 })
        .await
        .expect("denied peer requests snapshots with another claimed peer");
    sync.pump()
        .await
        .expect("requested export denied for actual peer");
    assert!(take_messages(&mut denied_messages).is_empty());
    assert!(take_messages(&mut allowed_messages).is_empty());
    permitted.store(false, Ordering::SeqCst);
    files.delete("initial").await.expect("local tombstone");
    sync.pump().await.expect("revoked publication discarded");
    sync.announce()
        .await
        .expect("revoked initial exchange discarded");
    assert!(take_messages(&mut denied_messages).is_empty());
    assert!(
        take_messages(&mut allowed_messages).is_empty(),
        "later denial applies on established exchange"
    );
    permitted.store(true, Ordering::SeqCst);
    sync.announce().await.expect("authorized retry");
    assert!(
        matches!(take_messages(&mut allowed_messages).as_slice(), [SyncMessage::SnapshotRequest { peer: 3 }, SyncMessage::SnapshotDelta { snapshots }] if snapshots.len() == 2)
    );
    assert!(take_messages(&mut denied_messages).is_empty());
    fs::remove_dir_all(root).expect("cleanup");
}

#[derive(Clone)]
struct PendingExchange {
    inner: MemoryTransport<u8>,
    block: Arc<AtomicU8>,
    started: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

struct PendingSend(Arc<AtomicBool>);
impl Drop for PendingSend {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Transport<u8> for PendingExchange {
    type Error = MemoryTransportError;
    fn peers(&self) -> Vec<u8> {
        self.inner.peers()
    }
    async fn send(&self, peer: u8, message: SyncMessage<u8>) -> Result<(), Self::Error> {
        let blocked = match self.block.load(Ordering::SeqCst) {
            1 => matches!(message, SyncMessage::SnapshotRequest { .. }),
            2 => matches!(message, SyncMessage::SnapshotDelta { .. }),
            _ => false,
        };
        if blocked {
            let _pending = PendingSend(Arc::clone(&self.dropped));
            self.started.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        self.inner.send(peer, message).await
    }
    async fn broadcast(&self, _: SyncMessage<u8>) -> Result<(), Self::Error> {
        panic!("metadata export must authorize each destination, not broadcast");
    }
    fn subscribe(&self) -> broadcast::Receiver<IncomingMessage<u8>> {
        self.inner.subscribe()
    }
}

impl FileSessionService<u8> for PendingExchange {
    type Handle = <MemoryTransport<u8> as FileSessionService<u8>>::Handle;
    async fn open(&self, peer: u8, request: OpenRequest) -> Result<Self::Handle, Self::Error> {
        self.inner.open(peer, request).await
    }
    fn subscribe_file_sessions(&self) -> broadcast::Receiver<SessionRequest<u8>> {
        self.inner.subscribe_file_sessions()
    }
}

#[derive(Clone, Copy, Debug)]
enum OutboundPath {
    AnnounceRequest,
    AnnounceDelta,
    Changes,
    Reply,
    RunStartup,
}

#[tokio::test]
async fn cancellation_releases_polled_pending_outbound_sends() {
    for path in [
        OutboundPath::AnnounceRequest,
        OutboundPath::AnnounceDelta,
        OutboundPath::Changes,
        OutboundPath::Reply,
        OutboundPath::RunStartup,
    ] {
        let root = std::env::temp_dir().join(format!("offs-outbound-{}", Uuid::now_v7()));
        let files = FileSystem::open(&root, 1_u8).expect("filesystem opens");
        files
            .set_residency("", Residency::Full)
            .await
            .expect("full residency");
        let earlier = files
            .write("retained", b"earlier committed content")
            .await
            .expect("earlier commit");
        let network = MemoryNetwork::new(32);
        let exchange = PendingExchange {
            inner: network.transport(1),
            block: Arc::new(AtomicU8::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            dropped: Arc::new(AtomicBool::new(false)),
        };
        let peer = network.transport(2);
        let _messages = peer.subscribe();
        let control = OperationControl::default();
        let mut sync = files
            .metadata_sync(exchange.clone())
            .with_operation_control(control.clone());
        sync.pump().await.expect("baseline publication completes");
        match path {
            OutboundPath::AnnounceRequest | OutboundPath::RunStartup => {
                exchange.block.store(1, Ordering::SeqCst)
            }
            _ => exchange.block.store(2, Ordering::SeqCst),
        }
        if matches!(path, OutboundPath::Changes) {
            files
                .write("later", b"later local change")
                .await
                .expect("change commits");
        }
        if matches!(path, OutboundPath::Reply) {
            peer.send(1, SyncMessage::SnapshotRequest { peer: 2 })
                .await
                .expect("request queues");
        }
        let mut pending: Pin<Box<dyn Future<Output = Result<(), Error>> + '_>> = match path {
            OutboundPath::AnnounceRequest | OutboundPath::AnnounceDelta => {
                Box::pin(sync.announce())
            }
            OutboundPath::Changes | OutboundPath::Reply => Box::pin(sync.pump()),
            OutboundPath::RunStartup => Box::pin(sync.run()),
        };
        std::future::poll_fn(|cx| {
            assert!(
                pending.as_mut().poll(cx).is_pending(),
                "{path:?}: send must be pending"
            );
            Poll::Ready(())
        })
        .await;
        assert!(
            exchange.started.load(Ordering::SeqCst),
            "{path:?}: send was actually polled"
        );
        assert!(!exchange.dropped.load(Ordering::SeqCst));
        control.cancel();
        let result = std::future::poll_fn(|cx| match pending.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                panic!("{path:?}: cancellation must release the pending outbound send")
            }
        })
        .await;
        assert!(matches!(result, Err(Error::Cancelled)), "{path:?}");
        assert!(
            exchange.dropped.load(Ordering::SeqCst),
            "{path:?}: pending send resources released"
        );
        drop(pending);
        assert_eq!(
            files
                .read("retained")
                .await
                .expect("earlier commit retained"),
            b"earlier committed content"
        );
        assert_eq!(
            files
                .revision("retained")
                .await
                .expect("earlier revision retained"),
            earlier.meta.revision
        );
        files
            .append("retained", b" offline")
            .await
            .expect("local writes still available");
        fs::remove_dir_all(root).expect("cleanup");
    }
}
