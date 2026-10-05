#![cfg(feature = "iroh")]

use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use file_system::{
    FileHandle, FileSessionService, FileSystem, IrohFileTransport, IrohResourceDescriptor,
    OpenRequest, Residency,
};
use iroh::{Endpoint, address_lookup::MemoryLookup, endpoint::presets};
use uuid::Uuid;

fn root() -> std::path::PathBuf {
    env::temp_dir().join(format!("file-system-iroh-{}", Uuid::now_v7()))
}

#[tokio::test]
async fn cancelled_sync_closes_its_peer_stream_and_releases_local_filesystem() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let lookup = MemoryLookup::new();
        let responder = Endpoint::builder(presets::Minimal)
            .alpns(vec![b"filesystem-cancel-test".to_vec()])
            .address_lookup(lookup.clone())
            .bind()
            .await
            .expect("bind responder");
        lookup.add_endpoint_info(responder.addr());
        let initiator = Endpoint::builder(presets::Minimal)
            .alpns(vec![b"filesystem-cancel-test".to_vec()])
            .address_lookup(lookup)
            .bind()
            .await
            .expect("bind initiator");
        let (connection, incoming) = tokio::join!(
            initiator.connect(responder.id(), b"filesystem-cancel-test"),
            async {
                responder
                    .accept()
                    .await
                    .expect("incoming connection")
                    .accept()
                    .expect("accept connection")
                    .await
            }
        );
        let connection = connection.expect("connect");
        let incoming = incoming.expect("accept connection");
        let resource = IrohResourceDescriptor {
            owner_subject: "cancel-owner".to_owned(),
            application_id: "cancel-app".to_owned(),
            filesystem_id: "cancel-files".to_owned(),
        };
        let initiator_transport =
            IrohFileTransport::open_authorized(&connection, resource.clone(), |_| async { true })
                .await
                .expect("open authorized stream");
        let (send, recv) = incoming.accept_bi().await.expect("accept stream");
        let (responder_transport, _, _) =
            IrohFileTransport::accept_authorized(&incoming, send, recv, |_| async { true })
                .await
                .expect("accept authorized stream");

        let initiator_root = root();
        let responder_root = root();
        let initiator_fs = Arc::new(
            FileSystem::open(&initiator_root, initiator.id()).expect("open initiator filesystem"),
        );
        let responder_fs = Arc::new(
            FileSystem::open(&responder_root, responder.id()).expect("open responder filesystem"),
        );
        initiator_fs
            .set_residency("", Residency::Full)
            .await
            .expect("initiator full residency");
        responder_fs
            .set_residency("", Residency::Full)
            .await
            .expect("responder full residency");
        initiator_fs
            .write("before-cancel.txt", b"sync is active")
            .await
            .expect("write before sync");

        let initiator_sync = {
            let filesystem = Arc::clone(&initiator_fs);
            tokio::spawn(async move { filesystem.sync_peer(initiator_transport).await })
        };
        let responder_sync = {
            let filesystem = Arc::clone(&responder_fs);
            tokio::spawn(async move { filesystem.sync_peer(responder_transport).await })
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if responder_fs.entry("before-cancel.txt").await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("sync becomes active before cancellation");
        initiator_sync.abort();
        assert!(
            initiator_sync
                .await
                .expect_err("sync task is aborted")
                .is_cancelled()
        );
        tokio::time::timeout(Duration::from_secs(3), responder_sync)
            .await
            .expect("peer sync exits after stream cancellation")
            .expect("peer sync task joins")
            .expect("peer sync exits without an error");

        initiator_fs
            .write("still-usable.txt", b"ok")
            .await
            .expect("canceled sync releases local filesystem");
        assert_eq!(
            initiator_fs
                .read("still-usable.txt")
                .await
                .expect("read local file"),
            b"ok"
        );
        initiator.close().await;
        responder.close().await;
        std::fs::remove_dir_all(initiator_root).expect("remove initiator test data");
        std::fs::remove_dir_all(responder_root).expect("remove responder test data");
    })
    .await
    .expect("sync cancellation cleanup test timed out");
}

#[tokio::test]
async fn syncs_metadata_and_remote_file_sessions_over_one_stream() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let lookup = MemoryLookup::new();
        let b = Endpoint::builder(presets::Minimal)
            .alpns(vec![b"filesystem-test".to_vec()])
            .address_lookup(lookup.clone())
            .bind()
            .await
            .expect("bind responder");
        lookup.add_endpoint_info(b.addr());
        let a = Endpoint::builder(presets::Minimal)
            .alpns(vec![b"filesystem-test".to_vec()])
            .address_lookup(lookup)
            .bind()
            .await
            .expect("bind initiator");

        let (connection, incoming) = tokio::join!(a.connect(b.id(), b"filesystem-test"), async {
            b.accept()
                .await
                .expect("incoming connection")
                .accept()
                .expect("accept")
                .await
        });

        let connection = connection.expect("connect");
        let incoming = incoming.expect("accept connection");
        let resource = IrohResourceDescriptor {
            owner_subject: "test-owner".to_owned(),
            application_id: "test-app".to_owned(),
            filesystem_id: "shared-files".to_owned(),
        };
        let initiator_authorization_checks = Arc::new(AtomicUsize::new(0));
        let initiator_checks = Arc::clone(&initiator_authorization_checks);
        let left_transport =
            IrohFileTransport::open_authorized(&connection, resource.clone(), move |_| {
                let checks = Arc::clone(&initiator_checks);
                async move {
                    checks.fetch_add(1, Ordering::SeqCst);
                    true
                }
            })
            .await
            .expect("open authorized stream");
        let (send, recv) = incoming.accept_bi().await.expect("accept stream");
        let authorization_checks = Arc::new(AtomicUsize::new(0));
        let allowed = Arc::new(AtomicBool::new(true));
        let checks = Arc::clone(&authorization_checks);
        let authorized = Arc::clone(&allowed);
        let (right_transport, accepted_resource, deleted) =
            IrohFileTransport::accept_authorized(&incoming, send, recv, move |_| {
                let checks = Arc::clone(&checks);
                let authorized = Arc::clone(&authorized);
                async move {
                    checks.fetch_add(1, Ordering::SeqCst);
                    authorized.load(Ordering::SeqCst)
                }
            })
            .await
            .expect("validate accepted stream");
        assert_eq!(accepted_resource, resource);
        assert!(!deleted);

        let left = Arc::new(FileSystem::open(root(), a.id()).expect("open left filesystem"));
        let right = Arc::new(FileSystem::open(root(), b.id()).expect("open right filesystem"));
        left.set_residency("", Residency::Full)
            .await
            .expect("left full residency");
        right
            .set_residency("", Residency::Full)
            .await
            .expect("right full residency");

        left.write("shared.txt", b"before")
            .await
            .expect("write initial content");

        let left_sync = {
            let left = Arc::clone(&left);
            tokio::spawn(async move { left.sync_peer(left_transport).await })
        };

        let right_sync = {
            let right = Arc::clone(&right);
            let sync_transport = right_transport.clone();
            tokio::spawn(async move { right.sync_peer(sync_transport).await })
        };

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if right.entry("shared.txt").await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("metadata entry replicated");
        assert!(
            authorization_checks.load(Ordering::SeqCst) > 1,
            "responder authorization is rechecked after the stream handshake"
        );
        assert!(
            initiator_authorization_checks.load(Ordering::SeqCst) > 1,
            "initiator authorization is rechecked during the stream"
        );

        let handle = right_transport
            .open(
                a.id(),
                OpenRequest {
                    path: "shared.txt".to_owned(),
                    revision: None,
                },
            )
            .await
            .expect("open remote file");
        let mut handle = Box::new(handle);
        assert_eq!(
            handle.read(0, 32).await.expect("read remote file"),
            Bytes::from_static(b"before")
        );
        handle
            .write(0, Bytes::from_static(b"after!"))
            .await
            .expect("write remote file");
        assert_eq!(
            left.read("shared.txt")
                .await
                .expect("read updated local file"),
            b"after!"
        );

        let checks_before_revocation = authorization_checks.load(Ordering::SeqCst);
        allowed.store(false, Ordering::SeqCst);
        left.write("revoked.txt", b"must not replicate")
            .await
            .expect("write after revocation");
        tokio::time::timeout(Duration::from_secs(5), async {
            while authorization_checks.load(Ordering::SeqCst) == checks_before_revocation {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("revoked frame is checked");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(right.entry("revoked.txt").await.is_err());

        tokio::time::timeout(Duration::from_secs(5), async {
            assert!(
                left_sync.await.expect("initiator sync task").is_ok(),
                "initiator sync ends cleanly when its stream closes"
            );
            assert!(
                right_sync.await.expect("responder sync task").is_ok(),
                "responder sync ends cleanly when its stream closes"
            );
        })
        .await
        .expect("revoked sync tasks close naturally");
    })
    .await
    .expect("Iroh file transport test timed out");
}
