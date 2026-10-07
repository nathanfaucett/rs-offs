use std::{collections::BTreeMap, fmt::Debug};

use ofdb_kv::KvSnapshot;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::{
    Error, FileHandle, FileHandleId, FileOperation, FileResponse, FileServiceError,
    FileSessionService, FileSystem, IncomingMessage, MAX_FILE_READ_LENGTH, OpenRequest,
    OperationControl, RemoteOperation, Residency, SessionRequest,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage<PeerId> {
    SnapshotRequest { peer: PeerId },
    SnapshotDelta { snapshots: Vec<KvSnapshot> },
}

pub struct MetadataSync<'a, PeerId, T>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: FileSessionService<PeerId> + Sync + Clone + Send + 'static,
{
    file_system: &'a FileSystem<PeerId>,
    transport: T,
    incoming: broadcast::Receiver<IncomingMessage<PeerId>>,
    sessions: broadcast::Receiver<SessionRequest<PeerId>>,
    changes: broadcast::Receiver<()>,
    handles: BTreeMap<(PeerId, FileHandleId), crate::LocalFileHandle<'a, PeerId>>,
    last_snapshots: Vec<KvSnapshot>,
    control: OperationControl<PeerId>,
}

impl<PeerId> FileSystem<PeerId>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn metadata_sync<T>(&self, transport: T) -> MetadataSync<'_, PeerId, T>
    where
        T: FileSessionService<PeerId> + Sync + Clone + Send + 'static,
    {
        MetadataSync {
            incoming: transport.subscribe(),
            sessions: transport.subscribe_file_sessions(),
            changes: self.changes.subscribe(),
            file_system: self,
            transport,
            handles: BTreeMap::new(),
            last_snapshots: Vec::new(),
            control: OperationControl::default(),
        }
    }

    pub async fn sync_peer<T>(&self, transport: T) -> Result<(), Error>
    where
        T: FileSessionService<PeerId> + Sync + Clone + Send + 'static,
    {
        self.metadata_sync(transport).run().await
    }
}

impl<PeerId, T> MetadataSync<'_, PeerId, T>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: FileSessionService<PeerId> + Sync + Clone + Send + 'static,
{
    pub fn with_operation_control(mut self, control: OperationControl<PeerId>) -> Self {
        self.control = control;
        self
    }

    pub async fn announce(&self) -> Result<(), Error> {
        let snapshots = self.export_snapshots().await?;
        for peer in self.transport.peers() {
            self.transport
                .send(
                    peer.clone(),
                    SyncMessage::SnapshotRequest { peer: peer.clone() },
                )
                .await
                .map_err(transport_error)?;
            self.transport
                .send(
                    peer,
                    SyncMessage::SnapshotDelta {
                        snapshots: snapshots.clone(),
                    },
                )
                .await
                .map_err(transport_error)?;
        }
        Ok(())
    }

    pub async fn fetch(&self, path: &str) -> Result<(), Error> {
        let entry = self.file_system.entry(path).await?;
        if entry.meta.kind == crate::FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        let revision = self.file_system.revision(path).await?;
        if self
            .file_system
            .content(entry.meta.file_id, revision)
            .await
            .is_ok()
        {
            return Ok(());
        }
        let provider = self.provider(&entry.meta).await?;
        let handle = self
            .control
            .wait(self.transport.open(
                provider.clone(),
                OpenRequest {
                    path: path.to_owned(),
                    revision: Some(revision),
                },
            ))
            .await?
            .map_err(|_| Error::Offline)?;
        let content =
            futures_util::stream::try_unfold((handle, 0_u64), |(mut handle, offset)| async move {
                let bytes = handle
                    .read(offset, MAX_FILE_READ_LENGTH)
                    .await
                    .map_err(|_| Error::Offline)?;
                if bytes.is_empty() {
                    Box::new(handle).close().await.map_err(|_| Error::Offline)?;
                    return Ok(None);
                }
                let next_offset = offset.checked_add(bytes.len() as u64).ok_or_else(|| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "file offset overflow during sync",
                    ))
                })?;
                Ok(Some((bytes, (handle, next_offset))))
            });
        self.file_system
            .store_content(
                entry.meta.file_id,
                revision,
                Box::pin(content),
                &self.control,
                provider,
            )
            .await
    }

    pub async fn set_residency(&self, path: &str, residency: Residency) -> Result<(), Error> {
        if residency == Residency::Full {
            self.fetch(path).await?;
        }
        self.file_system.set_residency(path, residency).await
    }

    pub async fn run(mut self) -> Result<(), Error> {
        self.announce().await?;
        self.last_snapshots = self.export_snapshots().await?;
        loop {
            tokio::select! {
                biased;
                () = self.control.cancelled() => return Err(Error::Cancelled),
                result = self.changes.recv() => match result {
                    Ok(()) => self.publish_changes().await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => self.publish_changes().await?,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
                result = self.incoming.recv() => match result {
                    Ok((peer, message)) => self.receive(peer, message).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => self.announce().await?,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
                result = self.sessions.recv() => match result {
                    Ok((peer, operation, response)) => self.receive_session(peer, operation, response).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => self.announce().await?,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
    }

    pub async fn pump(&mut self) -> Result<(), Error> {
        let result = self.pump_inner().await;
        if self.control.is_cancelled() {
            self.handles.clear();
            while self.sessions.try_recv().is_ok() {}
            while self.incoming.try_recv().is_ok() {}
        }
        result
    }

    async fn pump_inner(&mut self) -> Result<(), Error> {
        if self.control.is_cancelled() {
            self.handles.clear();
            while self.sessions.try_recv().is_ok() {}
            while self.incoming.try_recv().is_ok() {}
            return Err(Error::Cancelled);
        }
        while self.changes.try_recv().is_ok() {}
        self.publish_changes().await?;
        loop {
            match self.incoming.try_recv() {
                Ok((peer, message)) => self.receive(peer, message).await?,
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    self.announce().await?;
                    continue;
                }
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => break,
            }
        }
        while let Ok((peer, operation, response)) = self.sessions.try_recv() {
            self.receive_session(peer, operation, response).await?;
        }
        Ok(())
    }

    async fn export_snapshots(&self) -> Result<Vec<KvSnapshot>, Error> {
        self.file_system
            .database
            .export_snapshots()
            .await
            .map_err(metadata_error)
    }

    async fn publish_changes(&mut self) -> Result<(), Error> {
        let snapshots = self.export_snapshots().await?;
        if snapshots != self.last_snapshots {
            self.transport
                .broadcast(SyncMessage::SnapshotDelta {
                    snapshots: snapshots.clone(),
                })
                .await
                .map_err(transport_error)?;
            self.last_snapshots = snapshots;
        }
        Ok(())
    }

    async fn provider(&self, meta: &crate::FileMeta<PeerId>) -> Result<PeerId, Error> {
        let peers = self.transport.peers();
        if peers.is_empty() {
            return Err(Error::NoPeers);
        }
        meta.providers
            .iter()
            .find(|provider| peers.contains(provider))
            .cloned()
            .ok_or(Error::NoReachableProvider)
    }

    async fn receive(&mut self, peer: PeerId, message: SyncMessage<PeerId>) -> Result<(), Error> {
        match message {
            SyncMessage::SnapshotRequest { peer: _ } => {
                match self
                    .control
                    .admit(peer.clone(), RemoteOperation::ExportSnapshots)
                    .await
                {
                    Ok(()) => {}
                    Err(Error::PermissionDenied) => return Ok(()),
                    Err(error) => return Err(error),
                }
                let snapshots = self.export_snapshots().await?;
                self.transport
                    .send(peer, SyncMessage::SnapshotDelta { snapshots })
                    .await
                    .map_err(transport_error)
            }
            SyncMessage::SnapshotDelta { snapshots } => {
                let _execution = self.control.wait(self.file_system.execution.lock()).await?;
                match self
                    .control
                    .admit(peer, RemoteOperation::ImportSnapshots)
                    .await
                {
                    Ok(()) => {}
                    Err(Error::PermissionDenied) => return Ok(()),
                    Err(error) => return Err(error),
                }
                self.file_system
                    .database
                    .import_snapshots(snapshots)
                    .await
                    .map_err(metadata_error)?;
                self.last_snapshots = self.export_snapshots().await?;
                Ok(())
            }
        }
    }

    async fn receive_session(
        &mut self,
        peer: PeerId,
        operation: FileOperation,
        response: crate::SessionResponse<PeerId>,
    ) -> Result<(), Error> {
        let result: Result<Vec<FileResponse<PeerId>>, Error> = async {
            let execution = self.control.wait(self.file_system.execution.lock()).await?;
            let path = match &operation {
                FileOperation::Open(request) => Some(request.path.clone()),
                FileOperation::Read { handle, .. }
                | FileOperation::Write { handle, .. }
                | FileOperation::Scan { handle, .. }
                | FileOperation::Close { handle } => self
                    .handles
                    .get(&(peer.clone(), handle.clone()))
                    .map(|state| state.path.clone()),
            };
            self.control
                .admit(
                    peer.clone(),
                    RemoteOperation::Session {
                        operation: operation.clone(),
                        path,
                    },
                )
                .await?;
            let _execution = if matches!(operation, FileOperation::Write { .. }) {
                Some(execution)
            } else {
                drop(execution);
                None
            };
            match operation {
                FileOperation::Open(request) => {
                    let entry = self.file_system.entry(&request.path).await?;
                    let revision = self.file_system.revision(&request.path).await?;
                    if request
                        .revision
                        .is_some_and(|expected| expected != revision)
                    {
                        Err(Error::StaleRevision)
                    } else {
                        let id = self.transport.allocate_handle(&peer, &request);
                        let handle = self.file_system.open_handle(request).await?;
                        self.handles.insert((peer.clone(), id.clone()), handle);
                        Ok(vec![FileResponse::Opened {
                            handle: id,
                            entry,
                            revision,
                        }])
                    }
                }
                FileOperation::Read {
                    handle,
                    offset,
                    length,
                } => {
                    if length > MAX_FILE_READ_LENGTH {
                        return Err(Error::InvalidReadLength);
                    }
                    let handle = self
                        .handles
                        .get_mut(&(peer.clone(), handle))
                        .ok_or(Error::NotFound)?;
                    let bytes = handle.read(offset, length).await?;
                    Ok(vec![FileResponse::ReadChunk(bytes), FileResponse::ReadEnd])
                }
                FileOperation::Write {
                    handle,
                    expected_revision,
                    offset,
                    data,
                } => {
                    let state = self
                        .handles
                        .get_mut(&(peer.clone(), handle))
                        .ok_or(Error::NotFound)?;
                    if state.revision != expected_revision {
                        Err(Error::StaleRevision)
                    } else {
                        let count = self
                            .file_system
                            .write_at_inner(&state.path, expected_revision, offset, data)
                            .await?;
                        state.revision = self.file_system.revision(&state.path).await?;
                        Ok(vec![FileResponse::Written {
                            count,
                            revision: state.revision,
                        }])
                    }
                }
                FileOperation::Scan {
                    handle,
                    cursor,
                    limit,
                } => {
                    let state = self
                        .handles
                        .get_mut(&(peer.clone(), handle))
                        .ok_or(Error::NotFound)?;
                    Ok(vec![FileResponse::Page(state.scan(cursor, limit).await?)])
                }
                FileOperation::Close { handle } => {
                    let state = self
                        .handles
                        .remove(&(peer, handle))
                        .ok_or(Error::NotFound)?;
                    state.close().await?;
                    Ok(vec![FileResponse::Closed])
                }
            }
        }
        .await;
        if self.control.is_cancelled() {
            self.handles.clear();
        }
        let responses = result
            .map_err(service_error)
            .unwrap_or_else(|error| vec![FileResponse::Error(error)]);
        for item in responses {
            if let Err(tokio::sync::mpsc::error::TrySendError::Full(item)) = response.try_send(item)
            {
                let _ = self.control.wait(response.send(item)).await;
            }
        }
        if self.control.is_cancelled() {
            self.handles.clear();
        }
        Ok(())
    }
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}

fn transport_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(format!("transport error: {error}"))
}

fn service_error(error: Error) -> FileServiceError {
    match error {
        Error::NotFound => FileServiceError::NotFound,
        Error::NotDirectory => FileServiceError::NotDirectory,
        Error::IsDirectory => FileServiceError::IsDirectory,
        Error::InvalidReadLength => FileServiceError::InvalidReadLength,
        Error::InvalidScanCursor => FileServiceError::InvalidScanCursor,
        Error::InvalidScanLimit => FileServiceError::InvalidScanLimit,
        Error::StaleRevision => FileServiceError::StaleRevision,
        Error::PassthroughWrite => FileServiceError::PassthroughWrite,
        Error::ContentUnavailable => FileServiceError::ContentUnavailable,
        Error::PermissionDenied | Error::Cancelled => FileServiceError::PermissionDenied,
        _ => FileServiceError::Io,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::*;
    use crate::{FileServiceError, MemoryNetwork};

    async fn request(
        sync: &mut MetadataSync<'_, u8, crate::MemoryTransport<u8>>,
        peer: u8,
        operation: FileOperation,
    ) -> Vec<FileResponse<u8>> {
        let (sender, mut receiver) = mpsc::channel(4);
        sync.receive_session(peer, operation, sender)
            .await
            .expect("session request should complete");
        let mut responses = Vec::new();
        while let Some(response) = receiver.recv().await {
            responses.push(response);
        }
        responses
    }

    #[tokio::test]
    async fn rejects_oversized_file_read_before_allocating() {
        let root = std::env::temp_dir().join(format!("file-system-{}", uuid::Uuid::now_v7()));
        let file_system = FileSystem::open(&root, 1_u8).expect("file system should open");
        file_system
            .set_residency("", Residency::Full)
            .await
            .expect("full residency should be set");
        file_system
            .write("file.txt", b"ok")
            .await
            .expect("file should be written");
        let mut sync = file_system.metadata_sync(MemoryNetwork::new(16).transport(1));
        let opened = request(
            &mut sync,
            2,
            FileOperation::Open(OpenRequest {
                path: "file.txt".to_owned(),
                revision: None,
            }),
        )
        .await;
        let [FileResponse::Opened { handle, .. }] = opened.as_slice() else {
            panic!("file should be opened");
        };
        let handle = handle.clone();
        assert_eq!(
            request(
                &mut sync,
                2,
                FileOperation::Read {
                    handle: handle.clone(),
                    offset: 0,
                    length: MAX_FILE_READ_LENGTH + 1,
                },
            )
            .await,
            [FileResponse::Error(FileServiceError::InvalidReadLength)]
        );
        assert_eq!(
            request(
                &mut sync,
                2,
                FileOperation::Read {
                    handle,
                    offset: 0,
                    length: 2,
                },
            )
            .await,
            [
                FileResponse::ReadChunk(Bytes::from_static(b"ok")),
                FileResponse::ReadEnd
            ]
        );
        std::fs::remove_dir_all(root).expect("test directory should be removed");
    }

    #[tokio::test]
    async fn file_handles_are_scoped_to_requesting_peer() {
        let root = std::env::temp_dir().join(format!("file-system-{}", uuid::Uuid::now_v7()));
        let file_system = FileSystem::open(&root, 1_u8).expect("file system should open");
        file_system
            .set_residency("", Residency::Full)
            .await
            .expect("full residency should be set");
        file_system
            .write("file.txt", b"original")
            .await
            .expect("file should be written");
        file_system
            .create_dir("dir")
            .await
            .expect("directory should be created");
        let network = MemoryNetwork::new(16);
        let mut sync = file_system.metadata_sync(network.transport(1));

        for path in ["file.txt", "dir"] {
            let opened = request(
                &mut sync,
                2,
                FileOperation::Open(OpenRequest {
                    path: path.to_owned(),
                    revision: None,
                }),
            )
            .await;
            let [
                FileResponse::Opened {
                    handle, revision, ..
                },
            ] = opened.as_slice()
            else {
                panic!("owner should open {path}");
            };
            let handle = handle.clone();
            let revision = *revision;

            let attempts = if path == "file.txt" {
                vec![
                    FileOperation::Read {
                        handle: handle.clone(),
                        offset: 0,
                        length: 8,
                    },
                    FileOperation::Write {
                        handle: handle.clone(),
                        expected_revision: revision,
                        offset: 0,
                        data: Bytes::from_static(b"intruder"),
                    },
                ]
            } else {
                vec![FileOperation::Scan {
                    handle: handle.clone(),
                    cursor: None,
                    limit: 1,
                }]
            };
            for operation in attempts.into_iter().chain([FileOperation::Close {
                handle: handle.clone(),
            }]) {
                assert_eq!(
                    request(&mut sync, 3, operation).await,
                    [FileResponse::Error(FileServiceError::NotFound)]
                );
            }
            let own_operation = if path == "file.txt" {
                FileOperation::Read {
                    handle: handle.clone(),
                    offset: 0,
                    length: 8,
                }
            } else {
                FileOperation::Scan {
                    handle: handle.clone(),
                    cursor: None,
                    limit: 1,
                }
            };
            assert!(!matches!(
                request(&mut sync, 2, own_operation).await.as_slice(),
                [FileResponse::Error(_)]
            ));

            let other = request(
                &mut sync,
                3,
                FileOperation::Open(OpenRequest {
                    path: path.to_owned(),
                    revision: None,
                }),
            )
            .await;
            let [
                FileResponse::Opened {
                    handle: other_handle,
                    ..
                },
            ] = other.as_slice()
            else {
                panic!("second peer should open {path}");
            };
            assert_eq!(&handle, other_handle);
            assert_eq!(
                request(
                    &mut sync,
                    3,
                    FileOperation::Close {
                        handle: handle.clone()
                    }
                )
                .await,
                [FileResponse::Closed]
            );
            assert_eq!(
                request(&mut sync, 2, FileOperation::Close { handle }).await,
                [FileResponse::Closed]
            );
        }
        assert_eq!(
            file_system
                .read("file.txt")
                .await
                .expect("file should remain readable"),
            b"original"
        );
        std::fs::remove_dir_all(root).expect("test directory should be removed");
    }
}
