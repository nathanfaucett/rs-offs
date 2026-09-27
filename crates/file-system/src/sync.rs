use std::{collections::HashMap, fmt::Debug};

use kv_sync::KvSnapshot;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::{
    Error, FileHandle, FileHandleId, FileOperation, FileResponse, FileServiceError,
    FileSessionService, FileSystem, IncomingMessage, OpenRequest, Residency, SessionRequest,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage<PeerId> {
    SnapshotRequest { peer: PeerId },
    SnapshotDelta { snapshots: Vec<KvSnapshot> },
}

struct ServerHandle<'a, PeerId> {
    handle: Box<dyn FileHandle<PeerId, Error = Error> + 'a>,
    path: String,
    revision: uuid::Uuid,
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
    handles: HashMap<FileHandleId, ServerHandle<'a, PeerId>>,
    last_snapshots: Vec<KvSnapshot>,
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
            handles: HashMap::new(),
            last_snapshots: Vec::new(),
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
        let mut handle = self
            .transport
            .open(
                provider,
                OpenRequest {
                    path: path.to_owned(),
                    revision: Some(revision),
                },
            )
            .await
            .map_err(|_| Error::Offline)?;
        let bytes = handle
            .read(0, 16 * 1024 * 1024)
            .await
            .map_err(|_| Error::Offline)?;
        self.file_system
            .store_content(
                entry.meta.file_id,
                revision,
                Box::pin(futures_util::stream::once(async move { Ok(bytes) })),
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
        let transaction = self
            .file_system
            .metadata
            .transaction()
            .await
            .map_err(metadata_error)?;
        transaction
            .export_snapshots()
            .await
            .map(|snapshots| {
                snapshots
                    .into_iter()
                    .map(|(key, payload)| KvSnapshot { key, payload })
                    .collect()
            })
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
                let snapshots = self.export_snapshots().await?;
                self.transport
                    .send(peer, SyncMessage::SnapshotDelta { snapshots })
                    .await
                    .map_err(transport_error)
            }
            SyncMessage::SnapshotDelta { snapshots } => {
                let mut transaction = self
                    .file_system
                    .metadata
                    .transaction()
                    .await
                    .map_err(metadata_error)?;
                for snapshot in snapshots {
                    transaction
                        .import_snapshot((snapshot.key, snapshot.payload))
                        .await
                        .map_err(metadata_error)?;
                }
                transaction.commit().await.map_err(metadata_error)?;
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
                        let path = request.path.clone();
                        let id = self.transport.allocate_handle(&peer, &request);
                        let handle = self.file_system.open_handle(request).await?;
                        self.handles.insert(
                            id.clone(),
                            ServerHandle {
                                handle: Box::new(handle),
                                path,
                                revision,
                            },
                        );
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
                    let handle = self.handles.get_mut(&handle).ok_or(Error::NotFound)?;
                    let bytes = handle.handle.read(offset, length).await?;
                    Ok(vec![FileResponse::ReadChunk(bytes), FileResponse::ReadEnd])
                }
                FileOperation::Write {
                    handle,
                    expected_revision,
                    offset,
                    data,
                } => {
                    let state = self.handles.get_mut(&handle).ok_or(Error::NotFound)?;
                    if state.revision != expected_revision {
                        Err(Error::StaleRevision)
                    } else {
                        let count = state.handle.write(offset, data).await?;
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
                    let state = self.handles.get_mut(&handle).ok_or(Error::NotFound)?;
                    Ok(vec![FileResponse::Page(
                        state.handle.scan(cursor, limit).await?,
                    )])
                }
                FileOperation::Close { handle } => {
                    let state = self.handles.remove(&handle).ok_or(Error::NotFound)?;
                    state.handle.close().await?;
                    Ok(vec![FileResponse::Closed])
                }
            }
        }
        .await;
        let responses = result
            .map_err(service_error)
            .unwrap_or_else(|error| vec![FileResponse::Error(error)]);
        for item in responses {
            let _ = response.send(item).await;
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
        Error::InvalidScanCursor => FileServiceError::InvalidScanCursor,
        Error::InvalidScanLimit => FileServiceError::InvalidScanLimit,
        Error::StaleRevision => FileServiceError::StaleRevision,
        Error::PassthroughWrite => FileServiceError::PassthroughWrite,
        Error::ContentUnavailable => FileServiceError::ContentUnavailable,
        _ => FileServiceError::Io,
    }
}
