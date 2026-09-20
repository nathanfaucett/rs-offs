use std::fmt::Debug;

use deckv::{LwwRecord, Timestamp};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{Error, FileKind, FileMeta, FileSystem, IncomingMessage, Residency, Transport};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "PeerId: Ord + Deserialize<'de>"))]
pub enum SyncMessage<PeerId> {
    MetadataRequest {
        since: Option<Timestamp>,
    },
    MetadataDelta {
        records: Vec<(String, LwwRecord<PeerId, FileMeta<PeerId>>)>,
    },
    ContentRequest {
        file_id: Uuid,
        pointer: String,
    },
    ContentResponse {
        file_id: Uuid,
        pointer: String,
        content: Vec<u8>,
    },
}

pub struct MetadataSync<'a, PeerId, T>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: Transport<PeerId>,
{
    file_system: &'a FileSystem<PeerId>,
    transport: T,
    incoming: broadcast::Receiver<IncomingMessage<PeerId>>,
    changes: broadcast::Receiver<(String, LwwRecord<PeerId, FileMeta<PeerId>>)>,
}

impl<PeerId> FileSystem<PeerId>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn metadata_sync<T>(&self, transport: T) -> MetadataSync<'_, PeerId, T>
    where
        T: Transport<PeerId>,
    {
        MetadataSync {
            incoming: transport.subscribe(),
            changes: self.metadata.subscribe(),
            file_system: self,
            transport,
        }
    }
}

impl<PeerId, T> MetadataSync<'_, PeerId, T>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: Transport<PeerId>,
{
    pub async fn announce(&self) -> Result<(), Error> {
        for peer in self.transport.peers() {
            let since = self
                .file_system
                .metadata
                .get_latest_for_node_id(peer.clone())
                .await
                .map_err(metadata_error)?;
            self.transport
                .send(peer, SyncMessage::MetadataRequest { since })
                .await
                .map_err(transport_error)?;
        }
        Ok(())
    }

    pub async fn fetch(&self, path: &str) -> Result<(), Error> {
        let entry = self.file_system.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        if self
            .file_system
            .content(
                entry.meta.file_id,
                entry.meta.pointer.as_deref().unwrap_or(""),
            )
            .await
            .is_ok()
        {
            return Ok(());
        }
        let peers = self.transport.peers();
        if peers.is_empty() {
            return Err(Error::NoPeers);
        }
        let pointer = entry.meta.pointer.ok_or(Error::ContentUnavailable)?;
        let provider = entry
            .meta
            .providers
            .iter()
            .find(|provider| peers.contains(provider))
            .cloned()
            .ok_or(Error::NoReachableProvider)?;
        self.transport
            .send(
                provider,
                SyncMessage::ContentRequest {
                    file_id: entry.meta.file_id,
                    pointer,
                },
            )
            .await
            .map_err(|_| Error::Offline)
    }

    pub async fn set_residency(&self, path: &str, residency: Residency) -> Result<(), Error> {
        if residency == Residency::Full {
            self.fetch(path).await?;
        }
        self.file_system.set_residency(path, residency).await
    }

    pub async fn pump(&mut self) -> Result<(), Error> {
        loop {
            match self.changes.try_recv() {
                Ok((path, record)) => {
                    self.transport
                        .broadcast(SyncMessage::MetadataDelta {
                            records: vec![(path, record)],
                        })
                        .await
                        .map_err(transport_error)?;
                }
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }
        loop {
            match self.incoming.try_recv() {
                Ok((peer, message)) => self.receive(peer, message).await?,
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }
        Ok(())
    }

    async fn receive(&self, peer: PeerId, message: SyncMessage<PeerId>) -> Result<(), Error> {
        match message {
            SyncMessage::MetadataRequest { since } => {
                let records = self
                    .file_system
                    .metadata
                    .export(since)
                    .await
                    .map_err(metadata_error)?
                    .into_iter()
                    .collect();
                self.transport
                    .send(peer, SyncMessage::MetadataDelta { records })
                    .await
                    .map_err(transport_error)
            }
            SyncMessage::MetadataDelta { records } => {
                self.file_system
                    .metadata
                    .merge_peer_state(records.iter().cloned().collect())
                    .await
                    .map_err(metadata_error)?;
                for (path, record) in records {
                    let provider = record.node_id.clone();
                    let Some(meta) = record.value else {
                        continue;
                    };
                    let Some(pointer) = meta.pointer.as_ref() else {
                        continue;
                    };
                    if self.file_system.residency(&path)? == Residency::Full
                        && self
                            .file_system
                            .get(&path)
                            .await?
                            .is_some_and(|current| current == meta)
                    {
                        self.transport
                            .send(
                                provider,
                                SyncMessage::ContentRequest {
                                    file_id: meta.file_id,
                                    pointer: pointer.clone(),
                                },
                            )
                            .await
                            .map_err(transport_error)?;
                    }
                }
                Ok(())
            }
            SyncMessage::ContentRequest { file_id, pointer } => {
                let content = self.file_system.content(file_id, &pointer).await?;
                self.transport
                    .send(
                        peer,
                        SyncMessage::ContentResponse {
                            file_id,
                            pointer,
                            content,
                        },
                    )
                    .await
                    .map_err(transport_error)
            }
            SyncMessage::ContentResponse {
                file_id,
                pointer,
                content,
            } => {
                self.file_system
                    .store_content(file_id, &pointer, &content)
                    .await
            }
        }
    }
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}

fn transport_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(format!("transport error: {error}"))
}
