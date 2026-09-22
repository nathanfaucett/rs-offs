use std::fmt::Debug;

use deckv::{LwwRecord, Timestamp};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    ByteStream, Error, FileKind, FileMeta, FileService, FileSystem, IncomingMessage, Residency,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileRequest {
    pub file_id: Uuid,
    pub revision: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "PeerId: Ord + Deserialize<'de>"))]
pub enum SyncMessage<PeerId> {
    MetadataRequest {
        since: Option<Timestamp>,
    },
    MetadataDelta {
        records: Vec<(String, LwwRecord<PeerId, FileMeta<PeerId>>)>,
    },
}

pub struct MetadataSync<'a, PeerId, T>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: FileService<PeerId>,
{
    file_system: &'a FileSystem<PeerId>,
    transport: T,
    incoming: broadcast::Receiver<IncomingMessage<PeerId>>,
    file_requests: broadcast::Receiver<crate::FileRequestMessage<PeerId>>,
    changes: broadcast::Receiver<(String, LwwRecord<PeerId, FileMeta<PeerId>>)>,
}

impl<PeerId> FileSystem<PeerId>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn metadata_sync<T>(&self, transport: T) -> MetadataSync<'_, PeerId, T>
    where
        T: FileService<PeerId>,
    {
        MetadataSync {
            incoming: transport.subscribe(),
            file_requests: transport.subscribe_file_requests(),
            changes: self.metadata.subscribe(),
            file_system: self,
            transport,
        }
    }

    pub async fn sync_peer<T>(&self, transport: T) -> Result<(), Error>
    where
        T: FileService<PeerId>,
    {
        self.metadata_sync(transport).run().await
    }
}

impl<PeerId, T> MetadataSync<'_, PeerId, T>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: FileService<PeerId>,
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

    pub async fn stream(&self, path: &str) -> Result<ByteStream<Error>, Error> {
        let entry = self.file_system.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        if let Ok(stream) = self.file_system.stream(path, 64 * 1024).await {
            return Ok(Box::pin(stream));
        }
        let provider = self.provider(&entry.meta).await?;
        let stream = self
            .transport
            .open_file(
                provider,
                FileRequest {
                    file_id: entry.meta.file_id,
                    revision: self.file_system.revision(path).await?,
                },
            )
            .await
            .map_err(|_| Error::Offline)?;
        Ok(Box::pin(stream.map(|chunk| chunk.map_err(transport_error))))
    }

    pub async fn fetch(&self, path: &str) -> Result<(), Error> {
        let entry = self.file_system.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
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
        let stream = self
            .transport
            .open_file(
                provider,
                FileRequest {
                    file_id: entry.meta.file_id,
                    revision,
                },
            )
            .await
            .map_err(|_| Error::Offline)?;
        self.file_system
            .store_content(
                entry.meta.file_id,
                revision,
                Box::pin(stream.map(|chunk| chunk.map_err(transport_error))),
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
        loop {
            tokio::select! {
                result = self.changes.recv() => match result {
                    Ok((path, record)) => self.transport.broadcast(SyncMessage::MetadataDelta { records: vec![(path, record)] }).await.map_err(transport_error)?,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
                result = self.incoming.recv() => match result {
                    Ok((peer, message)) => self.receive(peer, message).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
                result = self.file_requests.recv() => match result {
                    Ok((_, request, sink)) => self.receive_file_request(request, sink).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
    }

    pub async fn pump(&mut self) -> Result<(), Error> {
        while let Ok((path, record)) = self.changes.try_recv() {
            self.transport
                .broadcast(SyncMessage::MetadataDelta {
                    records: vec![(path, record)],
                })
                .await
                .map_err(transport_error)?;
        }
        while let Ok((peer, message)) = self.incoming.try_recv() {
            self.receive(peer, message).await?;
        }
        while let Ok((_, request, sink)) = self.file_requests.try_recv() {
            self.receive_file_request(request, sink).await?;
        }
        Ok(())
    }

    async fn provider(&self, meta: &FileMeta<PeerId>) -> Result<PeerId, Error> {
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
                Ok(())
            }
        }
    }

    async fn receive_file_request(
        &self,
        request: FileRequest,
        sink: crate::FileSink,
    ) -> Result<(), Error> {
        let mut stream = self
            .file_system
            .content_stream(request.file_id, request.revision)
            .await?;
        while let Some(chunk) = stream.next().await {
            if sink.send(chunk?).await.is_err() {
                break;
            }
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
