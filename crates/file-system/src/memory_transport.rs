use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};

use crate::{
    FileHandle, FileHandleId, FileOperation, FileResponse, FileServiceError, FileSessionService,
    IncomingMessage, OpenRequest, ScanPage, SessionRequest, SyncMessage, Transport,
};

struct MemoryPeer<PeerId> {
    metadata: broadcast::Sender<IncomingMessage<PeerId>>,
    sessions: broadcast::Sender<SessionRequest<PeerId>>,
}

struct MemoryNetworkState<PeerId> {
    peers: Mutex<BTreeMap<PeerId, MemoryPeer<PeerId>>>,
    capacity: usize,
}

pub struct MemoryNetwork<PeerId> {
    state: Arc<MemoryNetworkState<PeerId>>,
}

impl<PeerId> Clone for MemoryNetwork<PeerId> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<PeerId> MemoryNetwork<PeerId> {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(MemoryNetworkState {
                peers: Mutex::new(BTreeMap::new()),
                capacity,
            }),
        }
    }
}

impl<PeerId: Clone + Ord> MemoryNetwork<PeerId> {
    #[must_use]
    pub fn transport(&self, peer: PeerId) -> MemoryTransport<PeerId> {
        let metadata = broadcast::channel(self.state.capacity).0;
        let sessions = broadcast::channel(self.state.capacity).0;
        self.state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .insert(peer.clone(), MemoryPeer { metadata, sessions });
        MemoryTransport {
            peer,
            network: self.clone(),
        }
    }
}

#[derive(Debug)]
pub enum MemoryTransportError {
    UnknownPeer,
    SessionClosed,
    Service(FileServiceError),
}
impl fmt::Display for MemoryTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownPeer => "memory transport peer was not found",
            Self::SessionClosed => "file session closed",
            Self::Service(_) => "file service request failed",
        })
    }
}
impl std::error::Error for MemoryTransportError {}

pub struct MemoryTransport<PeerId> {
    peer: PeerId,
    network: MemoryNetwork<PeerId>,
}
impl<PeerId: Clone> Clone for MemoryTransport<PeerId> {
    fn clone(&self) -> Self {
        Self {
            peer: self.peer.clone(),
            network: self.network.clone(),
        }
    }
}

impl<PeerId> Transport<PeerId> for MemoryTransport<PeerId>
where
    PeerId: Clone + Ord + Send + Sync + 'static,
{
    type Error = MemoryTransportError;
    fn peers(&self) -> Vec<PeerId> {
        self.network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .keys()
            .filter(|p| *p != &self.peer)
            .cloned()
            .collect()
    }
    async fn send(&self, peer: PeerId, message: SyncMessage<PeerId>) -> Result<(), Self::Error> {
        let sender = self
            .network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&peer)
            .map(|p| p.metadata.clone())
            .ok_or(MemoryTransportError::UnknownPeer)?;
        let _ = sender.send((self.peer.clone(), message));
        Ok(())
    }
    async fn broadcast(&self, message: SyncMessage<PeerId>) -> Result<(), Self::Error> {
        let senders = self
            .network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .iter()
            .filter(|(p, _)| *p != &self.peer)
            .map(|(_, p)| p.metadata.clone())
            .collect::<Vec<_>>();
        for sender in senders {
            let _ = sender.send((self.peer.clone(), message.clone()));
        }
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<IncomingMessage<PeerId>> {
        self.network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&self.peer)
            .expect("memory transport is not registered")
            .metadata
            .subscribe()
    }
}

impl<PeerId> FileSessionService<PeerId> for MemoryTransport<PeerId>
where
    PeerId: Clone + Ord + Send + Sync + 'static,
{
    type Handle = MemoryFileHandle<PeerId>;
    async fn open(&self, peer: PeerId, request: OpenRequest) -> Result<Self::Handle, Self::Error> {
        let revision = request.revision.unwrap_or(deckv::Timestamp {
            physical: 0,
            logical: 0,
        });
        let handle = deterministic_handle(&request.path, request.revision);
        let sender = self
            .network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&peer)
            .map(|p| p.sessions.clone())
            .ok_or(MemoryTransportError::UnknownPeer)?;
        let (tx, _rx) = mpsc::channel(self.network.state.capacity);
        sender
            .send((self.peer.clone(), FileOperation::Open(request), tx))
            .map_err(|_| MemoryTransportError::SessionClosed)?;
        Ok(MemoryFileHandle {
            transport: self.clone(),
            peer,
            handle,
            revision,
        })
    }
    fn subscribe_file_sessions(&self) -> broadcast::Receiver<SessionRequest<PeerId>> {
        self.network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&self.peer)
            .expect("memory transport is not registered")
            .sessions
            .subscribe()
    }

    fn allocate_handle(&self, _: &PeerId, request: &OpenRequest) -> FileHandleId {
        deterministic_handle(&request.path, request.revision)
    }
}

impl<PeerId> MemoryTransport<PeerId>
where
    PeerId: Clone + Ord + Send + Sync + 'static,
{
    fn start_request(
        &self,
        peer: PeerId,
        operation: FileOperation,
    ) -> Result<mpsc::Receiver<FileResponse<PeerId>>, MemoryTransportError> {
        let sender = self
            .network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&peer)
            .map(|p| p.sessions.clone())
            .ok_or(MemoryTransportError::UnknownPeer)?;
        let (tx, rx) = mpsc::channel(self.network.state.capacity);
        sender
            .send((self.peer.clone(), operation, tx))
            .map_err(|_| MemoryTransportError::SessionClosed)?;
        Ok(rx)
    }

    async fn collect(mut rx: mpsc::Receiver<FileResponse<PeerId>>) -> Vec<FileResponse<PeerId>> {
        let mut responses = Vec::new();
        while let Some(response) = rx.recv().await {
            let end = matches!(
                response,
                FileResponse::ReadEnd
                    | FileResponse::Opened { .. }
                    | FileResponse::Written { .. }
                    | FileResponse::Page(_)
                    | FileResponse::Closed
                    | FileResponse::Error(_)
            );
            responses.push(response);
            if end {
                break;
            }
        }
        responses
    }

    async fn request(
        &self,
        peer: PeerId,
        operation: FileOperation,
    ) -> Result<Vec<FileResponse<PeerId>>, MemoryTransportError> {
        Ok(Self::collect(self.start_request(peer, operation)?).await)
    }
}

fn deterministic_handle(path: &str, revision: Option<deckv::Timestamp>) -> FileHandleId {
    let mut bytes = [0_u8; 16];
    for (index, byte) in path.as_bytes().iter().enumerate() {
        bytes[index % 16] = bytes[index % 16]
            .wrapping_add(*byte)
            .rotate_left((index % 8) as u32);
    }
    if let Some(revision) = revision {
        for (index, byte) in revision
            .physical
            .to_le_bytes()
            .iter()
            .chain(revision.logical.to_le_bytes().iter())
            .enumerate()
        {
            bytes[index] ^= *byte;
        }
    }
    FileHandleId(uuid::Uuid::from_bytes(bytes))
}

pub struct MemoryFileHandle<PeerId> {
    transport: MemoryTransport<PeerId>,
    peer: PeerId,
    handle: FileHandleId,
    revision: deckv::Timestamp,
}
impl<PeerId> FileHandle<PeerId> for MemoryFileHandle<PeerId>
where
    PeerId: Clone + Ord + Send + Sync + 'static,
{
    type Error = MemoryTransportError;
    fn read<'a>(
        &'a mut self,
        offset: u64,
        length: u32,
    ) -> crate::FileFuture<'a, Result<Bytes, Self::Error>> {
        let receiver = match self.transport.start_request(
            self.peer.clone(),
            FileOperation::Read {
                handle: self.handle.clone(),
                offset,
                length,
            },
        ) {
            Ok(receiver) => receiver,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        Box::pin(async move {
            let responses = MemoryTransport::collect(receiver).await;
            let mut bytes = Vec::new();
            for response in responses {
                match response {
                    FileResponse::ReadChunk(chunk) => bytes.extend_from_slice(&chunk),
                    FileResponse::Error(error) => return Err(MemoryTransportError::Service(error)),
                    _ => {}
                }
            }
            Ok(Bytes::from(bytes))
        })
    }
    fn write<'a>(
        &'a mut self,
        offset: u64,
        data: Bytes,
    ) -> crate::FileFuture<'a, Result<u32, Self::Error>> {
        Box::pin(async move {
            let response = self
                .transport
                .request(
                    self.peer.clone(),
                    FileOperation::Write {
                        handle: self.handle.clone(),
                        expected_revision: self.revision,
                        offset,
                        data,
                    },
                )
                .await?;
            match response.into_iter().next() {
                Some(FileResponse::Written { count, revision }) => {
                    self.revision = revision;
                    Ok(count)
                }
                Some(FileResponse::Error(error)) => Err(MemoryTransportError::Service(error)),
                _ => Err(MemoryTransportError::SessionClosed),
            }
        })
    }
    fn scan<'a>(
        &'a mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> crate::FileFuture<'a, Result<ScanPage<PeerId>, Self::Error>> {
        Box::pin(async move {
            let response = self
                .transport
                .request(
                    self.peer.clone(),
                    FileOperation::Scan {
                        handle: self.handle.clone(),
                        cursor,
                        limit,
                    },
                )
                .await?;
            match response.into_iter().next() {
                Some(FileResponse::Page(page)) => Ok(page),
                Some(FileResponse::Error(error)) => Err(MemoryTransportError::Service(error)),
                _ => Err(MemoryTransportError::SessionClosed),
            }
        })
    }
    fn close(self: Box<Self>) -> crate::FileFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let response = self
                .transport
                .request(
                    self.peer.clone(),
                    FileOperation::Close {
                        handle: self.handle,
                    },
                )
                .await?;
            match response.into_iter().next() {
                Some(FileResponse::Closed) => Ok(()),
                Some(FileResponse::Error(error)) => Err(MemoryTransportError::Service(error)),
                _ => Err(MemoryTransportError::SessionClosed),
            }
        })
    }
}
