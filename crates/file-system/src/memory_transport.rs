use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::stream;
use tokio::sync::{broadcast, mpsc};

use crate::{
    ByteStream, FileRequest, FileRequestMessage, FileService, IncomingMessage, SyncMessage,
    Transport,
};

struct MemoryPeer<PeerId> {
    metadata: broadcast::Sender<IncomingMessage<PeerId>>,
    file_requests: broadcast::Sender<FileRequestMessage<PeerId>>,
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
        let file_requests = broadcast::channel(self.state.capacity).0;
        self.state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .insert(
                peer.clone(),
                MemoryPeer {
                    metadata,
                    file_requests,
                },
            );
        MemoryTransport {
            peer,
            network: self.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryTransportError {
    UnknownPeer,
}

impl fmt::Display for MemoryTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("memory transport peer was not found")
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
            .filter(|peer| *peer != &self.peer)
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
            .map(|peer| peer.metadata.clone())
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
            .filter(|(peer, _)| *peer != &self.peer)
            .map(|(_, peer)| peer.metadata.clone())
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

impl<PeerId> FileService<PeerId> for MemoryTransport<PeerId>
where
    PeerId: Clone + Ord + Send + Sync + 'static,
{
    async fn open_file(
        &self,
        peer: PeerId,
        request: FileRequest,
    ) -> Result<ByteStream<Self::Error>, Self::Error> {
        let sender = self
            .network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&peer)
            .map(|peer| peer.file_requests.clone())
            .ok_or(MemoryTransportError::UnknownPeer)?;
        let (response, receiver) = mpsc::channel(self.network.state.capacity);
        let _ = sender.send((self.peer.clone(), request, response));
        Ok(Box::pin(stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|chunk| (Ok(chunk), receiver))
        })))
    }

    fn subscribe_file_requests(&self) -> broadcast::Receiver<FileRequestMessage<PeerId>> {
        self.network
            .state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .get(&self.peer)
            .expect("memory transport is not registered")
            .file_requests
            .subscribe()
    }
}
