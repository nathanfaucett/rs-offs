use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use tokio::sync::broadcast;

use crate::{IncomingMessage, SyncMessage, Transport};

struct MemoryNetworkState<PeerId> {
    peers: Mutex<BTreeMap<PeerId, broadcast::Sender<IncomingMessage<PeerId>>>>,
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
        let sender = broadcast::channel(self.state.capacity).0;
        self.state
            .peers
            .lock()
            .expect("memory network lock poisoned")
            .insert(peer.clone(), sender);
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
            .cloned()
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
            .map(|(_, sender)| sender.clone())
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
            .subscribe()
    }
}
