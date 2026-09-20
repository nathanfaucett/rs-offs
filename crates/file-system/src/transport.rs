use core::{error::Error, future::Future};

use tokio::sync::broadcast;

use crate::SyncMessage;

pub type IncomingMessage<PeerId> = (PeerId, SyncMessage<PeerId>);

pub trait Transport<PeerId> {
    type Error: Error;

    fn peers(&self) -> Vec<PeerId>;

    fn send(
        &self,
        peer: PeerId,
        message: SyncMessage<PeerId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn broadcast(
        &self,
        message: SyncMessage<PeerId>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn subscribe(&self) -> broadcast::Receiver<IncomingMessage<PeerId>>;
}
