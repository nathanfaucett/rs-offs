use core::{error::Error, future::Future, pin::Pin};

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::{broadcast, mpsc};

use crate::{FileRequest, SyncMessage};

pub type ByteStream<E> = Pin<Box<dyn Stream<Item = Result<Bytes, E>> + Send>>;
pub type FileRequestMessage<PeerId> = (PeerId, FileRequest, FileSink);
pub type FileSink = mpsc::Sender<Bytes>;
pub type IncomingMessage<PeerId> = (PeerId, SyncMessage<PeerId>);

pub trait Transport<PeerId> {
    type Error: Error + 'static;

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

pub trait FileService<PeerId>: Transport<PeerId> {
    fn open_file(
        &self,
        peer: PeerId,
        request: FileRequest,
    ) -> impl Future<Output = Result<ByteStream<Self::Error>, Self::Error>> + Send;

    fn subscribe_file_requests(&self) -> broadcast::Receiver<FileRequestMessage<PeerId>>;
}
