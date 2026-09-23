use core::{error::Error, future::Future};

use tokio::sync::{broadcast, mpsc};

use crate::{FileHandle, FileHandleId, FileOperation, FileResponse, OpenRequest, SyncMessage};
use uuid::Uuid;

pub type IncomingMessage<PeerId> = (PeerId, SyncMessage<PeerId>);

pub type SessionResponse<PeerId> = mpsc::Sender<FileResponse<PeerId>>;
pub type SessionRequest<PeerId> = (PeerId, FileOperation, SessionResponse<PeerId>);

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

pub trait FileSessionService<PeerId>: Transport<PeerId> {
    type Handle: FileHandle<PeerId, Error = Self::Error> + Send;

    fn open(
        &self,
        peer: PeerId,
        request: OpenRequest,
    ) -> impl Future<Output = Result<Self::Handle, Self::Error>> + Send;

    fn subscribe_file_sessions(&self) -> broadcast::Receiver<SessionRequest<PeerId>>;

    fn allocate_handle(&self, _: &PeerId, _: &OpenRequest) -> FileHandleId {
        FileHandleId(Uuid::now_v7())
    }
}
