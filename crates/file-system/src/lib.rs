#![forbid(unsafe_code)]

mod error;
mod file_meta;
mod file_service;
mod file_system;
#[cfg(feature = "iroh")]
mod iroh_transport;
mod memory_transport;
mod path;
mod protocol;
mod residency;
mod resource;
mod stream;
mod sync;
mod transport;

pub use error::Error;
pub use file_meta::{FileKind, FileMeta};
pub(crate) use file_service::MAX_FILE_READ_LENGTH;
pub use file_service::{
    DEFAULT_SCAN_LIMIT, FileFuture, FileHandle, FileHandleId, FileOperation, FileResponse,
    FileServiceError, LocalFileHandle, OpenRequest, ScanPage,
};
pub use file_system::{Entry, FileSystem};
#[cfg(feature = "iroh")]
pub use iroh_transport::{IrohFileHandle, IrohFileTransport, IrohResourceDescriptor};
pub use memory_transport::{MemoryNetwork, MemoryTransport, MemoryTransportError};
pub use protocol::{
    FILESYSTEM_SYNC_PROTOCOL_VERSION, MAX_FILESYSTEM_SYNC_FRAME_SIZE, decode_sync_message,
    encode_sync_message,
};
pub use residency::Residency;
pub use resource::{CatalogEntry, FileSystemCatalog, FileSystemId, FileSystemResource};
pub use stream::ReadStream;
pub use sync::{MetadataSync, SyncMessage};
pub use transport::{
    FileSessionService, IncomingMessage, SessionRequest, SessionResponse, Transport,
};
