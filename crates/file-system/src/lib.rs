#![forbid(unsafe_code)]

mod error;
mod file_meta;
mod file_service;
mod file_system;
mod memory_transport;
mod path;
mod residency;
mod stream;
mod sync;
mod transport;

pub use error::Error;
pub use file_meta::{FileKind, FileMeta};
pub use file_service::{
    DEFAULT_SCAN_LIMIT, FileFuture, FileHandle, FileHandleId, FileOperation, FileResponse,
    FileServiceError, LocalFileHandle, OpenRequest, ScanPage,
};
pub use file_system::{Entry, FileSystem};
pub use memory_transport::{MemoryNetwork, MemoryTransport, MemoryTransportError};
pub use residency::Residency;
pub use stream::ReadStream;
pub use sync::{MetadataSync, SyncMessage};
pub use transport::{
    FileSessionService, IncomingMessage, SessionRequest, SessionResponse, Transport,
};
