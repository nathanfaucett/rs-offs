#![forbid(unsafe_code)]

mod error;
mod file_meta;
mod file_service;
mod file_system;
#[cfg(test)]
mod file_system_tests;

#[cfg(not(target_arch = "wasm32"))]
mod native;

mod managed_copy;
mod memory_transport;
mod operation;
mod path;
mod protocol;
mod residency;
mod resource;
#[cfg(not(target_arch = "wasm32"))]
mod staged_content;
mod stream;
mod sync;
mod transport;

mod content;
mod root;

#[cfg(all(target_arch = "wasm32", feature = "browser"))]
mod browser;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
pub mod browser_api;

pub use error::Error;
pub use file_meta::{FileKind, FileMeta};

pub use file_service::{
    DEFAULT_SCAN_LIMIT, FileFuture, FileHandle, FileHandleId, FileOperation, FileResponse,
    FileServiceError, LocalFileHandle, MAX_FILE_READ_LENGTH, OpenRequest, ScanPage,
};
pub use file_system::{Entry, FileSystem};

pub use memory_transport::{MemoryNetwork, MemoryTransport, MemoryTransportError};
pub use operation::{OperationControl, RemoteOperation};
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
