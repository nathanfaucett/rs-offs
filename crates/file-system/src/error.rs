use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    InvalidPath,
    InvalidResourceId,
    NotFound,
    NotDirectory,
    IsDirectory,
    TypeConflict,
    AlreadyExists,
    InvalidChunkSize,
    InvalidReadLength,
    InvalidScanLimit,
    InvalidScanCursor,
    StaleRevision,
    ContentUnavailable,
    PassthroughWrite,
    Offline,
    NoPeers,
    NoReachableProvider,
    UnsupportedSyncProtocolVersion(u8),
    SyncFrameTooLarge,
    InvalidSyncFrame(String),
    Io(io::Error),
    Metadata(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath => {
                formatter.write_str("path must be relative and cannot use reserved components")
            }
            Self::InvalidResourceId => formatter.write_str("filesystem ID must be a UUIDv7"),
            Self::NotFound => formatter.write_str("path was not found"),
            Self::NotDirectory => formatter.write_str("path is not a directory"),
            Self::IsDirectory => formatter.write_str("path names a directory"),
            Self::TypeConflict => formatter.write_str("path conflicts with an existing entry type"),
            Self::AlreadyExists => formatter.write_str("destination already exists"),
            Self::InvalidChunkSize => formatter.write_str("chunk size must not be zero"),
            Self::InvalidReadLength => formatter.write_str("read length is too large"),
            Self::InvalidScanLimit => formatter.write_str("scan limit is too large"),
            Self::InvalidScanCursor => formatter.write_str("scan cursor is invalid"),
            Self::StaleRevision => formatter.write_str("file handle revision is stale"),
            Self::ContentUnavailable => formatter.write_str("content is unavailable"),
            Self::PassthroughWrite => formatter.write_str("writes require full residency"),
            Self::Offline => formatter.write_str("storage is offline"),
            Self::NoPeers => formatter.write_str("no peers are online"),
            Self::NoReachableProvider => {
                formatter.write_str("no reachable provider advertises the content")
            }
            Self::UnsupportedSyncProtocolVersion(version) => {
                write!(
                    formatter,
                    "unsupported filesystem sync protocol version: {version}"
                )
            }
            Self::SyncFrameTooLarge => {
                formatter.write_str("filesystem sync frame exceeds size limit")
            }
            Self::InvalidSyncFrame(error) => {
                write!(formatter, "invalid filesystem sync frame: {error}")
            }
            Self::Io(error) => error.fmt(formatter),
            Self::Metadata(error) => write!(formatter, "metadata error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
