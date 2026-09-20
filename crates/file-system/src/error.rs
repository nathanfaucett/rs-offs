use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    InvalidPath,
    NotFound,
    NotDirectory,
    IsDirectory,
    TypeConflict,
    AlreadyExists,
    InvalidChunkSize,
    ContentUnavailable,
    PassthroughWrite,
    Offline,
    NoPeers,
    NoReachableProvider,
    Io(io::Error),
    Metadata(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath => {
                formatter.write_str("path must be relative and cannot use reserved components")
            }
            Self::NotFound => formatter.write_str("path was not found"),
            Self::NotDirectory => formatter.write_str("path is not a directory"),
            Self::IsDirectory => formatter.write_str("path names a directory"),
            Self::TypeConflict => formatter.write_str("path conflicts with an existing entry type"),
            Self::AlreadyExists => formatter.write_str("destination already exists"),
            Self::InvalidChunkSize => formatter.write_str("chunk size must not be zero"),
            Self::ContentUnavailable => formatter.write_str("content is unavailable"),
            Self::PassthroughWrite => formatter.write_str("writes require full residency"),
            Self::Offline => formatter.write_str("storage is offline"),
            Self::NoPeers => formatter.write_str("no peers are online"),
            Self::NoReachableProvider => {
                formatter.write_str("no reachable provider advertises the content")
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
