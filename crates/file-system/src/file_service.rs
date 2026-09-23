use bytes::{Bytes, BytesMut};
use core::{future::Future, pin::Pin};
use deckv::Timestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Entry, Error, FileKind, FileSystem};
use futures_util::StreamExt;

pub const DEFAULT_SCAN_LIMIT: u32 = 256;
pub const MAX_SCAN_LIMIT: u32 = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenRequest {
    pub path: String,
    pub revision: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct FileHandleId(pub Uuid);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "PeerId: Ord + Deserialize<'de>"))]
pub struct ScanPage<PeerId> {
    pub entries: Vec<Entry<PeerId>>,
    pub next: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileOperation {
    Open(OpenRequest),
    Read {
        handle: FileHandleId,
        offset: u64,
        length: u32,
    },
    Write {
        handle: FileHandleId,
        expected_revision: Timestamp,
        offset: u64,
        data: Bytes,
    },
    Scan {
        handle: FileHandleId,
        cursor: Option<String>,
        limit: u32,
    },
    Close {
        handle: FileHandleId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "PeerId: Ord + Deserialize<'de>"))]
pub enum FileResponse<PeerId> {
    Opened {
        handle: FileHandleId,
        entry: Entry<PeerId>,
        revision: Timestamp,
    },
    ReadChunk(Bytes),
    ReadEnd,
    Written {
        count: u32,
        revision: Timestamp,
    },
    Page(ScanPage<PeerId>),
    Closed,
    Error(FileServiceError),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileServiceError {
    NotFound,
    NotDirectory,
    IsDirectory,
    InvalidScanLimit,
    InvalidScanCursor,
    StaleRevision,
    PassthroughWrite,
    ContentUnavailable,
    PermissionDenied,
    UnknownHandle,
    Io,
}

pub type FileFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct LocalFileHandle<'a, PeerId>
where
    PeerId:
        Clone + core::fmt::Debug + Ord + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    file_system: &'a FileSystem<PeerId>,
    path: String,
    revision: Timestamp,
    kind: FileKind,
}

impl<'a, PeerId> LocalFileHandle<'a, PeerId>
where
    PeerId:
        Clone + core::fmt::Debug + Ord + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    pub(crate) fn new(
        file_system: &'a FileSystem<PeerId>,
        path: String,
        revision: Timestamp,
        kind: FileKind,
    ) -> Self {
        Self {
            file_system,
            path,
            revision,
            kind,
        }
    }

    pub async fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

pub trait FileHandle<PeerId>: Send + Sync {
    type Error: std::error::Error + 'static;

    fn read<'a>(
        &'a mut self,
        offset: u64,
        length: u32,
    ) -> FileFuture<'a, Result<Bytes, Self::Error>>;

    fn write<'a>(
        &'a mut self,
        offset: u64,
        data: Bytes,
    ) -> FileFuture<'a, Result<u32, Self::Error>>;

    fn scan<'a>(
        &'a mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> FileFuture<'a, Result<ScanPage<PeerId>, Self::Error>>;

    fn close(self: Box<Self>) -> FileFuture<'static, Result<(), Self::Error>>;
}

impl<PeerId> FileHandle<PeerId> for LocalFileHandle<'_, PeerId>
where
    PeerId: Clone
        + core::fmt::Debug
        + Ord
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    type Error = Error;

    fn read<'a>(&'a mut self, offset: u64, length: u32) -> FileFuture<'a, Result<Bytes, Error>> {
        Box::pin(async move {
            if self.kind == FileKind::Directory {
                return Err(Error::IsDirectory);
            }
            let mut stream = self.file_system.stream(&self.path, 64 * 1024).await?;
            let mut remaining = offset;
            let mut output = BytesMut::with_capacity(length as usize);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                if remaining >= chunk.len() as u64 {
                    remaining -= chunk.len() as u64;
                    continue;
                }
                let start = remaining as usize;
                remaining = 0;
                let available = &chunk[start..];
                let take = available.len().min(length as usize - output.len());
                output.extend_from_slice(&available[..take]);
                if output.len() == length as usize {
                    break;
                }
            }
            Ok(output.freeze())
        })
    }

    fn write<'a>(&'a mut self, offset: u64, data: Bytes) -> FileFuture<'a, Result<u32, Error>> {
        Box::pin(async move {
            if self.kind == FileKind::Directory {
                return Err(Error::IsDirectory);
            }
            let written = self
                .file_system
                .write_at(&self.path, self.revision, offset, data)
                .await?;
            self.revision = self.file_system.revision(&self.path).await?;
            Ok(written)
        })
    }

    fn scan<'a>(
        &'a mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> FileFuture<'a, Result<ScanPage<PeerId>, Error>> {
        Box::pin(async move {
            if self.kind != FileKind::Directory {
                return Err(Error::NotDirectory);
            }
            self.file_system
                .scan(&self.path, cursor.as_deref(), limit)
                .await
        })
    }

    fn close(self: Box<Self>) -> FileFuture<'static, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}

pub(crate) fn scan_limit(limit: u32) -> Result<usize, Error> {
    let limit = if limit == 0 {
        DEFAULT_SCAN_LIMIT
    } else {
        limit
    };
    (limit <= MAX_SCAN_LIMIT)
        .then_some(limit as usize)
        .ok_or(Error::InvalidScanLimit)
}

pub(crate) fn scan_page<PeerId: Clone + Ord>(
    entries: Vec<Entry<PeerId>>,
    cursor: Option<&str>,
    limit: u32,
) -> Result<ScanPage<PeerId>, Error> {
    let limit = scan_limit(limit)?;
    let start = cursor
        .map(|cursor| {
            entries
                .iter()
                .position(|entry| entry.path == cursor)
                .map(|position| position + 1)
                .ok_or(Error::InvalidScanCursor)
        })
        .transpose()?
        .unwrap_or(0);
    let entries_len = entries.len();
    let page = entries
        .into_iter()
        .skip(start)
        .take(limit)
        .collect::<Vec<_>>();
    let next = (start + page.len() < entries_len)
        .then(|| page.last().map(|entry| entry.path.clone()))
        .flatten();
    Ok(ScanPage {
        entries: page,
        next,
    })
}
