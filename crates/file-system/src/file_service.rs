use bytes::Bytes;
use deckv::Timestamp;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Entry, Error};

pub const DEFAULT_SCAN_LIMIT: u32 = 256;
pub const MAX_SCAN_LIMIT: u32 = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OpenRequest {
    pub path: String,
    pub revision: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    },
    Data(Bytes),
    Written(u32),
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

pub trait FileHandle<PeerId> {
    type Error: std::error::Error + 'static;

    fn read(
        &mut self,
        offset: u64,
        length: u32,
    ) -> impl core::future::Future<Output = Result<Bytes, Self::Error>> + Send;

    fn write(
        &mut self,
        offset: u64,
        data: Bytes,
    ) -> impl core::future::Future<Output = Result<u32, Self::Error>> + Send;

    fn scan(
        &mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> impl core::future::Future<Output = Result<ScanPage<PeerId>, Self::Error>> + Send;

    fn close(self) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send;
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
