use std::{
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use deckv::{RedbStorage, Store};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    Error, FileKind, FileMeta, ReadStream, Residency,
    file_service::{ScanPage, scan_page},
    path::{directory, file},
    residency::ResidencyRules,
};

pub(crate) type MetadataStore<PeerId> =
    Store<PeerId, String, FileMeta<PeerId>, RedbStorage<PeerId, String, FileMeta<PeerId>>>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(bound(deserialize = "PeerId: Ord + Deserialize<'de>"))]
pub struct Entry<PeerId = Uuid> {
    pub path: String,
    pub meta: FileMeta<PeerId>,
}

pub struct FileSystem<PeerId = Uuid>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + 'static,
{
    content_root: PathBuf,
    pub(crate) metadata: MetadataStore<PeerId>,
    node_id: PeerId,
    residency: Mutex<ResidencyRules>,
}

impl<PeerId> FileSystem<PeerId>
where
    PeerId: Clone + Debug + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn open(root: impl AsRef<Path>, node_id: PeerId) -> Result<Self, Error> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let content_root = root.join(".data");
        fs::create_dir_all(&content_root)?;
        let database = redb::Database::create(root.join("metadata.redb"))
            .map_err(|error| Error::Metadata(error.to_string()))?;
        let metadata = Store::new(
            node_id.clone(),
            RedbStorage::open(database).map_err(|error| Error::Metadata(error.to_string()))?,
            broadcast::channel(16).0,
        );

        Ok(Self {
            content_root,
            metadata,
            node_id,
            residency: Mutex::new(ResidencyRules::default()),
        })
    }

    pub fn residency(&self, path: &str) -> Result<Residency, Error> {
        self.residency
            .lock()
            .expect("residency lock poisoned")
            .get(path)
    }

    pub async fn set_residency(&self, path: &str, residency: Residency) -> Result<(), Error> {
        directory(path)?;
        if residency == Residency::Full {
            self.verify_content(path).await?;
        }
        self.residency
            .lock()
            .expect("residency lock poisoned")
            .set(path, residency)?;
        if residency == Residency::Passthrough {
            self.evict_unreferenced(path).await?;
        }
        Ok(())
    }

    pub async fn open_handle(
        &self,
        request: crate::OpenRequest,
    ) -> Result<crate::LocalFileHandle<'_, PeerId>, Error> {
        let entry = self.entry(&request.path).await?;
        let revision = self.revision(&request.path).await?;
        if request
            .revision
            .is_some_and(|expected| expected != revision)
        {
            return Err(Error::StaleRevision);
        }
        Ok(crate::file_service::LocalFileHandle::new(
            self,
            request.path,
            revision,
            entry.meta.kind,
        ))
    }

    pub async fn entry(&self, path: &str) -> Result<Entry<PeerId>, Error> {
        directory(path)?;
        if path.is_empty() {
            return Err(Error::NotFound);
        }
        self.metadata
            .get(&path.to_owned())
            .await
            .map_err(metadata_error)?
            .map(|meta| Entry {
                path: path.to_owned(),
                meta,
            })
            .ok_or(Error::NotFound)
    }

    pub async fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        let entry = self.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        fs::read(self.content_path(entry.meta.file_id)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::ContentUnavailable
            } else {
                Error::Io(error)
            }
        })
    }

    pub async fn stream(&self, path: &str, chunk_size: usize) -> Result<ReadStream, Error> {
        let entry = self.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        ReadStream::new(
            File::open(self.content_path(entry.meta.file_id))?,
            chunk_size,
        )
    }

    pub async fn write(&self, path: &str, content: &[u8]) -> Result<Entry<PeerId>, Error> {
        file(path)?;
        self.require_full(path)?;
        self.ensure_parent_directories(path).await?;
        let meta = match self.get(path).await? {
            Some(meta) if meta.kind == FileKind::Directory => return Err(Error::IsDirectory),
            Some(meta) => meta,
            None => FileMeta::file(Uuid::now_v7(), self.node_id.clone()),
        };
        write_file(&self.content_path(meta.file_id), content)?;
        self.put(path, meta.clone()).await?;
        Ok(Entry {
            path: path.to_owned(),
            meta,
        })
    }

    pub async fn write_at(
        &self,
        path: &str,
        expected_revision: deckv::Timestamp,
        offset: u64,
        data: Bytes,
    ) -> Result<u32, Error> {
        file(path)?;
        self.require_full(path)?;
        let entry = self.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        if self.revision(path).await? != expected_revision {
            return Err(Error::StaleRevision);
        }
        let length = u32::try_from(data.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "write is too large",
            ))
        })?;
        let mut content = OpenOptions::new()
            .write(true)
            .open(self.content_path(entry.meta.file_id))?;
        content.seek(SeekFrom::Start(offset))?;
        content.write_all(&data)?;
        content.sync_all()?;
        self.put(path, entry.meta).await?;
        Ok(length)
    }

    pub async fn append(&self, path: &str, content: &[u8]) -> Result<(), Error> {
        file(path)?;
        self.require_full(path)?;
        let entry = self.entry(path).await?;
        if entry.meta.kind == FileKind::Directory {
            return Err(Error::IsDirectory);
        }
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.content_path(entry.meta.file_id))?;
        file.write_all(content)?;
        file.sync_all()?;
        self.put(path, entry.meta).await
    }

    pub async fn create_dir(&self, path: &str) -> Result<Entry<PeerId>, Error> {
        file(path)?;
        self.require_full(path)?;
        self.ensure_parent_directories(path).await?;
        match self.get(path).await? {
            Some(meta) if meta.kind == FileKind::Directory => Ok(Entry {
                path: path.to_owned(),
                meta,
            }),
            Some(_) => Err(Error::TypeConflict),
            None => {
                let meta = FileMeta::directory(Uuid::now_v7(), self.node_id.clone());
                self.put(path, meta.clone()).await?;
                Ok(Entry {
                    path: path.to_owned(),
                    meta,
                })
            }
        }
    }

    pub async fn list(&self, path: &str) -> Result<Vec<Entry<PeerId>>, Error> {
        directory(path)?;
        if !path.is_empty() {
            let entry = self.entry(path).await?;
            if entry.meta.kind != FileKind::Directory {
                return Err(Error::NotDirectory);
            }
        }
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        let mut entries = self
            .metadata
            .entries()
            .await
            .map_err(metadata_error)?
            .into_iter()
            .filter_map(|(entry_path, record)| record.value.map(|meta| (entry_path, meta)))
            .filter(|(entry_path, _)| {
                entry_path
                    .strip_prefix(&prefix)
                    .is_some_and(|rest| !rest.contains('/'))
            })
            .map(|(path, meta)| Entry { path, meta })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    pub async fn scan(
        &self,
        path: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ScanPage<PeerId>, Error> {
        scan_page(self.list(path).await?, cursor, limit)
    }

    pub async fn delete(&self, path: &str) -> Result<(), Error> {
        let _ = self.entry(path).await?;
        self.metadata
            .delete(path.to_owned())
            .await
            .map_err(metadata_error)
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), Error> {
        file(from)?;
        file(to)?;
        let entry = self.entry(from).await?;
        if self.get(to).await?.is_some() {
            return Err(Error::AlreadyExists);
        }
        if entry.meta.kind == FileKind::Directory && to.starts_with(&format!("{from}/")) {
            return Err(Error::InvalidPath);
        }
        self.ensure_parent_directories(to).await?;
        let mut moves = vec![(from.to_owned(), to.to_owned(), entry.meta)];
        if moves[0].2.kind == FileKind::Directory {
            let prefix = format!("{from}/");
            for (path, record) in self.metadata.entries().await.map_err(metadata_error)? {
                if let Some(meta) = record.value.filter(|_| path.starts_with(&prefix)) {
                    moves.push((path.clone(), format!("{to}{}", &path[from.len()..]), meta));
                }
            }
        }
        for (_, destination, meta) in &moves {
            if destination != to && self.get(destination).await?.is_some() {
                return Err(Error::AlreadyExists);
            }
            self.put(destination, meta.clone()).await?;
        }
        for (source, _, _) in moves {
            self.metadata.delete(source).await.map_err(metadata_error)?;
        }
        Ok(())
    }

    fn require_full(&self, path: &str) -> Result<(), Error> {
        (self.residency(path)? == Residency::Full)
            .then_some(())
            .ok_or(Error::PassthroughWrite)
    }

    async fn verify_content(&self, path: &str) -> Result<(), Error> {
        for (entry_path, record) in self.metadata.entries().await.map_err(metadata_error)? {
            if within(path, &entry_path)
                && record.value.is_some_and(|meta| {
                    meta.kind == FileKind::File && !self.content_path(meta.file_id).is_file()
                })
            {
                return Err(Error::ContentUnavailable);
            }
        }
        Ok(())
    }

    async fn evict_unreferenced(&self, path: &str) -> Result<(), Error> {
        let entries = self.metadata.entries().await.map_err(metadata_error)?;
        for (entry_path, record) in &entries {
            if within(path, entry_path)
                && record
                    .value
                    .as_ref()
                    .is_some_and(|meta| meta.kind == FileKind::File)
            {
                let file_id = record.value.as_ref().expect("checked above").file_id;
                let retained = entries.iter().any(|(other_path, other_record)| {
                    other_path != entry_path
                        && other_record.value.as_ref().is_some_and(|other| {
                            other.kind == FileKind::File
                                && other.file_id == file_id
                                && matches!(self.residency(other_path), Ok(Residency::Full))
                        })
                });
                if !retained
                    && let Err(error) = fs::remove_file(self.content_path(file_id))
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    async fn ensure_parent_directories(&self, path: &str) -> Result<(), Error> {
        let mut parent = String::new();
        let components = path.split('/').collect::<Vec<_>>();
        for component in &components[..components.len() - 1] {
            if !parent.is_empty() {
                parent.push('/');
            }
            parent.push_str(component);
            match self.get(&parent).await? {
                Some(meta) if meta.kind == FileKind::Directory => {}
                Some(_) => return Err(Error::NotDirectory),
                None => {
                    self.put(
                        &parent,
                        FileMeta::directory(Uuid::now_v7(), self.node_id.clone()),
                    )
                    .await?
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn get(&self, path: &str) -> Result<Option<FileMeta<PeerId>>, Error> {
        self.metadata
            .get(&path.to_owned())
            .await
            .map_err(metadata_error)
    }

    fn content_path(&self, file_id: Uuid) -> PathBuf {
        self.content_root.join(file_id.to_string())
    }

    pub async fn revision(&self, path: &str) -> Result<deckv::Timestamp, Error> {
        self.metadata
            .entries()
            .await
            .map_err(metadata_error)?
            .into_iter()
            .find(|(entry_path, _)| entry_path == path)
            .map(|(_, record)| record.timestamp)
            .ok_or(Error::NotFound)
    }

    pub(crate) async fn content(
        &self,
        file_id: Uuid,
        revision: deckv::Timestamp,
    ) -> Result<Vec<u8>, Error> {
        if !self.has_revision(file_id, revision).await? {
            return Err(Error::ContentUnavailable);
        }

        let content = fs::read(self.content_path(file_id)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::ContentUnavailable
            } else {
                Error::Io(error)
            }
        })?;
        Ok(content)
    }

    pub(crate) async fn store_content<S>(
        &self,
        file_id: Uuid,
        revision: deckv::Timestamp,
        stream: S,
    ) -> Result<(), Error>
    where
        S: futures_core::Stream<Item = Result<Bytes, Error>>,
    {
        if !self.has_revision(file_id, revision).await? {
            return Ok(());
        }
        let path = self.content_path(file_id);
        let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
        let result = (|| -> Result<File, Error> { Ok(File::create(&temporary)?) })();
        let mut file = match result {
            Ok(file) => file,
            Err(error) => return Err(error),
        };
        futures_util::pin_mut!(stream);
        while let Some(chunk) = stream.next().await {
            if let Err(error) = file.write_all(&chunk?) {
                let _ = fs::remove_file(&temporary);
                return Err(error.into());
            }
        }
        file.sync_all()?;
        fs::rename(temporary, path)?;
        Ok(())
    }

    async fn has_revision(&self, file_id: Uuid, revision: deckv::Timestamp) -> Result<bool, Error> {
        let entries = self.metadata.entries().await.map_err(metadata_error)?;
        Ok(entries.into_iter().any(|(_, record)| {
            record.timestamp == revision
                && record
                    .value
                    .as_ref()
                    .is_some_and(|meta| meta.file_id == file_id)
        }))
    }

    async fn put(&self, path: &str, meta: FileMeta<PeerId>) -> Result<(), Error> {
        self.metadata
            .insert(path.to_owned(), meta)
            .await
            .map_err(metadata_error)
    }
}

fn write_file(path: &Path, content: &[u8]) -> Result<(), Error> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let result = (|| {
        let mut file = File::create(&temporary)?;
        file.write_all(content)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn within(scope: &str, path: &str) -> bool {
    scope.is_empty() || path == scope || path.starts_with(&format!("{scope}/"))
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}
