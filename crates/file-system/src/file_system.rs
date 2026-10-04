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

use ofdb_kv::{Client, Database, JsonValue, Value};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{
    Error, FileKind, FileMeta, ReadStream, Residency,
    file_service::{ScanPage, scan_page},
    path::{directory, file},
    residency::ResidencyRules,
};

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
    pub(crate) database: Database,
    pub(crate) metadata: Client,
    node_id: PeerId,
    residency: Mutex<ResidencyRules>,
    pub(crate) changes: broadcast::Sender<()>,
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
        let database = Database::open_with_table(root.join("metadata.redb"), "metadata")
            .map_err(metadata_error)?;
        let metadata = database.client();
        let changes = broadcast::channel(16).0;

        Ok(Self {
            content_root,
            database,
            metadata,
            node_id,
            residency: Mutex::new(ResidencyRules::default()),
            changes,
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
        self.get(path)
            .await?
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
        let mut meta = match self.get(path).await? {
            Some(meta) if meta.kind == FileKind::Directory => return Err(Error::IsDirectory),
            Some(meta) => meta,
            None => FileMeta::file(Uuid::now_v7(), self.node_id.clone()),
        };
        meta.revision = Uuid::now_v7();
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
        expected_revision: Uuid,
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
        let mut meta = entry.meta;
        meta.revision = Uuid::now_v7();
        self.put(path, meta).await?;
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
        let mut meta = entry.meta;
        meta.revision = Uuid::now_v7();
        self.put(path, meta).await
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
            .all_metadata()
            .await?
            .into_iter()
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
        self.metadata.delete(path).await.map_err(metadata_error)?;
        let _ = self.changes.send(());
        Ok(())
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
            for (path, meta) in self.all_metadata().await? {
                if path.starts_with(&prefix) {
                    moves.push((path.clone(), format!("{to}{}", &path[from.len()..]), meta));
                }
            }
        }
        for (_, destination, _) in &moves {
            if destination != to && self.get(destination).await?.is_some() {
                return Err(Error::AlreadyExists);
            }
        }
        let mut transaction = self.metadata.transaction().await.map_err(metadata_error)?;
        for (_, destination, meta) in &moves {
            let mut meta = meta.clone();
            meta.revision = Uuid::now_v7();
            transaction
                .set(
                    destination,
                    Value::Json(JsonValue::from(
                        serde_json::to_value(&meta).map_err(metadata_error)?,
                    )),
                    None,
                )
                .await
                .map_err(metadata_error)?;
        }
        for (source, _, _) in moves {
            transaction.delete(&source).await.map_err(metadata_error)?;
        }
        transaction.commit().await.map_err(metadata_error)?;
        let _ = self.changes.send(());
        Ok(())
    }

    fn require_full(&self, path: &str) -> Result<(), Error> {
        (self.residency(path)? == Residency::Full)
            .then_some(())
            .ok_or(Error::PassthroughWrite)
    }

    async fn verify_content(&self, path: &str) -> Result<(), Error> {
        for (entry_path, meta) in self.all_metadata().await? {
            if within(path, &entry_path)
                && meta.kind == FileKind::File
                && !self.content_path(meta.file_id).is_file()
            {
                return Err(Error::ContentUnavailable);
            }
        }
        Ok(())
    }

    async fn evict_unreferenced(&self, path: &str) -> Result<(), Error> {
        let entries = self.all_metadata().await?;
        for (entry_path, meta) in &entries {
            if within(path, entry_path) && meta.kind == FileKind::File {
                let file_id = meta.file_id;
                let retained = entries.iter().any(|(other_path, other)| {
                    other_path != entry_path
                        && other.kind == FileKind::File
                        && other.file_id == file_id
                        && matches!(self.residency(other_path), Ok(Residency::Full))
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

    fn content_path(&self, file_id: Uuid) -> PathBuf {
        self.content_root.join(file_id.to_string())
    }

    pub async fn revision(&self, path: &str) -> Result<Uuid, Error> {
        self.get(path)
            .await?
            .map(|meta| meta.revision)
            .ok_or(Error::NotFound)
    }

    pub(crate) async fn content(&self, file_id: Uuid, revision: Uuid) -> Result<Vec<u8>, Error> {
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
        revision: Uuid,
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
        if !self.has_revision(file_id, revision).await? {
            let _ = fs::remove_file(&temporary);
            return Ok(());
        }
        fs::rename(temporary, path)?;
        Ok(())
    }

    async fn has_revision(&self, file_id: Uuid, revision: Uuid) -> Result<bool, Error> {
        Ok(self
            .all_metadata()
            .await?
            .into_iter()
            .any(|(_, meta)| meta.revision == revision && meta.file_id == file_id))
    }

    pub(crate) async fn get(&self, path: &str) -> Result<Option<FileMeta<PeerId>>, Error> {
        self.metadata
            .get(path)
            .await
            .map_err(metadata_error)?
            .map(|value| {
                let json = value
                    .to_json()
                    .ok_or_else(|| metadata_error("metadata value is not JSON"))?;
                serde_json::from_value(to_serde_json(json)?).map_err(metadata_error)
            })
            .transpose()
    }

    async fn all_metadata(&self) -> Result<Vec<(String, FileMeta<PeerId>)>, Error> {
        self.metadata
            .scan_all()
            .await
            .map_err(metadata_error)?
            .into_iter()
            .map(|(path, value)| {
                let json = value
                    .to_json()
                    .ok_or_else(|| metadata_error("metadata value is not JSON"))?;
                serde_json::from_value(to_serde_json(json)?)
                    .map(|meta| (path, meta))
                    .map_err(metadata_error)
            })
            .collect()
    }

    async fn put(&self, path: &str, meta: FileMeta<PeerId>) -> Result<(), Error> {
        self.metadata
            .set(
                path,
                Value::Json(JsonValue::from(
                    serde_json::to_value(&meta).map_err(metadata_error)?,
                )),
                None,
            )
            .await
            .map_err(metadata_error)?;
        let _ = self.changes.send(());
        Ok(())
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

fn to_serde_json(value: ofdb_kv::JsonValue) -> Result<serde_json::Value, Error> {
    match value {
        ofdb_kv::JsonValue::Null => Ok(serde_json::Value::Null),
        ofdb_kv::JsonValue::Bool(value) => Ok(serde_json::Value::Bool(value)),
        ofdb_kv::JsonValue::Number(ofdb_kv::JsonNumber::I64(value)) => {
            Ok(serde_json::Value::Number(value.into()))
        }
        ofdb_kv::JsonValue::Number(ofdb_kv::JsonNumber::U64(value)) => {
            Ok(serde_json::Value::Number(value.into()))
        }
        ofdb_kv::JsonValue::Number(ofdb_kv::JsonNumber::F64(value)) => {
            serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| metadata_error("metadata contains a non-finite number"))
        }
        ofdb_kv::JsonValue::String(value) => Ok(serde_json::Value::String(value)),
        ofdb_kv::JsonValue::Array(values) => values
            .into_iter()
            .map(to_serde_json)
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        ofdb_kv::JsonValue::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, to_serde_json(value)?)))
            .collect::<Result<serde_json::Map<_, _>, Error>>()
            .map(serde_json::Value::Object),
    }
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}
