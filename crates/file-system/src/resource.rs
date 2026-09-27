use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, FileSystem};

const CATALOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("filesystem_resources");

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FileSystemId(Uuid);

impl FileSystemId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value).map_err(|_| Error::InvalidResourceId)?;
        if id.get_version_num() != 7 {
            return Err(Error::InvalidResourceId);
        }
        Ok(Self(id))
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for FileSystemId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileSystemResource {
    pub id: FileSystemId,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogEntry {
    pub resource: FileSystemResource,
    pub deleted: bool,
}

pub struct FileSystemCatalog {
    root: PathBuf,
    database: Arc<Database>,
}

impl FileSystemCatalog {
    pub fn open(namespace_root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = namespace_root.as_ref().to_owned();
        fs::create_dir_all(&root)?;
        let path = root.join("filesystem-catalog.redb");
        let existed = path.exists();
        let database = if existed {
            Database::open(path)
        } else {
            Database::create(path)
        }
        .map_err(metadata_error)?;
        if existed {
            let transaction = database.begin_read().map_err(metadata_error)?;
            transaction.open_table(CATALOG).map_err(metadata_error)?;
        } else {
            let transaction = database.begin_write().map_err(metadata_error)?;
            transaction.open_table(CATALOG).map_err(metadata_error)?;
            transaction.commit().map_err(metadata_error)?;
        }
        Ok(Self {
            root: root.join("filesystems"),
            database: Arc::new(database),
        })
    }

    pub fn create(&self, name: Option<String>) -> Result<FileSystemResource, Error> {
        let resource = FileSystemResource {
            id: FileSystemId::new(),
            name,
        };
        let transaction = self.database.begin_write().map_err(metadata_error)?;
        {
            let mut table = transaction.open_table(CATALOG).map_err(metadata_error)?;
            table
                .insert(
                    resource.id.0.as_bytes().as_slice(),
                    encode(&CatalogEntry {
                        resource: resource.clone(),
                        deleted: false,
                    })?
                    .as_slice(),
                )
                .map_err(metadata_error)?;
        }
        transaction.commit().map_err(metadata_error)?;
        Ok(resource)
    }

    pub fn open_filesystem<PeerId>(
        &self,
        id: FileSystemId,
        node_id: PeerId,
    ) -> Result<FileSystem<PeerId>, Error>
    where
        PeerId: Clone
            + std::fmt::Debug
            + Ord
            + Serialize
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + 'static,
    {
        if !self.list()?.iter().any(|resource| resource.id == id) {
            return Err(Error::NotFound);
        }
        FileSystem::open(self.root.join(id.0.to_string()), node_id)
    }

    pub fn list(&self) -> Result<Vec<FileSystemResource>, Error> {
        Ok(self
            .snapshot()?
            .into_iter()
            .filter(|entry| !entry.deleted)
            .map(|entry| entry.resource)
            .collect())
    }

    pub fn delete(&self, id: FileSystemId) -> Result<(), Error> {
        let transaction = self.database.begin_write().map_err(metadata_error)?;
        {
            let mut table = transaction.open_table(CATALOG).map_err(metadata_error)?;
            let mut entry: CatalogEntry = table
                .get(id.0.as_bytes().as_slice())
                .map_err(metadata_error)?
                .map(|value| postcard::from_bytes(value.value()).map_err(metadata_error))
                .transpose()?
                .ok_or(Error::NotFound)?;
            if entry.deleted {
                return Err(Error::NotFound);
            }
            entry.deleted = true;
            table
                .insert(id.0.as_bytes().as_slice(), encode(&entry)?.as_slice())
                .map_err(metadata_error)?;
        }
        transaction.commit().map_err(metadata_error)
    }

    pub fn snapshot(&self) -> Result<Vec<CatalogEntry>, Error> {
        let transaction = self.database.begin_read().map_err(metadata_error)?;
        let table = transaction.open_table(CATALOG).map_err(metadata_error)?;
        table
            .iter()
            .map_err(metadata_error)?
            .map(|entry| {
                let (_, value) = entry.map_err(metadata_error)?;
                postcard::from_bytes(value.value()).map_err(metadata_error)
            })
            .collect()
    }

    pub fn import_snapshot(&self, entries: &[CatalogEntry]) -> Result<(), Error> {
        if entries
            .iter()
            .any(|entry| entry.resource.id.0.get_version_num() != 7)
        {
            return Err(Error::InvalidResourceId);
        }
        let transaction = self.database.begin_write().map_err(metadata_error)?;
        {
            let mut table = transaction.open_table(CATALOG).map_err(metadata_error)?;
            for incoming in entries {
                let key = incoming.resource.id.0.as_bytes();
                let existing = table
                    .get(key.as_slice())
                    .map_err(metadata_error)?
                    .map(|value| {
                        postcard::from_bytes::<CatalogEntry>(value.value()).map_err(metadata_error)
                    })
                    .transpose()?;
                let merged = match existing {
                    Some(current) if current.deleted => current,
                    Some(_) if !incoming.deleted => continue,
                    _ => incoming.clone(),
                };
                table
                    .insert(key.as_slice(), encode(&merged)?.as_slice())
                    .map_err(metadata_error)?;
            }
        }
        transaction.commit().map_err(metadata_error)
    }
}

fn encode(entry: &CatalogEntry) -> Result<Vec<u8>, Error> {
    postcard::to_allocvec(entry).map_err(metadata_error)
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}
