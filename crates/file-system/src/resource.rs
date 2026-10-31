#![cfg_attr(target_arch = "wasm32", allow(dead_code))] // browser construction lands with the browser backend

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

#[cfg(not(target_arch = "wasm32"))]
use ofdb_kv::Database;
use ofdb_kv::{Client, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;

const CATALOG_PREFIX: &str = "catalog:";
const PROJECTED_PREFIX: &str = "projected:";
const CATALOG_TABLE: &str = "filesystem_catalog";

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

    fn key(&self) -> String {
        format!("{}{}", CATALOG_PREFIX, self.0)
    }

    fn projected_key(&self) -> String {
        format!("{}{}", PROJECTED_PREFIX, self.0)
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
    #[cfg(not(target_arch = "wasm32"))]
    namespace_root: PathBuf,
    client: Client,
}

impl FileSystemCatalog {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(namespace_root: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let root = namespace_root.as_ref().to_owned();
        std::fs::create_dir_all(&root)?;
        let database = Database::open_with_table(root.join("catalog.redb"), CATALOG_TABLE)
            .map_err(metadata_error)?;
        Ok(Self {
            namespace_root: root.join("filesystems"),
            client: database.client(),
        })
    }

    pub async fn create(&self, name: Option<String>) -> Result<FileSystemResource, Error> {
        let resource = FileSystemResource {
            id: FileSystemId::new(),
            name,
        };
        self.client
            .set(
                &resource.id.key(),
                Value::Blob(encode(&CatalogEntry {
                    resource: resource.clone(),
                    deleted: false,
                })?),
                None,
            )
            .await
            .map_err(metadata_error)?;
        Ok(resource)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_filesystem<PeerId>(
        &self,
        id: FileSystemId,
        node_id: PeerId,
    ) -> Result<crate::FileSystem<PeerId>, Error>
    where
        PeerId: Clone
            + std::fmt::Debug
            + Ord
            + Serialize
            + serde::de::DeserializeOwned
            + ofdb_platform::MaybeSend
            + ofdb_platform::MaybeSync
            + 'static,
    {
        if !self.list().await?.iter().any(|resource| resource.id == id) {
            return Err(Error::NotFound);
        }
        crate::FileSystem::open(self.namespace_root.join(id.0.to_string()), node_id).await
    }

    pub async fn list(&self) -> Result<Vec<FileSystemResource>, Error> {
        Ok(self
            .snapshot()
            .await?
            .into_iter()
            .filter(|entry| !entry.deleted)
            .map(|entry| entry.resource)
            .collect())
    }

    pub async fn delete(&self, id: FileSystemId) -> Result<(), Error> {
        let mut transaction = self.client.transaction().await.map_err(metadata_error)?;
        let mut entry: CatalogEntry = transaction
            .get(&id.key())
            .await
            .map_err(metadata_error)?
            .map(decode_value)
            .transpose()?
            .ok_or(Error::NotFound)?;
        if entry.deleted {
            return Err(Error::NotFound);
        }
        entry.deleted = true;
        transaction
            .set(&id.key(), Value::Blob(encode(&entry)?), None)
            .await
            .map_err(metadata_error)?;
        transaction.commit().await.map_err(metadata_error)
    }

    pub async fn mark_projected_selected(&self, id: FileSystemId) -> Result<bool, Error> {
        let mut transaction = self.client.transaction().await.map_err(metadata_error)?;
        let key = id.projected_key();
        let exists = transaction
            .get(&key)
            .await
            .map_err(metadata_error)?
            .is_some();
        if exists {
            transaction
                .set(&key, Value::Integer(1), None)
                .await
                .map_err(metadata_error)?;
        }
        transaction.commit().await.map_err(metadata_error)?;
        Ok(exists)
    }

    pub async fn mark_projected(&self, id: FileSystemId) -> Result<(), Error> {
        self.client
            .set(&id.projected_key(), Value::Integer(1), None)
            .await
            .map_err(metadata_error)
    }

    pub async fn projected_selected(&self) -> Result<Vec<FileSystemId>, Error> {
        self.client
            .scan_prefix(PROJECTED_PREFIX)
            .await
            .map_err(metadata_error)?
            .into_iter()
            .filter(|(_, value)| value == &Value::Integer(1))
            .map(|(key, _)| projected_id(&key))
            .collect()
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Permanently retires this device's managed copy without changing catalog identity or
    /// tombstones. Cancellation or failure leaves the durable gate closed; call again to
    /// finish cleanup.
    pub async fn remove_managed_copy(&self, id: FileSystemId) -> Result<(), Error> {
        if !self
            .snapshot()
            .await?
            .iter()
            .any(|entry| entry.resource.id == id)
        {
            return Err(Error::NotFound);
        }
        crate::native::remove_root(&self.namespace_root.join(id.0.to_string())).await?;
        let mut transaction = self.client.transaction().await.map_err(metadata_error)?;
        let key = id.projected_key();
        if transaction
            .get(&key)
            .await
            .map_err(metadata_error)?
            .is_some()
        {
            transaction
                .set(&key, Value::Integer(0), None)
                .await
                .map_err(metadata_error)?;
        }
        transaction.commit().await.map_err(metadata_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Retires a selected projection using the same owner lifecycle as created copies.
    pub async fn evict_projected_copy(&self, id: FileSystemId) -> Result<bool, Error> {
        if !self.projected_selected().await?.contains(&id) {
            return Ok(false);
        }
        self.remove_managed_copy(id).await?;
        Ok(true)
    }

    pub async fn snapshot(&self) -> Result<Vec<CatalogEntry>, Error> {
        self.client
            .scan_prefix(CATALOG_PREFIX)
            .await
            .map_err(metadata_error)?
            .into_iter()
            .map(|(_, value)| decode_value(value))
            .collect()
    }

    pub async fn apply_tombstone(&self, id: FileSystemId) -> Result<(), Error> {
        let existing = self
            .snapshot()
            .await?
            .into_iter()
            .find(|entry| entry.resource.id == id);
        let resource = existing
            .map(|entry| entry.resource)
            .unwrap_or(FileSystemResource { id, name: None });
        self.import_snapshot(&[CatalogEntry {
            resource,
            deleted: true,
        }])
        .await
    }

    pub async fn import_snapshot(&self, entries: &[CatalogEntry]) -> Result<(), Error> {
        if entries
            .iter()
            .any(|entry| entry.resource.id.0.get_version_num() != 7)
        {
            return Err(Error::InvalidResourceId);
        }
        let mut transaction = self.client.transaction().await.map_err(metadata_error)?;
        for incoming in entries {
            let key = incoming.resource.id.key();
            let existing = transaction
                .get(&key)
                .await
                .map_err(metadata_error)?
                .map(decode_value)
                .transpose()?;
            let merged = match existing {
                Some(current) if current.deleted => current,
                Some(_) if !incoming.deleted => continue,
                _ => incoming.clone(),
            };
            transaction
                .set(&key, Value::Blob(encode(&merged)?), None)
                .await
                .map_err(metadata_error)?;
        }
        transaction.commit().await.map_err(metadata_error)
    }
}

fn encode(entry: &CatalogEntry) -> Result<Vec<u8>, Error> {
    postcard::to_allocvec(entry).map_err(metadata_error)
}

fn decode_value(value: Value) -> Result<CatalogEntry, Error> {
    let Value::Blob(bytes) = value else {
        return Err(metadata_error("catalog value is not a blob"));
    };
    postcard::from_bytes(&bytes).map_err(metadata_error)
}

fn projected_id(key: &str) -> Result<FileSystemId, Error> {
    let id = key
        .strip_prefix(PROJECTED_PREFIX)
        .and_then(|rest| Uuid::parse_str(rest).ok())
        .ok_or(Error::InvalidResourceId)?;
    Ok(FileSystemId(id))
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}
