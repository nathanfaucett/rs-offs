use std::sync::Arc;

use bytes::Bytes;
use ofdb_kv::{Client, Database, SharedDatabase, Value};
use ofdb_platform::BoxFuture;
use uuid::Uuid;

use crate::{
    Error,
    content::{ContentStore, Staged, StagedObject},
    managed_copy::{ManagedCopy, Storage},
    root::Root,
};

fn error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}
fn revision_key(file: Uuid, revision: Uuid) -> String {
    format!("revision:{file}:{revision}")
}

struct SharedRoot {
    host: SharedDatabase,
    name: String,
    state: Client,
}

impl Root for SharedRoot {
    fn prepare(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
    fn retired(&self) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async { Ok(self.state.get("retired").await.map_err(error)?.is_some()) })
    }
    fn retire(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async {
            self.state
                .set("retired", Value::Bool(true), None)
                .await
                .map_err(error)
        })
    }
    fn remove(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async {
            for kind in ["metadata", "content"] {
                let name = format!("{}:{kind}", self.name);
                match self.host.namespace(&format!("kv:{name}")) {
                    Err(source) if source.kind() == std::io::ErrorKind::PermissionDenied => {
                        continue;
                    }
                    Err(source) => return Err(error(source)),
                    Ok(_) => {}
                }
                Database::open_shared(&self.host, &name)
                    .map_err(error)?
                    .retire_and_remove()
                    .await
                    .map_err(error)?;
            }
            Ok(())
        })
    }
}

#[derive(Clone)]
struct SharedContent(Client);

impl ContentStore for SharedContent {
    fn has(&self, file: Uuid, revision: Uuid) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            Ok(self
                .0
                .get(&revision_key(file, revision))
                .await
                .map_err(error)?
                .is_some())
        })
    }
    fn read(&self, file: Uuid, revision: Uuid) -> BoxFuture<'_, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            match self
                .0
                .get(&revision_key(file, revision))
                .await
                .map_err(error)?
            {
                Some(Value::Blob(bytes)) => Ok(bytes),
                None => Err(Error::ContentUnavailable),
                _ => Err(error("invalid content revision")),
            }
        })
    }
    fn read_range(
        &self,
        file: Uuid,
        revision: Uuid,
        offset: u64,
        length: u32,
    ) -> BoxFuture<'_, Result<Bytes, Error>> {
        Box::pin(async move {
            let bytes = self.read(file, revision).await?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let end = start.saturating_add(length as usize).min(bytes.len());
            Ok(Bytes::copy_from_slice(&bytes[start..end]))
        })
    }
    fn stage(
        &self,
        file: Uuid,
        revision: Uuid,
    ) -> BoxFuture<'_, Result<Box<dyn StagedObject>, Error>> {
        Box::pin(async move {
            Ok(Box::new(SharedStage {
                content: self.clone(),
                stage: format!("stage:{}", Uuid::now_v7()),
                destination: revision_key(file, revision),
                bytes: Vec::new(),
                published: false,
                committed: false,
            }) as Box<dyn StagedObject>)
        })
    }
    fn remove(&self, file: Uuid) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut transaction = self.0.transaction().await.map_err(error)?;
            for (key, _) in transaction
                .scan_prefix(&format!("revision:{file}:"))
                .await
                .map_err(error)?
            {
                transaction.delete(&key).await.map_err(error)?;
            }
            transaction.commit().await.map_err(error)
        })
    }
}

struct SharedStage {
    content: SharedContent,
    stage: String,
    destination: String,
    bytes: Vec<u8>,
    published: bool,
    committed: bool,
}

impl Staged for SharedStage {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        self.write_end(bytes)
    }
    fn copy_from(&mut self, file: Uuid, revision: Uuid) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.bytes = self.content.read(file, revision).await?;
            Ok(())
        })
    }
    fn write_at<'a>(
        &'a mut self,
        offset: u64,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let start = usize::try_from(offset).map_err(error)?;
            let end = start
                .checked_add(bytes.len())
                .ok_or_else(|| error("content size overflow"))?;
            if end > self.bytes.len() {
                self.bytes
                    .try_reserve(end - self.bytes.len())
                    .map_err(error)?;
                self.bytes.resize(end, 0);
            }
            self.bytes[start..end].copy_from_slice(bytes);
            Ok(())
        })
    }
    fn write_end<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.bytes.try_reserve(bytes.len()).map_err(error)?;
            self.bytes.extend_from_slice(bytes);
            Ok(())
        })
    }
    fn sync(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.content
                .0
                .set(&self.stage, Value::Blob(self.bytes.clone()), None)
                .await
                .map_err(error)
        })
    }
    fn publish(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut transaction = self.content.0.transaction().await.map_err(error)?;
            if transaction
                .get(&self.destination)
                .await
                .map_err(error)?
                .is_some()
            {
                return Err(Error::AlreadyExists);
            }
            transaction
                .set(&self.destination, Value::Blob(self.bytes.clone()), None)
                .await
                .map_err(error)?;
            transaction.commit().await.map_err(error)?;
            self.published = true;
            Ok(())
        })
    }
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for SharedStage {
    fn drop(&mut self) {
        // Drop cannot await. The worker also works when the caller is inside a runtime.
        let _ = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    futures::executor::block_on(async {
                        let mut transaction = self.content.0.transaction().await.map_err(error)?;
                        transaction.delete(&self.stage).await.map_err(error)?;
                        if self.published && !self.committed {
                            transaction.delete(&self.destination).await.map_err(error)?;
                        }
                        transaction.commit().await.map_err(error)
                    })
                })
                .join()
        });
    }
}

async fn locate(host: &SharedDatabase, name: &str) -> Result<Arc<ManagedCopy>, Error> {
    let state = Database::open_shared(host, &format!("{name}:root"))
        .map_err(error)?
        .client();
    let root = SharedRoot {
        host: host.clone(),
        name: name.to_owned(),
        state,
    };
    ManagedCopy::shared(format!("shared:{}:{name}", host.instance_id()), move |_| {
        Box::pin(async move { Ok(Box::new(root) as Box<dyn crate::root::RootObject>) })
    })
    .await
}

pub async fn open(host: &SharedDatabase, name: &str) -> Result<Arc<ManagedCopy>, Error> {
    let copy = locate(host, name).await?;
    copy.refresh().await?;
    if copy.storage_is_open() {
        return Ok(copy);
    }
    let mut storage = copy.try_storage()?;
    copy.check()?;
    if storage.is_none() {
        let metadata = Database::open_shared(host, &format!("{name}:metadata")).map_err(error)?;
        let content = Database::open_shared(host, &format!("{name}:content"))
            .map_err(error)?
            .client();
        for (key, _) in content.scan_prefix("stage:").await.map_err(error)? {
            content.delete(&key).await.map_err(error)?;
        }
        *storage = Some(Storage {
            local_state: Some(
                Database::open_shared(host, &format!("{name}:root"))
                    .map_err(error)?
                    .client(),
            ),
            client: metadata.client(),
            metadata,
            content: Arc::new(SharedContent(content)),
        });
    }
    drop(storage);
    Ok(copy)
}

pub async fn remove(host: &SharedDatabase, name: &str) -> Result<(), Error> {
    locate(host, name).await?.remove().await
}
