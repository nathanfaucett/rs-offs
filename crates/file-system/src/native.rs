use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use ofdb_kv::Database;
use ofdb_platform::BoxFuture;
use uuid::Uuid;

use crate::{
    Error,
    content::{ContentStore, StagedObject},
    managed_copy::{ManagedCopy, Storage},
    root::Root,
    staged_content::StagedContent,
};

/// Native root identity: a canonical directory, an advisory lease, and a durable
/// retirement marker.
struct NativeRoot {
    root: PathBuf,
    marker: PathBuf,
    lease: Option<File>,
}

impl NativeRoot {
    fn new(root: PathBuf) -> Self {
        let marker = root.with_file_name(format!(
            ".{}.retired",
            root.file_name().expect("root name").to_string_lossy()
        ));
        Self {
            root,
            marker,
            lease: None,
        }
    }

    fn acquire_lease(&mut self) -> Result<(), Error> {
        if self.lease.is_some() {
            return Ok(());
        }
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.marker.with_extension("lease"))?;
        lease.try_lock().map_err(std::io::Error::from)?;
        self.lease = Some(lease);
        Ok(())
    }
}

impl Root for NativeRoot {
    fn prepare(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { self.acquire_lease() })
    }

    fn retired(&self) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async { Ok(self.marker.try_exists()?) })
    }

    fn retire(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let marker = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.marker)?;
            marker.sync_all()?;
            File::open(self.marker.parent().expect("marker parent"))?.sync_all()?;
            Ok(())
        })
    }

    fn remove(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.lease = None;
            let lease = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.marker.with_extension("lease"))?;
            lease.try_lock().map_err(std::io::Error::from)?;
            match fs::remove_dir_all(&self.root) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            File::open(self.root.parent().expect("root parent"))?.sync_all()?;
            Ok(())
        })
    }
}

/// Native content store: complete revisions under a content root, staged through a
/// temporary sibling and published by rename.
struct NativeContent {
    content_root: PathBuf,
}

impl NativeContent {
    fn path(&self, file_id: Uuid, revision: Uuid) -> PathBuf {
        self.content_root
            .join(file_id.to_string())
            .join(revision.to_string())
    }

    fn read_error(error: std::io::Error) -> Error {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::ContentUnavailable
        } else {
            Error::Io(error)
        }
    }
}

impl ContentStore for NativeContent {
    fn has(&self, file_id: Uuid, revision: Uuid) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move { Ok(self.path(file_id, revision).is_file()) })
    }

    fn read(&self, file_id: Uuid, revision: Uuid) -> BoxFuture<'_, Result<Vec<u8>, Error>> {
        Box::pin(async move { fs::read(self.path(file_id, revision)).map_err(Self::read_error) })
    }

    fn read_range(
        &self,
        file_id: Uuid,
        revision: Uuid,
        offset: u64,
        length: u32,
    ) -> BoxFuture<'_, Result<Bytes, Error>> {
        Box::pin(async move {
            let mut file = File::open(self.path(file_id, revision)).map_err(Self::read_error)?;
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(offset))?;
            let mut chunk = vec![0; length as usize];
            let read = file.read(&mut chunk)?;
            chunk.truncate(read);
            Ok(Bytes::from(chunk))
        })
    }

    fn stage(
        &self,
        file_id: Uuid,
        revision: Uuid,
    ) -> BoxFuture<'_, Result<Box<dyn StagedObject>, Error>> {
        let content_root = self.content_root.clone();
        Box::pin(async move {
            let destination = content_root
                .join(file_id.to_string())
                .join(revision.to_string());
            Ok(Box::new(StagedContent::new(content_root, destination)?) as Box<dyn StagedObject>)
        })
    }

    fn remove(&self, file_id: Uuid) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            match fs::remove_dir_all(self.content_root.join(file_id.to_string())) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        })
    }
}

/// Opens the native managed copy for a filesystem root.
pub(crate) async fn open(root: &Path) -> Result<Arc<ManagedCopy>, Error> {
    let copy = locate(root).await?;
    copy.observe_retirement().await?;
    copy.check()?;
    if copy.storage_is_open() {
        return Ok(copy);
    }
    let mut native_root = copy.try_root()?;
    copy.check()?;
    native_root.prepare().await?;
    copy.check()?;
    drop(native_root);
    let mut storage = copy.try_storage()?;
    copy.check()?;
    let opened = storage.is_none();
    if opened {
        fs::create_dir_all(root)?;
        let content_root = root.join(".data");
        fs::create_dir_all(&content_root)?;
        let database = Database::open_with_table(root.join("metadata.redb"), "metadata")
            .map_err(|error| Error::Metadata(error.to_string()))?;
        let client = database.client();
        *storage = Some(Storage {
            metadata: database,
            client,
            content: Arc::new(NativeContent { content_root }),
        });
    }
    drop(storage);
    Ok(copy)
}

/// Permanently retires and removes the native managed copy at a filesystem root.
pub(crate) async fn remove_root(root: &Path) -> Result<(), Error> {
    locate(root).await?.remove().await
}

async fn locate(root: &Path) -> Result<Arc<ManagedCopy>, Error> {
    let root = std::path::absolute(root)?;
    let parent = root.parent().ok_or(Error::InvalidPath)?;
    fs::create_dir_all(parent)?;
    let root = parent
        .canonicalize()?
        .join(root.file_name().ok_or(Error::InvalidPath)?);
    if fs::symlink_metadata(&root).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(Error::InvalidPath);
    }
    ManagedCopy::shared(root.to_string_lossy().into_owned(), move |key| {
        let root = PathBuf::from(key);
        Box::pin(
            async move { Ok(Box::new(NativeRoot::new(root)) as Box<dyn crate::root::RootObject>) },
        )
    })
    .await
}
