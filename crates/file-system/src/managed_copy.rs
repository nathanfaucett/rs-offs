use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use ofdb_kv::{Client, Database};
use tokio::sync::{RwLock, RwLockReadGuard, watch};

use crate::Error;

pub(crate) struct NativeCopy {
    pub(crate) content_root: PathBuf,
    pub(crate) database: Database,
    pub(crate) metadata: Client,
    _lease: File,
}

pub(crate) struct ManagedCopy {
    root: PathBuf,
    marker: PathBuf,
    lease: PathBuf,
    retired: watch::Sender<bool>,
    native: RwLock<Option<NativeCopy>>,
    pub(crate) execution: tokio::sync::Mutex<()>,
    streams: Mutex<Vec<Weak<Mutex<Option<File>>>>>,
}

type Copies = BTreeMap<PathBuf, Weak<ManagedCopy>>;
static COPIES: OnceLock<Mutex<Copies>> = OnceLock::new();

impl ManagedCopy {
    fn locate(root: &Path) -> Result<Arc<Self>, Error> {
        let root = std::path::absolute(root)?;
        let parent = root.parent().ok_or(Error::InvalidPath)?;
        fs::create_dir_all(parent)?;
        let root = parent
            .canonicalize()?
            .join(root.file_name().ok_or(Error::InvalidPath)?);
        if fs::symlink_metadata(&root).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(Error::InvalidPath);
        }
        let mut copies = COPIES
            .get_or_init(Mutex::default)
            .lock()
            .expect("copy registry lock");
        copies.retain(|_, copy| copy.strong_count() != 0);
        if let Some(copy) = copies.get(&root).and_then(Weak::upgrade) {
            return Ok(copy);
        }
        let marker = root.with_file_name(format!(
            ".{}.retired",
            root.file_name().expect("root name").to_string_lossy()
        ));
        let copy = Arc::new(Self {
            retired: watch::channel(marker.try_exists()?).0,
            lease: marker.with_extension("lease"),
            marker,
            root: root.clone(),
            native: RwLock::new(None),
            execution: tokio::sync::Mutex::new(()),
            streams: Mutex::new(Vec::new()),
        });
        copies.insert(root, Arc::downgrade(&copy));
        Ok(copy)
    }

    pub(crate) fn open(root: &Path) -> Result<Arc<Self>, Error> {
        let copy = Self::locate(root)?;
        copy.check()?;
        if copy.native.try_read().is_ok_and(|native| native.is_some()) {
            return Ok(copy);
        }
        let mut native = copy.native.try_write().map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "filesystem is busy",
            ))
        })?;
        copy.check()?;
        if native.is_none() {
            let lease = copy.lock_native()?;
            copy.check()?;
            fs::create_dir_all(&copy.root)?;
            let content_root = copy.root.join(".data");
            fs::create_dir_all(&content_root)?;
            let database = Database::open_with_table(copy.root.join("metadata.redb"), "metadata")
                .map_err(|error| Error::Metadata(error.to_string()))?;
            let metadata = database.client();
            *native = Some(NativeCopy {
                content_root,
                database,
                metadata,
                _lease: lease,
            });
        }
        drop(native);
        Ok(copy)
    }

    fn lock_native(&self) -> Result<File, Error> {
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lease)?;
        lease.try_lock().map_err(std::io::Error::from)?;
        Ok(lease)
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        if *self.retired.borrow() || self.marker.try_exists()? {
            Err(Error::PermissionDenied)
        } else {
            Ok(())
        }
    }

    pub(crate) async fn admit(&self) -> Result<RwLockReadGuard<'_, Option<NativeCopy>>, Error> {
        let native = self.wait(self.native.read()).await?;
        self.check()?;
        if native.is_none() {
            return Err(Error::PermissionDenied);
        }
        Ok(native)
    }

    pub(crate) async fn retired(&self) {
        let mut receiver = self.retired.subscribe();
        let _ = receiver.wait_for(|retired| *retired).await;
    }

    pub(crate) async fn wait<F: Future>(&self, future: F) -> Result<F::Output, Error> {
        self.check()?;
        tokio::select! {
            biased;
            () = self.retired() => Err(Error::PermissionDenied),
            output = future => Ok(output),
        }
    }

    pub(crate) fn register_stream(&self, file: &Arc<Mutex<Option<File>>>) {
        let mut streams = self.streams.lock().expect("stream registry lock");
        streams.retain(|stream| stream.strong_count() != 0);
        streams.push(Arc::downgrade(file));
    }

    pub(crate) async fn remove(root: &Path) -> Result<(), Error> {
        let copy = Self::locate(root)?;
        {
            let marker = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&copy.marker)?;
            marker.sync_all()?;
            File::open(copy.marker.parent().expect("marker parent"))?.sync_all()?;
            copy.retired.send_replace(true);
        }
        let mut native = copy.native.write().await;
        for stream in copy
            .streams
            .lock()
            .expect("stream registry lock")
            .drain(..)
            .filter_map(|stream| stream.upgrade())
        {
            stream.lock().expect("stream file lock").take();
        }
        drop(native.take());
        let _lease = copy.lock_native()?;
        match fs::remove_dir_all(&copy.root) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        File::open(copy.root.parent().expect("copy parent"))?.sync_all()?;
        Ok(())
    }
}
