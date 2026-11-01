// wasm32: browser storage handles are Rc-based, so Arc here is a single-threaded refcount
#![cfg_attr(
    target_arch = "wasm32",
    allow(dead_code, clippy::arc_with_non_send_sync)
)]

use std::{collections::BTreeMap, future::Future, sync::Arc, sync::Weak};

#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Mutex, OnceLock};

#[cfg(target_arch = "wasm32")]
use std::cell::RefCell;

use ofdb_kv::{Client, Database};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, watch};

use crate::{Error, content::ContentObject, root::RootObject};

pub(crate) struct Storage {
    pub(crate) metadata: Database,
    pub(crate) client: Client,
    pub(crate) content: Arc<dyn ContentObject>,
}

pub(crate) struct ManagedCopy {
    retired: watch::Sender<bool>,
    root: RwLock<Box<dyn RootObject>>,
    storage: RwLock<Option<Storage>>,
    pub(crate) execution: tokio::sync::Mutex<()>,
}

type Copies = BTreeMap<String, Weak<ManagedCopy>>;

#[cfg(not(target_arch = "wasm32"))]
static COPIES: OnceLock<Mutex<Copies>> = OnceLock::new();

#[cfg(target_arch = "wasm32")]
thread_local! {
    static COPIES: RefCell<Copies> = const { RefCell::new(BTreeMap::new()) };
}

impl ManagedCopy {
    /// Returns the shared managed copy for `key`, creating it with `build` when absent.
    pub(crate) async fn shared(
        key: String,
        build: impl FnOnce(
            &str,
        )
            -> ofdb_platform::BoxFuture<'static, Result<Box<dyn RootObject>, Error>>,
    ) -> Result<Arc<Self>, Error> {
        if let Some(copy) = registry(|copies| {
            copies.retain(|_, copy| copy.strong_count() != 0);
            copies.get(&key).and_then(Weak::upgrade)
        }) {
            return Ok(copy);
        }
        let root = build(&key).await?;
        let copy = Arc::new(Self {
            retired: watch::channel(false).0,
            root: RwLock::new(root),
            storage: RwLock::new(None),
            execution: tokio::sync::Mutex::new(()),
        });
        Ok(registry(|copies| {
            copies.retain(|_, copy| copy.strong_count() != 0);
            if let Some(existing) = copies.get(&key).and_then(Weak::upgrade) {
                return existing;
            }
            copies.insert(key, Arc::downgrade(&copy));
            copy
        }))
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        if *self.retired.borrow() {
            Err(Error::PermissionDenied)
        } else {
            Ok(())
        }
    }

    pub(crate) fn storage_is_open(&self) -> bool {
        self.storage
            .try_read()
            .is_ok_and(|storage| storage.is_some())
    }

    pub(crate) fn try_root(&self) -> Result<RwLockWriteGuard<'_, Box<dyn RootObject>>, Error> {
        self.root.try_write().map_err(|_| busy())
    }

    pub(crate) fn try_storage(&self) -> Result<RwLockWriteGuard<'_, Option<Storage>>, Error> {
        self.storage.try_write().map_err(|_| busy())
    }

    /// Observes the durable retirement record.
    pub(crate) async fn observe_retirement(&self) -> Result<(), Error> {
        if self.root.read().await.retired().await? {
            self.retired.send_replace(true);
        }
        Ok(())
    }

    /// Observes the durable retirement record and fails when the root was retired.
    pub(crate) async fn refresh(&self) -> Result<(), Error> {
        self.observe_retirement().await?;
        self.check()
    }

    pub(crate) async fn admit(&self) -> Result<RwLockReadGuard<'_, Option<Storage>>, Error> {
        let storage = self.wait(self.storage.read()).await?;
        self.check()?;
        if storage.is_none() {
            return Err(Error::PermissionDenied);
        }
        Ok(storage)
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

    pub(crate) async fn remove(&self) -> Result<(), Error> {
        self.root.read().await.retire().await?;
        self.retired.send_replace(true);
        *self.storage.write().await = None;
        self.root.write().await.remove().await
    }
}

fn busy() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "filesystem is busy",
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn registry<R>(apply: impl FnOnce(&mut Copies) -> R) -> R {
    let mut copies = COPIES
        .get_or_init(Mutex::default)
        .lock()
        .expect("copy registry lock");
    apply(&mut copies)
}

#[cfg(target_arch = "wasm32")]
fn registry<R>(apply: impl FnOnce(&mut Copies) -> R) -> R {
    COPIES.with(|copies| apply(&mut copies.borrow_mut()))
}
