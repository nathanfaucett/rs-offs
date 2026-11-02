//! Browser-specific implementation of the file system.
//!
//! Uses OPFS for content and IndexedDB for metadata, with Web Locks for
//! exclusive root ownership and durable tombstones for retirement.

use std::sync::Arc;

use js_sys::Uint8Array;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_futures::JsFuture;

use ofdb_kv::{Database, Value};
use ofdb_platform::BoxFuture;
use uuid::Uuid;

use crate::{
    Error,
    content::{ContentStore, Staged, StagedObject},
    managed_copy::{ManagedCopy, Storage},
    root::{Root, RootObject},
};

#[wasm_bindgen(module = "/src/browser_storage.js")]
extern "C" {
    #[wasm_bindgen(js_name = storageRoot)]
    fn storage_root() -> js_sys::Promise;
    #[wasm_bindgen(js_name = removeTree)]
    async fn remove_tree(dir: &web_sys::FileSystemDirectoryHandle, name: &str);
    #[wasm_bindgen(js_name = deleteDatabase)]
    fn delete_database(name: &str);
    #[wasm_bindgen(js_name = acquireLock)]
    fn acquire_lock(name: &str) -> js_sys::Promise;
}

fn metadata_error(error: impl std::fmt::Display) -> Error {
    Error::Metadata(error.to_string())
}

fn storage_error(error: wasm_bindgen::JsValue) -> Error {
    Error::Io(std::io::Error::other(format!(
        "browser storage failed: {error:?}"
    )))
}

async fn js_promise(promise: js_sys::Promise) -> Result<wasm_bindgen::JsValue, Error> {
    JsFuture::from(promise).await.map_err(|error| {
        Error::Io(std::io::Error::other(format!(
            "browser storage failed: {error:?}"
        )))
    })
}

async fn origin_root() -> Result<web_sys::FileSystemDirectoryHandle, Error> {
    js_promise(storage_root())
        .await
        .map(|handle| handle.unchecked_into())
}

fn directory_options(create: bool) -> web_sys::FileSystemGetDirectoryOptions {
    let options = web_sys::FileSystemGetDirectoryOptions::new();
    options.set_create(create);
    options
}

fn file_options(create: bool) -> web_sys::FileSystemGetFileOptions {
    let options = web_sys::FileSystemGetFileOptions::new();
    options.set_create(create);
    options
}

async fn child_directory(
    parent: &web_sys::FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> Result<web_sys::FileSystemDirectoryHandle, Error> {
    js_promise(parent.get_directory_handle_with_options(name, &directory_options(create)))
        .await
        .map(|handle| handle.unchecked_into())
        .map_err(|_| Error::ContentUnavailable)
}

async fn revision_file(
    root: &web_sys::FileSystemDirectoryHandle,
    file_id: Uuid,
    revision: Uuid,
    create: bool,
) -> Result<web_sys::FileSystemFileHandle, Error> {
    let directory = child_directory(root, &file_id.to_string(), create).await?;
    js_promise(directory.get_file_handle_with_options(&revision.to_string(), &file_options(create)))
        .await
        .map(|handle| handle.unchecked_into())
        .map_err(|_| Error::ContentUnavailable)
}

fn metadata_database(root: &str) -> String {
    format!("offs:{root}:metadata")
}

fn tombstone_database(root: &str) -> String {
    format!("offs:{root}:retirement")
}

fn tombstone_key(root: &str) -> String {
    format!("retired:{root}")
}

/// Browser root identity: a single root string, owned through an exclusive Web Lock
/// scoped to the origin, retired through a durable tombstone in browser key-value storage.
struct BrowserRoot {
    origin: web_sys::FileSystemDirectoryHandle,
    root: String,
    lock_name: String,
    tombstone: ofdb_kv::Client,
    release: Option<js_sys::Function>,
}

impl BrowserRoot {
    async fn new(root: &str) -> Result<Self, Error> {
        let tombstone = Database::open_indexeddb(&tombstone_database(root))
            .await
            .map_err(metadata_error)?
            .client();
        Ok(Self {
            origin: origin_root().await?,
            root: root.to_owned(),
            lock_name: format!("offs:{root}"),
            tombstone,
            release: None,
        })
    }
}

impl Drop for BrowserRoot {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.call0(&wasm_bindgen::JsValue::NULL);
        }
    }
}

impl Root for BrowserRoot {
    fn retired(&self) -> BoxFuture<'_, Result<bool, Error>> {
        let key = tombstone_key(&self.root);
        Box::pin(async move {
            let future = self.tombstone.get(&key);
            Ok(future.await.map_err(metadata_error)?.is_some())
        })
    }

    fn retire(&self) -> BoxFuture<'_, Result<(), Error>> {
        let key = tombstone_key(&self.root);
        Box::pin(async move {
            let future = self.tombstone.set(&key, Value::Integer(1), None);
            future.await.map_err(metadata_error)
        })
    }

    fn prepare(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        let lock_name = self.lock_name.clone();
        Box::pin(async move {
            let release = match js_promise(acquire_lock(&lock_name)).await {
                Ok(release) => release.unchecked_into(),
                Err(_) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "filesystem root is owned by another context",
                    )));
                }
            };
            self.release = Some(release);
            Ok(())
        })
    }

    fn remove(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.release = None;
            let root_dir = child_directory(&self.origin, &self.root, false).await;
            if let Ok(root_dir) = root_dir {
                remove_tree(&root_dir, &self.root).await;
            }
            delete_database(&metadata_database(&self.root));
            Ok(())
        })
    }
}

/// Browser content store: complete revisions in the Origin Private File System, written
/// whole and selected by metadata, never renamed or moved.
struct BrowserContent {
    root: web_sys::FileSystemDirectoryHandle,
}

impl ContentStore for BrowserContent {
    fn has(&self, file_id: Uuid, revision: Uuid) -> BoxFuture<'_, Result<bool, Error>> {
        let root = self.root.clone();
        Box::pin(async move { Ok(revision_file(&root, file_id, revision, false).await.is_ok()) })
    }

    fn read(&self, file_id: Uuid, revision: Uuid) -> BoxFuture<'_, Result<Vec<u8>, Error>> {
        let root = self.root.clone();
        Box::pin(async move {
            let range = read_range(&root, file_id, revision, 0, u32::MAX).await?;
            Ok(range.to_vec())
        })
    }

    fn read_range(
        &self,
        file_id: Uuid,
        revision: Uuid,
        offset: u64,
        length: u32,
    ) -> BoxFuture<'_, Result<bytes::Bytes, Error>> {
        let root = self.root.clone();
        Box::pin(async move {
            Ok(read_range(&root, file_id, revision, offset, length)
                .await?
                .into())
        })
    }

    fn stage(
        &self,
        file_id: Uuid,
        revision: Uuid,
    ) -> BoxFuture<'_, Result<Box<dyn StagedObject>, Error>> {
        let root = self.root.clone();
        Box::pin(async move {
            Ok(Box::new(BrowserStaged {
                root,
                file_id,
                revision,
                buffer: Vec::new(),
                published: false,
                committed: false,
            }) as Box<dyn StagedObject>)
        })
    }

    fn remove(&self, file_id: Uuid) -> BoxFuture<'_, Result<(), Error>> {
        let root = self.root.clone();
        Box::pin(async move {
            if let Ok(directory) = child_directory(&root, &file_id.to_string(), false).await {
                remove_tree(&directory, &file_id.to_string()).await;
            }
            Ok(())
        })
    }
}

async fn read_range(
    root: &web_sys::FileSystemDirectoryHandle,
    file_id: Uuid,
    revision: Uuid,
    offset: u64,
    length: u32,
) -> Result<Vec<u8>, Error> {
    let file = js_promise(
        revision_file(root, file_id, revision, false)
            .await?
            .get_file(),
    )
    .await
    .map_err(|_| Error::ContentUnavailable)?
    .unchecked_into::<web_sys::Blob>();
    let start = offset;
    let end = offset.saturating_add(length as u64).min(file.size() as u64);
    if start >= end {
        return Ok(Vec::new());
    }
    let slice = file
        .slice_with_f64_and_f64(start as f64, end as f64)
        .map_err(storage_error)?;
    let buffer = js_promise(slice.array_buffer()).await?;
    let bytes = Uint8Array::new(&buffer).to_vec();
    Ok(bytes)
}

struct BrowserStaged {
    root: web_sys::FileSystemDirectoryHandle,
    file_id: Uuid,
    revision: Uuid,
    buffer: Vec<u8>,
    published: bool,
    committed: bool,
}

impl Drop for BrowserStaged {
    fn drop(&mut self) {
        if self.published && !self.committed {
            let root = self.root.clone();
            let file_id = self.file_id;
            let revision = self.revision;
            wasm_bindgen_futures::spawn_local(async move {
                if let Ok(file) = revision_file(&root, file_id, revision, false).await {
                    if let Ok(directory) = child_directory(&root, &file_id.to_string(), false).await {
                        remove_tree(&directory, &revision.to_string()).await;
                    }
                    drop(file);
                }
            });
        }
    }
}

impl BrowserStaged {
    async fn write_whole(&mut self) -> Result<(), Error> {
        let file = revision_file(&self.root, self.file_id, self.revision, true).await?;
        let stream = js_promise(file.create_writable()).await?;
        let stream = stream.unchecked_into::<web_sys::FileSystemWritableFileStream>();
        let write = stream
            .write_with_u8_array(self.buffer.as_slice())
            .map_err(storage_error)?;
        js_promise(write).await?;
        js_promise(stream.close()).await?;
        self.published = true;
        Ok(())
    }
}

impl Staged for BrowserStaged {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.buffer.extend_from_slice(bytes);
            Ok(())
        })
    }

    fn copy_from<'a>(
        &'a mut self,
        file_id: Uuid,
        revision: Uuid,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.buffer = read_range(&self.root, file_id, revision, 0, u32::MAX).await?;
            Ok(())
        })
    }

    fn write_at<'a>(
        &'a mut self,
        offset: u64,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let offset = offset as usize;
            if offset > self.buffer.len() {
                self.buffer.resize(offset, 0);
            }
            let end = (offset + bytes.len()).max(self.buffer.len());
            self.buffer.resize(end, 0);
            self.buffer[offset..offset + bytes.len()].copy_from_slice(bytes);
            Ok(())
        })
    }

    fn write_end<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.buffer.extend_from_slice(bytes);
            Ok(())
        })
    }

    fn sync<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }

    fn publish<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move { self.write_whole().await })
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

/// Opens the browser managed copy for a root filesystem identity.
pub(crate) async fn open(root: String) -> Result<Arc<ManagedCopy>, Error> {
    let key = format!("browser:{root}");
    let root_for_closure = root.clone();
    let copy = ManagedCopy::shared(key, move |_| {
        Box::pin(async move { Ok(Box::new(BrowserRoot::new(&root_for_closure).await?) as Box<dyn RootObject>) })
    })
    .await?;
    copy.observe_retirement().await?;
    copy.check()?;
    if copy.storage_is_open() {
        return Ok(copy);
    }
    let mut root_obj = copy.try_root()?;
    copy.check()?;
    root_obj.prepare().await?;
    drop(root_obj);
    let mut storage = copy.try_storage()?;
    copy.check()?;
    if storage.is_none() {
        let database = Database::open_indexeddb(&metadata_database(&root))
            .await
            .map_err(metadata_error)?;
        let client = database.client();
        let content_root = child_directory(&origin_root().await?, &root, true).await?;
        *storage = Some(Storage {
            metadata: database,
            client,
            content: Arc::new(BrowserContent { root: content_root }),
        });
    }
    drop(storage);
    Ok(copy)
}

/// Permanently retires and removes the browser managed copy for an identity.
pub(crate) async fn remove_root(root: String) -> Result<(), Error> {
    let key = format!("browser:{root}");
    let root_for_closure = root.clone();
    let copy = ManagedCopy::shared(key, move |_| {
        Box::pin(async move { Ok(Box::new(BrowserRoot::new(&root_for_closure).await?) as Box<dyn RootObject>) })
    })
    .await?;
    copy.remove().await
}
