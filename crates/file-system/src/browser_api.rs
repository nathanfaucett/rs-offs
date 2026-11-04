//! WebAssembly surface for the File System.
//!
//! Every operation is asynchronous and returns a promise. A JavaScript-supplied bridge
//! object satisfies the Transport and File Session Service contracts, so a browser peer
//! reconciles with native peers over whatever transport the application chooses.

use std::cell::RefCell;

use crate::{
    Entry, Error, FileEntryMeta, FileHandle, FileHandleId, FileOperation, FileResponse,
    FileSessionService, FileSystem, IncomingMessage, OpenRequest, Residency, ScanPage,
    SessionRequest, SyncMessage, Transport,
};
use serde::Serialize;
use tokio::sync::mpsc;
use uuid::Uuid;
use wasm_bindgen::prelude::*;
impl From<Error> for JsValue {
    fn from(error: Error) -> JsValue {
        JsError::new(&error.to_string()).into()
    }
}

impl From<JsValue> for Error {
    fn from(value: JsValue) -> Self {
        Error::Metadata(format!("browser bridge error: {value:?}"))
    }
}

fn error_value(error: Error) -> JsValue {
    error.into()
}

fn json_error(context: &str, error: impl std::fmt::Display) -> JsValue {
    JsError::new(&format!("{context}: {error}")).into()
}

fn json_value(value: &impl Serialize) -> Result<JsValue, JsValue> {
    serde_json::to_string(value)
        .map(|json| JsValue::from_str(&json))
        .map_err(|error| json_error("serialization failed", error))
}

fn js_value(value: &impl Serialize) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(|error| json_error("serialization failed", error))
}

/// JavaScript bridge for metadata exchange and file sessions.
///
/// The bridge object contract:
/// - `peers()` returns an array of peer identifiers.
/// - `send(peer, message)` and `broadcast(message)` exchange JSON metadata snapshots.
/// - `subscribe(handler)` delivers inbound metadata as `handler(peer, message)`.
/// - `openSession(peer, request)` resolves to a session object whose
///   `request(operation)` resolves to an array of JSON file responses.
/// - `subscribeSessions(handler)` delivers inbound session operations as
///   `handler(peer, operation, respond)`.
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(method, js_name = peers)]
    fn peers(this: &JsBridge) -> js_sys::Array;
    #[wasm_bindgen(method, js_name = send)]
    fn send(this: &JsBridge, peer: &str, message: JsValue) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = broadcast)]
    fn broadcast(this: &JsBridge, message: JsValue) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = subscribe)]
    fn subscribe(this: &JsBridge, handler: &js_sys::Function);
    #[wasm_bindgen(method, js_name = openSession)]
    fn open_session(this: &JsBridge, peer: &str, request: JsValue) -> js_sys::Promise;
    #[wasm_bindgen(method, js_name = subscribeSessions)]
    fn subscribe_sessions(this: &JsBridge, handler: &js_sys::Function);
}

/// Peer identifier type for the browser surface.
pub type PeerId = String;

/// Public entry for WASM serialization (excludes sync fields).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WasmEntry {
    pub path: String,
    pub meta: FileEntryMeta,
}

/// Public scan page for WASM serialization (excludes sync fields).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WasmScanPage {
    pub entries: Vec<WasmEntry>,
    pub next: Option<String>,
}

/// A Transport and File Session Service backed by a JavaScript bridge object.
#[derive(Clone)]
pub struct JsTransport {
    inner: JsValue,
    metadata: tokio::sync::broadcast::Sender<IncomingMessage<PeerId>>,
    sessions: tokio::sync::broadcast::Sender<SessionRequest<PeerId>>,
}

impl JsTransport {
    pub fn new(bridge: JsValue) -> Self {
        Self {
            inner: bridge,
            metadata: tokio::sync::broadcast::channel(256).0,
            sessions: tokio::sync::broadcast::channel(256).0,
        }
    }

    fn bridge(&self) -> &JsBridge {
        self.inner.unchecked_ref()
    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "FileTransport")]
    pub type JsBridge;
}

impl Transport<PeerId> for JsTransport {
    type Error = Error;

    fn peers(&self) -> Vec<PeerId> {
        self.bridge()
            .peers()
            .iter()
            .filter_map(|peer| peer.as_string())
            .collect()
    }

    async fn send(&self, peer: PeerId, message: SyncMessage<PeerId>) -> Result<(), Error> {
        let payload = json_value(&message)?;
        self.bridge()
            .send(&peer, payload)
            .await
            .map_err(|_| Error::Offline)
            .and(Ok(()))
    }

    async fn broadcast(&self, message: SyncMessage<PeerId>) -> Result<(), Error> {
        let payload = json_value(&message)?;
        self.bridge()
            .broadcast(payload)
            .await
            .map_err(|_| Error::Offline)
            .and(Ok(()))
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<IncomingMessage<PeerId>> {
        let sender = self.metadata.clone();
        let receiver = sender.subscribe();
        let handler =
            Closure::<dyn Fn(JsValue, JsValue)>::new(move |peer: JsValue, message: JsValue| {
                let Some(peer) = peer.as_string() else {
                    return;
                };
                let Some(message) = message
                    .as_string()
                    .and_then(|json| serde_json::from_str(&json).ok())
                else {
                    return;
                };
                let _ = sender.send((peer, message));
            });
        self.bridge().subscribe(handler.as_ref().unchecked_ref());
        handler.forget();
        receiver
    }
}

impl FileSessionService<PeerId> for JsTransport {
    type Handle = JsSessionHandle;

    async fn open(&self, peer: PeerId, request: OpenRequest) -> Result<Self::Handle, Error> {
        let request = json_value(&request)?;
        let session = self
            .bridge()
            .open_session(&peer, request)
            .await
            .map_err(|_| Error::Offline)?;
        Ok(JsSessionHandle {
            session,
            revision: Uuid::now_v7(),
        })
    }

    fn subscribe_file_sessions(&self) -> tokio::sync::broadcast::Receiver<SessionRequest<PeerId>> {
        let sender = self.sessions.clone();
        let receiver = sender.subscribe();
        let handler = Closure::<dyn Fn(JsValue, JsValue, js_sys::Function)>::new(
            move |peer: JsValue, operation: JsValue, respond: js_sys::Function| {
                let Some(peer) = peer.as_string() else {
                    return;
                };
                let Some(operation) = operation
                    .as_string()
                    .and_then(|json| serde_json::from_str::<FileOperation>(&json).ok())
                else {
                    return;
                };
                let (response, mut receiver) = mpsc::channel::<FileResponse<PeerId>>(32);
                let _ = sender.send((peer, operation, response));
                wasm_bindgen_futures::spawn_local(async move {
                    while let Some(value) = receiver.recv().await {
                        let Ok(payload) = json_value(&value) else {
                            return;
                        };
                        if respond.call1(&JsValue::NULL, &payload).is_err() {
                            return;
                        }
                    }
                });
            },
        );
        self.bridge()
            .subscribe_sessions(handler.as_ref().unchecked_ref());
        handler.forget();
        receiver
    }
}

/// A File Session handle served by the JavaScript bridge.
pub struct JsSessionHandle {
    session: JsValue,
    revision: Uuid,
}

impl JsSessionHandle {
    async fn request(
        &mut self,
        operation: FileOperation,
    ) -> Result<Vec<FileResponse<PeerId>>, Error> {
        let operation = json_value(&operation)?;
        let session = self.session.clone();
        let responses = js_sys::Function::from(session)
            .call1(&JsValue::NULL, &operation)
            .map_err(|_| Error::Offline)?
            .unchecked_into::<js_sys::Promise>()
            .await
            .map_err(|_| Error::Offline)?;
        serde_json::from_str::<Vec<FileResponse<PeerId>>>(
            responses.as_string().ok_or(Error::Offline)?.as_str(),
        )
        .map_err(|error| Error::InvalidSyncFrame(error.to_string()))
    }

    async fn collect(
        &mut self,
        operation: FileOperation,
    ) -> Result<Vec<FileResponse<PeerId>>, Error> {
        let responses = self.request(operation).await?;
        let mut collected = Vec::new();
        for response in responses {
            let terminal = matches!(
                response,
                FileResponse::ReadEnd
                    | FileResponse::Opened { .. }
                    | FileResponse::Written { .. }
                    | FileResponse::Page(_)
                    | FileResponse::Closed
                    | FileResponse::Error(_)
            );
            collected.push(response);
            if terminal {
                break;
            }
        }
        Ok(collected)
    }
}

impl FileHandle<PeerId> for JsSessionHandle {
    type Error = Error;

    fn read<'a>(
        &'a mut self,
        offset: u64,
        length: u32,
    ) -> crate::FileFuture<'a, Result<bytes::Bytes, Error>> {
        Box::pin(async move {
            for response in self
                .collect(FileOperation::Read {
                    handle: FileHandleId(Uuid::nil()),
                    offset,
                    length,
                })
                .await?
            {
                match response {
                    FileResponse::ReadChunk(bytes) => return Ok(bytes),
                    FileResponse::Error(error) => return Err(error_message(error)),
                    _ => {}
                }
            }
            Err(Error::Offline)
        })
    }

    fn write<'a>(
        &'a mut self,
        offset: u64,
        data: bytes::Bytes,
    ) -> crate::FileFuture<'a, Result<u32, Error>> {
        Box::pin(async move {
            for response in self
                .collect(FileOperation::Write {
                    handle: FileHandleId(Uuid::nil()),
                    expected_revision: self.revision,
                    offset,
                    data,
                })
                .await?
            {
                match response {
                    FileResponse::Written { count, revision } => {
                        self.revision = revision;
                        return Ok(count);
                    }
                    FileResponse::Error(error) => return Err(error_message(error)),
                    _ => {}
                }
            }
            Err(Error::Offline)
        })
    }

    fn scan<'a>(
        &'a mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> crate::FileFuture<'a, Result<ScanPage<PeerId>, Error>> {
        Box::pin(async move {
            for response in self
                .collect(FileOperation::Scan {
                    handle: FileHandleId(Uuid::nil()),
                    cursor,
                    limit,
                })
                .await?
            {
                match response {
                    FileResponse::Page(page) => return Ok(page),
                    FileResponse::Error(error) => return Err(error_message(error)),
                    _ => {}
                }
            }
            Err(Error::Offline)
        })
    }

    fn close(mut self: Box<Self>) -> crate::FileFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            for response in self
                .collect(FileOperation::Close {
                    handle: FileHandleId(Uuid::nil()),
                })
                .await?
            {
                match response {
                    FileResponse::Closed => return Ok(()),
                    FileResponse::Error(error) => return Err(error_message(error)),
                    _ => {}
                }
            }
            Err(Error::Offline)
        })
    }
}

fn error_message(error: crate::FileServiceError) -> Error {
    Error::Metadata(format!("file service error: {error:?}"))
}

/// An open File System in the browser.
#[wasm_bindgen]
pub struct FileSystemHandle {
    inner: RefCell<Option<FileSystem<String>>>,
}

impl FileSystemHandle {
    /// Clones the filesystem out of the cell so no borrow is held across an await.
    fn borrowed(&self) -> Result<FileSystem<String>, JsValue> {
        self.inner
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| JsError::new("filesystem is closed").into())
    }
}

#[wasm_bindgen]
impl FileSystemHandle {
    /// Opens a browser File System rooted at `root` for one peer.
    pub async fn open(root: String, node_id: Option<String>) -> Result<FileSystemHandle, JsValue> {
        let node_id = node_id.unwrap_or_else(|| "local".to_owned());
        let file_system = FileSystem::open_browser(root, node_id)
            .await
            .map_err(error_value)?;
        Ok(Self {
            inner: RefCell::new(Some(file_system)),
        })
    }

    pub async fn residency(&self, path: String) -> Result<String, JsValue> {
        let file_system = self.borrowed()?;
        Ok(match file_system.residency(&path).map_err(error_value)? {
            Residency::Full => "full".to_owned(),
            Residency::Passthrough => "passthrough".to_owned(),
        })
    }

    pub async fn set_residency(&self, path: String, residency: String) -> Result<(), JsValue> {
        let residency = match residency.as_str() {
            "full" => Residency::Full,
            "passthrough" => Residency::Passthrough,
            other => return Err(JsError::new(&format!("unknown residency {other}")).into()),
        };
        let file_system = self.borrowed()?;
        file_system
            .set_residency(&path, residency)
            .await
            .map_err(error_value)
    }

    pub async fn entry(&self, path: String) -> Result<JsValue, JsValue> {
        let file_system = self.borrowed()?;
        let entry: Entry<PeerId> = file_system.entry(&path).await.map_err(error_value)?;
        let wasm_entry = WasmEntry {
            path: entry.path,
            meta: FileEntryMeta {
                file_id: entry.meta.file_id,
                revision: entry.meta.revision,
                kind: entry.meta.kind,
                mode: entry.meta.mode,
            },
        };
        js_value(&wasm_entry)
    }

    pub async fn read(&self, path: String) -> Result<Vec<u8>, JsValue> {
        let file_system = self.borrowed()?;
        file_system.read(&path).await.map_err(error_value)
    }

    pub async fn write(&self, path: String, data: Vec<u8>) -> Result<JsValue, JsValue> {
        let file_system = self.borrowed()?;
        let entry = file_system.write(&path, &data).await.map_err(error_value)?;
        let wasm_entry = WasmEntry {
            path: entry.path,
            meta: FileEntryMeta {
                file_id: entry.meta.file_id,
                revision: entry.meta.revision,
                kind: entry.meta.kind,
                mode: entry.meta.mode,
            },
        };
        js_value(&wasm_entry)
    }

    pub async fn write_at(
        &self,
        path: String,
        expected_revision: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<u32, JsValue> {
        let expected_revision = parse_uuid(&expected_revision)?;
        let file_system = self.borrowed()?;
        file_system
            .write_at(&path, expected_revision, offset, data.into())
            .await
            .map_err(error_value)
    }

    pub async fn append(&self, path: String, data: Vec<u8>) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system.append(&path, &data).await.map_err(error_value)
    }

    pub async fn create_dir(&self, path: String) -> Result<JsValue, JsValue> {
        let file_system = self.borrowed()?;
        let entry = file_system.create_dir(&path).await.map_err(error_value)?;
        let wasm_entry = WasmEntry {
            path: entry.path,
            meta: FileEntryMeta {
                file_id: entry.meta.file_id,
                revision: entry.meta.revision,
                kind: entry.meta.kind,
                mode: entry.meta.mode,
            },
        };
        js_value(&wasm_entry)
    }

    pub async fn list(&self, path: String) -> Result<JsValue, JsValue> {
        let file_system = self.borrowed()?;
        let entries: Vec<Entry<PeerId>> = file_system.list(&path).await.map_err(error_value)?;
        let wasm_entries: Vec<WasmEntry> = entries
            .into_iter()
            .map(|entry| WasmEntry {
                path: entry.path,
                meta: FileEntryMeta {
                    file_id: entry.meta.file_id,
                    revision: entry.meta.revision,
                    kind: entry.meta.kind,
                    mode: entry.meta.mode,
                },
            })
            .collect();
        js_value(&wasm_entries)
    }

    pub async fn scan(
        &self,
        path: String,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<JsValue, JsValue> {
        let file_system = self.borrowed()?;
        let page: ScanPage<PeerId> = file_system
            .scan(&path, cursor.as_deref(), limit)
            .await
            .map_err(error_value)?;
        let wasm_page = WasmScanPage {
            entries: page
                .entries
                .into_iter()
                .map(|entry| WasmEntry {
                    path: entry.path,
                    meta: FileEntryMeta {
                        file_id: entry.meta.file_id,
                        revision: entry.meta.revision,
                        kind: entry.meta.kind,
                        mode: entry.meta.mode,
                    },
                })
                .collect(),
            next: page.next,
        };
        js_value(&wasm_page)
    }

    pub async fn delete(&self, path: String) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system.delete(&path).await.map_err(error_value)
    }

    pub async fn rename(&self, from: String, to: String) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system.rename(&from, &to).await.map_err(error_value)
    }

    pub async fn revision(&self, path: String) -> Result<String, JsValue> {
        let file_system = self.borrowed()?;
        file_system
            .revision(&path)
            .await
            .map(|revision| revision.to_string())
            .map_err(error_value)
    }

    /// Exports the metadata snapshot of this filesystem as JSON.
    pub async fn export_snapshots(&self) -> Result<JsValue, JsValue> {
        let file_system = self.borrowed()?;
        let access = file_system.access().await.map_err(error_value)?;
        let snapshots = access
            .database()
            .export_snapshots()
            .await
            .map_err(metadata_error)?;
        js_value(&snapshots)
    }

    /// Announces metadata to every peer over the JavaScript bridge.
    pub async fn announce(&self, bridge: JsBridge) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system
            .metadata_sync(JsTransport::new(bridge.into()))
            .announce()
            .await
            .map_err(error_value)
    }

    /// Fetches one file's content from its providers over the JavaScript bridge.
    pub async fn fetch(&self, bridge: JsBridge, path: String) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system
            .metadata_sync(JsTransport::new(bridge.into()))
            .fetch(&path)
            .await
            .map_err(error_value)
    }

    /// Runs one pass of the metadata sync loop over the JavaScript bridge.
    pub async fn pump(&self, bridge: JsBridge) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system
            .metadata_sync(JsTransport::new(bridge.into()))
            .pump()
            .await
            .map_err(error_value)
    }

    /// Reconciles with peers until the filesystem is retired or the bridge closes.
    pub async fn sync(&self, bridge: JsBridge) -> Result<(), JsValue> {
        let file_system = self.borrowed()?;
        file_system
            .metadata_sync(JsTransport::new(bridge.into()))
            .run()
            .await
            .map_err(error_value)
    }

    /// Closes this filesystem, releasing its exclusive ownership lock.
    pub fn close(&self) {
        self.inner.borrow_mut().take();
    }
}

fn parse_uuid(value: &str) -> Result<Uuid, JsValue> {
    Uuid::parse_str(value).map_err(|error| json_error("invalid revision", error))
}

fn metadata_error(error: ofdb_kv::Error) -> Error {
    Error::Metadata(error.to_string())
}
