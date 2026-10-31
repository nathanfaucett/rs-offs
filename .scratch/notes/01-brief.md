# Ticket 01: Storage-agnostic core

## Goal

- Make File System core storage-agnostic (no direct Redb/native FS access)
- Two internal storage ports:
  - Metadata port: KV Store facade (async API)
  - Content port: content revisions by file_id+revision
- Target-conditional Send/Sync bounds via ofdb-platform
- IndexedDB backend for wasm32 (ofdb-kv with indexeddb feature)
- No Tokio runtime in browser build
- Native tests unchanged

## Key files to modify

- crates/file-system/src/lib.rs (pub use ofdb_kv::Client; pub use ofdb_kv::Database)
- crates/file-system/src/file_system.rs (replace direct Redb access with KV facade)
- crates/file-system/src/resource.rs (catalog moves to KV facade)
- crates/file-system/Cargo.toml (add ofdb-platform dep, add browser feature, add wasm backend gating)

## Key design decisions from spec

- Catalog records and projected-copy selections move to KV Store facade, keyed by File System Resource id with distinct key prefixes for the catalog and the projected-selection set
- Catalog values stored as facade values (Value::Blob for postcard, or Value::Json for JSON)
- Catalog API becomes async (breaking change, crate is 0.1.0)
- Content port: stage, publish, read, remove operations addressed by file_id + revision
- Native content backend preserves staging discipline (temp file → sync → rename → metadata)
- Browser content backend: write bytes first, then commit metadata selecting revision
- Target-conditional bounds: use ofdb-platform's MaybeSend/MaybeSync/BoxFuture
- uuid gets `js` feature on wasm32
- Browser build: no Tokio runtime; tokio sync primitives only (macros, sync features)
- wasm32 compiles with ofdb-kv indexeddb feature

## Acceptance criteria

- [ ] The File System Resource catalog stores its records and projected-copy selections through the KV Store facade, and its API is asynchronous.
- [ ] Existing native roots remain readable: existing resource registrations and File Entries survive the catalog and metadata move.
- [ ] Content Revisions are read and written only through an internal content port.
- [ ] The public bounds on futures, handles, peer identifiers, and Transport callbacks are target-conditional: unchanged on native targets, single-threaded on the browser target, using the platform crate's existing conditional bound types.
- [ ] Identifier generation uses the platform's browser-appropriate randomness source on the browser target.
- [ ] The library compiles for wasm32-unknown-unknown, and the browser build constructs no Tokio runtime.
- [ ] The native test suite passes unchanged, and the full feature-powerset test command passes on native.
