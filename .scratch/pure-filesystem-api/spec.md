# Spec: Pure FileSystem API — Remove Sync Leakage

Status: ready-for-agent

## Problem Statement

The current TypeScript `FileSystem.open(namespace, name, nodeId)` API and `FileMeta` type leak sync concerns into the pure filesystem layer:

1. **`namespace` + `name`** are redundant — browser origin already isolates storage. They only construct IndexedDB keys (`offs:{ns}:{name}:metadata`) and OPFS paths.
2. **`nodeId` (PeerId)** is pure sync metadata — recorded as `owner`/`group`/`providers` on every created file/directory. The filesystem has no concept of peers.
3. **`FileMeta`** exposes sync fields (`owner: string`, `group: string`, `providers: string[]`, `local: boolean`) that belong in the sync layer, not the filesystem.

This violates the boundary between **File System** (offline-first replicated storage of paths, metadata, content) and **Transport/Sync** (caller-supplied exchange of metadata and file-session requests between peers).

## Solution

Refactor the public API to be a **pure filesystem** with no sync parameters:

- `FileSystem.open(root?: string)` — single optional root identifier (defaults to `"default"`)
- `FileMeta` contains only filesystem-relevant fields: `file_id`, `revision`, `kind`, `mode`
- Sync metadata (`owner`, `group`, `providers`, `local`) moves to a separate **SyncMetadata** map maintained by the sync layer

## User Stories

1. As a **web app developer**, I want to open a filesystem with a single string (`FileSystem.open("my-app")`), so that I don't need to understand sync concepts to use local storage.

2. As a **sync layer implementer**, I want to attach owner/group/providers to file IDs externally, so that the filesystem stays pure and I control sync policy.

3. As a **library maintainer**, I want the public TypeScript API to match the Rust `FileSystem` core (which is already peer-agnostic), so that bindings don't leak internal concerns.

4. As a **test author**, I want to test filesystem operations without mocking peer IDs, so that tests are simpler and faster.

5. As a **consumer of `FileMeta`**, I want only filesystem-relevant fields, so that my UI/logic doesn't accidentally depend on sync metadata.

## Implementation Decisions

### API Contract Changes

| Current                                                                        | Proposed                                     |
| ------------------------------------------------------------------------------ | -------------------------------------------- |
| `FileSystem.open(namespace: string, name: string, nodeId?: string)`            | `FileSystem.open(root?: string)`             |
| `FileMeta { file_id, revision, kind, providers[], local, mode, owner, group }` | `FileMeta { file_id, revision, kind, mode }` |

### New Types

- **`SyncMetadata`** (sync layer only): `{ file_id: string, owner: string, group: string, providers: string[], local: boolean }`
- **`SyncLayer`** (new): wraps `FileSystem`, maintains `Map<string, SyncMetadata>`, exposes sync-aware methods

### Rust Side (Already Clean)

The Rust `FileSystem<PeerId>` is generic over `PeerId` and only uses `node_id` internally for `providers` set initialization. The `PeerId = Uuid` default is an implementation detail. The browser WASM binding forces `PeerId = String` and exposes it — that binding should default to a no-op peer (e.g., `"local"`) and not require caller input.

### Migration Path

1. Add new `FileSystem.open(root?)` alongside old API (deprecated)
2. Add `FileMeta` without sync fields (new type, e.g., `FileEntryMeta`)
3. Create `SyncLayer` class that wraps `FileSystem` and manages `SyncMetadata`
4. Update WASM `FileSystemHandle.open` to default `node_id = "local"` and make it optional
5. Remove old API after transition

## Testing Decisions

- **Test the public TypeScript API only** — no implementation details
- **Test cases:**
  - `open()` with default root works
  - `open("custom-root")` creates isolated storage
  - `write`/`read`/`list`/`delete` work without peer config
  - `FileMeta` returned from `entry()`/`write()`/`list()` lacks sync fields
  - Sync layer correctly tracks owner/group/providers per file_id
- **Prior art:** Existing tests in `packages/file-system/` and `crates/file-system/tests/`

## Out of Scope

- Changing the Rust `FileSystem<PeerId>` generic (it's already correct)
- Sync protocol / transport implementation
- Multi-device replication logic
- OPFS/IndexedDB storage internals

## Further Notes

This aligns with the domain model:

- **File System** = paths, file metadata, content, residency (device-local)
- **Transport** = peer exchange (sync metadata lives here)
- **Resource** = named filesystem with stable identity (the `root` parameter)

The `nodeId` leak originated from the WASM binding forcing `PeerId = String` and exposing it in `FileSystemHandle.open`. The fix is to default it internally and remove it from the public surface.
