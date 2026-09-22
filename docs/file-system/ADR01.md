# ADR-0001: Offline-First, Eventually Consistent Distributed File System

## Status

Accepted

## Context

Rust library for local file access that:

- crates/file-system
- Exposes an FS-like API (open, read, write, stream, list), shaped so binaries can mount it via FUSE
- Works offline and online
- Syncs across peers over iroh (or another transport) with eventual consistency, no central coordinator
- Lets each device choose Full or Passthrough storage per path
- Keeps transport swappable behind a trait
- Resolves conflicts deterministically — same updates always converge, regardless of order

FUSE is **not** part of the library. Binaries that need a kernel mount implement FUSE on top of the API.

Metadata (path → file identity and attributes) is exactly the problem deckv solves. Re-implementing a flat LWW key-value layer would duplicate ADR-0001. This ADR therefore uses deckv for metadata and focuses on content, residency, and the FS API.

## Decision

### 1. Architecture

```
┌─────────────────────────────┐
│  Public API (FS-like)        │  open, read, write, stream, list
├─────────────────────────────┤
│  Sync Engine                 │  content fetch, residency, plugins
├─────────────────────────────┤
│  deckv (metadata)            │  path → FileMeta (per-key LWW)
├─────────────────────────────┤
│  Content storage (native FS) │  bytes under file_id (UUID v7)
├─────────────────────────────┤
│  Transport trait             │  typed send / broadcast / subscribe
├─────────────────────────────┤
│  iroh (default)              │  swappable
└─────────────────────────────┘
```

FUSE (Linux binaries only) sits above the public API. Not in the library.

### 2. Metadata — deckv

All path → metadata mapping is stored in **deckv** (`Store<Path, FileMeta>` or equivalent).

deckv supplies (see ADR-0001):

- Per-key LWW registers with HLC + replica_id tie-break
- Tombstones for deletes
- Watermark-based deltas and live change broadcast
- Pluggable storage backends (redb, in-memory, …)
- Transport-agnostic sync

**FileMeta** (the value stored in deckv) contains:

```
{ file_id, providers, local, tombstone, mode, owner, group }
```

- `file_id`: stable UUID v7. Rename changes the path key, not the id. UUID v7 can serve as a FUSE inode.
- **No file size in metadata.** Size changes often (appends); deriving it on demand avoids sync spam.
- **Permissions:** simple Unix-like `mode` plus opaque `owner` / `group`. The library stores and LWW-merges them; it does not interpret them. Not host uid/gid.
- Empty folders need an explicit marker object. Folder delete is multi-key and not atomic.
- Rename is first-class (change path key; `file_id` stays the same).

No Merkle tree and no shared-root CRDT over the keyspace — each path key is an independent root, as provided by deckv.

### 3. Transport — typed messages

> **Superseded in part** by [ADR 0001 — Direct Iroh sync streams](../adr/0001-gossip-blobs-mesh-transport.md): the default network stack uses root-scoped direct metadata and file-transfer protocols through an Iroh Router, not framed bi-di tunnels. Discovery is independent of the chained-endpoint allowlist. The Transport trait and message-enum shape below remain.

The library defines the message types (an enum). The transport encodes/decodes them and moves them between peers. No topics in the trait.

```rust
enum SyncMessage { /* metadata requests and deltas */ }

trait Transport {
    type PeerId;
    async fn send(&self, peer: Self::PeerId, msg: SyncMessage);
    async fn broadcast(&self, msg: SyncMessage);
    async fn subscribe(&self) -> Stream<(Self::PeerId, SyncMessage)>;
}
```

- Default: iroh (encode to bytes on the wire).
- In-memory transport for tests: pass `SyncMessage` by value (or `Arc`) with no serialization.

Metadata sync carries requests and deltas. File content uses a separate provider-targeted stream keyed by `file_id` and the metadata-record revision; content is not carried in metadata messages.

### 4. Sync engine

Owns content fetch, residency, and the coordination of metadata (via deckv) with content.

**Two conflict layers:**

1. **Key / metadata** (existence, rename, delete, mode, owner, group) → handled entirely by deckv (per-key LWW).
2. **File content** → by **file type** (extension / MIME), not a per-file setting:
   - Default for most types: **LWW** (one winning version of the whole file).
   - Types with a known extension (e.g. `.am`, `.automerge`) use a compiled-in plugin — typically a **diff-based CRDT with one independent root per file**. Peers exchange ops/diffs, not full state. No shared root across keys.
   - Plugins are compiled in and version-controlled. Same build ⇒ same plugin set.
   - Dispatch is a fixed map from type → strategy:

     ```rust
     enum MergeStrategy {
         Lww,
         AutomergeDoc, // e.g. .am / .automerge
     }
     // lookup by extension or MIME; not stored per key
     ```

   - Plugin result is written back into the deckv key-register under a new timestamp (feeds LWW, does not bypass it).
   - Unknown type or missing plugin ⇒ **LWW**.

**Determinism is required:** HLC (audited crate), tie-breaks, and plugin merges must be pure. No `HashMap` iteration order; use `BTreeMap` or sorted data. No wall-clock in merge logic.

- Tombstones: timed GC (default daily), delegated to / coordinated with deckv.
- Content fetch is separate from metadata sync.
- Local-first and reactive: never block on network round-trips to merge.

### 5. Content storage — file_id only

One scheme for all files:

- Content lives under `.data/<file_id>` (UUID v7). Not under a content hash.
- Overwrite and append update that object in place. No new identity per write (good for logs and hot files).
- Optional digest in FileMeta for integrity only — not the storage key.
- Large files may be chunked under the same `file_id`.
- GC when the key is tombstoned and past the GC window.
- Sync picks the winning version (LWW or plugin); storage writes those bytes under the same `file_id`.

### 6. Residency (device-local)

Not synced. No application concept — paths only.

- **Full** or **Passthrough** per path (prefix / folder / file). Longest match wins. Default: Passthrough.
- **Full:** metadata (in deckv) + content (`.data/<file_id>`) stored locally.
- **Passthrough:** metadata is stored and synced via deckv; **no local content**. Read-only. Writes require Full.
- Passthrough → Full: fetch and verify content, then mark as provider.
- Full → Passthrough: drop local content; GC if unreferenced.

### 7. Passthrough reads

- Read-only. Writes/creates fail locally (e.g. `EROFS`); nothing is published.
- Read from an online Full peer by `file_id`.
- **Unsupported** (clear error) when: no peers configured, no Full peer reachable, or network set offline.
- Peers must advertise durable content storage to serve reads.

### 8. Streaming

- Stream from `.data/<file_id>` (chunks if used).
- Local: native store. Passthrough: online Full peer. Same read API.

### 9. OS integration (binaries only)

- FUSE is not in the library. Binaries implement it on top of the API.
- API is shaped for easy FUSE mapping; UUID v7 works as inode.
- When mounted via FUSE:
  - Kernel page cache accepted as-is (passthrough reads may be briefly stale).
  - Locks: local only; FUSE lock/flock returns `ENOSYS`. No distributed locks. Conflicts resolved after the fact via merge.
  - Present passthrough paths as read-only to the kernel.
- Mode / owner / group are library metadata (stored in deckv). FUSE may map owner/group to kernel uid/gid; that mapping is outside the library. Permission checks are local only.

## Consequences

**Good**

- Metadata LWW, watermarks, and sync are implemented once (deckv) and reused.
- Transport is trivial to swap or mock; in-memory impl skips encode/decode for fast tests.
- Flat keyspace + per-key LWW (via deckv) avoids tree-merge conflicts.
- CRDTs only for content plugins, diff-based, independent roots.
- One storage path (`file_id`) for all file types — no content-hash churn.
- Path-level residency without an application model.
- Passthrough read-only simplifies remote writes.

**Trade-offs**

- HLC, tombstone GC, and serialization correctness remain critical (shared with deckv).
- Empty-folder markers; multi-key folder deletes are not atomic.
- Offline create-create on the same path: LWW discards the loser (no conflict copy).
- Non-deterministic plugins can cause silent permanent divergence.
- Passthrough needs an online Full peer; offline/no-peers ⇒ no passthrough reads.
- FUSE page cache can serve stale passthrough data between fetches.
- Writes require local Full residency.
- Optional digests must match on-disk bytes (local integrity only).
- Filesystem depends on deckv; version alignment matters.

## Alternatives considered

| Alternative                            | Reason for rejection / deferral                                         |
| -------------------------------------- | ----------------------------------------------------------------------- |
| Re-implement flat LWW KV in-tree       | Duplicates ADR-0001; deckv already provides it                          |
| Automerge / shared-root CRDT over tree | Tree merges are complex; flat per-key LWW is simpler and sufficient     |
| Content-addressed storage (hash)       | Causes identity churn on every append/overwrite; bad for logs/hot files |
| Merkle tree over keyspace              | Unnecessary while deckv watermarks are enough; deferred to deckv        |
| Built-in FUSE                          | Keeps the library portable and smaller; binaries own the mount          |
| Distributed locks                      | Conflicts are resolved after the fact via deterministic merge           |
| Per-file merge strategy setting        | Strategy is derived from file type; keeps metadata smaller              |

## Implementation outline

1. Depend on deckv for metadata (`path → FileMeta`)
2. Content storage under `file_id` (UUID v7)
3. Public FS-like API
4. Transport trait + iroh default + in-memory test transport
5. Sync engine (metadata via deckv + content fetch)
6. Residency (Full / Passthrough)
7. Content merge plugins (LWW default, Automerge-style for selected types)
8. Tombstone GC (coordinated with deckv)
9. Example FUSE binary (out of tree)

## References

- ADR-0001 — deckv: Distributed Eventually Consistent Key-Value Store
- Shapiro et al. — _A comprehensive study of Convergent and Commutative Replicated Data Types_
- Kulkarni et al. — _Hybrid Logical Clocks_
- iroh — peer-to-peer networking
- UUID v7 — time-ordered identifiers suitable as inodes
- Internal design discussions leading to this ADR
