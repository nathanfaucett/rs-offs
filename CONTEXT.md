# Domain Context

## KV Store

`kv` stores opaque byte values under UTF-8 keys. Each key has UUIDv7 generations and an Automerge-backed history. Deletes are retained as tombstones. Snapshot exchange is explicit; transport and peer state belong to the caller. Same-generation divergent histories are rejected rather than resolved with LWW.

## File System

The File System is a local-first replicated storage engine. It exposes open/read/write/scan operations, stores content under a stable file id, and stores path metadata in `kv`. Metadata synchronization exchanges complete winning-key snapshots, including tombstones. Missing content is fetched through a separate file service.

## File Entry

A File Entry is the metadata for one path: stable file id, kind, providers, locality, mode, owner, group, and content revision. The file id survives overwrite and rename; the path is the KV key. Deletes are KV tombstones rather than a field in `FileMeta`.

## Content Revision

A Content Revision is a persisted UUIDv7 in `FileMeta`. It changes on each metadata/content write, is independent of the KV generation, and is used for stale-write checks and content requests.

## Residency

Residency is a device-local choice per path: Full or Passthrough. Full stores metadata and content locally. Passthrough stores metadata only and obtains content from an online Full peer; it is read-only. The most-specific rule applies. Residency is never synchronized.

## Transport

A Transport provides typed send, broadcast, and subscribe operations for metadata snapshots and file sessions. It is abstracted so network implementations can be swapped; an in-memory transport exists for tests. Metadata snapshots are exchanged on connection/reconnection and after local committed changes. Lagged messages trigger a full exchange.
