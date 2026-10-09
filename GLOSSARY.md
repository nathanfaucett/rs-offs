# Domain Context

## KV Store

A KV Store holds values identified by keys, with retained generations and deletion tombstones. Reconciliation combines histories within a generation and selects the winning generation for each key.

## File System

A File System is offline-first replicated storage of paths, file metadata, and content. Filesystem reconciliation brings replicas together while retaining stable file identities, deletion tombstones, and device-local residency choices.

## File Entry

A File Entry is the metadata for one path: stable file id, kind, providers, locality, mode, owner, group, and content revision. Its file id survives overwrite and rename; a deleted path is represented by a tombstone.

## Content Revision

A Content Revision identifies a file's metadata/content version, independently of its metadata generation. It distinguishes the version requested or edited from stale versions.

## Residency

Residency is a device-local choice per path: Full retains metadata and content, while read-only Passthrough retains metadata and obtains content from an available Full peer. The most-specific choice applies, and residency is not replicated.

## Transport

A Transport is the caller-supplied exchange of filesystem metadata and file-session requests between peers, not filesystem persistence or reconciliation. A File Session is a peer-scoped exchange for opening, reading, writing, scanning, and closing a file handle.

## File System Resource

A File System Resource is a named or unnamed filesystem with a stable identity. Resource deletion is a replicated tombstone; device deselection removes only that device's projected local copy.
