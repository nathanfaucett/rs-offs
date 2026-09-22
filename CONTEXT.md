# Domain Context

## deckv

deckv is the embedded, offline-first, eventually consistent key-value store. Every value is a Last-Writer-Wins record; deletes are tombstones. Storage backends and transports are swappable; there is no central coordinator and no shared root.

## Last-Writer-Wins (LWW)

LWW is the only merge strategy in deckv. Concurrent updates on one key resolve by timestamp, then replica id tie-break. The loser is discarded; no conflict copies are kept.

## Tombstone

A Tombstone is a replicated deleted record. It prevents a deleted key from returning when replicas synchronize. Tombstones remain until garbage collection removes them.

## Delta

A Delta is the set of records one replica is missing, derived from the peer's latest known timestamp. Deltas drive anti-entropy synchronization.

## File System

The File System is the local-first replicated storage engine built on deckv. It exposes an FS-like API (open, read, write, stream, list), stores content under a stable file id, and keeps path metadata in deckv. It synchronizes metadata and obtains missing content from peers through a swappable transport.

## File Entry

A File Entry is the replicated metadata for one path: file id, kind, providers, locality, deletion state, and mode/owner/group. The file id is stable across renames; the path key is not.

## Content Revision

A Content Revision is the version of file bytes represented by the current metadata record. It is used with the file id to request a specific version from a provider. A checksum may verify a completed stream, but is not the protocol identity or storage key.

## Residency

Residency is a device-local choice per path: Full or Passthrough. Full stores metadata and content locally. Passthrough stores and synchronizes metadata only, and obtains content from an online Full peer when read; it is read-only. The most-specific rule applies. Residency is never synchronized.

## Merge Strategy

A Merge Strategy determines how concurrent file updates reconcile. Ordinary files use LWW metadata. Known document types (e.g. `.automerge`, `.am`) use a document-merging plugin whose result feeds back into the LWW record. The strategy is derived from file type, not stored per file.

## Transport

A Transport moves typed sync messages between peers. It is defined by a trait (send, broadcast, subscribe) so the network stack can be swapped or mocked; an in-memory transport exists for tests. Metadata broadcast fans out to allowlisted peers over direct metadata-sync streams; file bytes use a separate direct file-transfer stream.

## Endpoint ID

An Endpoint ID identifies one iroh endpoint (device) on the network. An Allowed Endpoint list decides which Endpoint IDs may connect.

## Pairing

Pairing is the handshake that introduces two endpoints and exchanges the payload needed to trust each other. A Pairing Offer is one side's pending invitation, answered over a dedicated pairing channel.

## Root ID

A Root ID identifies the filesystem being synchronized. It scopes every metadata-sync and file-transfer request for that root.

## Discovery

Discovery resolves endpoint addresses and connection paths. It does not authorize an endpoint: every discovered endpoint must pass the chained-endpoint allowlist before it can use application protocols.

## Root Transport

A Root Transport binds the generic transport to one Root ID and its allowlist, so the file system sync engine can use it without knowing network details.
