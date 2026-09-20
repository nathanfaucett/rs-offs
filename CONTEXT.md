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

## Residency

Residency is a device-local choice per path: Full or Passthrough. Full stores metadata and content locally. Passthrough stores and synchronizes metadata only, and obtains content from an online Full peer when read; it is read-only. The most-specific rule applies. Residency is never synchronized.

## Merge Strategy

A Merge Strategy determines how concurrent file updates reconcile. Ordinary files use LWW metadata. Known document types (e.g. `.automerge`, `.am`) use a document-merging plugin whose result feeds back into the LWW record. The strategy is derived from file type, not stored per file.

## Transport

A Transport moves typed sync messages between peers. It is defined by a trait (send, broadcast, subscribe) so iroh can be swapped or mocked; an in-memory transport exists for tests.

## Endpoint ID

An Endpoint ID identifies one iroh endpoint (device) on the network. Servers decide which Endpoint IDs may connect.

## Pairing

Pairing is the handshake that introduces two endpoints and exchanges the payload needed to trust each other. A Pairing Offer is one side's pending invitation, answered over a dedicated stream.

## Vault ID

A Vault ID identifies the filesystem being synchronized. Its hash scopes the iroh transport session.

## Transport Tunnel

A Transport Tunnel is an isolated iroh stream for one vault and endpoint pair. A Tunnel Authorizer validates each incoming tunnel before it is accepted; an Allowed Endpoint list decides which endpoints may connect at all.

## Scoped Transport

A Scoped Transport binds the generic iroh transport to one vault and one access-token provider, so the file system sync engine can use it without knowing iroh details.

## Iroh Client

The Iroh Client is the WASM/TypeScript binding that lets browser applications drive iroh connections, tunnels, and pairing events.
