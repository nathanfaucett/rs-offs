# Direct Iroh sync streams

**Status:** proposed

We keep deckv and the file-system library, but replace the hand-rolled persistent per-peer tunnel mesh with Router-mounted direct protocols: endpoint hooks enforce the allowlist, a metadata-sync ALPN exchanges root-scoped changes, a file-transfer ALPN streams content, and pairing remains a dedicated ALPN. Endpoint discovery is independent of synchronization and may use any Iroh discovery mechanism. We explicitly do not adopt `iroh-docs`, `iroh-gossip`, or `iroh-blobs`.

This supersedes the “default: framed iroh tunnels” transport choice in [file-system ADR01](../file-system/ADR01.md) §3 while leaving the Transport trait, deckv LWW metadata, residency, and `file_id` local storage intact.

## Decision

### Stack

```
Endpoint
  └── EndpointHooks          allowlist (and optional pre-auth)
  └── Router
        ├── metadata-sync ALPN   direct metadata announce, delta, and anti-entropy
        ├── file-transfer ALPN   stateful file handles and direct byte streams
        └── pairing ALPN         introduce / trust exchange (existing Pairing Offer)
```

- **Root ID** identifies one synchronized filesystem root. Every metadata-sync and file-transfer request carries its Root ID and is rejected if it does not match the local root.
- **Discovery** resolves endpoints and connection paths only. Any Iroh discovery mechanism is acceptable; a discovered endpoint must still pass the chained-endpoint allowlist before it reaches application logic.
- **Transport** is a control-plane message bus for typed `SyncMessage` values (encode for the wire; in-memory transport skips encode). `broadcast` fans out direct metadata-sync sends to allowlisted peers; directed `send` remains for anti-entropy and provider-targeted control messages when needed. It does not expose file reads, writes, streams, or server-side file sinks.
- **File service** is a separate stateful API above `Transport`. Opening a path returns a file handle. Handle operations are POSIX-style `read(offset, length)` and `write(offset, bytes)`, plus cursor-paginated directory `scan(cursor, limit)` and `close`.
- **Content** is requested from a provider by `file_id` and the current metadata-record revision, then streamed on the file-transfer ALPN. Local storage remains `.data/<file_id>`. Content hashes are neither wire addresses nor required file metadata; an implementation may verify a completed stream with a checksum without making it part of the protocol identity.
- **Writes** use `write_at` semantics and are accepted only against the handle's expected revision. Full-residency peers may serve and accept writes; Passthrough remains read-only unless a later decision adds write forwarding.
- **Allowlist** is enforced in Endpoint hooks after handshake. Trusted remotes may reach metadata-sync and file-transfer handlers. An untrusted remote may reach only the pairing handler while a short-lived pairing window is open; all other traffic is rejected. Root-scoped authorization (access token / authorizer) remains application policy on top where still required.
- **Client support** is native-only. There is no WASM/TypeScript transport implementation or compatibility surface to maintain.

### Out of scope / rejected

| Option                                         | Why not                                                                                                                    |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Keep framed bi-di tunnels as the sync backbone | A persistent mutexed stream couples metadata and large content and duplicates Router dispatch                              |
| Replace deckv/file-system with `iroh-docs`     | Wrong merge model (author-signed CRDT entries), CAS-forced values, no Full/Passthrough residency, different identity story |
| Metadata or content on one stream              | Metadata must remain responsive while files are streamed                                                                   |
| Discovery as authorization                     | Discovery locates endpoints; the chained-endpoint allowlist decides which endpoints may use application protocols          |

## Consequences

- `iroh-chain` shrinks to endpoint wiring, hooks, pairing, and Router registration — not a tunnel registry.
- `iroh-chain-file-system` becomes a root-scoped metadata `Transport` adapter plus a separate stateful file-service client/server.
- Passthrough reads request the current file revision from a provider and stream it without persistence; remote writes require a Full-residency provider.
- Discovery may be relay, DNS, PKARR, static addresses, or another configured Iroh mechanism; authorization remains the chained-endpoint allowlist.
- The protocol no longer requires a content-pointer format or a SHA-256 → BLAKE3 migration.
- Integration must grow a continuous sync loop; poll-only `pump` and missing `sync_peer` glue are not acceptable end states.

## Plan

Phased work; each phase leaves tests green and deletes obsolete code rather than wrapping it.

### Phase 0 — Stabilize the sync seam

Goal: one correct, continuously driven sync loop over the existing `Transport` trait.

- [x] Define and implement `sync_peer` on file-system. It announces metadata, drives `subscribe`, and broadcasts local changes continuously.
- [x] Remove the dead `local` storage-service Iroh runtime, including its broken `sync_peer` and `EndpointIdCodec` callers.
- [x] Keep the file-system-owned `SyncMessage` vocabulary at the transport boundary; deckv remains the metadata/LWW storage engine.
- [x] Add integration coverage that two memory peers converge metadata through `sync_peer`.
- [x] Add integration coverage that a Full peer serves content to a Passthrough peer through the current continuous sync seam.

### Phase 1 — Endpoint hooks, discovery, and Router

Goal: use configured Iroh discovery to find peers, enforce the chained-endpoint allowlist at the endpoint edge, and replace the custom accept loop.

- [x] Provide `Server::bind` and `Server::bind_with_secret_key` endpoint construction with `EndpointHooks` installed before bind; the hook rejects non-allowlisted remotes after handshake.
- [x] Allow an untrusted remote to reach only pairing while a short-lived pairing window is open; reject it for metadata sync and file transfer. Allow trusted remotes through the endpoint hook.
- [x] Configure endpoint discovery independently from authorization; accept any configured Iroh discovery source, but authorize only allowlisted chained Endpoint IDs.
- [x] Mount pairing, metadata-sync, and file-transfer handlers on `iroh::protocol::Router`.
- [x] Delete the custom `Server::listen` accept loop after Router handlers own all ALPN dispatch.
- [x] Remove unused `iroh-chain` dependencies, including `iroh-tickets`, `chrono`, `tracing`, `futures`, and `tokio-util`.

### Phase 2 — Root-scoped direct metadata sync

Goal: typed metadata moves directly between allowlisted endpoints for one root.

- [x] Define `RootId` as the stable identifier for one synchronized filesystem root and replace legacy identifier terminology and APIs in the native Iroh chain and active local consumers.
- [x] Require `RootId` on each metadata-sync request and reject requests for another root.
- [x] Implement `Transport::broadcast` as direct fan-out to discovered, allowlisted peers; implement `Transport::subscribe` from incoming metadata-sync streams; retain `Transport::send` for directed anti-entropy and provider-targeted control messages.
- [x] Delete the persistent bi-directional tunnel map and framed-postcard metadata path.
- [x] Collapse duplicate peer and connection state in `Server` and the transport adapter.
- [x] Rename `ScopedIrohTransport` to `RootIrohTransport`.
- [ ] Add a three-endpoint test: one peer is offline, then rejoins; metadata converges after discovery and the allowlist remains enforced.
- [ ] Keep file-service operations out of `Transport`; cover the control-plane-only trait with memory and Iroh implementations.

### Phase 3 — Stateful file service

Goal: file access follows a FUSE-like handle API; metadata control messages and file operations are separate, and file bytes never use metadata sync or persistent tunnels.

- [x] Remove pointer hashes from `FileMeta`, write, read, and stale-response paths; use the metadata-record revision to identify the requested file version.
- [x] Define a file-transfer request containing `file_id` and the expected metadata revision, with a separate response wire format.
- [x] Use provider Endpoint IDs as metadata hints and connect directly to one provider over the file-transfer ALPN.
- [x] Delete byte-carrying `ContentRequest` and `ContentResponse` transport messages and their payload handling.
- [x] Implement Passthrough reads as direct file streams without persistence.
- [x] Delete the 1 MiB content-frame limit and all content-over-tunnel code.
- [x] Add tests for stale revisions, unavailable providers, and Full-to-Passthrough transfer.
- [x] Move byte-stream operations out of the generic `Transport` trait into a separate file-service boundary.
- [ ] Define length-delimited file-session framing for `Open`, `Read`, `Write`, `Scan`, and `Close`.
- [ ] Return a stateful remote file handle from `Open`; do not expose raw file sinks through `Transport`.
- [ ] Implement POSIX `write(offset, bytes)` against Full-residency files.
- [ ] Include the expected metadata revision in writes and reject stale handles without overwriting newer content.
- [ ] Implement cursor-paginated directory scans with bounded limits and opaque continuation cursors.
- [ ] Stream `Read` responses as `Bytes` chunks without materializing the whole file.
- [ ] Add memory and Iroh tests for open/read, offset writes, stale-write rejection, multi-page scans, unknown handles, and close.

### Phase 4 — Simplify crates and docs

Goal: one root-scoped direct-sync model for native clients; browser/WASM support is explicitly out of scope.

- [x] Remove the WASM/TypeScript client crates and workspace integrations; native clients are the supported target.
- [ ] Reduce `iroh-chain` to endpoint construction, hooks, pairing, and Router registration; keep file-handle/session logic in `iroh-chain-file-system`.
- [x] Keep one `RootIrohTransport` adapter as the file-system boundary.
- [ ] Update `CONTEXT.md`, ADRs, APIs, tests, and user-facing documentation: `Root ID` identifies a synchronized filesystem root; direct protocol requests carry it as their isolation scope; discovery is separate from authorization; `Transport` is control-plane-only and file access uses stateful handles.
- [ ] Mark file-system ADR01 §3 superseded by this ADR; update deckv anti-entropy notes if sync-message ownership changed in Phase 0.
- [x] Run `cargo hack test --feature-powerset --all-targets` for each touched offs crate.
- [x] Run formatting and targeted Clippy for each touched crate.

## References

- [iroh Endpoint Hooks](https://docs.iroh.computer/connecting/endpoint-hooks)
- [iroh Write a Protocol / Router](https://docs.iroh.computer/protocols/writing-a-protocol)
- [iroh auth-hook example](https://github.com/n0-computer/iroh/blob/main/iroh/examples/auth-hook.rs)
- file-system ADR01, deckv ADR01, offs `CONTEXT.md`
