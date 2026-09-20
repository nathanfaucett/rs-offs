# ADR-0001: deckv — Distributed Eventually Consistent Key-Value Store

## Status

Accepted

## Context

Rust library for an embedded, offline-first key-value store that:

- crates/deckv
- Works offline and online
- Syncs across peers with eventual consistency, no central coordinator, no shared root
- Supports arbitrary key and value types (`K`, `V`)
- Keeps storage and transport swappable behind traits
- Resolves conflicts with **Last-Writer-Wins (LWW)** — same updates always converge, regardless of order

Existing solutions (SQLite + custom sync, Automerge, Yjs, CouchDB-style systems) are either too heavy, too document-oriented, or do not give a clean, pluggable embedded Rust API.

## Decision

### 1. Architecture

```
┌─────────────────────────────┐
│  Application                │
├─────────────────────────────┤
│  Store                      │  put, get, delete, apply, subscribe
├─────────────────────────────┤
│  Storage trait              │  durable persistence + delta queries
├─────────────────────────────┤
│  Sync engine                │  protocol over Transport
├─────────────────────────────┤
│  Transport trait            │  typed send / stream of messages
└─────────────────────────────┘
```

### 2. Core data model

Every value is stored as an LWW record:

```rust
struct LwwRecord<V> {
    value: Option<V>,          // None = tombstone
    timestamp: DateTime<Utc>,  // wall clock for now; HLC later
    node_id: Uuid,             // stable replica identity
}
```

- Keys and values are fully generic.
- Deletes are tombstones (same LWW rules).
- Merge compares timestamp, then `node_id` for tie-break. Pure, commutative, associative, idempotent.
- **LWW is the only merge strategy.** No pluggable merge trait.

### 3. Storage trait

```rust
trait Storage<K, V> {
    // put / get / remove / iter
    // get_latest_for_node_id(node) → Option<Timestamp>
    // iter_delta(node, since) → records from that node newer than since
}
```

- First concrete backend: **redb** (single-file, pure Rust).
- In-memory backend for tests.
- Latest timestamp and delta iteration live inside storage so the rest of the system stays simple.
- No cache inside `Store`; all reads/writes go to storage.
- Future: optional Merkle tree over the keyspace for large stores.

### 4. Store

Thin façade over `Storage`.

- `put` / `get` / `delete`
- `apply` / `apply_batch` for remote changes (LWW only)
- Broadcast channel: every local or remote change is emitted so subscribers (UI, indexes, sync) can react
- `get_latest_for_node_id(peer)` and `export_delta(peer, since)` for anti-entropy

### 5. Synchronisation

Transport-agnostic async trait:

```rust
trait Sync<K, V> {
    fn peer_id(&self) -> Uuid;
    async fn send(&mut self, msg: SyncMessage<K, V>);
    fn messages(&mut self) -> impl Stream;  // messages may arrive at any time
}
```

Protocol messages:

1. `Hello { node_id, since }` — highest timestamp already seen from the peer
2. `Delta { records }` — missing writes (sent eagerly and on demand)
3. `Done` — optional termination

- Sync engine subscribes to the Store’s change stream and forwards live local writes.
- Incoming deltas are applied back into the Store via `apply` (LWW).
- First contact (`since = None`) yields the full write set of the node.
- Per-peer get_latest_for_node_id is sufficient; a full VersionVector is not required.

### 6. Anti-entropy evolution path

| Phase | Mechanism                                 | When                         |
| ----- | ----------------------------------------- | ---------------------------- |
| 1     | Per-peer timestamp get_latest_for_node_id | Now                          |
| 2     | Hybrid Logical Clock                      | Before wide multi-device use |
| 3     | Merkle tree over key space                | Large stores / high churn    |
| 4     | Optional content-defined diff             | Very large values            |

## Consequences

**Good**

- True multi-master, offline-first behaviour with no central service.
- Extremely small core; storage and transport are replaceable.
- Live propagation of writes while a session is open.
- Clear, incremental path to more sophisticated anti-entropy.
- Suitable for embedding (single-file backend, minimal dependencies).
- Fully generic over key and value types.
- Single, well-understood conflict rule (LWW) — easy to reason about and test.

**Trade-offs**

- Concurrent updates on the same key: LWW discards the loser (no multi-value or conflict copies).
- Tombstones accumulate until a GC story is added.
- Wall-clock timestamps can mis-order events under clock skew (mitigated by planned HLC).
- Latest timestamp-based deltas are efficient only for “changes from this peer”; full-store reconciliation will need Merkle trees later.
- No built-in authentication or encryption — left to the transport.
- Each replica must persist a stable `node_id`.

## Alternatives considered

| Alternative                        | Reason for rejection / deferral                                     |
| ---------------------------------- | ------------------------------------------------------------------- |
| Full-state transfer only           | Too costly for non-trivial data sets                                |
| Central primary / leader           | Contradicts offline-first and “no shared root”                      |
| Operation-based CRDT log           | Higher complexity; state-based LWW is enough                        |
| Pluggable merge strategies         | Unnecessary complexity; LWW is sufficient for all planned use cases |
| Automerge / Yjs / similar          | Excellent for documents; heavier and less generic as a KV           |
| Opaque sync tokens                 | Unnecessary; a single per-peer timestamp is sufficient              |
| Mandatory Merkle tree from day one | Adds complexity before it is required                               |
| Built-in cache in Store            | Storage backends already provide concurrent access                  |

## Implementation outline

1. Core types + `Storage` trait
2. redb + in-memory backends
3. `Store` + LWW merge + change broadcast
4. Async `Sync` trait + get_latest_for_node_id protocol
5. Hybrid Logical Clock
6. Merkle-tree anti-entropy (optional)
7. Tombstone GC, compression, auth hooks
