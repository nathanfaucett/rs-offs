# Filesystem sync ownership and execution

Status: resolved
Scope: completed `offs` owner admission, content integrity and transport extraction slices 01–03. Detailed consumer policy and full Node acceptance remain outside this owner completion.

This spec retains the original contract and implementation plan. Historical baseline observations and gaps below describe the pre-implementation state; resolved tickets and the [map](map.md) record actual qualification and limits. Current owner behavior is documented in [execution](../../docs/execution.md).

## Goal and ownership

`offs` owns filesystem persistence and synchronization/reconciliation: receive, stage, validate, apply, write, cleanup, and generic cancellation/authorization-before-apply execution support, not concrete transport. `ofnet/iroh-chain` owns the reusable endpoint mesh, generic connections, routing, pairing, and allowlist enforcement. `of` owns domain trust, local authorization, authenticated peer/resource binding, domain descriptors, and protocol-specific exchange adapters composed over those networking facilities. `ofdb` supplies KV persistence and storage CRDT/history reconciliation primitives.

Retain the existing `Transport<PeerId>` and `FileSessionService<PeerId>` in `crates/file-system/src/transport.rs` as the exchange interface, with existing file handles, operations, responses, and `SyncMessage`. No speculative replacement trait, legacy wrapper, or parallel old adapter. Derive the smallest admission/cancellation API from the current executor during implementation; this plan does not invent its signature.

## Networking reuse and authorization boundary

Reuse existing [ofnet/iroh-chain](../../../ofnet/crates/iroh-chain/src/lib.rs) facilities before extracting or adding networking machinery. Its [Server](../../../ofnet/crates/iroh-chain/src/server.rs) exposes endpoint access, direct connections/streams, protocol routing, mesh messages, pairing, and allowlist hooks. Audit actual adapter needs during offs 03/of 02 co-delivery; only missing machinery that is genuinely reusable independently of DB/FS protocols belongs in `ofnet`. Do not prescribe a new trait, networking rewrite, or blanket move of filesystem codecs, framing, RPC or session semantics into `ofnet`. Generic filesystem messages/codecs/sessions remain in `offs`; filesystem exchange integration, domain descriptors and authorization belong in `of`.

Endpoint allowlist admission is not resource authorization. Pairing is a limited bootstrap exchange, not full DB/FS access or proof of domain trust. `of` derives trusted domain bindings and supplies allowed endpoint state; `ofnet` enforces networking admission. Both peers must still evaluate local domain/resource permission for each subsequent operation, including on established connections and file sessions. Locally learned trust removal, revocation, restriction or resource-policy denial prevents the next affected operation; opening a connection or handle is not permanent permission. Already-admitted operations retain the completion contract below.

Routed mesh messages do not establish transparent multihop DB/FS byte streams or authenticate an arbitrary claimed origin as the resource actor. Direct connection/stream facilities and routed messages are distinct. Extraction/reconnect acceptance must record the actual exchange path and authenticated peer binding without promising stream forwarding, availability through arbitrary intermediaries, or a new relay protocol.

## Offline-first contract

Every node has local IdP, Management, and Storage capabilities and permits changes locally. Node composition and policy evaluation belong to `of`. Storage CRDT reconciliation has no central authority and no synchronization-freshness expiry. A remote Management request is not required to permit locally authorized work; disconnection alone does not expire permission. Locally known denial remains denial.

Full-residency local reads/writes continue offline. Passthrough remains read-only and uncached content still needs an available Full provider. Reconnect uses existing metadata exchange and revision-specific content retrieval. Do not change residency or filesystem conflict behavior to imply unlimited offline content availability.

## Verified baseline

Historical pre-implementation source observations, not current missing owner capabilities.

- `sync.rs`: announce exchanges snapshot requests and complete winning-key `KvSnapshot` sets, including tombstones. Committed local changes trigger publication; lag in the running loop triggers full exchange. Caller-driven reconnection starts/announces the replacement exchange.
- Snapshot receive calls `database.import_snapshots` directly; file-session receive executes Open/Read/Write/Scan/Close with peer-scoped handles and revision checks. Neither currently provides an apply-boundary authorization gate.
- Sibling `ofdb/crates/kv/src/database.rs` imports the supplied snapshot set transactionally. `ofdb/crates/kv-store/src/store.rs` validates snapshots, ignores older generations, replaces them with newer generations, and merges same-generation divergent Automerge histories. Source tests cover divergent-history convergence; they were inspected, not run. The old glossary assertion that divergence is rejected was stale.
- Paths are metadata keys; stable file ids identify separately stored content. UUIDv7 content revisions are independent of KV generations. Path deletion is a KV tombstone; metadata rename uses a transaction.
- `store_content` stages fetched bytes, synchronizes the file, rechecks the revision, then renames it into place. Full/Passthrough longest-prefix rules remain device-local. Catalog resource deletion retains a tombstone; projected-copy eviction is local, not replicated resource deletion.
- `protocol.rs` has a version-1 generic metadata codec. The Iroh adapter has a separate version-2 envelope, stream marker, resource descriptor, handles, RPC and network lifecycle. Do not conflate these protocols or move generic reconciliation into transport.

This is specific filesystem behavior over CRDT-backed KV metadata, not proof of a generic filesystem/content CRDT, concurrent byte merging, or one atomic content-plus-metadata transaction. Preserve existing behavior; diagnose invalid merged filesystem metadata rather than inventing a new merge model.

## Execution and integrity contract

1. Reception, frame checks, decoding, staging, and enqueueing do not authorize later mutation. `of` evaluates policy locally; `offs` uses that decision at the actual boundary before each new remote apply/write operation. Serving session operations also needs caller admission; opening a handle is not indefinite permission for future operations.
2. Operations are existing semantic execution/mutation units, not invented timed batches or byte quotas. Denial or cancellation before admission prevents mutation and cancels pending authorization/unstarted work. Explicitly test admission-versus-cancellation races.
3. An already-authorized operation may finish its atomic unit after cancellation or a later denial. Do not reauthorize its chunks, abort a valid commit, or impose a deadline. Subsequent operations need a new decision. A lost response does not undo committed progress or establish exactly-once RPC.
4. Stage incomplete content separately. Validate requested identity/revision and complete transfer before publication; invalid, stale, denied, or interrupted work must not replace valid content. Revision checks are not cryptographic integrity proof; no digest protocol is asserted here.
5. Failed snapshot import applies none of that set. Failed file writes must not publish partial content or a mismatched visible revision. Establish safe failure boundaries for in-place writes and content/metadata ordering without changing offset semantics or claiming cross-store crash atomicity already exists.
6. Clean temporary artifacts and generic execution/file-handle resources on success, error, denial, and cancellation. `of` releases protocol streams, tasks, queues and pending responses using `ofnet` lifecycle facilities where applicable; reusable endpoint/connection lifecycle remains in `ofnet`. Stopping sync must not evict ordinary local data.
7. Retain earlier committed valid metadata and complete content. Incomplete staging may be removed and retried; partial-byte resumption is not required.

No custom quotas, operation timeouts, deadlines, freshness leases, or per-chunk permission-renewal rules. Existing frame/read/scan shape validation remains; it is not a quota/freshness system. Test watchdogs are test-only safeguards.

## Known gaps

Historical planning gaps: owner admission/staging/write-integrity fixes are recorded in tickets 01–02; extraction acceptance is in 03. Cross-content/database crash atomicity and detailed consumer policy are not established by owner completion.

- Current Iroh authorization is per frame and response chunk, not at queued filesystem apply. It must not become a mid-operation revocation rule under the target contract.
- `store_content` can leak its temporary file on stream error (`chunk?`), dropped future, `sync_all` error, and other early returns. Its revision-check-to-rename concurrency is not proven safe.
- `write_at`/`append` modify content in place before metadata update; `write` publishes content before metadata. Error/crash atomicity across content and metadata is unverified.
- Current sibling `of` authorization uses Management calls and explicit policy timeouts. [of 03 execution-boundary authorization](../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) owns replacing that wiring with local evaluation; this documentation-only change does not implement it.

## Vertical implementation slices

1. [01 Remote operation admission](issues/01-remote-operation-admission.md): receive-to-apply metadata/file-session admission, cancellation before admission, admitted-operation completion, generic tests.
2. [02 Staged content and write integrity](issues/02-staged-content-write-integrity.md): stage-to-publication cleanup, safe write failure boundaries, retained progress, generic fault/cancellation tests.
3. [03 Iroh extraction and offline reconnect](issues/03-iroh-extraction-offline-reconnect.md): adapter extraction co-delivered with [of 02 public persistence and sync integration](../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md), obsolete-surface removal and generic regression acceptance. Reuse the existing `ofnet/iroh-chain` networking facilities while placing filesystem exchange integration/domain adapters in `of`. Detailed local authorization acceptance follows in [of 03](../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md), not as an extraction prerequisite.

## Cross-workspace dependencies

- Current owner maps: [of/.scratch/offline-first-nodes/map.md](../../../of/.scratch/offline-first-nodes/map.md) and [ofdb/.scratch/storage-sync/map.md](../../../ofdb/.scratch/storage-sync/map.md). Tracker existence is verified; implementation acceptance is not claimed.
- Prerequisite acceptance for extraction: local 01–02 and database [ofdb 01](../../../ofdb/.scratch/storage-sync/issues/01-kv-facade-parity.md), [ofdb 02](../../../ofdb/.scratch/storage-sync/issues/02-native-sql-staging.md), [ofdb 03](../../../ofdb/.scratch/storage-sync/issues/03-sql-pre-apply-authorization.md), [ofdb 04](../../../ofdb/.scratch/storage-sync/issues/04-kv-pre-apply-authorization.md), [ofdb 05](../../../ofdb/.scratch/storage-sync/issues/05-local-regression-qualification.md) (facade parity, native staging, SQL/KV execution hooks, local qualification). Generic filesystem work can use controlled decisions and does not await external domain integration.
- Co-delivery: offs 03 and [of 02 public persistence and sync integration](../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) implement and accept the adapter move and consumer cutover together. Neither requires the other ticket to be resolved before starting or completing the shared cutover. Select the concrete `of` destination and audit reuse of `ofnet/iroh-chain` during implementation; neither a speculative destination nor new networking API is a planning blocker. Only an evidenced, actually reusable networking gap is an `ofnet` handoff; no new ticket-status dependency is introduced by this split.
- Follow-up: [of 03 execution-boundary authorization](../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) supplies detailed local execution integration after [of 01 local identity/policy/grants](../../../of/.scratch/offline-first-nodes/issues/01-local-identity-policy-and-grants.md) and of 02, using owner hooks. [of 08 full Node integration](../../../of/.scratch/offline-first-nodes/issues/08-offline-node-integration-and-final-validation.md) supplies final Node acceptance. Neither is an extraction completion gate.
- [ofdb 06 consumer integration handoff](../../../ofdb/.scratch/storage-sync/issues/06-consumer-integration-handoff.md) records downstream consumer/auth evidence after ofdb 05 and external integration; it does not block extraction or of 02 co-delivery.
- Known extraction consumers: `of/crates/storage-server/src/storage_protocol.rs`, `storage_protocol_tests.rs`, and `crates/storage-server/Cargo.toml`. Audit all consumers/features when implementing.

Only `offs` documentation changes now. Sibling tracker documents are read-only handoff references.

## Verification

Generic tests in `offs`: denial between reception and import/write, cancelled policy waits, admission race, invalid snapshot rollback, peer-scoped handles, stale revisions, cancelled polled multi-chunk fetch, fault cleanup and write publication, retained earlier commits, disconnected Full edits, reconnect/late join with tombstones, unchanged residency/Passthrough behavior.

Extraction acceptance co-delivered with of 02 covers adapter/consumer builds, moved Iroh regressions, transport lifecycle and generic filesystem reconnect evidence. Detailed authenticated local execution integration follows in [of 03](../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md): actual peer/resource binding, pairing/allowlist admission distinguished from resource permission, locally learned trust/allowlist and resource-policy changes preventing the next affected operation on both peers even on established connections/sessions, pending-policy/staging shutdown, admitted-operation completion, prevention of later work and cleanup. Full offline Node/reconnect acceptance follows in [of 08](../../../of/.scratch/offline-first-nodes/issues/08-offline-node-integration-and-final-validation.md). Elapsed disconnection or unavailable remote Management alone must not deny locally permitted work; these downstream suites do not block extraction completion.

The original implementation validation plan called for focused tests, feature-powerset, formatting/clippy and CRAP evaluation. Actual historical results and limitations belong to the resolved tickets and map; the later user validation override in the map governs reruns. This documentation checkpoint runs no builds/tests and makes no new code-test, complexity or consumer acceptance claim.
