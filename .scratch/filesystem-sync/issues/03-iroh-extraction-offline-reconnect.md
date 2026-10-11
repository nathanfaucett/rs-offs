# 03 Extract Iroh adapter and verify filesystem reconnect

Status: resolved
Type: task
Work status: completed; integrated extraction and original-path consumer adapter/reconnect acceptance verified
Blocked by: 01, 02

## Question / outcome

Can `of` own filesystem exchange integration, domain descriptors/trust and local authorization over reusable `ofnet/iroh-chain` networking while `offs` retains persistence/reconciliation and verified generic execution, integrity, cleanup and offline/reconnect semantics, without owning concrete transport?

## Prerequisites and co-delivery

Read [of offline-first-nodes map](../../../../of/.scratch/offline-first-nodes/map.md) and [ofdb storage-sync map](../../../../ofdb/.scratch/storage-sync/map.md). Prerequisite acceptance is offs 01–02 and [ofdb 01](../../../../ofdb/.scratch/storage-sync/issues/01-kv-facade-parity.md), [ofdb 02](../../../../ofdb/.scratch/storage-sync/issues/02-native-sql-staging.md), [ofdb 03](../../../../ofdb/.scratch/storage-sync/issues/03-sql-pre-apply-authorization.md), [ofdb 04](../../../../ofdb/.scratch/storage-sync/issues/04-kv-pre-apply-authorization.md), [ofdb 05](../../../../ofdb/.scratch/storage-sync/issues/05-local-regression-qualification.md). These are actual public-contract/generic-test handoffs, not tracker-existence acceptance.

Extraction is co-delivery with [of 02 public persistence and sync integration](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md): implement the move, consumer changes and shared cutover evidence together. Neither ticket requires the other to be resolved first. Choose the concrete `of` module during implementation; no speculative destination path blocks readiness.

Detailed local execution integration follows in [of 03 execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) after [of 01](../../../../of/.scratch/offline-first-nodes/issues/01-local-identity-policy-and-grants.md) and of 02, using hooks supplied by offs 01–02. Full Node acceptance follows in [of 08](../../../../of/.scratch/offline-first-nodes/issues/08-offline-node-integration-and-final-validation.md). Neither is required before extraction. [ofdb 06 consumer integration handoff](../../../../ofdb/.scratch/storage-sync/issues/06-consumer-integration-handoff.md) records downstream evidence, not a prerequisite; do not introduce a cycle through it.

## Scope

Extract filesystem-specific integration from `offs/crates/file-system/src/iroh_transport.rs` into `of`: domain resource descriptor/binding, protocol-specific handles, marker/wire envelope, RPC/framing, authorization and protocol stream/task/queue lifecycle. Compose this adapter over existing [ofnet/iroh-chain](../../../../ofnet/crates/iroh-chain/src/lib.rs) facilities, which own the reusable endpoint mesh, generic connections, routing, pairing and allowlist enforcement. Generic exchange interfaces, session semantics/messages, generic codec, MetadataSync, persistence and memory test support stay in `offs`.

Audit [Server facilities](../../../../ofnet/crates/iroh-chain/src/server.rs) against actual consumer needs before adding machinery: endpoint access, direct connections/streams, protocol routing, mesh messages, pairing and allowlist hooks already exist. Only genuinely reusable missing networking machinery belongs in `ofnet`; do not blanket relocate FS codecs, framing, RPC or session semantics there, invent new traits, or rewrite networking. Generic lifecycle belongs in `ofnet`; domain/protocol lifecycle and resource authorization remain `of` responsibilities.

Endpoint allowlisting admits networking, not resource operations. Pairing is limited bootstrap, not full DB/FS access or proof of domain trust. `of` supplies domain-derived trusted endpoint state and local resource decisions. Locally learned trust removal, revocation, restriction or resource denial must prevent subsequent affected operations on both peers, including established connections/file sessions, without interrupting an already-admitted atomic unit.

Mesh message routing is not a transparent multihop DB/FS stream guarantee or authentication of arbitrary origin claims. Direct connections/streams and routed messages are distinct; record the actual exchange path and authenticated peer binding instead of promising stream forwarding or provider reachability through intermediaries.

Known consumers are `of/crates/storage-server/src/storage_protocol.rs`, `storage_protocol_tests.rs`, and `Cargo.toml`; audit consumers/features again before removal. This documentation-only update edits neither `of` nor `ofnet`.

## Acceptance criteria

- [x] After prerequisite acceptance, co-deliver extraction with of 02; select destination, update all consumers and record shared cutover evidence. No mutual completion blocker, forwarding wrapper or old adapter implementation. Reuse ofnet adds no new ticket-status dependency or cycle.
- [x] Record existing ofnet facility reuse and any concrete gap with its reusability justification. Only actually reusable missing endpoint/connection/routing/pairing/allowlist machinery belongs in ofnet; keep FS exchange integration/domain descriptors/auth in of and generic FS codecs/session semantics in offs. No blanket FS protocol relocation or networking rewrite.
- [x] Remove obsolete Iroh module/re-exports, Iroh feature/dependency and adapter-only dependency features after usage audit. Change manifests/locks only during implementation; retain generic dependencies.
- [x] Move filesystem Iroh adapter regressions and adapter authorization tests to `of`; generic filesystem tests stay in `offs`, and regressions for genuinely reusable networking machinery belong in `ofnet`. Distinguish moved transport/stub tests from future real local-policy acceptance.
- [x] of 02 consumes delivered generic APIs/hooks without duplicating filesystem execution. Document the local execution handoff to of 03; per-frame checks are not apply admission. Detailed policy wiring and replacing obsolete timer/remote-policy behavior belong to of 03, not this extraction completion gate.
- [x] `offs` verifies disconnected Full edits, reconnect/announce reconciliation, tombstones/late join, revision-specific content retrieval and unchanged device-local residency/Passthrough restrictions, plus 01–02 execution/cleanup regressions. No arbitrary concurrent-byte merge claim.
- [x] Co-delivery verifies consumer/adapter builds, moved Iroh regressions, protocol stream/task/queue lifecycle over reused ofnet facilities and filesystem exchange/reconnect without a new central-authority or freshness requirement. Record direct connection/stream versus routed-message use and actual authenticated peer binding; no transparent multihop DB/FS stream guarantee is inferred from mesh routing. Every node having local IdP/Management/Storage and real locally permitted authenticated reconnect are downstream `of` acceptance, not an implemented `offs` claim.
- [x] Link the follow-up of 03 acceptance for both peer paths: actual authenticated peer/resource binding, pairing/allowlist admission distinguished from resource permission, locally learned trust/allowlist and resource-policy changes denying the next affected operation on established connections/sessions, denial between receive/apply and queued writes, pending-policy cancellation, admitted-operation completion, prevention of later work and retained valid progress. Link of 08 full offline Node acceptance; neither must pass before extraction resolves.
- [x] No custom quotas, production timeouts/deadlines, freshness leases or speculative exchange trait. Preserve protocol-shape validation and generic integrity/cleanup guarantees.
- [x] Record prerequisite APIs/evidence and shared extraction results in map/stable docs; leave detailed auth follow-up open in its owner rather than marking it proven or blocking extraction on it.

## Complexity, risk, priority

Complexity: medium (cross-workspace orchestration and reuse audit). Risk: high (losing execution/cleanup guarantees or treating network admission as domain/resource permission). Priority: third, after offs 01–02 and ofdb 01–05 acceptance; co-deliver with of 02.

## Verification

Follow the [shared qualification policy](../spec.md#shared-qualification-policy) and [map summary](../map.md#shared-qualification-policy). Current gates are watchdog-bounded focused native checks on Ubuntu Linux x86_64: selected generic admission/outbound/integrity/native/reconciliation/session/catalog/residency regressions, focused adapter/public-consumer compilation and moved `filesystem_transport::` regressions with the needed `storage-server` network features during of 02 co-delivery, plus selected router/resource-binding/lifecycle tests. Record host/revisions, exact package/target/filter/features, commands/bounds and results or gaps. No `cargo hack`, feature-powerset, workspace/all-target/all-feature lint, full coverage or CRAP sweep is a current closure gate.

Keep ownership, ofnet reuse, consumer/feature audit, obsolete-adapter removal and co-delivery criteria unchanged. Real protocol/reconnect qualification still needs isolated same-host peers with separate endpoint identities, stores and lifecycle state, actual authenticated peer/resource binding and the direct stream/router path. Verify offline Full edits, metadata/tombstone reconciliation, revision-specific complete retrieval, denial/shutdown cleanup and retained local usability. Do not substitute mocks for the real adapter path or call controlled callbacks real-domain policy. Label fresh streams on an existing authenticated connection separately from new-connection recovery; historical evidence is not upgraded to broader reconnect coverage.

Cross-platform/architecture, real WAN/NAT/relay and physical multi-device deployment/endurance are follow-up, not closure gates; real local protocol correctness, admission, cancellation/completion and integrity/cleanup are not waived. Real local execution authorization and full Node offline/reconnect acceptance remain of 03/of 08 responsibilities, without becoming extraction blockers or deployment-only deferrals. Source inspection is not passing implementation validation. Historical evidence below, Status and dependencies are unchanged; this docs-only update runs no builds/tests and adds no acceptance claim.

## Owner extraction evidence (2026-10-07; partial)

Owner worktree `.worktrees/filesystem-sync`, branch `filesystem-sync-owners`, started at `5e72a68dc562f11c6d5446bbbdbc2b51a22a6ca0`; integration ancestry confirmed. Loaded tdd and ponytail; complexity medium / risk high / priority third as assessed above. Original modified GLOSSARY.md and docs/metadata.md, original source/spec/AGENTS, and all sibling code/trackers remain untouched. Stable current API documentation is in owner `docs/execution.md` and `docs/metadata.md`.

### Source and consumer contract

Coordinated [of 02](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) comment confirms its agent read the original adapter in full and owns `of/crates/storage-server/src/filesystem_transport.rs`; source remains available in original offs and pre-extraction commit `5e72a68`. Removed concrete adapter/module/re-exports/feature/dependency and adapter-only production Tokio time/io-util/rt features. Generic codec, sessions, MetadataSync, memory exchange, Serde/Postcard, resource catalog redb and sibling dependency `../../../ofdb/crates/kv` remain. Cargo pruned 279 unused package versions; no added package versions or invented sibling paths. The existing untracked `.worktrees/ofdb -> ../../ofdb` symlink resolves the unchanged baseline dependency.

Consumer implements existing `Transport`, `FileSessionService`, `FileHandle`; session operation/response types and metadata version-1 codecs remain public. Newly public `MAX_FILE_READ_LENGTH` exposes the existing 1 MiB request shape bound. Construct handles from actual Opened entry/revision, not requested `None`; require ReadEnd for completion. Resource-bound `OperationControl` uses the authenticated peer for incoming requests and checks admission after execution access, not at frame receive. Pending sends/policy/queued access are cancellable; admitted mutations complete and later units recheck. PublishContent revalidates staged identity/revision before publication. Default policy permits: consumer must configure production local policy, not duplicate apply/staging or hold FS execution-lock APIs inside policy callbacks.

Consumer audit additionally found `of/crates/storage-server/src/data_protocol.rs` importing `file_system::FILESYSTEM_STREAM_KIND`; update it together with storage_protocol.rs, storage_protocol_tests.rs, and Cargo feature selection. Stream marker 2, version-2 envelope Hello/Metadata/Request/Response, resource descriptor fields owner_subject/application_id/filesystem_id and protocol RPC/tasks belong to consumer, not generic version-1 metadata codec.

Read-only ofnet audit found Server endpoint/router helpers, connect_direct/connect_direct_with_alpn/open_stream, pairing/is_allowed/peers/allowlist hooks. No demonstrated reusable gap; no ofnet edits or new dependency gate. Consumer must record actual shared endpoint/router composition and Connection::remote_id binding. Direct byte streams are not routed mesh messages or transparent multihop streams. Actual composition is not proven by this source audit.

### Regression ownership and TDD

Removed obsolete Iroh-dependent owner suite, rehomed generic behavior in transport-neutral `tests/admission.rs`:

- `generic_session_codecs_preserve_open_read_write_scan_and_close_semantics`: serialized operations/responses, revision-None open resolves actual metadata/revision, existing oversized-read rejection, read/write/revised read, directory scan, close/unknown handle, local edits after executor shutdown.
- `generic_metadata_codec_preserves_live_snapshots_and_tombstones`: versioned codec with actual live/deleted snapshots, imported live entry/deletion.

Adapter tests to port in of 02: cancelled_sync_closes_its_peer_stream_and_releases_local_filesystem; syncs_metadata_and_remote_file_sessions_over_one_stream; shutdown_during_live_multichunk_file_read_closes_stream. Adapter-internal denied_policy_cannot_send_filesystem_response and closing_transport_cancels_policy_wait_between_read_response_chunks require consumer lifecycle audit, not mechanical per-chunk mutation revocation. Generic denied/queued/pending-policy/pending-send/admitted-completion coverage remains in admission/outbound/integrity/native tests. Old boolean policy/transport regressions are not real local-policy acceptance.

Red before removal: Cargo metadata assertion failed `AssertionError: offs still owns concrete Iroh feature`. Session test first passed with existing baseline behavior, then consumer-limit import failed `E0603 MAX_FILE_READ_LENGTH is private`; public export and extraction made it green. Ownership assertion command (red then green):

```sh
cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; p=next(p for p in json.load(sys.stdin)["packages"] if p["name"]=="file-system"); assert "iroh" not in p["features"], "offs still owns concrete Iroh feature"; assert not any(d["name"]=="iroh" for d in p["dependencies"]), "offs still depends on concrete Iroh"; print("transport-neutral Cargo contract passed")'
```

Exact final verification in owner worktree:

```sh
CARGO_INCREMENTAL=0 cargo test -p file-system --test admission --test file_system --test integrity --test outbound --test resource
CARGO_INCREMENTAL=0 cargo hack test --feature-powerset --all-targets
CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings
CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git --no-pager diff --check
cargo tree --workspace --edges normal | python3 -c 'import sys; s=sys.stdin.read(); assert "iroh" not in s.lower(), "concrete Iroh in normal dependency graph"; print("normal dependency graph has no Iroh")'
CARGO_INCREMENTAL=0 just crap --summary
CARGO_INCREMENTAL=0 cargo llvm-cov report --lcov --output-path target/filesystem-sync-lcov.info
cargo crap --workspace --lcov target/filesystem-sync-lcov.info --summary
```

Focused suites: 41 passed (8 admission, 21 filesystem, 5 integrity, 2 outbound, 5 resource). Powerset: both remaining empty-feature configurations (`--no-default-features`, `--all-features`), 53 tests each including 12 unit/native/codec tests, pass. Both clippy commands, formatting/whitespace, metadata ownership and normal-graph no-Iroh checks pass. Includes reconnect_exchange_recovers_missed_updates_and_deletes, metadata_sync_converges_tombstones_and_late_joiners, syncs_content_and_rejects_stale_responses, unchanged residency/Passthrough and 01–02 regressions. Production timers/quotas/freshness were not added; dev Tokio time remains for test watchdogs.

CRAP runs completed; isolated LCOV repeated result: 137 functions, 4 above threshold 30, worst Error::fmt 386.7. Scope warning remains: file_system_tests.rs analyzed without LCOV match and transport.rs LCOV without analyzed match. This is not clean complexity/scope acceptance; unrelated complexity was not rewritten.

### Prior shared acceptance gap (superseded)

Historical checkpoint only: the integrated original-path verification in Answer below supersedes this gap. Consumer directory inspected read-only after owner validation: `of/.worktrees/public-persistence/crates/storage-server/src` still lacks filesystem_transport.rs. No consumer build, moved adapter tests, shared-endpoint lifecycle or real locally permitted authenticated reconnect was run/proven. Leave Status claimed / Work status partial; cross-owner acceptance stays unchecked. Do not run obsolete adapter-dependent offs suite as a substitute or claim extraction resolved. Detailed authenticated peer/resource/local policy integration and denial/revocation on both established-peer paths follow [of 03](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md), including pending-policy cancellation and admitted-operation retention; full offline Node acceptance follows [of 08](../../../../of/.scratch/offline-first-nodes/issues/08-offline-node-integration-and-final-validation.md). Neither downstream ticket status is introduced as a cycle, but this user's shared authenticated reconnect evidence is still absent.

## Commit and branch handoff (historical; superseded by Answer and map cleanup receipt)

Extraction committed separately as `516b6926136c348ba3e7a183e4145b8fde7c1fd5` (`Extract concrete filesystem transport ownership and retain generic codec coverage`). Re-ran final powerset/format/whitespace immediately before commit: pass. Then `GIT_EDITOR=true git merge --no-edit filesystem-sync-integration` returned `Already up to date`; integration tip `5e72a68dc562f11c6d5446bbbdbc2b51a22a6ca0` is an ancestor of final HEAD (`git merge-base --is-ancestor filesystem-sync-integration HEAD` exit 0), and owner status is clean. Owners was NOT integrated back. Original trackers are ignored, recorded here outside the commit; original modified domain documents remain preserved. Shared consumer acceptance gaps above remain open.

## Answer

Resolved **own extraction acceptance only**, after coordinated originals integrated: `of` `e7526fecc9ff570838e685eb9d37103d81d3df45`, `offs` `516b6926136c348ba3e7a183e4145b8fde7c1fd5`, `ofdb` `25b92516df6ef884d9cdf723da65c64568acdbd0`. All three original HEADs were verified directly; offs ancestry includes the extraction commit. Earlier missing-consumer-file/provisional-source-override/partial conclusions are superseded, not current blockers. of 02 may remain claimed for its broader domain/persistence scope; its completion status is not an offs03 gate.

Read the original [of 02](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) handoff, consumer [public-persistence contract](../../../../of/docs/public-persistence.md), concrete filesystem adapter and all moved tests. Current owner contract is [offs execution](../../../docs/execution.md); consumer docs record delivered APIs, extraction destination, direct-stream/router composition, transport lifecycle, owner-test split and downstream policy limits. Their earlier provisional-source/Management-integrity qualification notes describe pre-integration observations; the original-path results below supersede only extraction qualification, not unrelated domain acceptance.

### Integrated original-path verification

Ran from **`/home/nathan/code/rust/aicacia/of`**, with normal sibling manifest paths: no `--config paths` override, absolute source replacement or worktree-selected owner dependency:

```sh
CARGO_INCREMENTAL=0 RUST_MIN_STACK=16777216 cargo test -p storage-server --no-default-features --features network --lib filesystem_transport::
CARGO_INCREMENTAL=0 RUST_MIN_STACK=16777216 cargo test -p storage-server --no-default-features --features network --lib
```

First command: **6 passed / 0 failed / 44 filtered**. Second: **50 passed / 0 failed**, including the same six and DATA-router/resource-binding/shutdown tests. Compilation explicitly used original `/ofdb/crates/...`, `/offs/crates/file-system`, `/ofnet/crates/iroh-chain`, and `/of/crates/storage-server`. This independently confirms merger's reported original-source Storage50 result, rather than inheriting provisional worktree override evidence.

Moved six tests:

1. `filesystem_transport::authorization_tests::denied_policy_cannot_send_filesystem_response`.
2. `filesystem_transport::authorization_tests::closing_transport_cancels_policy_wait_between_read_response_chunks`.
3. `filesystem_transport::tests::delivered_read_chunk_then_channel_closure_is_not_successful_eof` — deterministic delivered-chunk/barrier then close yields BrokenPipe, not partial success.
4. `filesystem_transport::tests::cancelled_sync_closes_its_peer_stream_and_releases_local_filesystem` — peer stream/task exit and continued local filesystem use.
5. `filesystem_transport::tests::shutdown_during_live_multichunk_file_read_closes_stream` — pending read fails, peer sync exits, retained local usability.
6. `filesystem_transport::tests::syncs_metadata_and_remote_file_sessions_over_one_stream` — actual authenticated Iroh connection, descriptor handshake, metadata/remote read/write, established-handle controlled-policy denial before transmission without peer mutation; then offline Full edit, fresh stream on the **same authenticated connection**, exact-resource local callback acceptance, metadata reconciliation, retrieved offline bytes, handle/stream/task cleanup.

The last test is real transport/session reconnect with controlled local callbacks, **not** real IdP/Management action-policy or full Node qualification. Response-chunk denial/cancellation tests concern transport response lifecycle, not permission renewal inside an admitted mutation. Generic executor/native completion guarantees remain the previously qualified offs01–02 tests. The broader 50 also pass `data_protocol::tests::shutdown_cancels_inbound_stream_waiting_for_marker`, both spoofed-descriptor admission tests, filesystem server-shutdown cancellation, host-owned Server admission preservation, and persisted/restored endpoint identity. HTTP Management fixtures are not local-domain authorization proof.

Audit of original consumers found no old `file_system::Iroh*`, `file_system::FILESYSTEM_STREAM_KIND` or `file-system/iroh` use. `data_protocol.rs` now takes marker 2 from the local adapter; marker 1 stays database. Both dispatch over existing `iroh-chain::DATA_ALPN` and one host Server/router. Adapter owns descriptor/envelope/RPC queues/tasks/handles and uses authenticated `Connection::remote_id`; no duplicate Endpoint/mesh/pairing stack or reusable-networking gap was introduced. Direct byte streams are not routed mesh messages; no transparent multihop claim. Both storage-protocol peer paths install resource-bound `OperationControl` before `MetadataSync::run`; shutdown calls cancel and awaits the executor rather than aborting an admitted mutation. Generic content staging/apply/codec stays in offs, without compatibility adapter.

Also reran original offs Cargo metadata ownership check (no Iroh feature/dependency): pass. Earlier owner powerset/clippy/fmt/CRAP evidence remains applicable to the unchanged extraction implementation; not rerun solely for ignored tracker edits. CRAP limitations remain 4/137 above threshold and documented source/coverage scope warning; no clean metric claim.

Merger additionally reports the formerly intermittent Management sync regression **10/10 passed** after ofdb `25b92516`; this is attributed merger evidence, not an offs-owner rerun or certification of broader Management/history acceptance. It does not block this extraction. No full consumer powerset/CRAP success is claimed here.

### Boundaries and preservation

Detailed local authenticated actor/resource/action checks, learned trust/revocation on both established peer paths, replacing legacy bounded remote Management/timer behavior and in-flight local-policy acceptance remain [of 03](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md). Every Node's local IdP/Management/Storage composition and full offline Node acceptance remain [of 08](../../../../of/.scratch/offline-first-nodes/issues/08-offline-node-integration-and-final-validation.md). **Neither is an extraction completion blocker.** No quotas/timers/freshness or speculative exchange seam added by offs; pre-existing consumer timeout behavior is a downstream of03 obligation, not newly qualified by this extraction.

Only original offs03/map trackers changed in this completion pass; no implementation source, other owner ticket or dirty domain docs edited, no new commit/merge and no worktree cleanup. Original offs GLOSSARY.md SHA256 `ec93a9cd20a651f40d27af82b185bc8cdf5b5dba94d5e594f74982b3734e73c2` and docs/metadata.md `7d7d7f493c7046970b2a9eb79095b22bb6101f684c733b9e918d4f10bee3bf41` stayed unchanged across validation. Extraction hash remains `516b6926136c348ba3e7a183e4145b8fde7c1fd5`.

## Comments (historical planning; resolved Answer supersedes open/partial status)

Claimed for implementation on `filesystem-sync-owners` at clean integration tip `5e72a68dc562f11c6d5446bbbdbc2b51a22a6ca0`. User confirms ofdb 01–05 resolved/integrated and offs 01–02 resolved. Consumer owner is porting the original adapter into `of/crates/storage-server/src/filesystem_transport.rs`; offs owner edits offs only. TDD seams agreed by request: public generic session/metadata codec and in-memory execution/reconnect, preserving meaningful adapter-independent coverage. Consumer authenticated reconnect/build evidence is a shared acceptance requirement; absent evidence leaves this ticket partial, not resolved.

Coordination uncertainty is satisfied by current [of 02](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) and [ofdb 05](../../../../ofdb/.scratch/storage-sync/issues/05-local-regression-qualification.md) handoffs. The approved networking split refines reuse/ownership without changing the ticket graph: offs 01–02/ofdb 01–05 prerequisite acceptance, shared offs 03/of 02 cutover, then detailed of 03 authorization and of 08 Node acceptance. Any ofnet gap must be evidenced and actually reusable, not a speculative new prerequisite. ready-for-agent is a triage state, not prerequisite resolution; Work status remains open. [Spec dependencies](../spec.md#cross-workspace-dependencies).
