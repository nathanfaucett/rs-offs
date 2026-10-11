# 01 Remote operation admission at filesystem apply

Status: resolved
Type: task
Work status: complete; P1 outbound review fixed
Blocked by: None

## Question / outcome

Can queued metadata and file-session work use caller-supplied local authorization at actual execution, with cancellation before admission and safe completion afterward?

## Scope

Implement minimal generic admission/cancellation support against `sync.rs`, `transport.rs`, and `file_service.rs`; retain `Transport`/`FileSessionService`. Derive and document the concrete API during implementation, not a speculative replacement trait. `of` evaluates identity/resource policy locally; `offs` does not call Management or implement identity, clock-freshness, deadline or quota policy. Local filesystem APIs remain offline-capable.

## Acceptance criteria

- [x] Hold received snapshots before import, switch caller decision to deny, release work: no denied metadata/tombstone mutation.
- [x] Hold a queued session write before execution, then deny: content/revision unchanged. Opening a connection or handle, endpoint allowlisting, and pairing do not indefinitely admit subsequent resource operations; cover admission of other session operations too. Controlled locally learned trust removal/revocation/restriction or resource denial must stop the next affected operation on established sessions; actual domain/network wiring follows in of 03.
- [x] Cancel pending authorization/unstarted work: no mutation and generic resources released. Deterministically test cancellation racing admission.
- [x] An admitted operation may finish its atomic unit after later denial/cancellation; subsequent work does not start. No chunk reauthorization or rollback of earlier valid commits because a response channel closes.
- [x] Invalid snapshot-set import applies none of the set and retains earlier successful imports, using existing transaction boundaries.
- [x] Preserve snapshots, revisions, peer-scoped handles, tombstones and Full/Passthrough semantics; no compatibility layer or speculative exchange trait.
- [x] Tests live in `offs` with in-memory exchange and controlled local decisions; no live IdP/Management/Iroh requirement, custom quota, timeout, deadline or freshness lease.
- [x] Record operation boundaries, admission-race behavior and actual cancellation/commit guarantees. Content publication/fault coverage is 02, not already proven here.
- [x] P1 review: initial exchange, changes and snapshot replies check `ExportSnapshots` for each actual destination; denied peers receive no initial/changed snapshots or initial requests. Polled pending outbound send waits are cancellable and release resources without interrupting admitted mutations.

## Dependencies and handoff

Generic tests use controlled local decisions and can complete before authenticated wiring. [of 03 execution-boundary authorization](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) integrates the delivered hook after [of 01](../../../../of/.scratch/offline-first-nodes/issues/01-local-identity-policy-and-grants.md) and [of 02](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md); it is not a prerequisite for this ticket. Coordinate KV import/transaction support with [ofdb 01 facade parity](../../../../ofdb/.scratch/storage-sync/issues/01-kv-facade-parity.md) and [ofdb 04 KV pre-apply authorization](../../../../ofdb/.scratch/storage-sync/issues/04-kv-pre-apply-authorization.md), qualified in [ofdb 05](../../../../ofdb/.scratch/storage-sync/issues/05-local-regression-qualification.md). See [storage-sync map](../../../../ofdb/.scratch/storage-sync/map.md); assume no unverified API or acceptance.

## Complexity, risk, priority

Complexity: medium (admission/cancellation race). Risk: high (queued mutation after transport checks). Priority: first.

## Verification

Run focused `cargo test -p file-system` admission/executor tests and record results. Final powerset/clippy/format/CRAP validation is tracked in 03.

## Comments

Reopened after P1 review: `announce` and `publish_changes` bypassed destination-local `ExportSnapshots` decisions, and outbound send waits bypassed cancellation. Fixing in owners only; ticket 03 and sibling repos remain untouched. Requested test seams: public `announce`/`pump`/`run`, transport-visible delivery per destination, and destruction of a polled pending send future.

Claimed on 2026-10-07 in `filesystem-sync-owners`, worktree `offs/.worktrees/filesystem-sync`. Confirmed `filesystem-sync-integration` (7501cfe) is an ancestor and initial HEAD; worktree clean. Scope is tickets 01 then 02 only.

[Spec](../spec.md#execution-and-integrity-contract). Receiving work is not apply authorization; later local denial stops the next operation, not an admitted commit.

## Answer

Implemented on `filesystem-sync-owners` in commit `ad32e62`. `OperationControl<PeerId>::new(callback)`, `cancel`, `is_cancelled`, `cancelled`, and `MetadataSync::with_operation_control` retain existing transport/session interfaces. `RemoteOperation` describes snapshot import/export, session operation plus peer-scoped path, and content publication (02). The execution lock precedes policy; the final cancellation read linearizes admission. Pending policy/lock cancellation prevents execution; admitted semantic operations finish without cancellation selection. Response backpressure is cancellable after commit, lost responses do not undo writes, and handles/queued resources are released. See worktree `docs/execution.md` for exact boundaries.

Evidence: `cargo test -p file-system --test admission` initially failed with missing interface (red). `CARGO_INCREMENTAL=0 cargo test -p file-system --test admission` passed all 6 tests: `queued_snapshots_recheck_local_decision_before_import` (tombstone denial/retry), `established_handle_rechecks_every_queued_operation` (all five operations and lost write response), `cancellation_drops_pending_policy_and_prevents_queued_mutation`, `cancellation_wins_when_policy_completes_in_the_same_poll`, `admitted_write_survives_cancellation_during_response_backpressure`, `invalid_snapshot_set_rolls_back_and_retains_prior_imports_and_tombstones`. Response-backpressure regression hung before the fix (60s test-only watchdog), then passed. `CARGO_INCREMENTAL=0 cargo test -p file-system --no-default-features` passed 6 unit + 6 admission + 21 filesystem + 5 catalog tests. Formatting and diff whitespace checks passed.

A short watchdog killed compilation and left incremental linker artifacts; disabling incremental compilation recovered validation. The unchanged baseline sibling path `../../../ofdb/crates/kv` resolves through untracked worktree-local symlink `offs/.worktrees/ofdb -> ../../ofdb`, not invented versions. Current embedded redb storage futures run synchronously once polled; snapshot import is transactional, verified by rollback regression. No forced task-abort/process-crash completion, authenticated domain wiring, content fault acceptance, or Iroh extraction is claimed here.

## P1 review correction

Reopened/claimed before source changes, then fixed and re-resolved in commit `5e72a68dc562f11c6d5446bbbdbc2b51a22a6ca0` on `filesystem-sync-owners`. Complexity: low (one shared send seam); risk/priority: high/P1 (metadata disclosure and shutdown stalls).

`MetadataSync::send_metadata` now supplies the single destination-local `RemoteOperation::ExportSnapshots` gate for each initial request/delta, changed snapshot set, and snapshot-request reply. `publish_changes` enumerates peers and sends individually instead of unconditional broadcast. Replies remain bound to the actual received peer, not the claimed peer in the request payload. Initial request and delta check independently, so admission of one message is not permanent permission for the next. Every outbound transport send is awaited through `OperationControl::wait`; cancellation drops pending transmission and prevents later sends. Already delivered messages are not retracted and committed filesystem progress is not undone. Snapshot import/session write/native publication mutation completion paths are unchanged. Existing queued-execution test counts session policy invocation separately from newly authorized outbound metadata decisions.

Exact red/green evidence:

- `CARGO_INCREMENTAL=0 cargo test -p file-system --test outbound initial_and_changed_snapshots_are_authorized_per_destination` failed before the authorization fix: `denied peer must receive no initial exchange`; then passed. Test verifies denied peer 2 receives neither initial exchange nor changes, allowed peer 3 receives complete initial/change sets, a denied request claiming peer 3 cannot obtain a reply or redirect it, later local revocation stops changes/tombstones and re-announcement, and reauthorization plus announce retries only to the allowed peer.
- `CARGO_INCREMENTAL=0 cargo test -p file-system --test outbound cancellation_releases_polled_pending_outbound_sends` failed after the authorization fix but before cancellable waiting: `AnnounceRequest: cancellation must release the pending outbound send`; then passed. Five deterministic paths are polled until the send starts and remains pending: announce request, announce delta, changed publication, snapshot reply, and `run` startup. After `cancel`, the next poll returns `Error::Cancelled`, the pending-send guard is destroyed before manually dropping the outer future, prior content/revision remains exact, and offline append still succeeds. No live networking/policy service or production timer is involved.
- Final `CARGO_INCREMENTAL=0 cargo test -p file-system --test outbound` passed 2/2 tests; `CARGO_INCREMENTAL=0 cargo test -p file-system --no-default-features` passed 51 tests (12 unit, 6 admission, 21 filesystem, 5 integrity, 2 outbound, 5 catalog). This includes `admitted_write_survives_cancellation_during_response_backpressure` and `admitted_session_write_finishes_when_cancelled_at_native_publication`, preserving the admitted mutation completion guarantee.
- Final `CARGO_INCREMENTAL=0 cargo hack test --feature-powerset --all-targets` passed all four combinations, including existing Iroh authorization/transport tests without modifying their sources. `CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed. No new all-feature clippy clean claim: the earlier unchanged Iroh test-module-order gap remains outside this fix.
- `CARGO_INCREMENTAL=0 just crap --summary` ran coverage and evaluation: 173 functions analyzed, 12 above threshold 30, worst `Error::fmt` 386.7. The default-feature report has the same documented analyzer/coverage scope mismatch (test sources and Iroh excluded from LCOV). Complexity is evaluated, not claimed clean.

Scope: source changes only in owners `sync.rs`, the operation-control doc comment, the existing session-policy counter regression, new transport-neutral `tests/outbound.rs`, and `docs/execution.md`. Original 01/map records updated; ticket 02, ticket 03, original domain docs, dependencies and other repositories untouched. `git merge --no-edit filesystem-sync-integration` returned `Already up to date`; ancestry confirmed, owners worktree clean, and no owners-to-integration merge performed.
