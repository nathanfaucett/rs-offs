# 02 Staged content and write integrity through cancellation

Status: resolved
Type: task
Work status: complete
Blocked by: 01

## Question / outcome

Can fetch/write failure, denial or cancellation avoid staging leaks, partial publication and visible content/revision mismatch while retaining valid committed progress?

## Scope

Exercise 01 through `MetadataSync::fetch`, `store_content`, session writes and `file_system.rs` write/write_at/append/write_file paths. Persistence, validation and cleanup remain in `offs`. Preserve stable ids, revision matching, offset/append results, provider selection, tombstones and residency.

## Acceptance criteria

- [x] Stage fetched content separately; publish only after complete transfer, current identity/revision validation and admission. Stale, denied, interrupted or pre-admission cancelled work cannot replace valid content.
- [x] Poll fetch until staging exists and contains bytes, then cancel. Inject stream, write, sync and publish errors and revision change before publication. Assert cleanup and no partial visible replacement or loss of the valid previous file/revision where applicable.
- [x] Establish safe failure boundaries for in-place writes and content-before-metadata ordering. Inject persistence failure after content preparation but before metadata completion: no partial content or mismatched visible revision. Preserve successful offset/append behavior.
- [x] An already-admitted write finishes its atomic unit without a deadline or mid-commit cancellation; later writes need new admission.
- [x] Keep earlier successful metadata sets and complete files after a later failure. Remove only abandoned staging/execution resources, not ordinary local data or replicated tombstones.
- [x] Cancelled work can retry through existing exchange; no resumable partial-byte protocol is required.
- [x] Release relevant generic handles/resources on termination and verify local Full reads/writes after stopping sync. Protocol task/stream/queue teardown belongs to `of`, using reusable `ofnet` lifecycle facilities where applicable; `offs` owns no concrete transport.
- [x] Generic fault/cancellation tests live in `offs`, without online policy/Iroh, custom quotas/timers/deadlines, generic content-CRDT merge or speculative digest protocol.
- [x] Record proven atomicity/concurrency boundaries separately from unproven cross-content/database crash atomicity; establish any concrete lower-engine dependency before using it.

## Dependencies and handoff

Local: 01. Coordinate relevant transaction/cancellation/durability guarantees with [ofdb 04 KV pre-apply authorization](../../../../ofdb/.scratch/storage-sync/issues/04-kv-pre-apply-authorization.md) and [ofdb 05 local qualification](../../../../ofdb/.scratch/storage-sync/issues/05-local-regression-qualification.md) under the [storage-sync map](../../../../ofdb/.scratch/storage-sync/map.md); transactional KV import is not a content-plus-metadata transaction. [of 02](../../../../of/.scratch/offline-first-nodes/issues/02-public-persistence-and-sync-integration.md) consumes the delivered filesystem API during offs 03 co-delivery; [of 03](../../../../of/.scratch/offline-first-nodes/issues/03-execution-boundary-authorization.md) supplies detailed local execution integration afterward. Neither downstream ticket completion blocks this generic ticket.

## Complexity, risk, priority

Complexity: medium-to-high (separate content/metadata persistence). Risk: high (data integrity and staging leaks). Priority: second.

## Verification

Follow the [shared qualification policy](../spec.md#shared-qualification-policy) and [map summary](../map.md#shared-qualification-policy). On current Ubuntu Linux x86_64, run only watchdog-bounded focused integrity and native `file_system_tests` targets, plus selected session/revision/residency regressions as relevant; use the necessary native feature selection, not broad final validation delegated to 03. Record host/revision, exact commands/bounds, results and gaps; no `cargo hack`, feature-powerset, workspace or CRAP sweeps are closure gates.

Use deterministic polled streams, decisions and fault injection; dropping an unpolled future is not staging-cleanup evidence. Preserve complete revision-bound publication, stale/denied/interrupted rejection, native write-failure boundaries, admitted completion, retry, handle/stage cleanup and retained valid progress. Generic fixtures do not prove real transport or real-domain authorization; 03 retains real isolated same-host adapter/reconnect qualification. Cross-platform, real WAN and physical multi-device deployment are follow-up, not closure gates, without extending the proven in-process guarantees to crash atomicity or multi-instance concurrency. Historical evidence below, Status and dependencies are unchanged; no implementation validation is run or upgraded by this documentation update.

## Comments

Claimed on 2026-10-07 after ticket 01 commit `ad32e62` and focused/regression acceptance. Same owners worktree; no extraction or sibling changes.

Current `store_content` can leak temporary files on `chunk?`, cancellation and other early returns; write_at/append mutate in place before metadata update. These are gaps, not established safe behavior. [Spec](../spec.md#known-gaps).

## Answer

Implemented after 01 in commit `c5acf9d21fe070bf52a00bf0851ddbe565f3df44` on `filesystem-sync-owners`. Native immutable complete content lives at `.data/<stable-file-id>/<revision>`; metadata selects the visible version. Overwrite/offset/append prepare and synchronize a new version without touching the previous bytes. RAII cleanup removes incomplete stages and uncommitted published versions on errors/drop. Metadata failure after preparation/publication retains the old content and revision. Rename prepares destination revision content before its existing metadata transaction. Read/stream/residency and mutations serialize with publication per filesystem instance; imports/writes/rename/delete cannot race the final identity/revision check and rename.

`MetadataSync::fetch` selects cancellation against provider open and streaming, then reacquires execution access, rechecks identity/revision, and requests a new `RemoteOperation::PublishContent` decision immediately before publication. Complete old revisions are retained; incomplete work retries via existing exchange. EOF closes the generic handle; cancellation/error/drop releases the owned handle. Local session Read rejects a provider revision change between requests (avoids mixed-version transfers), and admitted Write updates the handle revision. In-memory reads require explicit `ReadEnd`; a closed response channel is not successful EOF. Server native handles no longer duplicate revision state behind an obsolete dynamic wrapper. No concrete transport ownership, adapter extraction, domain evaluator, quota, timer, freshness or digest protocol was added.

### Exact behavioral evidence

- `CARGO_INCREMENTAL=0 cargo test -p file-system --test integrity`: five passing transport-neutral tests. `cancelling_a_polled_multichunk_fetch_cleans_staging_and_retries` polls until one stage exists with literal `partial` bytes, cancels, checks no visible content/stage, verifies handle destruction and successful retry. `dropping_a_polled_fetch_and_stream_errors_remove_only_abandoned_staging` covers dropped polled futures and stream errors, retaining an earlier complete file. `revision_change_during_staging_cannot_replace_newer_valid_content`, `complete_staging_still_requires_a_new_publication_decision`, and `imported_new_revision_needs_new_content_and_keeps_earlier_complete_bytes` cover stale/denied publication and retained old versions/new retrieval.
- Final `CARGO_INCREMENTAL=0 cargo test -p file-system --lib file_system_tests`: six tests passed. `write_offset_and_append_faults_preserve_content_revision_and_cleanup` runs 12 combinations (overwrite/offset/append × partial-stage-write/sync/publish/pre-metadata-completion fault), comparing exact old revision/content and the native file inventory, then successful offset/append retries. `fetch_write_sync_and_publish_faults_keep_metadata_and_earlier_files` covers the three native fetch fault points and retry. `admitted_session_write_finishes_when_cancelled_at_native_publication` cancels after admission at the real publish checkpoint and verifies complete bytes/new revision plus offline append after sync stops. `cancellation_while_queued_for_execution_never_reaches_policy_or_mutation` holds execution access, polls queued work, cancels before policy invocation, and verifies no mutation/resources. `revision_bound_session_reads_reject_provider_changes_between_reads` and `a_closed_read_response_is_not_successful_transfer_completion` reproduced failures (red), then passed with revision/ReadEnd fixes.
- The initial polled-fetch cancellation test hung before the fix (30s test-only terminal watchdog); it then passed. The first feature-powerset attempt timed out at 300s during Iroh compilation; same-bound cached retry passed, and final rerun after all fixes passed all four combinations: `CARGO_INCREMENTAL=0 cargo hack test --feature-powerset --all-targets`. Final no-Iroh combinations each pass 49 tests (12 unit, 6 admission, 21 filesystem, 5 integrity, 5 catalog); Iroh-enabled combinations also pass existing authorization/transport regressions, including all three Iroh integration tests. No Iroh source/tests were modified.
- `CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed. All-feature clippy additionally attempted and failed only at pre-existing `iroh_transport.rs:873/989` `clippy::items_after_test_module`; diff against integration confirms that file unchanged. This is a recorded gap, not a clean all-feature lint claim.
- `CARGO_INCREMENTAL=0 just crap` ran coverage/tests and evaluation, reporting 12 functions over threshold 30. Its shared `/tmp/lcov.info` was later overwritten by another workspace's coverage run, so final reproducible evidence uses `RUST_MIN_STACK=67108864 CARGO_INCREMENTAL=0 cargo hack llvm-cov --workspace --lcov --output-path target/filesystem-sync-lcov.info` then `cargo crap --workspace --lcov target/filesystem-sync-lcov.info --summary`: 172 functions, 12 over threshold, worst `Error::fmt` 386.7. Default-feature coverage excludes Iroh and test sources while the analyzer includes them; it warns about scope mismatch. The touched `FileSystem::rename` is 31.0 (98.1% coverage). Complexity is evaluated, not claimed clean.

### Guarantees and gaps

Proven: in-process failure/cancellation-safe staging and revision-bound publication, admitted native write completion under delivered cancellation, transactional metadata-set rollback, retained earlier complete progress and unchanged offset/append/stable-id/residency/offline semantics on the new layout. Current original sibling redb-backed facade is the concrete dependency: its storage futures complete synchronously once polled and whole-set import rolls back on error. No new lower-engine API/version was assumed; baseline dependency path is unchanged and resolves through the same untracked local symlink noted in 01.

Not claimed: cross-content/database crash atomicity or directory fsync/crash recovery, cryptographic verification, byte CRDT, independently opened concurrent filesystem instances, forcibly aborted task completion, authenticated domain wiring or extraction. Content layout changes; no old flat `.data/<file-id>` compatibility/migration was added under project refactoring rules. Existing flat roots need an explicit migration or rematerialization before this layout. Complete old versions remain until residency eviction; no quota/retention timer is introduced. See worktree `docs/execution.md`.

After commit, `git merge --no-edit filesystem-sync-integration` returned `Already up to date`; integration tip `7501cfe020bd619bcb7ba348c67b4c26716a9a04` remains an ancestor of owners. Owners worktree is clean. No owners-to-integration merge was performed. Original GLOSSARY/metadata/AGENTS/spec and ticket 03 are preserved.
