# Managed-copy retirement owner follow-up

Status: resolved
Work status: complete; independently reviewed and integrated into local main
Type: task
Label: ready-for-agent

Base: filesystem-sync-integration `9a25f8150f6467c92e33a4343aac3034af6ba038`.
Owner: offs only; no of/ofdb changes, no transport implementation.

## Contract and seams

Public FileSystemCatalog, FileSystem, LocalFileHandle/FileHandle, ReadStream and MetadataSync native behavior, as requested. Retire projected and created local copies without mutating catalog identities/tombstones or unrelated copies. Durable no-new-operation gate; admitted native operations drain before native handles/content/metadata/stages are removed. Failure, cancellation, retry and reopen remain fail-closed. Generic delete/rename enforce Passthrough. Residency stays process-local unless needed for correctness; restart defaults deny writes.

## Complexity / risk / priority

High priority owner capability gap. Main risks: nested admission deadlocks, native-handle leaks, reopen race, cancelled cleanup, descendant residency in rename. Reuse one owner lifecycle and existing operation serialization; no dependency or consumer cleanup implementation.

## Comments (historical; independent review below supersedes claimed status)

Read original of audit and public-persistence document read-only. Referenced managed-copy-retirement heading is not present in current original public-persistence; explicit audit contract used. Original offs clean at requested base. Independent parent review required before integration; this ticket remains claimed.

## Implementation / owner handoff (pre-review checkpoint)

Public `FileSystemCatalog::remove_managed_copy(id).await -> Result<(), Error>` retires created and projected copies. `evict_projected_copy(id).await -> Result<bool, Error>` is now async and shares this owner path. One fsynced durable sibling gate, shared native admission/drain slot, stream descriptor invalidation and OS-exclusive native lease implement cleanup without dropping catalog identity/Tombstones. Delete/rename enforce Full before mutation (all moved source/destination descendants for rename).

Retirement is permanent for this root/identity; same-ID reselection/reactivation is deliberately not implemented. Default/process-local residency rules are not persisted and reopen remains Passthrough. External native process ownership blocks cleanup with WouldBlock until released. No forceful foreign-descriptor or transport teardown claim.

Final focused results: retirement 7, resource 5, native/library 14, selected existing filesystem 7; all pass. Focused Clippy clean; touched-file formatting and diff checks pass. No CRAP/hack/sweep. Native RED/green logs, subprocess restart/lease and error/cancel/retry evidence: [evidence](../evidence/README.md). Contract: [managed-copy retirement](../../../docs/managed-copy-retirement.md).

Branch `owner/managed-copy-retirement`, base `9a25f81`; worktree `offs/target/managed-retirement-worktree`. Status stays claimed pending independent parent review. Do not integrate this commit/branch yet.

## Answer — independent owner review (2026-10-09)

Reviewed c7bafda against original of owner-api-audit and public-persistence.md, Managed-copy retirement: public owner handoff (lines 212–230), read-only. The heading is present in the current consumer document; the earlier comment records the implementation-time observation only. Code/QA, lifecycle/design, persistence/security and public API compatibility were reviewed directly; change-review was loaded from the global skill file because skill lookup omitted it. Parallel subagents were unavailable.

Two concrete outbound gaps reproduced RED: pending transport send survived retirement; pending export policy retirement was swallowed as successful export. Fixed on the existing owner branch in c1d61d0: owner-aware send wait and owner-error preservation, retaining ordinary peer-policy skip behavior. Active regressions cover announce request/delta, changed snapshots, reply, run startup, and pending export policy. No remaining merge blocker found within the documented native Linux contract.

Exact consumer APIs:

- `FileSystemCatalog::remove_managed_copy(&self, id: FileSystemId) -> impl Future<Output = Result<(), Error>>`: created/projected/tombstoned identities; durable one-way no-new-operation gate; native admission drain, owner-slot release, stream invalidation and targeted content/metadata/stage removal; retry after cancellation/error/restart. Unknown identity: NotFound.
- `FileSystemCatalog::evict_projected_copy(&self, id: FileSystemId) -> impl Future<Output = Result<bool, Error>>`: now async, await required; selected projections share removal; nonselected/nonprojected returns false.
- Native future opens/data operations through issued filesystem, file, stream and sync/session owners deny retirement; close is release. Catalog identities/Tombstones, unrelated roots/content and peers are retained. No global delete or consumer unlink.

Independent qualification: retirement 7, resource 5, outbound 4, native retirement drain 2, existing residency 2, existing rename 2: 22 tests pass; subprocess restart and foreign lease are active. Focused Clippy with -D warnings, touched-file rustfmt and diff checks pass. Exact commands/results: evidence/17–22 logs. No CRAP/hack/powerset/sweep. Own target: target/independent-retirement-review only; original/shared and prior owner target untouched.

Consumer limits remain explicit: permanent gate, no same-ID reactivation/rematerialization; path residency per-instance/process-local and reopen Passthrough; foreign process ownership returns IO WouldBlock until owners release, no forced descriptor close or idle-loop wake across processes; no transport teardown, already-delivered byte recall, unmanaged raw backend revocation, hostile directory replacement, cross-store cleanup transaction, content+metadata crash atomicity or cross-platform durability qualification. Of05 orchestration and ofdb SQL/KV removal remain consumer/separate-owner work, not resolved by this ticket.

Ready for of05 public-owner integration, not a claim that of05 itself is complete. Reviewed owner changes are now on local main; the integrated owner worktree and short-lived branch were removed after preserving identical root evidence. See the [map cleanup receipt](../map.md#cleanup-receipt).
