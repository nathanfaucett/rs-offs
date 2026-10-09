# Managed-copy retirement owner follow-up

Status: claimed
Type: task
Label: ready-for-agent

Base: filesystem-sync-integration `9a25f8150f6467c92e33a4343aac3034af6ba038`.
Owner: offs only; no of/ofdb changes, no transport implementation.

## Contract and seams

Public FileSystemCatalog, FileSystem, LocalFileHandle/FileHandle, ReadStream and MetadataSync native behavior, as requested. Retire projected and created local copies without mutating catalog identities/tombstones or unrelated copies. Durable no-new-operation gate; admitted native operations drain before native handles/content/metadata/stages are removed. Failure, cancellation, retry and reopen remain fail-closed. Generic delete/rename enforce Passthrough. Residency stays process-local unless needed for correctness; restart defaults deny writes.

## Complexity / risk / priority

High priority owner capability gap. Main risks: nested admission deadlocks, native-handle leaks, reopen race, cancelled cleanup, descendant residency in rename. Reuse one owner lifecycle and existing operation serialization; no dependency or consumer cleanup implementation.

## Comments

Read original of audit and public-persistence document read-only. Referenced managed-copy-retirement heading is not present in current original public-persistence; explicit audit contract used. Original offs clean at requested base. Independent parent review required before integration; this ticket remains claimed.


## Implementation / owner handoff

Public `FileSystemCatalog::remove_managed_copy(id).await -> Result<(), Error>` retires created and projected copies. `evict_projected_copy(id).await -> Result<bool, Error>` is now async and shares this owner path. One fsynced durable sibling gate, shared native admission/drain slot, stream descriptor invalidation and OS-exclusive native lease implement cleanup without dropping catalog identity/Tombstones. Delete/rename enforce Full before mutation (all moved source/destination descendants for rename).

Retirement is permanent for this root/identity; same-ID reselection/reactivation is deliberately not implemented. Default/process-local residency rules are not persisted and reopen remains Passthrough. External native process ownership blocks cleanup with WouldBlock until released. No forceful foreign-descriptor or transport teardown claim.

Final focused results: retirement 7, resource 5, native/library 14, selected existing filesystem 7; all pass. Focused Clippy clean; touched-file formatting and diff checks pass. No CRAP/hack/sweep. Native RED/green logs, subprocess restart/lease and error/cancel/retry evidence: [evidence](../evidence/README.md). Contract: [managed-copy retirement](../../../docs/managed-copy-retirement.md).

Branch `owner/managed-copy-retirement`, base `9a25f81`; worktree `offs/target/managed-retirement-worktree`. Status stays claimed pending independent parent review. Do not integrate this commit/branch yet.
