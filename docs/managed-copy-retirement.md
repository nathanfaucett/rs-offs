# Managed-copy retirement

## Public owner APIs

```rust
impl FileSystemCatalog {
    pub async fn remove_managed_copy(&self, id: FileSystemId) -> Result<(), Error>;
    pub async fn evict_projected_copy(&self, id: FileSystemId) -> Result<bool, Error>;
}
```

`remove_managed_copy` retires a created or projected device-local copy, including a copy whose catalog entry is already tombstoned. An unknown catalog identity returns `NotFound`. Retrying an already removed copy returns `Ok(())`. Catalog identity, name, deletion state, unrelated catalog records, and unrelated resource roots are unchanged. A projection is marked deselected only after native cleanup succeeds.

`evict_projected_copy` uses the same retirement path and returns `false` for an unselected/non-projected identity; a selected projection returns `true` after removal. **This method is now async**: existing callers must add `.await`. There is no synchronous blocking compatibility path and no consumer-owned unlink requirement.

## Gate, admission and drain

Retirement synchronously creates and fsyncs a resource-specific gate beside the managed UUID directory, then fsyncs its parent before publishing in-process retirement. New operations fail with `Error::PermissionDenied`. An owner read lease admits native filesystem/handle operations; retirement takes exclusive access after those admitted operations finish. Internal nested work uses that same admission, rather than reacquiring a gate mid-operation. Retirement does not interrupt an admitted write/metadata commit or an admitted handle Read halfway through its chunks.

All public filesystem data operations and future catalog/direct-root opens honor the gate. Retained filesystem `Arc` clones, separately opened instances of that root, local file handles, streams, metadata executors and established sessions cannot start new native work. A standalone stream admits one chunk at a time; retirement closes its native file even if the caller never polls/drops it. Its next poll reports `PermissionDenied`, then terminates. Handle `close` remains an allowed, idempotent release operation. Constructing/configuring a metadata executor is not admission; its executing methods are gated.

Retirement wakes idle in-process `run` loops, cancels pending transport/policy/staging waits, and drains native admissions. Prepared but unadmitted fetched content unwinds its existing stage guard; admitted native commits finish. Response backpressure is cancellable after commit. As with operation-control cancellation, forcibly dropping an operation future or killing the process is not a promise to complete its commit.

Native metadata Database/Client handles live in the owner slot, not in issued filesystem/session handles. Exclusive drain drops that slot, closes retained stream descriptors, and only then removes the selected UUID directory (metadata, all content revisions, and orphan stages). The parent directory is fsynced after cleanup. The namespace catalog and small sibling retirement/OS-lease control files are retained, not confused with content or removed to erase the gate.

## Failure, cancellation and restart

Errors never clear the gate. Dropping a polled removal while it awaits drain also leaves it closed; retry the same API to finish. A crash during directory removal can leave a partial directory; the durable sibling gate prevents reopen, and idempotent retry removes the remainder. A cleanup/parent-sync/catalog-deselection failure returns an error rather than successful completion. No completion flag or copied catalog schema is necessary: every retry drains and checks native cleanup again.

An OS-exclusive native lease prevents cleanup from succeeding while another process still owns this copy. That case returns an IO `WouldBlock` after persisting the gate. The other process denies subsequent native operations by reading the gate; it must release its filesystem owners before retry can finish. In-process retirement notifications do not wake idle executors in another process, and this API cannot forcibly close another process's descriptors. No unlink-while-foreign-owner-live success is claimed.

## Residency and boundaries

Delete is a write and requires Full residency. Rename requires Full for both endpoints and every moved source/destination, checked before parent creation or staging. Full rename/delete behavior and stable file identities remain unchanged; Passthrough no longer permits metadata-only rename/delete.

Residency rules remain **per filesystem instance, process-local**, not durable owner policy. Reopen starts in read-only Passthrough. Callers restore explicit rules when required. Retirement persistence is independent of residency and cannot be bypassed by setting Full, importing catalog snapshots, or marking a projection selected.

Retirement is one-way for this root/identity. There is no reactivation/rematerialization API in this follow-up; a caller must not promise that reselection ungates it. A future explicit reactivation contract would need a fresh owner generation while all previously issued handles remain revoked; silently removing the gate is not supported. Already returned bytes/metadata cannot be retracted.

The namespace is trusted caller-owned storage; root symlinks are rejected and parent paths are canonicalized. Concurrent hostile external directory replacement, arbitrary raw native backend/file handles obtained outside `offs`, cross-store/domain cleanup transactions, and atomic content-plus-metadata crash commits are not owner guarantees. No transport, domain policy, global Tombstone, remote-peer erasure, or of/ofdb implementation is added.

## Qualification

`tests/retirement.rs` exercises retained native handles, created-copy failure/retry, native metadata exclusivity after release, a fresh-process restart, a separately running native owner lease, unadmitted stage cancellation, pending sync-policy denial, established sessions, idle run invalidation, unrelated persisted content and catalog/target tombstone retention. `src/file_system_tests.rs::retirement_drains_admitted_native_write_and_cancelled_drain_retries` uses the existing native metadata checkpoint to qualify admitted-write completion and cancelled drain for created and projected copies; `retirement_allows_admitted_session_write_to_finish_and_revokes_cloned_owner` qualifies admitted remote Write completion and later session/owner denial. Red/green evidence and focused commands are in the owner [follow-up tracker](../.scratch/managed-copy-retirement/issues/01-owner-retirement.md); independent parent review is required before integration.
