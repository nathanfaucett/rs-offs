# Filesystem operation execution

## Caller interface

`FileSystem::metadata_sync(transport).with_operation_control(control)` installs an `OperationControl<PeerId>`. Construct it with a `Send + Sync` callback `(PeerId, RemoteOperation) -> FileFuture<'static, bool>`. The callback evaluates caller-local policy, not online Management, endpoint allowlisting, or pairing. It is invoked for each imported snapshot set, requested snapshot export, and Open/Read/Write/Scan/Close operation. Session decisions include the operation and the peer-scoped handle's resolved path (or `None` for unknown handles). The filesystem/resource binding belongs to the caller configuring this executor; it is not supplied by a remote payload.

The default control permits operations, for caller-controlled local/in-memory exchange. Production callers must install their resource policy. `OperationControl::cancel()` is one-way; clones share cancellation. A replacement executor needs a new control. There are no deadlines, freshness leases, quotas, or chunk renewals.

## Admission and cancellation

Import and session execution acquire the filesystem execution lock **before** consulting policy. Receiving/decoding/enqueueing and opening a handle confer no admission for subsequent operations. Cancellation selects against queued lock acquisition and pending policy. The final cancellation read after policy returns true is the admission linearization point: cancellation visible there wins, otherwise the existing semantic operation is admitted. A policy callback must not call a mutating filesystem operation while admission holds the execution lock.

After admission the executor awaits the operation to completion without selecting on cancellation. Snapshot import is one supplied-set transaction; a session Write is one offset write and revision update. The baseline embedded redb-backed facade executes its transaction/read/write futures synchronously once polled; there is no detached async storage task or mid-commit cancellation selection. This completion contract uses `cancel()`, not a promise that forcibly aborting a task or killing the process completes it. Content/write failure integrity is a separate guarantee established by ticket 02.

Responses do not authorize operations. A dropped response does not undo a commit. Response backpressure is cancellable **after** operation completion. Cancellation clears executor handles and drains unstarted requests/messages on `pump`; `run` termination drops its executor and receivers. This releases generic resources, not caller-owned transport connections/tasks or ordinary filesystem content. Local Full operations remain available after sync stops.

The execution lock serializes filesystem mutations per `FileSystem` instance (a deliberate throughput ceiling; per-file locking is unnecessary until measured contention warrants it). The owned metadata facade is private to `offs`. No guarantee is asserted for independently opened instances concurrently mutating the same root, cross-store crash atomicity, authenticated domain wiring, or exactly-once RPC.

## Evidence

Transport-neutral integration tests in `tests/admission.rs` cover queued snapshot/tombstone denial and retry, all five established-session operations, pending-policy destruction, deterministic same-poll cancellation/admission, admitted write retention while cancellation releases response backpressure, response loss, and failed snapshot-set rollback preserving earlier imported state. Existing peer-handle, revision, residency, offline and reconciliation regressions remain applicable. Concrete Iroh extraction and authenticated local policy integration are later coordinated work, not part of this interface.
