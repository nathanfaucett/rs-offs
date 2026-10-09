# Owner qualification evidence

Base: `9a25f8150f6467c92e33a4343aac3034af6ba038` (`filesystem-sync-integration`).
Branch: `owner/managed-copy-retirement`.
Worktree: `offs/target/managed-retirement-worktree` (isolated, ignored by original offs).
Build target: worktree-local `target/owner-retirement` via literal `CARGO_TARGET_DIR`.

No changes in original of/ofdb/offs worktrees, no Cargo dependency/source overrides, no new dependencies, and no concrete transport. Worktree dependency resolution uses an ignored sibling symlink to original ofdb. No integration, CRAP, cargo-hack, powerset, or broad test sweep was run.

## TDD and qualification

- `01-red.log`: original public projected eviction leaves a retained native filesystem usable (expected RED).
- `02-check.log`: intermediate compiler errors while wiring the new native slot into sync; resolved before GREEN.
- `03-green.log`: native filesystem Arc/local-handle/stream/future-open denial after root lifecycle implementation.
- `04-policy-red.log`: generic delete incorrectly succeeds under Passthrough (expected RED).
- `05-policy-green.log`: delete and rename deny Passthrough, including moved descendants and destination paths.
- `06-retry-green.log`: created-copy deterministic cleanup obstruction, retained owners, native metadata release, retry, catalog/tombstone/unrelated content preservation.
- `07-drain.log`: admitted native write completes through retirement; projected/created × cancelled/completed drain, then retry/reopen.
- `08-sync-restart.log`: intermediate sync fixture initially did not poll lazy MemoryTransport Write, so nothing was queued. This fixture error was corrected, not hidden or ignored.
- `09-sync-green.log`: corrected pending-policy fixture, established remote handles, metadata methods and idle run denial; real child-process reopen.
- `11-stage-process.log`: retirement cancels unpublished fetched content staging; separate native-owner process prevents false successful cleanup and denies subsequent data operations.
- `12-clippy.log`: `cargo clippy -p file-system --lib --test retirement --test resource --test file_system` passes with no warnings.
- `13-resource.log`: catalog/resource 5 pass; selected existing filesystem regressions: residency 2, rename 2, local_file_handle 1, persists_metadata 1, metadata_sync_converges_tombstones 1.
- `14-final.log`: final retirement 7/7, resource 5/5, native/library 14/14. Retirement cases include a fresh-process reopen and a concurrently active native-owner process; no ignored regression.

Every Cargo command above used `CARGO_TARGET_DIR=target/owner-retirement`. Commands were bounded to 120 seconds after the initial 180-second compile. Touched-file rustfmt check and `git diff --check` pass. No runtime timeouts were introduced; process fixture watchdogs are test-only.

- `15-session-drain.log`: intermediate remote-write fixture used an unpinned memory-handle revision and rejected as stale before mutation; no owner code was changed to bypass revision validation.
- `16-session-drain-green.log`: revision-pinned admitted session Write commits/responds after retirement begins; old cloned owner/session subsequently deny.

## Public contract and limits

See [managed-copy retirement](../../../docs/managed-copy-retirement.md). `remove_managed_copy` is async, local-only, idempotent, and available for created/projected/tombstoned identities. `evict_projected_copy` is now async and delegates to the same owner path. Retirement gates remain durable and one-way; no same-ID reactivation API is promised. Close is allowed as release; previously returned bytes cannot be recalled. Residency remains process-local, with Passthrough on reopen. Foreign native ownership returns IO WouldBlock until released, not physical-removal success. Native Linux behavior is qualified; no cross-platform filesystem-durability, hostile-directory, unmanaged raw-handle, transport-teardown, or content-plus-metadata crash-atomicity guarantee is claimed.

Status remains claimed. Commit is for independent parent review only; do not integrate before that review.

## Independent recovery review

Original candidate c7bafda reviewed; c1d61d0 fixes two reproduced outbound retirement gaps. Review RED logs 17 and 19; GREEN logs 18 and 20; focused native qualification 21 (22 passing tests); focused warning-denied Clippy 22. Review commands use only `CARGO_TARGET_DIR=target/independent-retirement-review`. The original build target above was not cleaned or reused. Independent review now resolves the owner ticket; historical claimed statements above describe pre-review status. See [resolved ticket](../issues/01-owner-retirement.md) and [map](../map.md).
