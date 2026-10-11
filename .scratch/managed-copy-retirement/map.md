# Managed-copy retirement owner map

## Notes

Scope: offs native managed-copy lifecycle only. Consumer contract: of/docs/public-persistence.md, Managed-copy retirement: public owner handoff, and of/.scratch/offline-first-nodes/evidence/05-completion/owner-api-audit.md (read-only).

## Cleanup receipt

2026-10-11: owner integration `dc91886` is on local main; the root was tracked-clean at `662f702` before this documentation checkpoint. Independent review resolved the owner ticket. The integrated owner worktree and short-lived branch were safely removed; root declarations/evidence remain. One root worktree on main remains, with `filesystem-sync-integration` retained and aligned as the ongoing task base. See the [filesystem-sync cleanup receipt](../filesystem-sync/map.md#cleanup-receipt) for exact cleanup and Linux-only metadata validation. No new build/test or consumer acceptance is claimed; all three linked scratch folders are retained.

## Decisions-so-far

- [01 — resolved](issues/01-owner-retirement.md): c7bafda durable local whole-resource gate qualified after independent TDD outbound fixes c1d61d0. Use `remove_managed_copy(id).await` for created/projected/tombstoned identities; `evict_projected_copy(id).await` is async and projection-only. Native admissions drain, issued owners invalidate, exact target persistence disappears, catalog/Tombstones/unrelated copies remain. [Public contract](../../docs/managed-copy-retirement.md); [exact evidence](evidence/21-independent-native.log).

## Fog / consumer boundaries

- No same-ID reactivation API; reselection cannot ungate.
- Path residency remains process-local; restore caller policy on reopen.
- Foreign process ownership requires release and retry (WouldBlock); no forced descriptor/transport teardown.
- Linux qualification only; trusted namespace; no raw unmanaged handle or cross-store atomicity guarantee.
- Of05 consumer orchestration and ofdb SQL/KV removal remain outside this resolved owner ticket.
