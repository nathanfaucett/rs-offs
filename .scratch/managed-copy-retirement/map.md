# Managed-copy retirement owner map

## Notes

Scope: offs native managed-copy lifecycle only. Consumer contract: of/docs/public-persistence.md, Managed-copy retirement: public owner handoff, and of/.scratch/offline-first-nodes/evidence/05-completion/owner-api-audit.md (read-only).

## Decisions-so-far

- [01 — resolved](issues/01-owner-retirement.md): c7bafda durable local whole-resource gate qualified after independent TDD outbound fixes c1d61d0. Use `remove_managed_copy(id).await` for created/projected/tombstoned identities; `evict_projected_copy(id).await` is async and projection-only. Native admissions drain, issued owners invalidate, exact target persistence disappears, catalog/Tombstones/unrelated copies remain. [Public contract](../../docs/managed-copy-retirement.md); [exact evidence](evidence/21-independent-native.log).

## Fog / consumer boundaries

- No same-ID reactivation API; reselection cannot ungate.
- Path residency remains process-local; restore caller policy on reopen.
- Foreign process ownership requires release and retry (WouldBlock); no forced descriptor/transport teardown.
- Linux qualification only; trusted namespace; no raw unmanaged handle or cross-store atomicity guarantee.
- Of05 consumer orchestration and ofdb SQL/KV removal remain outside this resolved owner ticket.
