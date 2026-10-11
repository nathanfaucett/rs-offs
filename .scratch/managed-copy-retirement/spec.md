# Managed-copy retirement owner follow-up

Status: resolved
Scope: reviewed and integrated `offs` native owner contract only; no consumer or separate-owner completion claim.

Implement the public offs owner contract from the read-only of owner-api audit at base `9a25f8150f6467c92e33a4343aac3034af6ba038`. No writes in of/ofdb. No new transport or consumer unlink implementation.

Seams: public catalog, filesystem, file handle, stream, metadata executor/session, backed by native redb/content and real process reopen. TDD regression first for retained handles and Passthrough writes; reuse one owner lifecycle for created/projected copies, cancellation, failure and retry. No path-residency schema: process-local policy restarts fail-closed.

Implementation: [ticket 01](issues/01-owner-retirement.md). Contract: [managed-copy retirement](../../docs/managed-copy-retirement.md). Independent owner review resolved the ticket after outbound fixes `c1d61d0`; reviewed integration `dc91886` is on local main. The former claimed/no-integration state is historical. See the [map cleanup receipt](map.md#cleanup-receipt) for removal of the integrated worktree and preservation of evidence. Existing native Linux qualification and documented limits remain unchanged.
