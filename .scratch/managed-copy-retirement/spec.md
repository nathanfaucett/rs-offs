# Managed-copy retirement owner follow-up

Implement the public offs owner contract from the read-only of owner-api audit at base `9a25f8150f6467c92e33a4343aac3034af6ba038`. No writes in of/ofdb. No new transport or consumer unlink implementation.

Seams: public catalog, filesystem, file handle, stream, metadata executor/session, backed by native redb/content and real process reopen. TDD regression first for retained handles and Passthrough writes; reuse one owner lifecycle for created/projected copies, cancellation, failure and retry. No path-residency schema: process-local policy restarts fail-closed.

Implementation: [ticket 01](issues/01-owner-retirement.md). Contract: [managed-copy retirement](../../docs/managed-copy-retirement.md). Status remains claimed pending independent parent review; do not integrate this branch.
