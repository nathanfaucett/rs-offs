# Run `offs` in browsers using navigator storage APIs

Status: ready-for-agent

## Problem Statement

`offs` is the offline-first File System library: it reconciles File Entries and Content
Revisions across peers, tracks Residency per device, and exchanges metadata and File
Session traffic through a caller-supplied Transport. Today it can only run natively.

The library is hard-wired to native persistence in two places:

- **Metadata and catalog.** The File System's metadata lives in a Redb database opened from
  a path inside the filesystem root. The File System Resource catalog is even more tightly
  bound: it opens Redb directly, owns two native tables, encodes records with Postcard,
  and exposes a synchronous API. Redb is a native-only dependency.
- **Content.** Content Revisions are complete immutable files stored on the local
  filesystem under a content root, staged through a temporary file and published with a
  rename. Streaming reads wrap a native file handle and read it synchronously while
  polling.

Both assumptions break in a browser. A browser has no paths, no native file handles, and
no advisory file locks, and the browser's storage APIs are asynchronous.

At the same time, `offs` is written as if it were multi-threaded. The public traits and
futures require `Send + Sync`, the Transport futures are `Send`, and the File Handle trait
requires `Send + Sync`. Browser futures are single-threaded and are not `Send`, so the
library cannot even be compiled for `wasm32-unknown-unknown` without weakening those
bounds.

Consumers who want an offline-first File System in a browser therefore have no path
forward today. They would have to reimplement the reconciliation, Content Revision, and
Residency semantics against browser storage, which is exactly the part of `offs` that
must not be reimplemented twice.

## Solution

`offs` becomes a browser-capable library with **one implementation of the File System
semantics** and **one pluggable persistence boundary**.

The **core** — paths, File Entries, Content Revisions, Residency rules, reconciliation,
the sync protocol, and the File Session codecs — stops talking to native storage
directly. It talks to two internal storage ports: a metadata port satisfied by the KV
Store facade, and a content port that stores complete Content Revisions addressed by File
id and Content Revision id.

Two **backends** satisfy those ports:

- A **native backend**, preserving today's behavior: Redb for metadata and catalog, the
  local filesystem for Content Revisions, the advisory lease and retirement marker.
- A **browser backend** that uses the navigator APIs: the Origin Private File System
  obtained from the Storage Manager for Content Revisions, the browser's IndexedDB for
  metadata and the File System Resource catalog, and the Web Locks API for the exclusive
  ownership lease that replaces the native advisory file lock.

The **browser surface** is delivered as a WebAssembly module: the library compiles to
`wasm32-unknown-unknown` and exposes the File System through WebAssembly bindings with
generated TypeScript declarations, so TypeScript libraries can consume it directly. The
`Send + Sync` bounds become target-conditional, so native callers keep the current
guarantees while browser callers get a single-threaded API.

The **semantics do not fork**. Native and browser builds run the same behavioral
conformance suite against the same public File System API.

## User Stories

1. As a browser application developer, I want `offs` to compile for
   `wasm32-unknown-unknown`, so that I can use the File System in a web page without a
   native server.
2. As a browser application developer, I want Content Revisions stored through the
   navigator Storage Manager, so that the browser owns the bytes and applies its own
   quota and eviction rules.
3. As a browser application developer, I want the File System's metadata stored in the
   browser's transactional key-value store, so that a snapshot import is atomic and a
   failed import commits nothing.
4. As a browser application developer, I want the File System Resource catalog stored in
   the browser's transactional key-value store, so that resource tombstones and
   projected-copy selections behave exactly as they do natively.
5. As a browser application developer, I want one implementation of reconciliation and
   Content Revision selection, so that a browser peer and a native peer agree on the
   winning state.
6. As a browser application developer, I want TypeScript declarations for the File
   System API, so that my editor can type-check my calls and my models.
7. As a TypeScript library author, I want to wrap the WebAssembly module in a
   TypeScript library, so that I can offer an idiomatic API without reimplementing File
   System semantics.
8. As a browser application developer, I want the WebAssembly module to work in both a
   Window and a Dedicated Worker context, so that I can choose the execution context that
   fits my application.
9. As a browser application developer, I want streaming reads to work in the browser, so
   that I can serve large Content Revisions without loading them entirely into memory.
10. As a browser application developer, I want offset writes and appends to work in the
    browser, so that the whole File Session write contract is available.
11. As a browser application developer, I want an exclusive ownership lease across tabs
    of the same origin, so that two tabs do not treat the same filesystem root as two
    independent owners.
12. As a browser application developer, I want to retire a managed copy and have every
    tab observe the retirement, so that removal is not silently ignored by a peer tab.
13. As a browser application developer, I want a failed Content Revision write to leave no
    partially published revision behind, so that a later read never observes a
    half-written file.
14. As a browser application developer, I want an interrupted transfer to clean up its
    staged revision, so that the browser's quota is not consumed by abandoned staging.
15. As a browser application developer, I want the sync protocol to work unchanged over a
    browser Transport, so that a browser peer can reconcile with native peers.
16. As a browser application developer, I want the File Session contract, including Open,
    Read, Write, Scan, and Close, to work in the browser, so that a peer can fetch content
    from a browser-hosted File System.
17. As a browser application developer, I want policy admission and cancellation to behave
    identically in the browser, so that a denied remote operation is never applied
    locally.
18. As a native application developer, I want the native build to keep its current
    `Send + Sync` guarantees, so that existing multi-threaded callers are unaffected.
19. As a native application developer, I want the native persistence behavior to be
    unchanged, so that existing roots remain readable after upgrading.
20. As a native application developer, I want the File System Resource catalog to remain
    readable after its storage moves to the KV Store facade, so that existing resource
    registrations survive.
21. As a maintainer, I want the File System semantics to live in one place with
    backend-specific code confined to the storage ports, so that a browser divergence is a
    local defect rather than a forked implementation.
22. As a maintainer, I want the native and browser builds to run the same behavioral
    conformance suite, so that "browser-supported" is a measured claim.
23. As a maintainer, I want the browser build's dependency graph checked so that native
    storage crates cannot be pulled in accidentally, so that the browser build stays
    honest.
24. As a maintainer, I want the browser build to avoid a Tokio runtime, so that browser
    callers do not need to construct one.
25. As a maintainer, I want the browser's release validation to run in real Chromium and
    Firefox engines, so that storage and eviction claims are backed by evidence.
26. As a maintainer, I want the browser build documented with its supported features and
    limits, so that consumers know what is and is not proven.
27. As a maintainer, I want the existing native test suite to keep passing unchanged, so
    that the storage refactor is proven not to alter native behavior.

## Implementation Decisions

### Layering

- The File System core becomes storage-agnostic. It no longer names Redb, native paths,
  or native file handles.
- Two internal storage ports replace direct native access:
  - A **metadata port** for File Entry and catalog records, satisfied by the KV Store
    facade, which already abstracts Redb, in-memory, and browser key-value backends.
  - A **content port** for complete Content Revisions, addressed by File id and Content
    Revision id, with stage, publish, read, and remove operations.
- The port boundary is internal to the library. It is not a new public extension point and
  is not intended for third-party backends.

### Metadata and catalog

- The File System Resource catalog stops opening Redb directly. Catalog records and
  projected-copy selections move to the KV Store facade, keyed by File System Resource id
  with distinct key prefixes for the catalog and the projected-selection set.
- Catalog values are stored as facade values rather than raw Postcard bytes, so the browser
  backend inherits the same encoding without a second codec.
- The catalog's API becomes asynchronous, because the KV Store facade is asynchronous. This
  is a breaking change to a pre-1.0, unpublished crate.
- The File System's metadata table keeps its existing logical table name so that native
  roots stay readable.
- **The browser backend uses two separate physical browser key-value stores** — one for
  File System metadata, one for the File System Resource catalog — opened under distinct
  database names. The KV Store facade's browser open path fixes its logical table name,
  but the database name is the caller's choice, so separate physical stores give the
  required namespace separation without changing the sibling KV Store crate.

### Content store

- Content Revisions remain complete immutable files addressed by File id and Content
  Revision id, matching the current model and its documented "retained complete progress"
  semantics.
- The native content backend preserves today's staging discipline: write a temporary
  sibling, synchronize it, publish by rename, then update metadata.
- The browser content backend is rooted in the Origin Private File System obtained from the
  Storage Manager, with one directory per filesystem root and the same content-root layout
  as native, so the two backends are structurally comparable.
- **The browser backend does not depend on a rename or move primitive.** The Origin Private
  File System's move support is not uniform across engines, so relying on it would make
  browser support engine-dependent. Because a Content Revision is immutable and metadata
  selects the visible revision, the browser backend writes the new revision's bytes and
  only then commits the metadata that selects it. A failed or interrupted write leaves an
  unreferenced revision that no reader can observe.
- The browser backend writes complete revisions sequentially. It does not use random
  position writes, because those are not uniformly available. Offset writes and appends
  already copy the previous revision into a new revision before modifying it, so the
  browser backend reads the previous revision, applies the modification, and writes the new
  revision whole. The cost is that a modified revision is held in memory while it is
  rewritten; this ceiling is documented.
- Streaming reads are driven by asynchronous chunk reads. The current synchronous file read
  inside stream polling cannot work in a browser, so the read stream becomes asynchronous
  and yields chunks from the content port.

### Exclusive ownership and retirement

- The native backend keeps the advisory lease file and the durable retirement marker.
- The browser backend replaces the advisory lease with an exclusive Web Lock scoped to the
  origin and the filesystem root. This is the browser's own cross-context exclusivity
  primitive and is the closest analogue to the native lease.
- The browser backend records retirement as a tombstone in its browser key-value store, so
  retirement is observable by every tab of the origin and survives the closing of the tab
  that requested it.
- Root identity in the browser is a namespace plus a name, not a path. The native
  backend's path canonicalization, symlink rejection, and absolute-path resolution have no
  browser counterpart and are not emulated.

### Target-conditional bounds

- The library's public bounds on futures, handles, peer identifiers, and Transport
  callbacks become target-conditional: the strong bounds on native targets, and
  single-threaded bounds on the browser target.
- The conditional bound types are taken from the sibling platform crate that already
  defines them, rather than being redefined here.
- Identifier generation switches to the platform's browser-appropriate randomness source on
  the browser target, because version-7 identifiers need an entropy source that the browser
  target provides.
- The browser build uses synchronization primitives only and never constructs a Tokio
  runtime, matching the browser build policy already proven for the KV Store and SQL
  facades.

### WebAssembly and TypeScript surface

- The library compiles for `wasm32-unknown-unknown` and exposes the File System through
  WebAssembly bindings in a dedicated module, keeping the crate root thin.
- TypeScript declarations come from the WebAssembly binding tool's generated typings, not
  from a hand-written or separately generated declaration set. Rust types are exposed to
  JavaScript only when the binding tool supports them directly.
- The browser-facing API is asynchronous throughout: opening a filesystem, reading, writing,
  scanning, syncing, and closing all return promises.
- A Transport and a File Session Service supplied from JavaScript satisfy the same public
  contracts as the native ones, so the browser module can reconcile with native peers over
  whatever transport the application chooses.
- The WebAssembly artifact and its declarations are the deliverable of this library.
  Packaging them as a published npm library, and the consumer-side TypeScript library that
  wraps them, belong to the consuming application repository and are not part of this spec.

### Repository shape

- The existing workspace gains a browser test harness crate, modeled on the sibling
  repository's harness, so that browser tests do not pollute the library's own test targets.
- The library gains a `browser` feature that is only meaningful on the browser target, and
  the native backend remains the default on native targets.
- Documentation gains a browser guide describing supported features, the storage lifecycle,
  quota and eviction limits, and the release validation baseline, in the style of the sibling
  repository's browser guide.

## Testing Decisions

- A good test asserts externally observable File System behavior only: what a caller reads,
  writes, scans, reconciles, or is denied. It does not assert on backend internals, on key
  layout inside the KV Store, or on the presence of specific files in the browser's private
  file system.
- **The single seam is the public File System API.** The conformance suite is written once
  against that API and compiled twice: once for native, once for the browser target. Both
  builds run the same expectations.
- The conformance suite covers:
  - File Entry creation, overwrite, delete, and rename, including directory renames that
    move descendants.
  - Content Revision identity across overwrite, rename, offset write, and append, and the
    rule that a read returns exactly the revision metadata selects.
  - Retention of earlier complete revisions, and the fact that an unreferenced or
    interrupted revision is never observable through the public API.
  - Residency transitions, including the requirement that writes require Full residency and
    that Passthrough reads obtain content from a Full peer.
  - Reconciliation of imported snapshots, including tombstones and the rule that a failed
    import leaves earlier state intact.
  - Admission and cancellation: denied and cancelled remote operations are not applied, and
    an admitted operation completes.
  - The File Session contract over the generic transport: Open, Read, Write, Scan, and
    Close, with correct returned revisions and the requirement that a read ends on an
    explicit end marker rather than channel closure.
- The browser harness runs the suite in a real browser in both a Window and a Dedicated
  Worker context, in Chromium and Firefox, using the same harness structure, driver
  handling, and evidence capture as the sibling repository's browser harness.
- The browser harness additionally asserts the browser-only contracts that have no native
  analogue: exclusive ownership across two contexts of the same origin, retirement observed
  across contexts, and the absence of an observable partial revision after an interrupted
  write.
- The build checks for the browser target include a dependency-graph check that fails when
  native storage crates appear in the browser dependency graph, so that a mistakenly enabled
  native backend cannot pass silently.
- The native test suite continues to run unchanged and is the regression evidence that the
  storage refactor did not alter native behavior. The final native gate runs the full
  feature-powerset test command required by this repository.
- Prior art: the sibling repository's browser harness and browser validation scripts are the
  model for the browser harness; this repository's existing admission, outbound, integrity,
  and native File System tests are the model for the conformance suite.

## Out of Scope

- Transport implementation for the browser, including WebRTC, WebSocket, and any
  peer-to-peer mesh. The Transport remains caller-supplied.
- A native server or daemon serving a browser client.
- SharedWorker hosting, cross-tab live change notification, and automatic cross-context
  coordination beyond the ownership lease and retirement record.
- Quota reservation, guaranteed persistence, and eviction-resistant storage. The browser
  owns these policies and the library reports what the browser reports.
- Reporting the browser's storage estimate or persistence state to callers.
- Migrating an existing native filesystem root into a browser root, or the reverse.
- A WebKit release requirement. The validated baseline is Chromium and Firefox, with WebKit
  explicitly unvalidated.
- Private-mode and mobile-browser behavior guarantees.
- Publishing packages to registries, and the consumer-side TypeScript library.
- Changing the sync wire protocol or the File Session message shapes.
- Changing File System semantics, reconciliation rules, Residency rules, or the cancellation
  and admission contract.
- Any change to the sibling KV Store crate; the browser backend works within the facade's
  existing capabilities.

## Further Notes

- The browser's transactional key-value store charges a cost proportional to the whole
  database per transaction, and the Origin Private File System charges per-origin quota.
  These ceilings are inherited from the sibling repository's browser backend and are
  documented rather than engineered around here.
- Retirement in the browser is a durable tombstone in transactional metadata, not a
  filesystem marker. Cleanup of the browser's stored bytes is best-effort and may leave
  unreferenced bytes until the browser reclaims them.
- The sibling repository's browser guide and browser validation document are the reference
  for tone, scope language, and the distinction between validated and unvalidated claims.
  This spec intends the same discipline: a browser claim is only made where real-browser
  evidence exists.
- The existing execution and metadata documentation describe the native persistence and
  execution contracts. They are updated as part of this work so that the browser contracts
  are documented alongside the native ones rather than in a separate, divergent document.
