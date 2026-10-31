# 01: Make the File System core storage-agnostic and wasm-compilable

**What to build:** The File System core no longer talks to Redb, native paths, or native
file handles directly. The File System Resource catalog moves off direct Redb onto the KV
Store facade (its API becomes asynchronous), and Content Revisions are reached through an
internal content port instead of the local filesystem. The library compiles for
`wasm32-unknown-unknown`, with target-conditional `Send`/`Sync` bounds taken from the
sibling platform crate, and the native build behaves exactly as it does today.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] The File System Resource catalog stores its records and projected-copy selections
      through the KV Store facade, and its API is asynchronous.
- [ ] Existing native roots remain readable: existing resource registrations and File
      Entries survive the catalog and metadata move.
- [ ] Content Revisions are read and written only through an internal content port.
- [ ] The public bounds on futures, handles, peer identifiers, and Transport callbacks are
      target-conditional: unchanged on native targets, single-threaded on the browser
      target, using the platform crate's existing conditional bound types.
- [ ] Identifier generation uses the platform's browser-appropriate randomness source on
      the browser target.
- [ ] The library compiles for `wasm32-unknown-unknown`, and the browser build constructs
      no Tokio runtime.
- [ ] The native test suite passes unchanged, and the full feature-powerset test command
      passes on native.
