# 02: Tests — Verify new pure filesystem API

**What to build:** Test the new `FileSystem.open(root?)` API and clean `FileMeta` output.

**Blocked by:** 01-wasm-ts-clean-api (done)

**Status:** done

- [x] `FileSystem.open()` works (default root)
- [x] `FileSystem.open("custom-root")` creates isolated storage
- [x] `write`/`read`/`list`/`delete`/`entry`/`scan` all work
- [x] All returned `FileMeta` have exactly 4 fields: `file_id`, `revision`, `kind`, `mode`
- [x] No `owner`, `group`, `providers`, `local` in any response
