# 02: Tests — Verify new pure filesystem API

**What to build:** Test the new `FileSystem.open(root?)` API and clean `FileMeta` output.

**Blocked by:** 01-wasm-ts-clean-api

**Status:** ready-for-agent

- [ ] `FileSystem.open()` works (default root)
- [ ] `FileSystem.open("custom-root")` creates isolated storage
- [ ] `write`/`read`/`list`/`delete`/`entry`/`scan` all work
- [ ] All returned `FileMeta` have exactly 4 fields: `file_id`, `revision`, `kind`, `mode`
- [ ] No `owner`, `group`, `providers`, `local` in any response
