# 01: WASM + TS — Single root param, clean FileMeta

**What to build:** Collapse `namespace` + `name` + `nodeId` → single `root` string. Make `node_id` optional in WASM (default `"local"`). TypeScript `FileMeta` keeps only 4 filesystem fields.

**Blocked by:** None (can start immediately)

**Status:** done

- [x] WASM: `FileSystemHandle.open(root: String, node_id: Option<String>)` — defaults to `"local"`
- [x] WASM: Serialize only `file_id`, `revision`, `kind`, `mode` for `entry`/`write`/`create_dir`/`list`/`scan`
- [x] Rust: `open_browser(root: &str, node_id: PeerId)` uses single root for DB/OPFS paths
- [x] TypeScript: `FileSystem.open(root?: string)` — defaults to `"default"`
- [x] TypeScript: `FileMeta` = `{ file_id, revision, kind, mode }` only
- [x] No deprecated APIs, no migration layer
