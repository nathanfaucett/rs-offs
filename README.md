# Offline First File System

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE-MIT)
![Test Status](https://github.com/nathanfaucett/rs-offs/actions/workflows/ci.yml/badge.svg)

Local-first, eventually consistent file system primitives for Rust. Full operation without network access, with metadata and content reconciliation across peers.

## Features

- **Offline-First** — Full read/write operation with no network dependency.
- **Reconciliation** — Metadata and content sync between peers with stable file identities, tombstones, and conflict resolution.
- **Residency Control** — Device-local choice per path: `Full` (metadata + content) or `Passthrough` (metadata only, fetch content from a `Full` peer).
- **Native & Browser** — Runs on native targets (via `redb`) and in the browser (via IndexedDB / OPFS).

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
file-system = { version = "0.1", package = "file-system" }
```

For browser/WASM usage, enable the `browser` feature:

```toml
[dependencies]
file-system = { version = "0.1", package = "file-system", features = ["browser"] }
```

## Usage

```rust
use file_system::{FileSystem, Residency};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), file_system::Error> {
    // Open a filesystem rooted at a local path, identified by a peer ID.
    let fs = FileSystem::open("./my-data", Uuid::now_v7()).await?;

    // Keep metadata and content locally for this path.
    fs.set_residency("", Residency::Full).await?;

    // Write and read files — all operations work offline.
    fs.write("notes.txt", b"hello world").await?;
    let entry = fs.read("notes.txt").await?;

    Ok(())
}
```

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
