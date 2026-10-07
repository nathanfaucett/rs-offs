# Metadata storage

Each file-system root stores metadata in `metadata.redb`, managed by the public `ofdb-kv` facade in its `metadata` table. `FileSystem::open` uses `Database::open_with_table` so existing metadata remains in place.

Filesystem exchange is caller-supplied through `Transport` and `FileSessionService`; this crate has no concrete Iroh adapter or `iroh` feature. Generic messages, versioned metadata codecs, sessions, persistence, reconciliation and memory exchange remain here. See [execution and adapter handoff](execution.md) for the current admission, immutable revision content, cleanup and consumer contracts. Domain resource descriptors, authenticated peer binding, RPC envelopes and network lifecycle belong to the consumer, not persistence.
