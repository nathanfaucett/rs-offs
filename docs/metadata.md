# Metadata storage

Each file-system root stores metadata in `metadata.redb`, managed by the public `ofdb-kv` facade in its `metadata` table. `FileSystem::open` uses `Database::open_with_table` so existing metadata remains in place.
