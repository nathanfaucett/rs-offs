use std::{env, fs};

use file_system::{CatalogEntry, Error, FileSystemCatalog, FileSystemId};
use uuid::Uuid;

fn root() -> std::path::PathBuf {
    env::temp_dir().join(format!("file-system-catalog-{}", Uuid::now_v7()))
}

#[test]
fn resources_have_distinct_roots_duplicate_names_and_persist() {
    let root = root();
    let catalog = FileSystemCatalog::open(&root).expect("open catalog");
    let first = catalog
        .create(Some("same".to_owned()))
        .expect("create first");
    let second = catalog
        .create(Some("same".to_owned()))
        .expect("create second");
    let first_id = first.id;
    assert_ne!(first.id, second.id);
    catalog
        .open_filesystem(first.id, 1_u8)
        .expect("open first FS");
    catalog
        .open_filesystem(second.id, 1_u8)
        .expect("open second FS");
    assert!(
        root.join("filesystems")
            .join(first.id.as_uuid().to_string())
            .exists()
    );
    assert!(
        root.join("filesystems")
            .join(second.id.as_uuid().to_string())
            .exists()
    );
    drop(catalog);

    let reopened = FileSystemCatalog::open(&root).expect("reopen catalog");
    assert_eq!(
        reopened.list().expect("list resources"),
        vec![first, second]
    );
    assert!(reopened.open_filesystem(first_id, 1_u8).is_ok());
    fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn deletion_is_a_durable_tombstone_exported_in_snapshot() {
    let root = root();
    let catalog = FileSystemCatalog::open(&root).expect("open catalog");
    let resource = catalog.create(None).expect("create resource");
    let filesystem_root = root
        .join("filesystems")
        .join(resource.id.as_uuid().to_string());
    catalog
        .open_filesystem(resource.id, 1_u8)
        .expect("open filesystem");
    catalog.delete(resource.id).expect("delete resource");
    assert!(filesystem_root.exists());
    assert!(matches!(
        catalog.open_filesystem(resource.id, 1_u8),
        Err(Error::NotFound)
    ));
    assert!(catalog.list().expect("list resources").is_empty());
    let snapshot = catalog.snapshot().expect("export snapshot");
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot[0].deleted);
    drop(catalog);

    let reopened = FileSystemCatalog::open(&root).expect("reopen catalog");
    let imported_root = root.join("other-namespace");
    let imported = FileSystemCatalog::open(&imported_root).expect("open import catalog");
    imported
        .import_snapshot(&reopened.snapshot().expect("snapshot after restart"))
        .expect("import snapshot");
    assert!(imported.list().expect("list imported").is_empty());
    assert!(matches!(
        imported.open_filesystem(resource.id, 1_u8),
        Err(Error::NotFound)
    ));
    assert!(matches!(imported.delete(resource.id), Err(Error::NotFound)));
    fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn imported_tombstone_wins_over_a_live_catalog_entry() {
    let root = root();
    let catalog = FileSystemCatalog::open(&root).expect("open catalog");
    let resource = catalog
        .create(Some("duplicate ok".to_owned()))
        .expect("create resource");
    let tombstone = CatalogEntry {
        resource: resource.clone(),
        deleted: true,
    };
    catalog
        .import_snapshot(&[tombstone])
        .expect("import tombstone");
    catalog
        .import_snapshot(&[CatalogEntry {
            resource: resource.clone(),
            deleted: false,
        }])
        .expect("import stale live record");
    assert!(catalog.list().expect("list resources").is_empty());
    fs::remove_dir_all(root).expect("remove test root");
}

#[tokio::test]
async fn deselecting_projected_resource_evicts_only_local_copy_without_tombstone() {
    let root = root();
    let catalog = FileSystemCatalog::open(&root).expect("open catalog");
    let resource = catalog
        .create(Some("shared".to_owned()))
        .expect("create resource");
    catalog
        .mark_projected(resource.id)
        .expect("mark projection");
    let filesystem = catalog
        .open_filesystem(resource.id, 1_u8)
        .expect("open projected filesystem");
    filesystem
        .set_residency("", file_system::Residency::Full)
        .await
        .expect("set full residency");
    filesystem
        .write("cached.txt", b"local copy")
        .await
        .expect("write local copy");
    let local_root = root
        .join("filesystems")
        .join(resource.id.as_uuid().to_string());
    assert!(local_root.exists());

    assert!(
        catalog
            .evict_projected_copy(resource.id)
            .expect("evict copy")
    );
    assert!(!local_root.exists());
    assert_eq!(
        catalog.list().expect("catalog retains identity"),
        [resource]
    );
    assert!(!catalog.snapshot().expect("snapshot remains live")[0].deleted);
    assert!(
        catalog
            .projected_selected()
            .expect("projection deselected")
            .is_empty()
    );

    drop(filesystem);
    drop(catalog);
    fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn filesystem_id_is_uuid_v7_backed_and_parsed_ids_are_checked() {
    let id = FileSystemId::new();
    assert_eq!(id.as_uuid().get_version_num(), 7);
    assert_eq!(
        FileSystemId::parse(&id.as_uuid().to_string()).expect("parse UUIDv7"),
        id
    );
    assert!(FileSystemId::parse("not-an-id").is_err());
    assert!(FileSystemId::parse(&Uuid::nil().to_string()).is_err());
}
