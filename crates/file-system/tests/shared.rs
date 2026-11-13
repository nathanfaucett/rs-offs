use file_system::{FileSystemCatalog, Residency};
use ofdb_kv::SharedDatabase;
use uuid::Uuid;

#[tokio::test]
async fn shared_filesystem_retains_content_catalog_and_retirement_in_one_file() {
    let root = std::env::temp_dir().join(format!("offs-shared-{}", Uuid::now_v7()));
    std::fs::create_dir(&root).expect("host root");
    let path = root.join("unified.redb");
    let host = SharedDatabase::create(&path).expect("host");
    let catalog = FileSystemCatalog::open_shared(&host, "owner/app").expect("catalog");
    let resource = catalog
        .create(Some("documents".into()))
        .await
        .expect("resource");
    let fs = catalog
        .open_filesystem(resource.id, 1_u8)
        .await
        .expect("filesystem");
    fs.set_residency("", Residency::Full)
        .await
        .expect("full copy");
    fs.write("old.txt", b"saved").await.expect("write");
    fs.rename("old.txt", "saved.txt").await.expect("rename");
    fs.append("saved.txt", b" content").await.expect("append");
    drop(fs);
    drop(catalog);
    drop(host);
    let host = SharedDatabase::create(&path).expect("restart host");
    let catalog = FileSystemCatalog::open_shared(&host, "owner/app").expect("restart catalog");
    assert_eq!(
        catalog.list().await.expect("retained catalog"),
        vec![resource.clone()]
    );
    let fs = catalog
        .open_filesystem(resource.id, 1_u8)
        .await
        .expect("retained filesystem");
    assert_eq!(
        fs.residency("").expect("retained residency"),
        Residency::Full
    );
    assert_eq!(
        fs.read("saved.txt").await.expect("retained contents"),
        b"saved content"
    );
    catalog
        .remove_managed_copy(resource.id)
        .await
        .expect("retire copy");
    assert!(fs.read("saved.txt").await.is_err());
    assert!(catalog.open_filesystem(resource.id, 1_u8).await.is_err());
    assert_eq!(
        catalog.list().await.expect("identity retained"),
        vec![resource.clone()]
    );
    assert!(path.is_file());
    assert_eq!(
        std::fs::read_dir(&root)
            .expect("single file layout")
            .count(),
        1
    );
    drop(fs);
    drop(catalog);
    drop(host);
    let host = SharedDatabase::create(&path).expect("restart retired host");
    let catalog = FileSystemCatalog::open_shared(&host, "owner/app").expect("retained catalog");
    assert!(catalog.open_filesystem(resource.id, 1_u8).await.is_err());
    catalog.delete(resource.id).await.expect("delete identity");
    assert!(catalog.snapshot().await.expect("tombstone")[0].deleted);
    drop(catalog);
    drop(host);
    std::fs::remove_dir_all(root).expect("cleanup");
}
