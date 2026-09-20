use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use iroh::EndpointId;
use tokio::sync::RwLock;

use crate::AllowedEndpointId;

#[derive(Clone, Default)]
pub struct DynamicEndpointIdStore {
    scopes: Arc<RwLock<BTreeMap<String, BTreeSet<EndpointId>>>>,
}

impl DynamicEndpointIdStore {
    pub async fn replace(&self, ids: impl IntoIterator<Item = EndpointId>) {
        self.replace_scope(String::new(), ids).await;
    }

    pub async fn replace_scope(&self, scope: String, ids: impl IntoIterator<Item = EndpointId>) {
        self.scopes
            .write()
            .await
            .insert(scope, ids.into_iter().collect());
    }

    pub async fn insert_scope(&self, scope: String, id: EndpointId) {
        self.scopes
            .write()
            .await
            .entry(scope)
            .or_default()
            .insert(id);
    }
}

impl AllowedEndpointId for DynamicEndpointIdStore {
    async fn allowed(&self, id: EndpointId) -> bool {
        self.scopes
            .read()
            .await
            .values()
            .any(|ids| ids.contains(&id))
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::{AllowedEndpointId, DynamicEndpointIdStore};

    #[tokio::test]
    async fn replaces_allowed_ids() {
        let first = SecretKey::generate().public();
        let second = SecretKey::generate().public();
        let store = DynamicEndpointIdStore::default();
        store.replace([first]).await;
        assert!(store.allowed(first).await);
        assert!(!store.allowed(second).await);
        store.replace([second]).await;
        assert!(!store.allowed(first).await);
        assert!(store.allowed(second).await);
    }

    #[tokio::test]
    async fn combines_scoped_allowlists() {
        let first = SecretKey::generate().public();
        let second = SecretKey::generate().public();
        let store = DynamicEndpointIdStore::default();
        store.replace_scope("first".into(), [first]).await;
        store.replace_scope("second".into(), [second]).await;
        assert!(store.allowed(first).await);
        assert!(store.allowed(second).await);
        store.replace_scope("first".into(), []).await;
        assert!(!store.allowed(first).await);
        assert!(store.allowed(second).await);
    }
}
