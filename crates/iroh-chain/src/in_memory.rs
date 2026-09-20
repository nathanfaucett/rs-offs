use std::sync::Arc;

use dashmap::DashSet;
use iroh::EndpointId;

use crate::store::AllowedEndpointId;

#[derive(Default, Clone)]
pub struct InMemoryEndpointIdStore {
    inner: Arc<DashSet<EndpointId>>,
}

impl InMemoryEndpointIdStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AllowedEndpointId for InMemoryEndpointIdStore {
    async fn allowed(&self, id: EndpointId) -> bool {
        self.inner.contains(&id)
    }
}

impl InMemoryEndpointIdStore {
    pub fn add(&self, id: EndpointId) {
        self.inner.insert(id);
    }

    pub fn remove(&self, id: EndpointId) -> bool {
        self.inner.remove(&id).is_some()
    }
}
