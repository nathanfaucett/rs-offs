use core::error::Error;

use futures_core::Stream;

use crate::{Timestamp, lww_record::LwwRecord};

/// Pluggable durable (or in-memory) backend.
pub trait Storage<Id, K, V> {
    type Error: Error;

    fn insert(
        &self,
        key: &K,
        record: &LwwRecord<Id, V>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    fn get(&self, key: &K) -> impl Future<Output = Result<Option<LwwRecord<Id, V>>, Self::Error>>;

    fn remove(&self, key: &K) -> impl Future<Output = Result<(), Self::Error>>;

    fn clock(&self) -> impl Future<Output = Result<Timestamp, Self::Error>>;

    fn set_clock(&self, timestamp: Timestamp) -> impl Future<Output = Result<(), Self::Error>>;

    fn get_latest_for_node_id(
        &self,
        node_id: Id,
    ) -> impl Future<Output = Result<Option<Timestamp>, Self::Error>>;

    fn stream(
        &self,
        node_id: Id,
        since: Option<Timestamp>,
    ) -> impl Stream<Item = Result<(K, LwwRecord<Id, V>), Self::Error>>;

    fn entries(&self) -> impl Stream<Item = Result<(K, LwwRecord<Id, V>), Self::Error>>;
}
