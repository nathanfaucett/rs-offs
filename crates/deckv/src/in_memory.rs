use core::{convert::Infallible, hash::Hash};

use async_stream::stream;
use dashmap::DashMap;
use futures_core::Stream;

use crate::{LwwRecord, Storage, Timestamp};

pub struct InMemoryStorage<Id, K, V> {
    clock: DashMap<(), Timestamp>,
    records: DashMap<K, LwwRecord<Id, V>>,
}

impl<Id, K, V> Default for InMemoryStorage<Id, K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            clock: DashMap::default(),
            records: DashMap::default(),
        }
    }
}

impl<Id, K, V> InMemoryStorage<Id, K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Id, K, V> Storage<Id, K, V> for InMemoryStorage<Id, K, V>
where
    Id: PartialEq + Clone,
    K: Clone + Eq + Hash,
    V: Clone,
{
    type Error = Infallible;

    async fn insert(&self, key: &K, record: &LwwRecord<Id, V>) -> Result<(), Self::Error> {
        self.records.insert(key.clone(), record.clone());
        Ok(())
    }

    async fn get(&self, key: &K) -> Result<Option<LwwRecord<Id, V>>, Self::Error> {
        Ok(self.records.get(key).map(|r| r.value().clone()))
    }

    async fn remove(&self, key: &K) -> Result<(), Self::Error> {
        self.records.remove(key);
        Ok(())
    }

    async fn clock(&self) -> Result<Timestamp, Self::Error> {
        Ok(self
            .clock
            .get(&())
            .map_or(Timestamp::default(), |clock| *clock))
    }

    async fn set_clock(&self, timestamp: Timestamp) -> Result<(), Self::Error> {
        self.clock.insert((), timestamp);
        Ok(())
    }

    async fn get_latest_for_node_id(&self, node_id: Id) -> Result<Option<Timestamp>, Self::Error> {
        let latest = self
            .records
            .iter()
            .filter_map(|r| {
                if r.value().node_id == node_id {
                    Some(r.value().timestamp)
                } else {
                    None
                }
            })
            .max();

        Ok(latest)
    }

    fn stream(
        &self,
        node_id: Id,
        since: Option<Timestamp>,
    ) -> impl Stream<Item = Result<(K, LwwRecord<Id, V>), Self::Error>> {
        stream! {
            for entry in self.records.iter() {
                let record = entry.value();

                if record.node_id == node_id && since.is_none_or(|s| record.timestamp > s) {
                    yield Ok((entry.key().clone(), record.clone()));
                }
            }
        }
    }

    fn entries(&self) -> impl Stream<Item = Result<(K, LwwRecord<Id, V>), Self::Error>> {
        stream! {
            for entry in self.records.iter() {
                yield Ok((entry.key().clone(), entry.value().clone()));
            }
        }
    }
}
