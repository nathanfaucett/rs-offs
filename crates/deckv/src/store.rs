use alloc::vec::Vec;

use core::hash::Hash;
use futures_util::{StreamExt, pin_mut};
use hashbrown::HashMap;
use tokio::sync::{
    Mutex,
    broadcast::{Receiver, Sender},
};

use crate::{Hlc, LwwRecord, Storage, Timestamp};

pub struct Store<Id, K, V, S>
where
    S: Storage<Id, K, V>,
{
    clock: Mutex<()>,
    node_id: Id,
    storage: S,
    sender: Sender<(K, LwwRecord<Id, V>)>,
}

impl<Id, K, V, S> Store<Id, K, V, S>
where
    Id: PartialOrd + Clone,
    K: Eq + Hash,
    S: Storage<Id, K, V>,
{
    pub fn new(node_id: Id, storage: S, sender: Sender<(K, LwwRecord<Id, V>)>) -> Self {
        Self {
            clock: Mutex::new(()),
            node_id,
            storage,
            sender,
        }
    }

    pub fn subscribe(&self) -> Receiver<(K, LwwRecord<Id, V>)> {
        self.sender.subscribe()
    }

    pub async fn insert(&self, key: K, value: V) -> Result<(), S::Error> {
        let timestamp = self.next_timestamp().await?;
        self.apply(
            key,
            LwwRecord::new(Some(value), timestamp, self.node_id.clone()),
        )
        .await
    }

    pub async fn delete(&self, key: K) -> Result<(), S::Error> {
        let timestamp = self.next_timestamp().await?;
        self.apply(key, LwwRecord::new(None, timestamp, self.node_id.clone()))
            .await
    }

    async fn next_timestamp(&self) -> Result<Timestamp, S::Error> {
        let _guard = self.clock.lock().await;
        let mut clock = Hlc::new(self.storage.clock().await?);
        let timestamp = clock.next_timestamp();
        self.storage.set_clock(clock.timestamp()).await?;
        Ok(timestamp)
    }

    async fn observe(&self, timestamp: Timestamp) -> Result<(), S::Error> {
        let _guard = self.clock.lock().await;
        let mut clock = Hlc::new(self.storage.clock().await?);
        clock.observe(timestamp);
        self.storage.set_clock(clock.timestamp()).await
    }

    pub async fn apply(&self, key: K, record: LwwRecord<Id, V>) -> Result<(), S::Error> {
        self.observe(record.timestamp).await?;
        let should_apply = match self.storage.get(&key).await? {
            Some(current) => record.is_newer_than(&current),
            None => true,
        };

        if should_apply {
            self.storage.insert(&key, &record).await?;
            let _ = self.sender.send((key, record));
        }

        Ok(())
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>, S::Error> {
        match self.storage.get(key).await? {
            Some(rec) => Ok(rec.value),
            None => Ok(None),
        }
    }

    pub async fn entries(&self) -> Result<Vec<(K, LwwRecord<Id, V>)>, S::Error> {
        let mut entries = Vec::new();
        let stream = self.storage.entries();
        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            entries.push(item?);
        }

        Ok(entries)
    }

    pub async fn get_latest_for_node_id(&self, node_id: Id) -> Result<Option<Timestamp>, S::Error> {
        self.storage.get_latest_for_node_id(node_id).await
    }

    pub async fn export_for(
        &self,
        peer: Id,
        since: Option<Timestamp>,
    ) -> Result<HashMap<K, LwwRecord<Id, V>>, S::Error> {
        let mut delta = HashMap::new();
        let stream = self.storage.stream(peer, since);
        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            let (key, rec) = item?;
            delta.insert(key, rec);
        }

        Ok(delta)
    }

    pub async fn export(
        &self,
        since: Option<Timestamp>,
    ) -> Result<HashMap<K, LwwRecord<Id, V>>, S::Error> {
        self.export_for(self.node_id.clone(), since).await
    }

    pub async fn merge_peer_state(
        &self,
        remote: HashMap<K, LwwRecord<Id, V>>,
    ) -> Result<(), S::Error> {
        for (key, remote_rec) in remote {
            self.apply(key, remote_rec).await?;
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
    use tokio::sync::broadcast;

    use crate::{InMemoryStorage, LwwRecord, Store, Timestamp};

    #[tokio::test]
    async fn local_write_is_newer_after_observing_future_record() {
        let store = Store::new(1_u8, InMemoryStorage::new(), broadcast::channel(1).0);
        let remote = Timestamp::new(u64::MAX - 1, 4);

        store
            .apply("remote", LwwRecord::new(Some("value"), remote, 2))
            .await
            .unwrap();
        store.insert("local", "value").await.unwrap();

        assert!(store.get_latest_for_node_id(1).await.unwrap().unwrap() > remote);
    }
}
