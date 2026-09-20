use alloc::vec::Vec;
use core::{any::type_name, cmp::Ordering, fmt::Debug, marker::PhantomData};

use async_stream::stream;
use futures_core::Stream;
use postcard::{from_bytes, to_allocvec};
use redb::{
    Database, Error, Key, ReadableDatabase, ReadableTable, TableDefinition, TypeName, Value,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{LwwRecord, Storage, Timestamp};

const CLOCK: TableDefinition<'static, &str, RedbValue> = TableDefinition::new("clock");
const RECORDS: TableDefinition<'static, RedbKey, RedbValue> = TableDefinition::new("records");

pub struct RedbStorage<Id, K, V> {
    database: Database,
    marker: PhantomData<(Id, K, V)>,
}

impl<Id, K, V> RedbStorage<Id, K, V> {
    pub fn open(database: Database) -> Result<Self, Error> {
        let transaction = database.begin_write()?;
        {
            let _clock = transaction.open_table(CLOCK)?;
            let _records = transaction.open_table(RECORDS)?;
        }
        transaction.commit()?;
        Ok(Self {
            database,
            marker: PhantomData,
        })
    }
}

impl<Id, K, V> From<Database> for RedbStorage<Id, K, V> {
    fn from(database: Database) -> Self {
        Self {
            database,
            marker: PhantomData,
        }
    }
}

impl<Id, K, V> Storage<Id, K, V> for RedbStorage<Id, K, V>
where
    Id: Clone + Debug + Ord + Serialize + DeserializeOwned + 'static,
    K: Clone + Debug + Ord + Serialize + DeserializeOwned + 'static,
    V: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type Error = Error;

    async fn insert(&self, key: &K, record: &LwwRecord<Id, V>) -> Result<(), Self::Error> {
        let transaction = self.database.begin_write()?;
        {
            let mut records = transaction.open_table(RECORDS)?;
            records.insert(
                RedbKey(to_allocvec(key).expect("key serialization must succeed")),
                RedbValue(to_allocvec(record).expect("record serialization must succeed")),
            )?;
        }
        Ok(transaction.commit()?)
    }

    async fn get(&self, key: &K) -> Result<Option<LwwRecord<Id, V>>, Self::Error> {
        let transaction = self.database.begin_read()?;
        let records = transaction.open_table(RECORDS)?;
        records
            .get(RedbKey(
                to_allocvec(key).expect("key serialization must succeed"),
            ))?
            .map(|value| from_bytes(&value.value().0).expect("record deserialization must succeed"))
            .map_or(Ok(None), |record| Ok(Some(record)))
    }

    async fn remove(&self, key: &K) -> Result<(), Self::Error> {
        let transaction = self.database.begin_write()?;
        {
            let mut records = transaction.open_table(RECORDS)?;
            records.remove(RedbKey(
                to_allocvec(key).expect("key serialization must succeed"),
            ))?;
        }
        Ok(transaction.commit()?)
    }

    async fn clock(&self) -> Result<Timestamp, Self::Error> {
        let transaction = self.database.begin_read()?;
        let clock = transaction.open_table(CLOCK)?;
        clock
            .get("clock")?
            .map(|value| from_bytes(&value.value().0).expect("clock deserialization must succeed"))
            .map_or(Ok(Timestamp::default()), Ok)
    }

    async fn set_clock(&self, timestamp: Timestamp) -> Result<(), Self::Error> {
        let transaction = self.database.begin_write()?;
        {
            let _records = transaction.open_table(RECORDS)?;
            let mut clock = transaction.open_table(CLOCK)?;
            clock.insert(
                "clock",
                RedbValue(to_allocvec(&timestamp).expect("clock serialization must succeed")),
            )?;
        }
        Ok(transaction.commit()?)
    }

    async fn get_latest_for_node_id(&self, node_id: Id) -> Result<Option<Timestamp>, Self::Error> {
        let transaction = self.database.begin_read()?;
        let records = transaction.open_table(RECORDS)?;
        let mut latest = None;

        for entry in records.iter()? {
            let (_, value) = entry?;
            let record: LwwRecord<Id, V> =
                from_bytes(&value.value().0).expect("record deserialization must succeed");
            if record.node_id == node_id
                && latest.is_none_or(|timestamp| record.timestamp > timestamp)
            {
                latest = Some(record.timestamp);
            }
        }

        Ok(latest)
    }

    fn stream(
        &self,
        node_id: Id,
        since: Option<Timestamp>,
    ) -> impl Stream<Item = Result<(K, LwwRecord<Id, V>), Self::Error>> {
        stream! {
            let transaction = match self.database.begin_read() {
                Ok(transaction) => transaction,
                Err(error) => {
                    yield Err(error.into());
                    return;
                }
            };
            let records = match transaction.open_table(RECORDS) {
                Ok(records) => records,
                Err(error) => {
                    yield Err(error.into());
                    return;
                }
            };
            let entries = match records.iter() {
                Ok(entries) => entries,
                Err(error) => {
                    yield Err(error.into());
                    return;
                }
            };

            for entry in entries {
                let (key, value) = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        yield Err(error.into());
                        return;
                    }
                };
                let key: K = from_bytes(&key.value().0).expect("key deserialization must succeed");
                let record: LwwRecord<Id, V> =
                    from_bytes(&value.value().0).expect("record deserialization must succeed");

                if record.node_id == node_id && since.is_none_or(|timestamp| record.timestamp > timestamp) {
                    yield Ok((key, record));
                }
            }
        }
    }

    fn entries(&self) -> impl Stream<Item = Result<(K, LwwRecord<Id, V>), Self::Error>> {
        stream! {
            let transaction = match self.database.begin_read() {
                Ok(transaction) => transaction,
                Err(error) => {
                    yield Err(error.into());
                    return;
                }
            };
            let records = match transaction.open_table(RECORDS) {
                Ok(records) => records,
                Err(error) => {
                    yield Err(error.into());
                    return;
                }
            };
            let entries = match records.iter() {
                Ok(entries) => entries,
                Err(error) => {
                    yield Err(error.into());
                    return;
                }
            };

            for entry in entries {
                let (key, value) = match entry {
                    Ok(entry) => entry,
                    Err(error) => {
                        yield Err(error.into());
                        return;
                    }
                };
                let key = from_bytes(&key.value().0).expect("key deserialization must succeed");
                let record = from_bytes(&value.value().0).expect("record deserialization must succeed");
                yield Ok((key, record));
            }
        }
    }
}

#[derive(Debug)]
struct RedbKey(Vec<u8>);

impl Value for RedbKey {
    type SelfType<'a> = RedbKey;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(data.to_vec())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value.0.clone()
    }

    fn type_name() -> TypeName {
        TypeName::new(type_name::<Self>())
    }
}

impl Key for RedbKey {
    fn compare(left: &[u8], right: &[u8]) -> Ordering {
        left.cmp(right)
    }
}

#[derive(Debug)]
struct RedbValue(Vec<u8>);

impl Value for RedbValue {
    type SelfType<'a> = RedbValue;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(data.to_vec())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value.0.clone()
    }

    fn type_name() -> TypeName {
        TypeName::new(type_name::<Self>())
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use redb::Database;
    use tokio::sync::broadcast;
    use uuid::Uuid;

    use super::RedbStorage;
    use crate::{LwwRecord, Storage, Store, Timestamp};

    #[tokio::test]
    async fn persists_and_filters_node_deltas() {
        let path = std::env::temp_dir().join(format!("deckv-{}.redb", Uuid::now_v7()));
        let database = Database::create(&path).unwrap();
        let storage = RedbStorage::<u8, String, String>::from(database);
        let timestamp = Timestamp::new(1, 0);

        storage
            .insert(
                &"first".into(),
                &LwwRecord::new(Some("one".into()), timestamp, 1),
            )
            .await
            .unwrap();
        storage
            .insert(
                &"second".into(),
                &LwwRecord::new(None, Timestamp::new(2, 0), 1),
            )
            .await
            .unwrap();
        storage
            .insert(
                &"third".into(),
                &LwwRecord::new(Some("three".into()), Timestamp::new(3, 0), 2),
            )
            .await
            .unwrap();

        assert_eq!(
            storage.get(&"second".into()).await.unwrap(),
            Some(LwwRecord::new(None, Timestamp::new(2, 0), 1))
        );
        assert_eq!(
            storage.get_latest_for_node_id(1).await.unwrap(),
            Some(Timestamp::new(2, 0))
        );

        let delta: Vec<_> = storage
            .stream(1, Some(timestamp))
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(
            delta,
            vec![(
                "second".into(),
                LwwRecord::new(None, Timestamp::new(2, 0), 1),
            )]
        );

        drop(storage);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn persists_clock_across_restart() {
        let path = std::env::temp_dir().join(format!("deckv-{}.redb", Uuid::now_v7()));
        let storage = RedbStorage::<u8, String, String>::from(Database::create(&path).unwrap());
        storage
            .set_clock(Timestamp::new(u64::MAX - 1, 0))
            .await
            .unwrap();
        let store = Store::new(1, storage, broadcast::channel(1).0);
        store.insert("first".into(), "one".into()).await.unwrap();
        let first = store.get_latest_for_node_id(1).await.unwrap().unwrap();
        drop(store);

        let storage = RedbStorage::<u8, String, String>::from(Database::open(&path).unwrap());
        let store = Store::new(1, storage, broadcast::channel(1).0);
        store.insert("second".into(), "two".into()).await.unwrap();
        let second = store.get_latest_for_node_id(1).await.unwrap().unwrap();

        assert!(second > first);
        drop(store);
        std::fs::remove_file(path).unwrap();
    }
}
