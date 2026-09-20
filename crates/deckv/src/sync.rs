use alloc::vec::Vec;
use core::error::Error;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{LwwRecord, Timestamp};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMessage<Id, K, V> {
    /// “I am node X and the highest timestamp I already have from you is T”
    Initial {
        node_id: Id,
        /// Highest timestamp the sender already possesses for the *receiver’s* writes.
        /// `None` = first contact.
        since: Option<Timestamp>,
    },

    /// Records the receiver is missing.
    Delta {
        records: Vec<(K, LwwRecord<Id, V>)>,
    },

    Done,
}

pub trait Sync<Id, K, V> {
    type Error: Error;

    fn send(&self, msg: SyncMessage<Id, K, V>) -> impl Future<Output = Result<(), Self::Error>>;
    fn stream(&self) -> impl Stream<Item = Result<SyncMessage<Id, K, V>, Self::Error>>;
}
