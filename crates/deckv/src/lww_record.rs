use serde::{Deserialize, Serialize};

use crate::Timestamp;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LwwRecord<Id, V> {
    /// The value of the record. If `None`, it represents a tombstone (deletion).
    pub value: Option<V>,
    pub timestamp: Timestamp,
    pub node_id: Id,
}

impl<Id, V> LwwRecord<Id, V> {
    pub fn new(value: Option<V>, timestamp: Timestamp, node_id: Id) -> Self {
        Self {
            value,
            timestamp,
            node_id,
        }
    }
}

impl<Id, V> LwwRecord<Id, V>
where
    Id: PartialOrd,
{
    pub fn is_newer_than(&self, other: &Self) -> bool {
        self.timestamp > other.timestamp
            || (self.timestamp == other.timestamp && self.node_id > other.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::LwwRecord;
    use crate::Timestamp;

    #[test]
    fn orders_timestamp_then_node_id() {
        let timestamp = Timestamp::new(1, 0);
        let older = LwwRecord::new(Some("older"), timestamp, 2_u8);
        let newer = LwwRecord::new(Some("newer"), Timestamp::new(2, 0), 1_u8);
        let tied_higher_node = LwwRecord::new(Some("tied"), timestamp, 3_u8);

        assert!(newer.is_newer_than(&older));
        assert!(tied_higher_node.is_newer_than(&older));
        assert!(!older.is_newer_than(&newer));
    }
}
