use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp {
    pub physical: u64,
    pub logical: u64,
}

impl Timestamp {
    pub const fn new(physical: u64, logical: u64) -> Self {
        Self { physical, logical }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Hlc {
    timestamp: Timestamp,
}

impl Hlc {
    pub const fn new(timestamp: Timestamp) -> Self {
        Self { timestamp }
    }

    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }

    pub fn next_timestamp(&mut self) -> Timestamp {
        self.advance(physical_now(), None)
    }

    pub fn observe(&mut self, remote: Timestamp) {
        self.advance(physical_now(), Some(remote));
    }

    fn advance(&mut self, physical: u64, remote: Option<Timestamp>) -> Timestamp {
        let maximum = remote.map_or(self.timestamp.physical, |timestamp| {
            self.timestamp.physical.max(timestamp.physical)
        });
        let physical = physical.max(maximum);
        let logical = if physical == self.timestamp.physical
            && remote.is_some_and(|timestamp| physical == timestamp.physical)
        {
            self.timestamp.logical.max(remote.unwrap().logical) + 1
        } else if physical == self.timestamp.physical {
            self.timestamp.logical + 1
        } else if remote.is_some_and(|timestamp| physical == timestamp.physical) {
            remote.unwrap().logical + 1
        } else {
            0
        };

        self.timestamp = Timestamp::new(physical, logical);
        self.timestamp
    }
}

#[cfg(feature = "std")]
fn physical_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(not(feature = "std"))]
const fn physical_now() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::{Hlc, Timestamp};

    #[test]
    fn observes_remote_timestamp() {
        let mut clock = Hlc::default();
        clock.observe(Timestamp::new(u64::MAX - 1, 4));

        assert!(clock.next_timestamp() > Timestamp::new(u64::MAX - 1, 4));
    }
}
