#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod hlc;
#[cfg(feature = "in-memory")]
mod in_memory;
mod lww_record;
#[cfg(feature = "redb")]
mod redb;
mod storage;
mod store;
mod sync;

pub use hlc::{Hlc, Timestamp};
#[cfg(feature = "in-memory")]
pub use in_memory::InMemoryStorage;
pub use lww_record::LwwRecord;
#[cfg(feature = "redb")]
pub use redb::RedbStorage;
pub use storage::Storage;
pub use store::Store;
pub use sync::{Sync, SyncMessage};
