#![forbid(unsafe_code)]

mod dynamic;
mod hooks;
#[cfg(feature = "in-memory")]
mod in_memory;
mod pairing;
mod server;
mod store;

pub use dynamic::DynamicEndpointIdStore;
pub use hooks::AllowlistHook;
#[cfg(feature = "in-memory")]
pub use in_memory::InMemoryEndpointIdStore;
pub use pairing::{MAX_PAIRING_PAYLOAD_LENGTH, PairingEvent, PairingOffer};
pub use server::{
    FILE_TRANSFER_ALPN, METADATA_SYNC_ALPN, PAIRING_ALPN, RootAuthorizer, RootId, Server,
};
pub use store::AllowedEndpointId;
