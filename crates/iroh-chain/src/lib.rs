#![forbid(unsafe_code)]

mod dynamic;
#[cfg(feature = "in-memory")]
mod in_memory;
mod pairing;
mod server;
mod store;

pub use dynamic::DynamicEndpointIdStore;
#[cfg(feature = "in-memory")]
pub use in_memory::InMemoryEndpointIdStore;
pub use pairing::{MAX_PAIRING_PAYLOAD_LENGTH, PairingEvent, PairingOffer};
pub use server::{
    PAIRING_ALPN, Server, TUNNEL_ALPN, Tunnel, TunnelAuthorizer, TunnelEvent, TunnelReader,
    TunnelWriter, VaultId,
};
pub use store::AllowedEndpointId;
