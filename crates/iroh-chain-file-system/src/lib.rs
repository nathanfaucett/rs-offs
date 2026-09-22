#![forbid(unsafe_code)]

mod scoped_transport;

pub use scoped_transport::{AccessTokenProvider, RootIrohTransport, StaticAccessToken};
