#![cfg_attr(target_arch = "wasm32", allow(dead_code))] // browser construction lands with the browser backend

use ofdb_platform::{BoxFuture, MaybeSend, MaybeSync};

use crate::Error;

/// Object-safe root boundary shared across threads on native targets.
pub(crate) trait RootObject: Root + MaybeSend + MaybeSync {}
impl<T: ?Sized + Root + MaybeSend + MaybeSync> RootObject for T {}

/// Internal port for exclusive ownership, durable retirement, and root removal of one
/// filesystem identity. The boundary is internal to the library.
pub(crate) trait Root: MaybeSend + MaybeSync {
    /// Whether a durable retirement record exists.
    fn retired(&self) -> BoxFuture<'_, Result<bool, Error>>;

    /// Writes the durable retirement record.
    fn retire(&self) -> BoxFuture<'_, Result<(), Error>>;

    /// Acquires exclusive ownership of the root, failing when another owner holds it.
    fn prepare(&mut self) -> BoxFuture<'_, Result<(), Error>>;

    /// Releases exclusive ownership and removes the root's stored bytes.
    fn remove(&mut self) -> BoxFuture<'_, Result<(), Error>>;
}
