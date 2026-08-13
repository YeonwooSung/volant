//! Key-value state stores for stream operators.
//!
//! - [`MemoryStore`] — process-local, lost on restart
//! - [`DurableStore`] — redb-backed, survives process restart (Phase 149)

mod durable;
mod memory;

pub use durable::{DurableStore, StreamStateError};
pub use memory::MemoryStore;

use bytes::Bytes;

/// Key-value store used by stateful operators (`reduce`, windows).
///
/// Methods are infallible at the trait boundary. Durable backends may
/// panic on unrecoverable storage I/O after a successful [`DurableStore::open`].
pub trait KeyValueStore: Send {
    /// Look up a value by key.
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    /// Insert or replace a key.
    fn put(&mut self, key: Bytes, value: Bytes);
    /// Remove a key if present.
    fn delete(&mut self, key: &[u8]);
    /// Iterate all entries in key order.
    fn iter(&self) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + '_>;
    /// Number of entries.
    fn len(&self) -> usize;
    /// Whether the store is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
