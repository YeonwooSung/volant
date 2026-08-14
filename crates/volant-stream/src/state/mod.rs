//! Key-value state stores for stream operators (`reduce`, tumbling windows).
//!
//! - [`MemoryStore`] — process-local, lost on restart (default ALO path)
//! - [`DurableStore`] — redb-backed, survives process restart (Phase 149)
//! - Checkpoint staging (Phase 153): optional begin/commit/abort so EOS can
//!   stage durable puts until the broker transaction succeeds
//!
//! Window buckets use the same store + overlay. Restart restore is
//! in-process only — not distributed EOS / 2PC.

mod durable;
mod memory;

pub use durable::{DurableStore, StreamStateError};
pub use memory::MemoryStore;

use bytes::Bytes;

/// Key-value store used by stateful operators (`reduce`, windows).
///
/// Methods are infallible at the trait boundary (except
/// [`commit_checkpoint`](Self::commit_checkpoint)). Durable backends may
/// panic on unrecoverable storage I/O after a successful [`DurableStore::open`].
///
/// # Checkpoints (Phase 153)
///
/// Defaults are no-ops. [`DurableStore`] stages puts/deletes in memory while a
/// checkpoint is open and only fsyncs on [`commit_checkpoint`](Self::commit_checkpoint).
/// Outside a checkpoint, durable stores keep immediate-put behavior (ALO).
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

    /// Enter staging mode (no-op by default).
    fn begin_checkpoint(&mut self) {}

    /// Persist staged mutations (fsync as applicable). No-op when not staging.
    fn commit_checkpoint(&mut self) -> Result<(), StreamStateError> {
        Ok(())
    }

    /// Discard staged mutations; restore view to last committed durable state.
    fn abort_checkpoint(&mut self) {}

    /// Whether a checkpoint is open (staging active).
    fn in_checkpoint(&self) -> bool {
        false
    }
}
