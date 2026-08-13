//! Stream operator trait.

use volant_core::{Record, Result};

/// A pure or stateful transform over records.
pub trait Operator: Send {
    /// Transform an input record into zero or more output records.
    fn process(&mut self, record: Record) -> Result<Vec<Record>>;

    /// Optional operator name for metrics / debugging.
    fn name(&self) -> &str {
        "operator"
    }

    /// Flush window/state timers; default no-op.
    ///
    /// Runtime calls this each poll with wall-clock or event time `now_ms`.
    fn punctuate(&mut self, _now_ms: i64) -> Result<Vec<Record>> {
        Ok(vec![])
    }

    /// Enter state-store staging for an EOS step (Phase 153). Default no-op.
    fn begin_checkpoint(&mut self) {}

    /// Persist staged state after a successful EndTxn. Default no-op.
    fn commit_checkpoint(&mut self) -> Result<()> {
        Ok(())
    }

    /// Discard staged state after txn abort or empty step. Default no-op.
    fn abort_checkpoint(&mut self) {}
}
