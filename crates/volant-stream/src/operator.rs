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
}
