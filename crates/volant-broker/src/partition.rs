//! Single partition handle.

use volant_core::PartitionId;
use volant_storage::PartitionLog;

/// A live partition owning its append-only log.
#[derive(Debug)]
pub struct Partition {
    /// Partition index.
    pub id: PartitionId,
    /// Underlying log store.
    pub log: PartitionLog,
}
