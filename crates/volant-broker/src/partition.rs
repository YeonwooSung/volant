//! Single partition handle with replica metadata.

use std::collections::HashMap;
use std::time::Instant;

use volant_core::PartitionId;
use volant_storage::PartitionLog;

/// A live partition owning its append-only log and replica state.
#[derive(Debug)]
pub struct Partition {
    /// Partition index.
    pub id: PartitionId,
    /// Underlying log store.
    pub log: PartitionLog,
    /// Current leader broker id.
    pub leader: u32,
    /// Full replica set.
    pub replicas: Vec<u32>,
    /// In-sync replicas.
    pub isr: Vec<u32>,
    /// Leader epoch.
    pub leader_epoch: u32,
    /// Committed high watermark (client-visible). Single-node: equals LEO after produce.
    pub committed_hwm: u64,
    /// Follower LEOs observed by the leader (`replica_id → LEO`).
    pub follower_leo: HashMap<u32, u64>,
    /// Last time a follower was observed with lag ≤ `replica_lag_max_messages`
    /// (Phase 125 time-based ISR shrink).
    pub follower_caught_up_at: HashMap<u32, Instant>,
}

impl Partition {
    /// Create a partition with single-node replica metadata.
    pub fn new_single(id: PartitionId, log: PartitionLog, node_id: u32) -> Self {
        Self {
            id,
            log,
            leader: node_id,
            replicas: vec![node_id],
            isr: vec![node_id],
            leader_epoch: 0,
            committed_hwm: 0,
            follower_leo: HashMap::new(),
            follower_caught_up_at: HashMap::new(),
        }
    }

    /// Local log-end offset (next offset to write).
    pub fn leo(&self) -> u64 {
        self.log.log_end_offset().raw()
    }

    /// Whether `node_id` is the leader.
    pub fn is_leader(&self, node_id: u32) -> bool {
        self.leader == node_id
    }

    /// Whether `node_id` is in the replica set.
    pub fn is_replica(&self, node_id: u32) -> bool {
        self.replicas.contains(&node_id)
    }

    /// Recompute committed HWM from leader LEO + follower LEOs for ISR members.
    ///
    /// Must be called on the leader. `node_id` is this broker's id (should equal leader).
    pub fn recompute_hwm(&mut self, _node_id: u32) {
        if self.isr.is_empty() {
            return;
        }
        let leader_leo = self.leo();
        let mut min_leo = u64::MAX;
        for &id in &self.isr {
            let leo = if id == self.leader {
                leader_leo
            } else {
                *self.follower_leo.get(&id).unwrap_or(&0)
            };
            min_leo = min_leo.min(leo);
        }
        if min_leo == u64::MAX {
            min_leo = 0;
        }
        // HWM never goes backwards.
        if min_leo > self.committed_hwm {
            self.committed_hwm = min_leo;
        }
    }

    /// Sync committed_hwm to LEO (single-node / sole-ISR path).
    pub fn catch_up_hwm(&mut self) {
        let leo = self.leo();
        if leo > self.committed_hwm {
            self.committed_hwm = leo;
        }
    }
}
