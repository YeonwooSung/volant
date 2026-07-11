//! Persisted and in-memory topic/partition assignment.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};
use volant_protocol::{ClusterPartitionState, ClusterTopicState};

/// Per-partition replica assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionAssignment {
    /// Replica broker ids (order: preferred leader first).
    pub replicas: Vec<u32>,
    /// Current leader.
    pub leader: u32,
    /// In-sync replicas.
    pub isr: Vec<u32>,
    /// Leader epoch.
    pub leader_epoch: u32,
}

/// Per-topic assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicAssignment {
    /// Topic id.
    pub topic_id: u32,
    /// Topic name.
    pub name: String,
    /// Partition id → assignment.
    pub partitions: HashMap<u32, PartitionAssignment>,
}

/// Full cluster assignment snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssignmentSnapshot {
    /// Monotonic generation (controller-authored).
    pub generation: u32,
    /// Topics by name.
    pub topics: HashMap<String, TopicAssignment>,
}

impl AssignmentSnapshot {
    /// Convert to wire `ClusterTopicState` list.
    pub fn to_wire_topics(&self) -> Vec<ClusterTopicState> {
        let mut topics: Vec<_> = self
            .topics
            .values()
            .map(|t| {
                let mut partitions: Vec<_> = t
                    .partitions
                    .iter()
                    .map(|(pid, p)| ClusterPartitionState {
                        partition_id: *pid,
                        leader: p.leader,
                        leader_epoch: p.leader_epoch,
                        replicas: p.replicas.clone(),
                        isr: p.isr.clone(),
                    })
                    .collect();
                partitions.sort_by_key(|p| p.partition_id);
                ClusterTopicState {
                    name: t.name.clone(),
                    topic_id: t.topic_id,
                    partitions,
                }
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        topics
    }

    /// Apply a wire cluster-state snapshot (replace topics).
    pub fn apply_wire(
        &mut self,
        generation: u32,
        topics: &[ClusterTopicState],
    ) {
        self.generation = generation;
        self.topics.clear();
        for t in topics {
            let mut partitions = HashMap::new();
            for p in &t.partitions {
                partitions.insert(
                    p.partition_id,
                    PartitionAssignment {
                        replicas: p.replicas.clone(),
                        leader: p.leader,
                        isr: p.isr.clone(),
                        leader_epoch: p.leader_epoch,
                    },
                );
            }
            self.topics.insert(
                t.name.clone(),
                TopicAssignment {
                    topic_id: t.topic_id,
                    name: t.name.clone(),
                    partitions,
                },
            );
        }
    }
}

/// Path to assignment.json under data_dir.
pub fn assignment_path(data_dir: &Path) -> PathBuf {
    data_dir.join("cluster").join("assignment.json")
}

/// Load assignment from disk (empty if missing).
pub fn load_assignment(data_dir: &Path) -> Result<AssignmentSnapshot> {
    let path = assignment_path(data_dir);
    if !path.exists() {
        return Ok(AssignmentSnapshot::default());
    }
    let raw = fs::read_to_string(&path).map_err(|e| {
        Error::Storage(format!("read assignment {}: {e}", path.display()))
    })?;
    serde_json::from_str(&raw)
        .map_err(|e| Error::Storage(format!("parse assignment: {e}")))
}

/// Persist assignment to disk.
pub fn save_assignment(data_dir: &Path, snap: &AssignmentSnapshot) -> Result<()> {
    let dir = data_dir.join("cluster");
    fs::create_dir_all(&dir).map_err(|e| {
        Error::Storage(format!("create cluster dir {}: {e}", dir.display()))
    })?;
    let path = assignment_path(data_dir);
    let raw = serde_json::to_string_pretty(snap)
        .map_err(|e| Error::Storage(format!("serialize assignment: {e}")))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, raw.as_bytes())
        .map_err(|e| Error::Storage(format!("write assignment tmp: {e}")))?;
    fs::rename(&tmp, &path)
        .map_err(|e| Error::Storage(format!("rename assignment: {e}")))?;
    Ok(())
}
