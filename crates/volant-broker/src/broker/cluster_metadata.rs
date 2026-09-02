//! v0.12 `__cluster_metadata` topic + per-partition Raft broker hooks.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tracing::warn;
use volant_core::{Error, Message, Offset, PartitionId, Result, TopicId, TopicName};

use crate::cluster::{
    cluster_metadata_replicas, save_assignment, AssignmentSnapshot, PartitionAssignment,
    TopicAssignment, CLUSTER_METADATA_HEADER, CLUSTER_METADATA_HEADER_VALUE,
    CLUSTER_METADATA_TOPIC,
};
use crate::replica::{PartitionRaftState, PARTITION_RAFT_DIR, PARTITION_RAFT_LOG_FILE};
use crate::topic::Topic;
use crate::topic_config::TopicConfig;

use super::*;

impl Broker {
    /// Whether `__cluster_metadata` snapshot appends are enabled.
    pub fn cluster_metadata_topic_enabled(&self) -> bool {
        self.cluster_metadata_topic_enabled.load(Ordering::Relaxed)
    }

    /// Runtime toggle for tests / ops (`VOLANT_CLUSTER_METADATA_TOPIC`).
    pub fn set_cluster_metadata_topic_enabled(&self, enabled: bool) {
        self.cluster_metadata_topic_enabled
            .store(enabled, Ordering::Relaxed);
    }

    /// Whether new topics get a per-partition Raft log.
    pub fn partition_raft_new_topics_enabled(&self) -> bool {
        self.partition_raft_new_topics.load(Ordering::Relaxed)
    }

    /// Runtime toggle: enable partition Raft for subsequently created topics.
    pub fn set_partition_raft_new_topics(&self, enabled: bool) {
        self.partition_raft_new_topics
            .store(enabled, Ordering::Relaxed);
    }

    /// Controller: ensure `__cluster_metadata` exists (1 partition, RF=min(3,N)).
    ///
    /// No-op when the flag is off, single-node, not controller, or the topic
    /// is already in the assignment.
    pub fn ensure_cluster_metadata_topic(&self) -> Result<()> {
        if !self.cluster_metadata_topic_enabled() {
            return Ok(());
        }
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        if !cluster.membership.read().is_controller() {
            return Ok(());
        }
        if cluster
            .assignment
            .read()
            .topics
            .contains_key(CLUSTER_METADATA_TOPIC)
        {
            return Ok(());
        }

        let broker_ids = cluster.config.read().broker_ids();
        let replicas = cluster_metadata_replicas(&broker_ids);
        if replicas.is_empty() {
            return Ok(());
        }
        let id = TopicId(self.next_topic_id.fetch_add(1, Ordering::SeqCst));
        let name = TopicName::new(CLUSTER_METADATA_TOPIC);
        let leader = replicas[0];

        {
            let mut asg = cluster.assignment.write();
            if asg.topics.contains_key(CLUSTER_METADATA_TOPIC) {
                return Ok(());
            }
            asg.generation = asg.generation.saturating_add(1);
            let mut part_map = HashMap::new();
            part_map.insert(
                0,
                PartitionAssignment {
                    replicas: replicas.clone(),
                    leader,
                    isr: replicas.clone(),
                    leader_epoch: 0,
                },
            );
            asg.topics.insert(
                CLUSTER_METADATA_TOPIC.to_string(),
                TopicAssignment {
                    topic_id: id.0,
                    name: CLUSTER_METADATA_TOPIC.to_string(),
                    partitions: part_map,
                },
            );
            save_assignment(&cluster.data_dir, &asg)?;
        }

        if replicas.contains(&self.node_id) {
            let mut topics = self.topics.write();
            if !topics.contains_key(&name) {
                let replica_sets = vec![replicas.clone()];
                let topic = Topic::create_with_replicas(
                    id,
                    name.clone(),
                    1,
                    &self.storage,
                    self.node_id,
                    Some(&replica_sets),
                    &TopicConfig::default(),
                )?;
                topics.insert(name.clone(), topic);
            }
            self.rr_counters
                .write()
                .entry(name)
                .or_insert_with(|| AtomicU64::new(0));
        }

        if let Err(e) = self.append_cluster_metadata_snapshot() {
            warn!(error = %e, "cluster metadata snapshot append after ensure failed");
        }
        Ok(())
    }

    /// Append one assignment snapshot record to `__cluster_metadata-0`.
    ///
    /// Key = generation decimal; value = JSON [`AssignmentSnapshot`];
    /// header `volant-cluster-metadata=1`.
    pub fn append_cluster_metadata_snapshot(&self) -> Result<()> {
        if !self.cluster_metadata_topic_enabled() {
            return Ok(());
        }
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let snap = cluster.assignment.read().clone();
        let gen = snap.generation;
        let value = serde_json::to_vec(&snap)
            .map_err(|e| Error::Storage(format!("serialize cluster metadata snapshot: {e}")))?;
        let mut msg = Message::with_key(gen.to_string(), value);
        msg.headers.push((
            CLUSTER_METADATA_HEADER.to_string(),
            Bytes::from_static(CLUSTER_METADATA_HEADER_VALUE),
        ));
        let topic = TopicName::new(CLUSTER_METADATA_TOPIC);
        // Append locally (controller is leader of this internal topic).
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(&topic)
            .ok_or_else(|| Error::NotFound(format!("topic {CLUSTER_METADATA_TOPIC}")))?;
        let part = t
            .partitions
            .get_mut(&PartitionId(0))
            .ok_or_else(|| Error::NotFound(format!("partition {CLUSTER_METADATA_TOPIC}-0")))?;
        if self.cluster.is_some() && !part.is_leader(self.node_id) {
            return Err(Error::InvalidArgument(
                "not leader for __cluster_metadata-0".into(),
            ));
        }
        part.log.append(msg)?;
        if part.isr.len() <= 1 {
            part.catch_up_hwm();
        } else {
            part.recompute_hwm(self.node_id);
        }
        Ok(())
    }

    /// After a successful assignment.json mutation: ensure + append snapshot.
    pub(super) fn maybe_append_cluster_metadata(&self) {
        if !self.cluster_metadata_topic_enabled() {
            return;
        }
        if let Err(e) = self.ensure_cluster_metadata_topic() {
            warn!(error = %e, "ensure __cluster_metadata failed");
            return;
        }
        if let Err(e) = self.append_cluster_metadata_snapshot() {
            warn!(error = %e, "append __cluster_metadata snapshot failed");
        }
    }

    /// Last `__cluster_metadata-0` record (log LEO, not client HWM).
    ///
    /// Returns `(generation_from_key, snapshot)`.
    pub fn last_cluster_metadata_snapshot(&self) -> Option<(u32, AssignmentSnapshot)> {
        let topic = TopicName::new(CLUSTER_METADATA_TOPIC);
        let topics = self.topics.read();
        let part = topics.get(&topic)?.partitions.get(&PartitionId(0))?;
        let leo = part.leo();
        if leo == 0 {
            return None;
        }
        let recs = part.log.read(Offset::ZERO, leo as usize).ok()?;
        let last = recs.last()?;
        let gen = last
            .key
            .as_ref()
            .and_then(|k| std::str::from_utf8(k).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let snap: AssignmentSnapshot = serde_json::from_slice(&last.value).ok()?;
        Some((gen, snap))
    }

    /// Re-open any `{data_dir}/__partition_raft/{topic}/{partition}/` logs.
    ///
    /// Does not create logs for topics that never had one (new-topics-only).
    pub(super) fn reopen_existing_partition_rafts(&self) {
        let root = self.storage.data_dir.join(PARTITION_RAFT_DIR);
        let Ok(topics) = std::fs::read_dir(&root) else {
            return;
        };
        for topic_ent in topics.flatten() {
            let topic_path = topic_ent.path();
            if !topic_path.is_dir() {
                continue;
            }
            let topic = topic_ent.file_name().to_string_lossy().into_owned();
            let Ok(parts) = std::fs::read_dir(&topic_path) else {
                continue;
            };
            for part_ent in parts.flatten() {
                let part_path = part_ent.path();
                if !part_path.is_dir() {
                    continue;
                }
                let Ok(pid) = part_ent.file_name().to_string_lossy().parse::<u32>() else {
                    continue;
                };
                if part_path.join(PARTITION_RAFT_LOG_FILE).exists() {
                    self.enable_partition_raft(&topic, pid);
                }
            }
        }
    }

    /// Enable the per-partition Raft log for `(topic, partition)` (test helper).
    pub fn enable_partition_raft(&self, topic: &str, partition: u32) {
        let mut g = self.partition_rafts.lock();
        g.entry((topic.to_string(), partition))
            .or_insert_with(|| PartitionRaftState::open(&self.storage.data_dir, topic, partition));
    }

    /// Whether `(topic, partition)` has an open partition Raft log.
    pub fn partition_raft_enabled_for(&self, topic: &str, partition: u32) -> bool {
        self.partition_rafts
            .lock()
            .contains_key(&(topic.to_string(), partition))
    }

    /// Commit index of the partition Raft log (`0` if not enabled).
    pub fn partition_raft_commit_index(&self, topic: &str, partition: u32) -> u64 {
        self.partition_rafts
            .lock()
            .get(&(topic.to_string(), partition))
            .map(|s| s.commit_index())
            .unwrap_or(0)
    }

    /// Last index of the partition Raft log (`0` if not enabled).
    pub fn partition_raft_last_index(&self, topic: &str, partition: u32) -> u64 {
        self.partition_rafts
            .lock()
            .get(&(topic.to_string(), partition))
            .map(|s| s.last_index())
            .unwrap_or(0)
    }

    /// Simulate a replica ack of the latest partition Raft entry (tests).
    ///
    /// Records `match_index` for `replica_id` and tries majority commit using
    /// `replica_count` (typically ISR size).
    pub fn ack_partition_raft(
        &self,
        topic: &str,
        partition: u32,
        replica_id: u32,
        replica_count: usize,
    ) -> bool {
        let g = self.partition_rafts.lock();
        let Some(s) = g.get(&(topic.to_string(), partition)) else {
            return false;
        };
        let idx = s.last_index();
        if idx == 0 {
            return false;
        }
        s.record_match(replica_id, idx);
        s.try_commit_majority(replica_count.max(1))
    }

    /// Enable partition Raft for each new partition id in `[from, to)`.
    pub(super) fn maybe_enable_partition_raft_range(&self, topic: &str, from: u32, to: u32) {
        if !self.partition_raft_new_topics_enabled() {
            return;
        }
        if topic == CLUSTER_METADATA_TOPIC {
            return;
        }
        for pid in from..to {
            self.enable_partition_raft(topic, pid);
        }
    }

    /// Dual-write a produce into the partition Raft log (best-effort).
    ///
    /// Does **not** fail the produce path. Majority commit only when enough
    /// match indexes are recorded (ISR size 1 commits immediately).
    pub(super) fn partition_raft_on_produce(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        crc: u32,
        isr_len: usize,
        acks: u8,
    ) {
        if !self.partition_raft_enabled_for(topic, partition) {
            return;
        }
        let g = self.partition_rafts.lock();
        let Some(s) = g.get(&(topic.to_string(), partition)) else {
            return;
        };
        let entry = s.append_produce(offset, crc);
        s.record_match(self.node_id, entry.index);
        if acks == 255 || isr_len <= 1 {
            if !s.try_commit_majority(isr_len.max(1)) && isr_len > 1 {
                s.note_append_fail();
            }
        }
    }
}
