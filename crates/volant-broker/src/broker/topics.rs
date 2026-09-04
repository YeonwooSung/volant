//! Topic create/delete, partitions, offsets, and config helpers.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::Ordering;

use volant_core::{Error, Offset, PartitionId, Result, TopicId, TopicName};
use volant_protocol::{ErrorCode, REASSIGN_ALL_PARTITIONS};

use crate::cluster::{
    assign_replicas, elect_leader, save_assignment, PartitionAssignment, TopicAssignment,
};
use crate::leader_epoch::ensure_entry;
use crate::topic::Topic;
use crate::topic_catalog::{CatalogTopic, TopicCatalogFile};
use crate::topic_config::TopicConfig;

use super::*;

impl Broker {
    /// Create a topic with the given partition count.
    ///
    /// In multi-node mode only the controller may create topics.
    pub fn create_topic(&self, name: impl Into<TopicName>, partitions: u32) -> Result<TopicId> {
        self.create_topic_with_configs(name, partitions, &[])
    }

    /// Create a topic with optional config key/value pairs (Phase 13).
    pub fn create_topic_with_configs(
        &self,
        name: impl Into<TopicName>,
        partitions: u32,
        config_entries: &[(String, String)],
    ) -> Result<TopicId> {
        let name = name.into();
        if partitions == 0 {
            return Err(Error::InvalidArgument(
                "topic must have at least one partition".into(),
            ));
        }
        let topic_cfg = TopicConfig::from_entries(config_entries)?;

        if self.cluster.is_some() {
            if !self.is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    self.controller_id()
                )));
            }
            return self.create_topic_cluster(name, partitions, &topic_cfg, None);
        }

        // Single-node path.
        let mut topics = self.topics.write();
        if topics.contains_key(&name) {
            return Err(Error::InvalidArgument(format!(
                "topic already exists: {}",
                name.as_str()
            )));
        }
        let id = TopicId(self.next_topic_id.fetch_add(1, Ordering::SeqCst));
        let topic =
            Topic::create_with_config(id, name.clone(), partitions, &self.storage, &topic_cfg)?;
        topics.insert(name.clone(), topic);
        self.rr_counters
            .write()
            .insert(name.clone(), AtomicU64::new(0));
        self.topic_configs.save(name.as_str(), &topic_cfg)?;
        self.maybe_enable_partition_raft_range(name.as_str(), 0, partitions);
        // Seed epoch 0 @ start 0 for each new partition (Phase 87).
        {
            let mut epochs = self.leader_epochs.write();
            for pid in 0..partitions {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        drop(topics);
        self.persist_topic_catalog()?;
        Ok(id)
    }

    /// Create a topic with an explicit replication factor (v0.13 internal topics).
    pub(super) fn create_topic_with_replication(
        &self,
        name: TopicName,
        partitions: u32,
        rf: u32,
    ) -> Result<TopicId> {
        if partitions == 0 {
            return Err(Error::InvalidArgument(
                "topic must have at least one partition".into(),
            ));
        }
        let topic_cfg = TopicConfig::default();
        if self.cluster.is_some() {
            if !self.is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    self.controller_id()
                )));
            }
            return self.create_topic_cluster(name, partitions, &topic_cfg, Some(rf));
        }
        self.create_topic_with_configs(name, partitions, &[])
    }

    pub(super) fn create_topic_cluster(
        &self,
        name: TopicName,
        partitions: u32,
        topic_cfg: &TopicConfig,
        rf_override: Option<u32>,
    ) -> Result<TopicId> {
        let cluster = self.cluster.as_ref().expect("cluster");
        {
            let topics = self.topics.read();
            if topics.contains_key(&name) {
                return Err(Error::InvalidArgument(format!(
                    "topic already exists: {}",
                    name.as_str()
                )));
            }
            let asg = cluster.assignment.read();
            if asg.topics.contains_key(name.as_str()) {
                return Err(Error::InvalidArgument(format!(
                    "topic already exists: {}",
                    name.as_str()
                )));
            }
        }

        let cfg = cluster.config.read();
        let broker_ids = cfg.broker_ids();
        let rf = rf_override
            .unwrap_or(cfg.default_replication_factor)
            .min(broker_ids.len() as u32)
            .max(1);
        let broker_racks: Vec<(u32, Option<&str>)> = cfg
            .brokers
            .iter()
            .map(|b| (b.id, b.rack.as_deref()))
            .collect();
        let (replica_sets, rack_aware) =
            assign_replicas(name.as_str(), partitions, broker_racks.iter().copied(), rf);
        if rack_aware {
            self.rack_aware_assignment_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let id = TopicId(self.next_topic_id.fetch_add(1, Ordering::SeqCst));

        let mut part_map = HashMap::new();
        for (i, replicas) in replica_sets.iter().enumerate() {
            let leader = replicas[0];
            part_map.insert(
                i as u32,
                PartitionAssignment {
                    replicas: replicas.clone(),
                    leader,
                    isr: replicas.clone(),
                    leader_epoch: 0,
                },
            );
        }

        {
            let mut asg = cluster.assignment.write();
            asg.generation = asg.generation.saturating_add(1);
            asg.topics.insert(
                name.as_str().to_owned(),
                TopicAssignment {
                    topic_id: id.0,
                    name: name.as_str().to_owned(),
                    partitions: part_map,
                },
            );
            save_assignment(&cluster.data_dir, &asg)?;
        }

        // Open local partitions.
        {
            let mut topics = self.topics.write();
            let topic = Topic::create_with_replicas(
                id,
                name.clone(),
                partitions,
                &self.storage,
                self.node_id,
                Some(&replica_sets),
                topic_cfg,
            )?;
            topics.insert(name.clone(), topic);
        }
        {
            let mut epochs = self.leader_epochs.write();
            for pid in 0..partitions {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        self.rr_counters
            .write()
            .insert(name.clone(), AtomicU64::new(0));
        self.topic_configs.save(name.as_str(), topic_cfg)?;
        self.maybe_enable_partition_raft_range(name.as_str(), 0, partitions);
        self.maybe_append_cluster_metadata();
        Ok(id)
    }

    /// Delete a topic and remove its on-disk data directory.
    pub fn delete_topic(&self, name: &TopicName) -> Result<()> {
        let mut topics = self.topics.write();
        let removed = topics
            .remove(name)
            .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
        drop(removed);
        self.rr_counters.write().remove(name);
        if let Some(cluster) = &self.cluster {
            let mut asg = cluster.assignment.write();
            asg.topics.remove(name.as_str());
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
        }
        let dir = self.storage.data_dir.join(name.as_str());
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| {
                Error::Storage(format!("failed to remove topic dir {}: {e}", dir.display()))
            })?;
        }
        let _ = self.topic_configs.delete(name.as_str());
        drop(topics);
        if self.cluster.is_some() {
            self.maybe_append_cluster_metadata();
        }
        // Best-effort: prune stale truncate-journal watermarks for deleted topic.
        // Must not fail delete_topic (persist errors only increment metrics).
        let _ = self.truncate_journal.remove_topic(name.as_str());
        if self.cluster.is_none() {
            self.persist_topic_catalog()?;
        }
        Ok(())
    }

    /// Increase a topic's partition count to `total_count` (Phase 15).
    ///
    /// `total_count` must be strictly greater than the current count.
    /// Single-node updates the durable catalog; multi-node requires controller.
    pub fn create_partitions(&self, topic: &str, total_count: u32) -> Result<u32> {
        if total_count == 0 {
            return Err(Error::InvalidArgument(
                "total partition count must be at least 1".into(),
            ));
        }
        let name = TopicName::new(topic);

        if self.cluster.is_some() {
            if !self.is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    self.controller_id()
                )));
            }
            return self.create_partitions_cluster(name, total_count);
        }

        let topic_cfg = self.topic_configs.load(topic).unwrap_or_default();
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(&name)
            .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
        let current = t.partitions.len() as u32;
        if total_count <= current {
            return Err(Error::InvalidArgument(format!(
                "total_count {total_count} must be greater than current {current}"
            )));
        }
        t.add_partitions_from(
            current,
            total_count,
            &self.storage,
            self.node_id,
            None,
            &topic_cfg,
        )?;
        drop(topics);
        {
            let mut epochs = self.leader_epochs.write();
            for pid in current..total_count {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        self.persist_topic_catalog()?;
        self.maybe_enable_partition_raft_range(name.as_str(), current, total_count);
        Ok(total_count)
    }

    pub(super) fn create_partitions_cluster(
        &self,
        name: TopicName,
        total_count: u32,
    ) -> Result<u32> {
        let cluster = self.cluster.as_ref().expect("cluster");
        let topic_cfg = self.topic_configs.load(name.as_str()).unwrap_or_default();

        let (current, topic_id, mut all_replica_sets) = {
            let asg = cluster.assignment.read();
            let ta = asg
                .topics
                .get(name.as_str())
                .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
            let current = ta.partitions.len() as u32;
            if total_count <= current {
                return Err(Error::InvalidArgument(format!(
                    "total_count {total_count} must be greater than current {current}"
                )));
            }
            let mut sets: Vec<Vec<u32>> = Vec::with_capacity(total_count as usize);
            for i in 0..current {
                let pa = ta
                    .partitions
                    .get(&i)
                    .ok_or_else(|| Error::Storage(format!("missing partition assignment {i}")))?;
                sets.push(pa.replicas.clone());
            }
            (current, ta.topic_id, sets)
        };

        let cfg = cluster.config.read();
        let broker_ids = cfg.broker_ids();
        let rf = cfg
            .default_replication_factor
            .min(broker_ids.len() as u32)
            .max(1);
        let broker_racks: Vec<(u32, Option<&str>)> = cfg
            .brokers
            .iter()
            .map(|b| (b.id, b.rack.as_deref()))
            .collect();

        let mut new_part_map = HashMap::new();
        let mut any_rack_aware = false;
        for pid in current..total_count {
            // Distinct placement seed per partition id.
            let (sets, rack_aware) = assign_replicas(
                &format!("{}#{pid}", name.as_str()),
                1,
                broker_racks.iter().copied(),
                rf,
            );
            any_rack_aware |= rack_aware;
            let replicas = sets
                .into_iter()
                .next()
                .unwrap_or_else(|| vec![self.node_id]);
            let leader = replicas.first().copied().unwrap_or(self.node_id);
            all_replica_sets.push(replicas.clone());
            new_part_map.insert(
                pid,
                PartitionAssignment {
                    isr: replicas.clone(),
                    replicas,
                    leader,
                    leader_epoch: 0,
                },
            );
        }
        if any_rack_aware {
            self.rack_aware_assignment_total
                .fetch_add(1, Ordering::Relaxed);
        }

        {
            let mut asg = cluster.assignment.write();
            let ta = asg
                .topics
                .get_mut(name.as_str())
                .ok_or_else(|| Error::NotFound(format!("topic {}", name.as_str())))?;
            for (pid, pa) in new_part_map {
                ta.partitions.insert(pid, pa);
            }
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
        }

        {
            let mut topics = self.topics.write();
            let t = topics.entry(name.clone()).or_insert_with(|| Topic {
                id: TopicId(topic_id),
                name: name.clone(),
                partitions: HashMap::new(),
            });
            t.add_partitions_from(
                current,
                total_count,
                &self.storage,
                self.node_id,
                Some(&all_replica_sets),
                &topic_cfg,
            )?;
        }
        {
            let mut epochs = self.leader_epochs.write();
            for pid in current..total_count {
                let key = (name.as_str().to_owned(), pid);
                let e = epochs.entry(key).or_default();
                ensure_entry(e, 0, 0);
            }
        }
        self.persist_leader_epochs();
        self.maybe_enable_partition_raft_range(name.as_str(), current, total_count);
        self.maybe_append_cluster_metadata();
        Ok(total_count)
    }

    /// Reassign replicas for a topic (or one partition) (v0.18).
    ///
    /// `partition == REASSIGN_ALL_PARTITIONS` updates every partition.
    /// Empty `replicas` recomputes placement with the current effective
    /// broker list (`assign_replicas`, same as CreateTopic). New replicas
    /// start empty (LEO=0); there is no live segment copy.
    ///
    /// Returns the new assignment generation.
    pub fn reassign_partitions(
        &self,
        topic: &str,
        partition: u32,
        replicas: &[u32],
    ) -> Result<u32> {
        let cluster = self
            .cluster
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("reassign requires cluster mode".into()))?;
        if !cluster.membership.read().is_controller() {
            return Err(Error::InvalidArgument(format!(
                "not controller; controller_id={}",
                cluster.membership.read().controller_id()
            )));
        }
        {
            let asg = cluster.assignment.read();
            if !asg.topics.contains_key(topic) {
                return Err(Error::NotFound(format!("topic {topic}")));
            }
        }

        let cfg = cluster.config.read();
        let member_ids = cfg.broker_ids();
        let broker_racks: Vec<(u32, Option<&str>)> = cfg
            .brokers
            .iter()
            .map(|b| (b.id, b.rack.as_deref()))
            .collect();
        let rf = cfg
            .default_replication_factor
            .min(member_ids.len() as u32)
            .max(1);

        let explicit = normalize_replica_list(replicas)?;
        if let Some(ref ids) = explicit {
            validate_replicas_in_membership(ids, &member_ids)?;
        }

        let n_parts = {
            let asg = cluster.assignment.read();
            asg.topics
                .get(topic)
                .map(|t| t.partitions.len() as u32)
                .unwrap_or(0)
        };
        if n_parts == 0 {
            return Err(Error::NotFound(format!("topic {topic}")));
        }
        if partition != REASSIGN_ALL_PARTITIONS && partition >= n_parts {
            return Err(Error::InvalidArgument(format!(
                "partition {partition} out of range (topic {topic} has {n_parts})"
            )));
        }

        let auto_sets = if explicit.is_none() {
            let (sets, rack_aware) =
                assign_replicas(topic, n_parts, broker_racks.iter().copied(), rf);
            if rack_aware {
                self.rack_aware_assignment_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(sets)
        } else {
            None
        };
        drop(cfg);

        let mut epoch_bumps: Vec<(u32, u32, u64)> = Vec::new();
        let generation = {
            let mut asg = cluster.assignment.write();
            let ta = asg
                .topics
                .get_mut(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let pids: Vec<u32> = if partition == REASSIGN_ALL_PARTITIONS {
                let mut ids: Vec<u32> = ta.partitions.keys().copied().collect();
                ids.sort_unstable();
                ids
            } else {
                vec![partition]
            };
            for pid in pids {
                let new_replicas = if let Some(ref ids) = explicit {
                    ids.clone()
                } else {
                    auto_sets
                        .as_ref()
                        .and_then(|sets| sets.get(pid as usize).cloned())
                        .ok_or_else(|| {
                            Error::InvalidArgument(format!(
                                "auto-reassign missing placement for {topic}/{pid}"
                            ))
                        })?
                };
                let pa = ta
                    .partitions
                    .get_mut(&pid)
                    .ok_or_else(|| Error::NotFound(format!("partition {topic}/{pid}")))?;
                if let Some(bump) = apply_replica_set(pa, new_replicas) {
                    let start = self
                        .topics
                        .read()
                        .get(&TopicName::new(topic))
                        .and_then(|t| t.partitions.get(&PartitionId(pid)))
                        .map(|p| p.leo())
                        .unwrap_or(0);
                    epoch_bumps.push((pid, bump, start));
                }
            }
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
            asg.generation
        };
        for (pid, new_epoch, start) in epoch_bumps {
            self.record_epoch_start(topic, pid, new_epoch, start);
        }
        self.apply_local_assignment()?;
        self.maybe_append_cluster_metadata();
        Ok(generation)
    }

    /// Elect the preferred leader for `topic`/`partition` (v0.236).
    ///
    /// Preferred = first replica in ISR ∩ live ([`elect_leader`]). Same
    /// leader is a no-op (generation unchanged). A different live ISR replica
    /// writes the assignment and bumps generation. Unclean (outside ISR) is
    /// not performed here — the Kafka handler refuses ElectionType 1 with 87.
    ///
    /// Single-node: `Ok(0)` when this node is the local leader, else
    /// [`Error::NotFound`].
    ///
    /// Returns the live assignment generation (0 on single-node).
    pub fn elect_preferred_leader(&self, topic: &str, partition: u32) -> Result<u32> {
        let Some(cluster) = &self.cluster else {
            let topics = self.topics.read();
            let t = topics
                .get(&TopicName::new(topic))
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let part = t
                .partitions
                .get(&PartitionId(partition))
                .ok_or_else(|| Error::NotFound(format!("partition {topic}/{partition}")))?;
            if part.is_leader(self.node_id) {
                return Ok(0);
            }
            return Err(Error::NotFound(format!("partition {topic}/{partition}")));
        };
        if !cluster.membership.read().is_controller() {
            return Err(Error::InvalidArgument(format!(
                "not controller; controller_id={}",
                cluster.membership.read().controller_id()
            )));
        }
        {
            let asg = cluster.assignment.read();
            if !asg.topics.contains_key(topic) {
                return Err(Error::NotFound(format!("topic {topic}")));
            }
        }

        let live = cluster.membership.read().live_brokers();
        let (generation, new_epoch, start) = {
            let mut asg = cluster.assignment.write();
            let ta = asg
                .topics
                .get_mut(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let pa = ta
                .partitions
                .get_mut(&partition)
                .ok_or_else(|| Error::NotFound(format!("partition {topic}/{partition}")))?;
            let Some(new_leader) = elect_leader(&pa.replicas, &pa.isr, &live) else {
                return Err(Error::InvalidArgument(
                    "eligible leaders not available".into(),
                ));
            };
            if pa.leader == new_leader {
                return Ok(asg.generation);
            }
            pa.leader = new_leader;
            let new_epoch = pa.leader_epoch.saturating_add(1);
            pa.leader_epoch = new_epoch;
            if !pa.isr.contains(&new_leader) {
                pa.isr.push(new_leader);
            }
            let start = self
                .topics
                .read()
                .get(&TopicName::new(topic))
                .and_then(|t| t.partitions.get(&PartitionId(partition)))
                .map(|p| p.leo())
                .unwrap_or(0);
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
            (asg.generation, new_epoch, start)
        };
        self.record_epoch_start(topic, partition, new_epoch, start);
        self.apply_local_assignment()?;
        self.maybe_append_cluster_metadata();
        Ok(generation)
    }

    /// Expand under-replicated topics onto `new_id` after AddBroker (v0.18).
    ///
    /// For each partition, if `new_id` is not already a replica and
    /// `replicas.len() < min(default_rf, N)`, append `new_id`. Leader and ISR
    /// stay put (the new replica starts empty, not in ISR). Best-effort: no
    /// generation bump when nothing changes. Leader dispatch (v0.39) restores
    /// the pre-add assignment when joint overlay rollback runs.
    pub(super) fn auto_reassign_after_add(&self, new_id: u32) -> Result<Option<u32>> {
        let Some(cluster) = &self.cluster else {
            return Ok(None);
        };
        let cfg = cluster.config.read();
        if cfg.broker(new_id).is_none() {
            return Ok(None);
        }
        let n = cfg.brokers.len() as u32;
        let rf = cfg.default_replication_factor.max(1);
        let cap = rf.min(n);
        drop(cfg);

        let mut changed = false;
        let generation = {
            let mut asg = cluster.assignment.write();
            for ta in asg.topics.values_mut() {
                for pa in ta.partitions.values_mut() {
                    if pa.replicas.contains(&new_id) {
                        continue;
                    }
                    let unique = unique_replica_count(&pa.replicas);
                    if unique >= cap {
                        continue;
                    }
                    pa.replicas.push(new_id);
                    changed = true;
                }
            }
            if !changed {
                return Ok(None);
            }
            asg.generation = asg.generation.saturating_add(1);
            save_assignment(&cluster.data_dir, &asg)?;
            asg.generation
        };
        self.apply_local_assignment()?;
        self.maybe_append_cluster_metadata();
        Ok(Some(generation))
    }

    /// List earliest/latest offsets for topic partitions (Phase 15).
    ///
    /// Empty `partitions` means all known partitions. Returns
    /// `(partition, earliest, latest)` triples. Latest is LEO.
    pub fn list_offsets(&self, topic: &str, partitions: &[u32]) -> Result<Vec<(u32, u64, u64)>> {
        self.list_offsets_at(topic, partitions, volant_protocol::LIST_OFFSETS_LATEST)
    }

    /// List offsets for `timestamp_ms` (v0.239). Isolation is uncommitted.
    ///
    /// `-1` latest = LEO, `-2` earliest = log start, `>= 0` first record
    /// with `timestamp_ms >= T` (else LEO). Other negatives are
    /// `InvalidArgument`.
    pub fn list_offsets_at(
        &self,
        topic: &str,
        partitions: &[u32],
        timestamp_ms: i64,
    ) -> Result<Vec<(u32, u64, u64)>> {
        self.list_offsets_isolated(topic, partitions, timestamp_ms, 0)
    }

    /// List offsets with native isolation (v0.240).
    ///
    /// `isolation` 0 = READ_UNCOMMITTED (latest = LEO). `1` = READ_COMMITTED:
    /// latest (`-1`) is LSO. Earliest (`-2`) is unchanged. A `>= 0` timestamp
    /// scan is capped at LSO. Other isolation values are `InvalidArgument`.
    pub fn list_offsets_isolated(
        &self,
        topic: &str,
        partitions: &[u32],
        timestamp_ms: i64,
        isolation: u8,
    ) -> Result<Vec<(u32, u64, u64)>> {
        if timestamp_ms < volant_protocol::LIST_OFFSETS_EARLIEST {
            return Err(Error::InvalidArgument(format!(
                "invalid list offsets timestamp {timestamp_ms}"
            )));
        }
        if isolation > volant_protocol::LIST_OFFSETS_READ_COMMITTED {
            return Err(Error::InvalidArgument(format!(
                "invalid list offsets isolation {isolation}"
            )));
        }

        let name = TopicName::new(topic);
        let mut out;
        {
            let topics = self.topics.read();

            let partition_ids: Vec<u32> = if !partitions.is_empty() {
                partitions.to_vec()
            } else if let Some(cluster) = &self.cluster {
                let asg = cluster.assignment.read();
                let ta = asg
                    .topics
                    .get(topic)
                    .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
                let mut ids: Vec<u32> = ta.partitions.keys().copied().collect();
                ids.sort_unstable();
                ids
            } else {
                let t = topics
                    .get(&name)
                    .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
                let mut ids: Vec<u32> = t.partitions.keys().map(|p| p.0).collect();
                ids.sort_unstable();
                ids
            };

            // Ensure topic exists in single-node map when filters used.
            if self.cluster.is_none() && !topics.contains_key(&name) {
                return Err(Error::NotFound(format!("topic {topic}")));
            }
            if self.cluster.is_some() {
                let asg = self.cluster.as_ref().unwrap().assignment.read();
                if !asg.topics.contains_key(topic) {
                    return Err(Error::NotFound(format!("topic {topic}")));
                }
            }

            out = Vec::with_capacity(partition_ids.len());
            for pid in partition_ids {
                if let Some(t) = topics.get(&name) {
                    if let Some(part) = t.partitions.get(&PartitionId(pid)) {
                        let earliest = part.log.log_start_offset().raw();
                        let leo = part.log.log_end_offset().raw();
                        let latest = match timestamp_ms {
                            volant_protocol::LIST_OFFSETS_LATEST => leo,
                            volant_protocol::LIST_OFFSETS_EARLIEST => earliest,
                            ts => first_offset_at_or_after(&part.log, ts)?.unwrap_or(leo),
                        };
                        out.push((pid, earliest, latest));
                        continue;
                    }
                }
                // Known in assignment but no local log.
                out.push((pid, 0, 0));
            }
        }
        // LSO after dropping `topics` — `last_stable_offset` also reads it.
        if isolation == volant_protocol::LIST_OFFSETS_READ_COMMITTED {
            for (pid, _, latest) in &mut out {
                let lso = self.last_stable_offset(topic, *pid);
                match timestamp_ms {
                    volant_protocol::LIST_OFFSETS_LATEST => *latest = lso,
                    ts if ts >= 0 => *latest = (*latest).min(lso),
                    _ => {}
                }
            }
        }
        Ok(out)
    }

    /// Offset of the record with the maximum timestamp in a partition (KIP-734).
    ///
    /// Scans the local log. Empty partition → `None`. Returns
    /// `(offset, max_timestamp_ms)`.
    pub fn max_timestamp_offset(&self, topic: &str, partition: u32) -> Result<Option<(u64, i64)>> {
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let t = topics
            .get(&name)
            .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
        let part = t
            .partitions
            .get(&PartitionId(partition))
            .ok_or_else(|| Error::NotFound(format!("partition {topic}/{partition}")))?;
        let start = part.log.log_start_offset();
        let end = part.log.log_end_offset();
        if start.raw() >= end.raw() {
            return Ok(None);
        }
        // Chunked scan — max timestamp wins; ties keep the later offset.
        let mut best: Option<(u64, i64)> = None;
        let mut cursor = start;
        while cursor.raw() < end.raw() {
            let batch = part.log.read(cursor, 512)?;
            if batch.is_empty() {
                break;
            }
            for r in &batch {
                match best {
                    None => best = Some((r.offset.raw(), r.timestamp_ms)),
                    Some((_, ts)) if r.timestamp_ms >= ts => {
                        best = Some((r.offset.raw(), r.timestamp_ms));
                    }
                    _ => {}
                }
            }
            let next = batch
                .last()
                .map(|r| r.offset.raw().saturating_add(1))
                .unwrap_or(end.raw());
            if next <= cursor.raw() {
                break;
            }
            cursor = Offset::new(next);
        }
        Ok(best)
    }

    /// Delete records before `before_offset` on a partition (Phase 14).
    ///
    /// Drops whole sealed segments only. Returns `(low_watermark, error_code)`.
    /// Leader-only in cluster mode.
    ///
    /// Phase 104/111: after a successful truncate, drop aborted soft markers
    /// fully below the new log start (`end_offset <= low_watermark`) and clip
    /// straddlers (`first_offset = log_start` when the range still overlaps
    /// live offsets); persist `__txn_markers` when any change occurs.
    ///
    /// Phase 113: this method only mutates the **local** log. Cluster fan-out to
    /// other replicas is best-effort via [`crate::net::fanout_delete_records`]
    /// (native + Kafka request handlers), not here — so in-process unit tests
    /// remain single-node.
    pub fn delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> Result<(u64, u16)> {
        let name = TopicName::new(topic);
        let low = {
            let mut topics = self.topics.write();
            let t = topics
                .get_mut(&name)
                .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
            let part = t
                .partitions
                .get_mut(&PartitionId(partition))
                .ok_or_else(|| Error::NotFound(format!("partition {topic}/{partition}")))?;
            if self.cluster.is_some() && !part.is_leader(self.node_id) {
                return Ok((0, ErrorCode::NotLeaderForPartition as u16));
            }
            part.log.delete_records(Offset::new(before_offset))?.raw()
        };
        // Phase 104/111: GC / clip soft markers vs new log start.
        self.gc_and_persist_aborted_markers(topic, partition, low);
        Ok((low, 0))
    }

    /// Reload topics from the durable single-node catalog (Phase 14).
    pub(super) fn reload_single_node_topics(&self) -> Result<()> {
        if self.cluster.is_some() {
            return Ok(());
        }
        let catalog = self.topic_catalog.load()?;
        let mut topics = self.topics.write();
        for (name, meta) in &catalog.topics {
            if meta.partitions == 0 {
                continue;
            }
            let tname = TopicName::new(name.clone());
            if topics.contains_key(&tname) {
                continue;
            }
            let cfg = self.topic_configs.load(name).unwrap_or_default();
            let topic = Topic::create_with_config(
                TopicId(meta.id),
                tname.clone(),
                meta.partitions,
                &self.storage,
                &cfg,
            )?;
            topics.insert(tname.clone(), topic);
            self.rr_counters
                .write()
                .entry(tname)
                .or_insert_with(|| AtomicU64::new(0));
        }
        let next = catalog.next_id.max(1);
        let cur = self.next_topic_id.load(Ordering::SeqCst);
        if next > cur {
            self.next_topic_id.store(next, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Persist the single-node topic catalog from live topics (Phase 14).
    pub(super) fn persist_topic_catalog(&self) -> Result<()> {
        if self.cluster.is_some() {
            return Ok(());
        }
        let topics = self.topics.read();
        let mut file = TopicCatalogFile {
            next_id: self.next_topic_id.load(Ordering::SeqCst),
            topics: HashMap::new(),
        };
        for (name, t) in topics.iter() {
            file.topics.insert(
                name.as_str().to_owned(),
                CatalogTopic {
                    id: t.id.0,
                    partitions: t.partitions.len() as u32,
                },
            );
        }
        self.topic_catalog.save(&file)
    }

    /// Describe topic configs (Phase 13).
    pub fn describe_configs(&self, topic: &str) -> Result<(u32, u32, TopicConfig)> {
        let name = TopicName::new(topic);
        let (topic_id, partition_count) = {
            if let Some(cluster) = &self.cluster {
                let asg = cluster.assignment.read();
                if let Some(t) = asg.topics.get(topic) {
                    (t.topic_id, t.partitions.len() as u32)
                } else {
                    return Err(Error::NotFound(format!("topic {topic}")));
                }
            } else {
                let topics = self.topics.read();
                let t = topics
                    .get(&name)
                    .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
                (t.id.0, t.partitions.len() as u32)
            }
        };
        let cfg = self.topic_configs.load(topic)?;
        Ok((topic_id, partition_count, cfg))
    }

    /// Alter topic configs and apply to live partitions (Phase 13).
    pub fn alter_configs(&self, topic: &str, entries: &[(String, String)]) -> Result<TopicConfig> {
        let name = TopicName::new(topic);
        // Ensure topic exists.
        {
            if let Some(cluster) = &self.cluster {
                if !cluster.assignment.read().topics.contains_key(topic) {
                    return Err(Error::NotFound(format!("topic {topic}")));
                }
            } else if !self.topics.read().contains_key(&name) {
                return Err(Error::NotFound(format!("topic {topic}")));
            }
        }
        let mut cfg = self.topic_configs.load(topic)?;
        cfg.apply_entries(entries)?;
        self.topic_configs.save(topic, &cfg)?;
        {
            let mut topics = self.topics.write();
            if let Some(t) = topics.get_mut(&name) {
                t.apply_topic_config(&cfg);
            }
        }
        Ok(cfg)
    }

    /// Run retention on all local partition logs (Phase 13 background task).
    ///
    /// Phase 104/111: after retention advances log starts, drop/clip aborted
    /// soft markers vs each partition's new log start and persist when needed.
    pub fn apply_retention_all(&self) -> Result<()> {
        {
            let mut topics = self.topics.write();
            for t in topics.values_mut() {
                t.apply_retention_all()?;
            }
        }
        // Phase 104: same GC rule as DeleteRecords (end_offset <= log_start).
        let _ = self.gc_stale_aborted_markers_all();
        Ok(())
    }

    /// Force key-compaction on topics with `cleanup.policy=compact` (Phase 16).
    pub fn compact_all(&self) -> Result<()> {
        let mut topics = self.topics.write();
        for t in topics.values_mut() {
            t.compact_all()?;
        }
        Ok(())
    }

    /// Broker `data_dir` advertised as the single local log dir (v0.235).
    pub(crate) fn local_log_dir_path(&self) -> String {
        self.storage.data_dir.display().to_string()
    }

    /// Local open-partition log rows for DescribeLogDirs (not remote replica dirs).
    pub(crate) fn local_log_dir_rows(&self, filter: &LocalLogDirFilter) -> Vec<LocalLogDirTopic> {
        let topics = self.topics.read();
        match filter {
            LocalLogDirFilter::All => {
                let mut names: Vec<_> = topics.keys().cloned().collect();
                names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                names
                    .into_iter()
                    .filter_map(|n| topics.get(&n).map(topic_log_dir_rows_all))
                    .collect()
            }
            LocalLogDirFilter::Topics(req) => req
                .iter()
                .map(
                    |(name, parts)| match topics.get(&TopicName::new(name.as_str())) {
                        Some(t) => {
                            let only = if parts.is_empty() {
                                None
                            } else {
                                Some(parts.as_slice())
                            };
                            topic_log_dir_rows(t, only)
                        }
                        None => LocalLogDirTopic {
                            name: name.clone(),
                            partitions: Vec::new(),
                        },
                    },
                )
                .collect(),
        }
    }

    /// Number of partitions for a topic.
    pub fn partition_count(&self, topic: &TopicName) -> Result<u32> {
        // Prefer assignment in cluster mode (may not have all partitions local).
        if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            if let Some(t) = asg.topics.get(topic.as_str()) {
                return Ok(t.partitions.len() as u32);
            }
        }
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        Ok(t.partitions.len() as u32)
    }

    /// Select a partition for produce when the client sends `partition = -1`.
    pub fn select_partition(&self, topic: &TopicName, key: Option<&[u8]>) -> Result<PartitionId> {
        let n = self.partition_count(topic)?;
        if n == 0 {
            return Err(Error::InvalidArgument("topic has zero partitions".into()));
        }
        let idx = match key {
            Some(k) => {
                let h = murmur2(k);
                let positive = h & 0x7fff_ffff;
                positive % n
            }
            None => {
                {
                    let mut counters = self.rr_counters.write();
                    counters
                        .entry(topic.clone())
                        .or_insert_with(|| AtomicU64::new(0));
                }
                let counters = self.rr_counters.read();
                let counter = counters.get(topic).expect("rr counter inserted above");
                let seq = counter.fetch_add(1, Ordering::Relaxed);
                (seq % u64::from(n)) as u32
            }
        };
        Ok(PartitionId(idx))
    }
}

/// Filter for [`Broker::local_log_dir_rows`] (Kafka DescribeLogDirs).
#[derive(Debug, Clone)]
pub(crate) enum LocalLogDirFilter {
    /// Every partition this process has open.
    All,
    /// Named topics. Empty `partitions` = all local partitions of that topic.
    Topics(Vec<(String, Vec<i32>)>),
}

/// One topic's local log-dir rows.
#[derive(Debug, Clone)]
pub(crate) struct LocalLogDirTopic {
    pub name: String,
    pub partitions: Vec<LocalLogDirPartition>,
}

/// First record offset with `timestamp_ms >= T`. None if the log is empty
/// or every record is earlier than `T` (caller uses LEO).
fn first_offset_at_or_after(
    log: &volant_storage::PartitionLog,
    timestamp_ms: i64,
) -> Result<Option<u64>> {
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    if start.raw() >= end.raw() {
        return Ok(None);
    }
    let mut cursor = start;
    while cursor.raw() < end.raw() {
        let batch = log.read(cursor, 512)?;
        if batch.is_empty() {
            break;
        }
        for r in &batch {
            if r.timestamp_ms >= timestamp_ms {
                return Ok(Some(r.offset.raw()));
            }
        }
        let next = batch
            .last()
            .map(|r| r.offset.raw().saturating_add(1))
            .unwrap_or(end.raw());
        if next <= cursor.raw() {
            break;
        }
        cursor = Offset::new(next);
    }
    Ok(None)
}

/// One partition row in a DescribeLogDirs response.
#[derive(Debug, Clone)]
pub(crate) struct LocalLogDirPartition {
    pub partition: i32,
    pub size: i64,
    pub offset_lag: i64,
    pub is_future: bool,
}

fn topic_log_dir_rows_all(t: &Topic) -> LocalLogDirTopic {
    topic_log_dir_rows(t, None)
}

fn topic_log_dir_rows(t: &Topic, only: Option<&[i32]>) -> LocalLogDirTopic {
    let mut pids: Vec<u32> = match only {
        Some(only) => only
            .iter()
            .filter(|&&p| p >= 0)
            .map(|&p| p as u32)
            .collect(),
        None => t.partitions.keys().map(|p| p.0).collect(),
    };
    pids.sort_unstable();
    pids.dedup();
    let mut partitions = Vec::with_capacity(pids.len());
    for pid in pids {
        let Some(part) = t.partitions.get(&PartitionId(pid)) else {
            continue;
        };
        let leo = part.leo();
        let hwm = part.committed_hwm;
        partitions.push(LocalLogDirPartition {
            partition: pid as i32,
            size: part.log.total_size() as i64,
            offset_lag: leo.saturating_sub(hwm) as i64,
            is_future: false,
        });
    }
    LocalLogDirTopic {
        name: t.name.as_str().to_owned(),
        partitions,
    }
}

/// Dedup preserving order. `None` means auto (input was empty).
fn normalize_replica_list(replicas: &[u32]) -> Result<Option<Vec<u32>>> {
    if replicas.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(replicas.len());
    for &id in replicas {
        if !out.contains(&id) {
            out.push(id);
        }
    }
    if out.is_empty() {
        return Err(Error::InvalidArgument(
            "replica set must not be empty".into(),
        ));
    }
    Ok(Some(out))
}

fn validate_replicas_in_membership(replicas: &[u32], member_ids: &[u32]) -> Result<()> {
    for &id in replicas {
        if !member_ids.contains(&id) {
            return Err(Error::InvalidArgument(format!(
                "replica id {id} is not in membership"
            )));
        }
    }
    Ok(())
}

fn unique_replica_count(replicas: &[u32]) -> u32 {
    let mut seen = Vec::with_capacity(replicas.len());
    for &id in replicas {
        if !seen.contains(&id) {
            seen.push(id);
        }
    }
    seen.len() as u32
}

/// Apply a new replica set. Leader stays if still in the set; otherwise first
/// replica. ISR is the intersection of the old ISR with the new set (leader
/// always first). New replicas are **not** added to ISR (they start empty).
///
/// Returns `Some(new_epoch)` when the leader changed.
fn apply_replica_set(pa: &mut PartitionAssignment, new_replicas: Vec<u32>) -> Option<u32> {
    let old_leader = pa.leader;
    let new_leader = if new_replicas.contains(&old_leader) {
        old_leader
    } else {
        new_replicas[0]
    };
    let mut new_isr = Vec::with_capacity(pa.isr.len().max(1));
    new_isr.push(new_leader);
    for &id in &pa.isr {
        if id != new_leader && new_replicas.contains(&id) && !new_isr.contains(&id) {
            new_isr.push(id);
        }
    }
    let epoch = if new_leader != old_leader {
        let e = pa.leader_epoch.saturating_add(1);
        pa.leader_epoch = e;
        Some(e)
    } else {
        None
    };
    pa.replicas = new_replicas;
    pa.leader = new_leader;
    pa.isr = new_isr;
    epoch
}
