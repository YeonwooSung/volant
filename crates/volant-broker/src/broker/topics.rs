//! Topic create/delete, partitions, offsets, and config helpers.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::Ordering;

use volant_core::{Error, Offset, PartitionId, Result, TopicId, TopicName};
use volant_protocol::ErrorCode;

use crate::cluster::{assign_replicas, save_assignment, PartitionAssignment, TopicAssignment};
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

        if let Some(cluster) = &self.cluster {
            if !cluster.membership.read().is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    cluster.membership.read().controller_id()
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
        if let Some(cluster) = &self.cluster {
            if !cluster.membership.read().is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    cluster.membership.read().controller_id()
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

        if let Some(cluster) = &self.cluster {
            if !cluster.membership.read().is_controller() {
                return Err(Error::InvalidArgument(format!(
                    "not controller; controller_id={}",
                    cluster.membership.read().controller_id()
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

    /// List earliest/latest offsets for topic partitions (Phase 15).
    ///
    /// Empty `partitions` means all known partitions. Returns
    /// `(partition, earliest, latest)` triples.
    pub fn list_offsets(&self, topic: &str, partitions: &[u32]) -> Result<Vec<(u32, u64, u64)>> {
        let name = TopicName::new(topic);
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

        let mut out = Vec::with_capacity(partition_ids.len());
        for pid in partition_ids {
            if let Some(t) = topics.get(&name) {
                if let Some(part) = t.partitions.get(&PartitionId(pid)) {
                    let earliest = part.log.log_start_offset().raw();
                    let latest = part.log.log_end_offset().raw();
                    out.push((pid, earliest, latest));
                    continue;
                }
            }
            // Known in assignment but no local log.
            out.push((pid, 0, 0));
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
