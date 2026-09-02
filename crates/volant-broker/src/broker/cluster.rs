//! Metadata snapshot, replica fetch, membership, and ISR helpers.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use volant_core::{Error, Offset, PartitionId, Record, Result, TopicId, TopicName};
use volant_protocol::{ClusterTopicState, ErrorCode, FetchRecord};

use crate::cluster::{
    elect_leader, reconcile_isr, save_assignment, shrink_isr, shrink_isr_by_time,
    AssignmentSnapshot, CLUSTER_METADATA_TOPIC,
};
use crate::leader_epoch::{self, end_offset_for, ensure_entry};
use crate::topic::Topic;

use super::*;

impl Broker {
    /// Build a metadata snapshot.
    pub fn metadata(&self, topics: Option<&[TopicName]>) -> MetadataSnapshot {
        let host = self.advertised_host.read().clone();
        let port = self.advertised_port.load(Ordering::Relaxed) as u16;

        let brokers = if let Some(cluster) = &self.cluster {
            cluster
                .config
                .read()
                .brokers
                .iter()
                .map(|b| {
                    // Prefer live advertised for self.
                    if b.id == self.node_id {
                        (b.id, host.clone(), port)
                    } else {
                        (b.id, b.host.clone(), b.port)
                    }
                })
                .collect()
        } else {
            vec![(self.node_id, host.clone(), port)]
        };

        let controller_id = self.controller_id();

        // Build topic list from assignment (cluster) or local topics.
        // Phase 152: when consensus is enabled and committed-only is on, serve
        // majority-committed snapshot. Bootstrap (live gen == 0) still uses live
        // empty assignment; once live advances past committed, hide uncommitted
        // topics (do not fall back to live). committed_only=false → always live.
        let topic_meta = if let Some(cluster) = &self.cluster {
            let use_committed =
                self.assignment_consensus_enabled() && self.assignment_metadata_committed_only();
            let live_guard = cluster.assignment.read();
            let committed_gen = self.assignment_consensus.committed_generation();
            let committed_owned = if use_committed {
                if committed_gen > 0 {
                    // Prefer durable snap; if missing, empty (never fall back to
                    // a live assignment that may lead committed_gen).
                    Some(
                        self.assignment_consensus
                            .committed_snapshot()
                            .unwrap_or_default(),
                    )
                } else if live_guard.generation == 0 {
                    // True bootstrap: empty cluster, live == committed == 0.
                    None // fall through to live (also empty)
                } else {
                    // Live has uncommitted work; serve empty until first majority.
                    Some(AssignmentSnapshot::default())
                }
            } else {
                None
            };
            let asg: &AssignmentSnapshot = match committed_owned.as_ref() {
                Some(snap) => snap,
                None => &*live_guard,
            };
            let local = self.topics.read();
            let names: Vec<String> = match topics {
                None | Some([]) => asg.topics.keys().cloned().collect(),
                Some(list) => list.iter().map(|t| t.as_str().to_owned()).collect(),
            };
            let mut out = Vec::new();
            for name in names {
                if let Some(t) = asg.topics.get(&name) {
                    let mut partitions: Vec<PartitionMetadata> = t
                        .partitions
                        .iter()
                        .map(|(pid, p)| {
                            let local_part = local
                                .get(&TopicName::new(&name))
                                .and_then(|lt| lt.partitions.get(&PartitionId(*pid)));
                            let hwm = local_part.map(|lp| lp.committed_hwm).unwrap_or(0);
                            // Phase 142: when this node is the partition leader,
                            // prefer live local ISR / epoch over assignment lag.
                            let is_local_leader = p.leader == self.node_id
                                || local_part
                                    .map(|lp| lp.is_leader(self.node_id))
                                    .unwrap_or(false);
                            let (isr, leader_epoch) = if is_local_leader {
                                if let Some(lp) = local_part {
                                    (lp.isr.clone(), lp.leader_epoch)
                                } else {
                                    (p.isr.clone(), p.leader_epoch)
                                }
                            } else {
                                (p.isr.clone(), p.leader_epoch)
                            };
                            PartitionMetadata {
                                partition_id: PartitionId(*pid),
                                leader: p.leader,
                                hwm,
                                replicas: p.replicas.clone(),
                                isr,
                                leader_epoch,
                            }
                        })
                        .collect();
                    partitions.sort_by_key(|p| p.partition_id.0);
                    out.push(TopicMetadata {
                        name: TopicName::new(name),
                        topic_id: TopicId(t.topic_id),
                        partitions,
                    });
                }
            }
            out.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
            out
        } else {
            let map = self.topics.read();
            let names: Vec<TopicName> = match topics {
                None | Some([]) => map.keys().cloned().collect(),
                Some(list) => list.to_vec(),
            };
            let mut topic_meta = Vec::with_capacity(names.len());
            for name in names {
                if let Some(t) = map.get(&name) {
                    let mut partitions: Vec<PartitionMetadata> = t
                        .partitions
                        .iter()
                        .map(|(pid, p)| PartitionMetadata {
                            partition_id: *pid,
                            leader: p.leader,
                            hwm: p.committed_hwm.max(p.leo()), // single-node: LEO
                            replicas: p.replicas.clone(),
                            isr: p.isr.clone(),
                            leader_epoch: p.leader_epoch,
                        })
                        .collect();
                    partitions.sort_by_key(|p| p.partition_id.0);
                    topic_meta.push(TopicMetadata {
                        name,
                        topic_id: t.id,
                        partitions,
                    });
                }
            }
            topic_meta.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
            topic_meta
        };

        MetadataSnapshot {
            node_id: self.node_id,
            host,
            port,
            brokers,
            topics: topic_meta,
            controller_id,
        }
    }

    /// Access the group coordinator.
    pub fn groups(&self) -> &GroupCoordinator {
        &self.groups
    }

    /// Partition count lookup for group assignment (`None` if topic missing).
    pub fn partition_count_opt(&self, topic: &str) -> Option<u32> {
        let name = TopicName::new(topic);
        self.partition_count(&name).ok()
    }

    // ── Cluster protocol handlers ──────────────────────────────────────

    /// Handle ReplicaFetch from a follower (must be leader).
    pub fn handle_replica_fetch(
        &self,
        topic: &str,
        partition: u32,
        from_offset: u64,
        max_bytes: u32,
        replica_id: u32,
    ) -> Result<(u16, u64, u32, Vec<FetchRecord>)> {
        let topic_name = TopicName::new(topic);
        let pid = PartitionId(partition);

        let (error, hwm, epoch, records) = {
            let mut topics = self.topics.write();
            let t = match topics.get_mut(&topic_name) {
                Some(t) => t,
                None => {
                    return Ok((ErrorCode::NotFound as u16, 0, 0, vec![]));
                }
            };
            let part = match t.partitions.get_mut(&pid) {
                Some(p) => p,
                None => return Ok((ErrorCode::NotFound as u16, 0, 0, vec![])),
            };
            if !part.is_leader(self.node_id) {
                return Ok((
                    ErrorCode::NotLeaderForPartition as u16,
                    part.committed_hwm,
                    part.leader_epoch,
                    vec![],
                ));
            }

            // Update follower LEO (they request from their current LEO).
            part.follower_leo.insert(replica_id, from_offset);

            // Phase 118/125: offset + time lag shrink + catch-up rejoin.
            if let Some(cluster) = &self.cluster {
                let max_lag = cluster.config.read().replica_lag_max_messages;
                let max_lag_ms = self.effective_replica_lag_max_ms();
                let leader_leo = part.leo();
                // Stamp last-caught-up when lag is within the message threshold.
                if leader_leo.saturating_sub(from_offset) <= max_lag {
                    part.follower_caught_up_at
                        .insert(replica_id, Instant::now());
                }
                let committed_hwm = part.committed_hwm;
                let leo_map = part.follower_leo.clone();
                let caught_map = part.follower_caught_up_at.clone();
                let replicas = part.replicas.clone();
                let old_isr = part.isr.clone();
                let now = Instant::now();
                let (isr, time_n) = reconcile_isr(
                    part.leader,
                    &old_isr,
                    &replicas,
                    leader_leo,
                    committed_hwm,
                    max_lag,
                    max_lag_ms,
                    now,
                    Some((replica_id, from_offset)),
                    |id| {
                        if id == part.leader {
                            leader_leo
                        } else {
                            *leo_map.get(&id).unwrap_or(&0)
                        }
                    },
                    |id| caught_map.get(&id).copied(),
                );
                if isr != old_isr {
                    self.note_isr_delta(&old_isr, &isr);
                    // Fresh stamp for any newly expanded members.
                    for &id in &isr {
                        if !old_isr.contains(&id) && id != part.leader {
                            part.follower_caught_up_at.insert(id, Instant::now());
                        }
                    }
                    // Drop stamps for members that left.
                    part.follower_caught_up_at
                        .retain(|id, _| isr.contains(id) || *id == part.leader);
                }
                self.note_isr_time_shrink(time_n);
                part.isr = isr;
                part.recompute_hwm(self.node_id);

                // Persist ISR into assignment. Controller bumps generation so
                // peers pull; non-controller leaders report to controller (Phase 142).
                let isr_changed = {
                    let isr_now = part.isr.clone();
                    let mut asg = cluster.assignment.write();
                    let mut changed = false;
                    if let Some(ta) = asg.topics.get_mut(topic) {
                        if let Some(pa) = ta.partitions.get_mut(&partition) {
                            if pa.isr != isr_now {
                                pa.isr = isr_now;
                                changed = true;
                                if self.is_controller() {
                                    asg.generation = asg.generation.saturating_add(1);
                                    let _ = save_assignment(&cluster.data_dir, &asg);
                                }
                            }
                        }
                    }
                    changed
                };
                if isr_changed && !self.is_controller() {
                    self.enqueue_isr_report(PendingIsrReport {
                        topic: topic.to_owned(),
                        partition,
                        leader_id: part.leader,
                        leader_epoch: part.leader_epoch,
                        isr: part.isr.clone(),
                        generation_hint: cluster.assignment.read().generation,
                    });
                }
            } else {
                part.catch_up_hwm();
            }

            let hwm = part.committed_hwm;
            let epoch = part.leader_epoch;
            let max_msgs = 10_000usize;
            let recs =
                part.log
                    .read_bytes(Offset::new(from_offset), max_msgs, max_bytes as usize)?;
            let wire: Vec<FetchRecord> = recs
                .into_iter()
                .map(|r| FetchRecord {
                    offset: r.offset.raw(),
                    timestamp_ms: r.timestamp_ms,
                    key: r.key,
                    value: r.value,
                    headers: r.headers,
                })
                .collect();
            (0u16, hwm, epoch, wire)
        };

        // Wake acks=all waiters.
        self.hwm_cvar.notify_all();
        Ok((error, hwm, epoch, records))
    }

    /// Append records fetched from the leader onto a follower log.
    pub fn append_replica_records(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        records: &[Record],
        leader_epoch: u32,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        if part.is_leader(self.node_id) {
            return Ok(()); // shouldn't happen
        }
        part.leader_epoch = leader_epoch;
        part.log.append_records_with_offsets(records)?;
        // Follower does not advance committed_hwm past what leader reported separately;
        // client fetch on follower is rare — typically clients go to leader.
        // Still advance committed_hwm conservatively to local LEO only when sole replica.
        if part.isr.len() <= 1 {
            part.catch_up_hwm();
        }
        Ok(())
    }

    /// Handle HeartbeatBroker (controller path).
    pub fn handle_heartbeat_broker(
        &self,
        broker_id: u32,
        _controller_id_known: u32,
        _generation: u32,
    ) -> (u16, u32, u32, Vec<u32>) {
        let Some(cluster) = &self.cluster else {
            return (0, self.node_id, 0, vec![self.node_id]);
        };
        {
            let mut m = cluster.membership.write();
            m.heartbeat(broker_id);
            m.touch_self();
        }
        // Expire dead brokers and handle failover if we are controller.
        let dead = cluster.membership.write().expire();
        if !dead.is_empty() && cluster.membership.read().is_controller() {
            for d in dead {
                let _ = self.on_broker_death(d);
            }
        }
        let m = cluster.membership.read();
        let controller_id = m.controller_id();
        let alive = m.live_brokers();
        let generation = cluster.assignment.read().generation;
        // Only the true controller should accept; others still respond with redirect info.
        (0, controller_id, generation, alive)
    }

    /// Build ClusterState response snapshot.
    pub fn cluster_state_snapshot(&self) -> (u16, u32, u32, Vec<ClusterTopicState>) {
        let Some(cluster) = &self.cluster else {
            return (0, 0, self.node_id, vec![]);
        };
        let asg = cluster.assignment.read();
        let controller_id = cluster.membership.read().controller_id();
        (0, asg.generation, controller_id, asg.to_wire_topics())
    }

    /// Apply a ClusterState snapshot from the controller.
    ///
    /// Phase 137: topics removed from the assignment are pruned from the
    /// local truncate journal so peers that never ran `delete_topic` drop
    /// watermarks (anti-linger / peer prune).
    pub fn apply_cluster_state(
        &self,
        generation: u32,
        controller_id: u32,
        topics: &[ClusterTopicState],
    ) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let removed: Vec<String> = {
            let mut asg = cluster.assignment.write();
            if generation < asg.generation {
                return Ok(()); // stale
            }
            let old: HashSet<String> = asg.topics.keys().cloned().collect();
            asg.apply_wire(generation, topics);
            save_assignment(&cluster.data_dir, &asg)?;
            let new: HashSet<_> = asg.topics.keys().cloned().collect();
            old.into_iter().filter(|t| !new.contains(t)).collect()
        };
        // Best-effort journal prune outside the assignment write lock.
        for t in &removed {
            let _ = self.truncate_journal.remove_topic(t);
        }
        let _ = controller_id;
        self.apply_local_assignment()?;
        if self.partition_raft_new_topics_enabled() {
            for t in topics {
                if t.name == CLUSTER_METADATA_TOPIC {
                    continue;
                }
                for p in &t.partitions {
                    if p.replicas.contains(&self.node_id) {
                        self.enable_partition_raft(&t.name, p.partition_id);
                    }
                }
            }
        }
        Ok(())
    }

    /// Open/update local partitions from current assignment.
    ///
    /// After overlaying leader/ISR from the assignment, recomputes HWM on
    /// partitions this node leads so ISR shrink (follower death) unblocks
    /// `acks=all` waiters when ClusterState is applied (Phase 108).
    ///
    /// Phase 118: when we lead, preserve previous local ISR members that are
    /// still in-sync (live, lag ≤ max, LEO ≥ HWM) so a controller assignment
    /// that still lists a death-shrunk set does not undo a leader-local rejoin.
    pub(super) fn apply_local_assignment(&self) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let asg = cluster.assignment.read().clone();
        let max_lag = cluster.config.read().replica_lag_max_messages;
        let max_lag_ms = self.effective_replica_lag_max_ms();
        let live: HashSet<u32> = cluster
            .membership
            .read()
            .live_brokers()
            .into_iter()
            .collect();
        let mut topics = self.topics.write();
        let mut hwm_changed = false;
        for (name, ta) in &asg.topics {
            let tname = TopicName::new(name.clone());
            let topic = topics.entry(tname.clone()).or_insert_with(|| Topic {
                id: TopicId(ta.topic_id),
                name: tname.clone(),
                partitions: HashMap::new(),
            });
            topic.id = TopicId(ta.topic_id);
            for (pid, pa) in &ta.partitions {
                // Snapshot leader-local ISR / LEO / caught-up before assignment overwrite.
                let prev = topic.partitions.get(&PartitionId(*pid)).map(|p| {
                    (
                        p.isr.clone(),
                        p.follower_leo.clone(),
                        p.follower_caught_up_at.clone(),
                        p.committed_hwm,
                    )
                });
                topic.ensure_partition(
                    PartitionId(*pid),
                    &self.storage,
                    self.node_id,
                    pa.leader,
                    pa.replicas.clone(),
                    pa.isr.clone(),
                    pa.leader_epoch,
                )?;
                if let Some(part) = topic.partitions.get_mut(&PartitionId(*pid)) {
                    if part.is_leader(self.node_id) {
                        let before = part.committed_hwm;
                        if let Some((prev_isr, prev_leo, prev_caught, prev_hwm)) = prev {
                            // Restore LEO / caught-up observations for candidates we may keep.
                            for (id, leo) in &prev_leo {
                                part.follower_leo.entry(*id).or_insert(*leo);
                            }
                            for (id, at) in &prev_caught {
                                part.follower_caught_up_at.entry(*id).or_insert(*at);
                            }
                            let leader_leo = part.leo();
                            let hwm = part.committed_hwm.max(prev_hwm);
                            let mut isr = part.isr.clone();
                            for &id in &prev_isr {
                                if isr.contains(&id) || id == part.leader {
                                    continue;
                                }
                                if !part.replicas.contains(&id) || !live.contains(&id) {
                                    continue;
                                }
                                let leo = *part.follower_leo.get(&id).unwrap_or(&0);
                                let lag = leader_leo.saturating_sub(leo);
                                if lag <= max_lag && leo >= hwm {
                                    isr.push(id);
                                }
                            }
                            let leo_map = part.follower_leo.clone();
                            let caught_map = part.follower_caught_up_at.clone();
                            let after_offset =
                                shrink_isr(part.leader, &isr, leader_leo, max_lag, |id| {
                                    if id == part.leader {
                                        leader_leo
                                    } else {
                                        *leo_map.get(&id).unwrap_or(&0)
                                    }
                                });
                            let now = Instant::now();
                            let reconciled = shrink_isr_by_time(
                                part.leader,
                                &after_offset,
                                max_lag_ms,
                                now,
                                |id| caught_map.get(&id).copied(),
                            );
                            let mut time_n = 0u64;
                            for &id in &after_offset {
                                if id != part.leader && !reconciled.contains(&id) {
                                    time_n += 1;
                                }
                            }
                            if reconciled != part.isr {
                                self.note_isr_delta(&part.isr, &reconciled);
                                part.isr = reconciled;
                            }
                            self.note_isr_time_shrink(time_n);
                        }
                        // Drop LEO / caught-up entries for brokers no longer in ISR.
                        part.follower_leo.retain(|id, _| part.isr.contains(id));
                        part.follower_caught_up_at
                            .retain(|id, _| part.isr.contains(id));
                        if part.isr.len() <= 1 {
                            part.catch_up_hwm();
                        } else {
                            part.recompute_hwm(self.node_id);
                        }
                        if part.committed_hwm != before {
                            hwm_changed = true;
                        }
                    }
                }
            }
            // Overlay durable topic config onto local partition logs.
            if let Ok(cfg) = self.topic_configs.load(name) {
                topic.apply_topic_config(&cfg);
            }
            self.rr_counters
                .write()
                .entry(tname)
                .or_insert_with(|| AtomicU64::new(0));
        }
        // Bump next_topic_id.
        let max_id = asg.topics.values().map(|t| t.topic_id).max().unwrap_or(0);
        let cur = self.next_topic_id.load(Ordering::SeqCst);
        if max_id + 1 > cur {
            self.next_topic_id.store(max_id + 1, Ordering::SeqCst);
        }
        drop(topics);
        if hwm_changed {
            self.hwm_cvar.notify_all();
        }
        Ok(())
    }

    /// Clone the live cluster assignment (`None` when not clustered).
    pub fn clone_live_assignment(&self) -> Option<AssignmentSnapshot> {
        self.cluster.as_ref().map(|c| c.assignment.read().clone())
    }

    /// Restore a previously cloned live assignment after a wait-path majority miss.
    ///
    /// Writes `prev` into `cluster.assignment` and `{data_dir}/cluster/assignment.json`
    /// only when live generation still equals `expected_gen` (the generation this
    /// request wrote). If another admin already advanced generation, skip rewind.
    /// Drops local topics / extra partitions (and their on-disk dirs) not in `prev`,
    /// then `apply_local_assignment` so a rolled-back delete can reopen logs.
    /// Does not touch `committed_snapshot.json`.
    pub fn restore_live_assignment(
        &self,
        prev: &AssignmentSnapshot,
        expected_gen: u32,
    ) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        {
            let mut asg = cluster.assignment.write();
            if asg.generation != expected_gen {
                tracing::warn!(
                    live_gen = asg.generation,
                    expected_gen,
                    "skip assignment restore; live generation advanced"
                );
                return Ok(());
            }
            let live = asg.clone();
            *asg = prev.clone();
            if let Err(e) = save_assignment(&cluster.data_dir, &asg) {
                *asg = live;
                return Err(e);
            }
        }
        self.maybe_append_cluster_metadata();
        let keep: HashSet<String> = prev.topics.keys().cloned().collect();
        let mut drop_dirs: Vec<std::path::PathBuf> = Vec::new();
        {
            let mut topics = self.topics.write();
            for name in topics.keys() {
                if !keep.contains(name.as_str()) {
                    drop_dirs.push(self.storage.data_dir.join(name.as_str()));
                }
            }
            topics.retain(|name, _| keep.contains(name.as_str()));
            for (name, topic) in topics.iter_mut() {
                let Some(ta) = prev.topics.get(name.as_str()) else {
                    continue;
                };
                let keep_pids: HashSet<u32> = ta.partitions.keys().copied().collect();
                for pid in topic.partitions.keys() {
                    if !keep_pids.contains(&pid.0) {
                        drop_dirs.push(
                            self.storage
                                .data_dir
                                .join(name.as_str())
                                .join(format!("{}", pid.0)),
                        );
                    }
                }
                topic.partitions.retain(|pid, _| keep_pids.contains(&pid.0));
            }
        }
        self.rr_counters
            .write()
            .retain(|name, _| keep.contains(name.as_str()));
        for dir in drop_dirs {
            if dir.exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        self.apply_local_assignment()
    }

    /// Remove `dead_id` from every local partition ISR and advance HWM when we lead.
    ///
    /// Called from [`Self::on_broker_death`] on **every** node that observes the death
    /// (not only the controller). Without this, `acks=all` waits forever for a dead
    /// follower's LEO because HWM = min(ISR LEOs) still includes the stale member
    /// (Phase 108). Phase 118 also increments `isr_shrink_total` per removal.
    pub(super) fn shrink_local_isr_for_dead(&self, dead_id: u32) {
        let mut topics = self.topics.write();
        let mut any = false;
        let mut shrink_n = 0u64;
        for t in topics.values_mut() {
            for part in t.partitions.values_mut() {
                let before = part.isr.len();
                part.isr.retain(|id| *id != dead_id);
                part.follower_leo.remove(&dead_id);
                part.follower_caught_up_at.remove(&dead_id);
                if part.isr.len() == before {
                    continue;
                }
                shrink_n += 1;
                any = true;
                if part.is_leader(self.node_id) {
                    if part.isr.len() <= 1 {
                        part.catch_up_hwm();
                    } else {
                        part.recompute_hwm(self.node_id);
                    }
                }
            }
        }
        drop(topics);
        if shrink_n > 0 {
            self.isr_shrink_total.fetch_add(shrink_n, Ordering::Relaxed);
        }
        if any {
            self.hwm_cvar.notify_all();
        }
    }

    /// Handle a dead broker: shrink ISR, elect new leaders from remaining ISR.
    ///
    /// Every observer removes the dead broker from **local** partition ISR and
    /// recomputes HWM (unblocks `acks=all`). The controller additionally updates
    /// the durable assignment (including pure ISR shrink — generation bump) so
    /// peers learn via ClusterState pull. Non-controller leaders also enqueue
    /// Phase 142 IsrUpdate reports so controller assignment converges.
    pub fn on_broker_death(&self, dead_id: u32) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        // Mark dead first so controller_id recomputes (lowest remaining live id).
        cluster.membership.write().mark_dead(dead_id);
        // Local ISR shrink on every observer (leader may not be controller).
        self.shrink_local_isr_for_dead(dead_id);
        if !cluster.membership.read().is_controller() {
            // Phase 142: report leader-local ISR shrink to controller (best-effort).
            self.enqueue_isr_reports_for_local_leaders();
            return Ok(());
        }
        let live = cluster.membership.read().live_brokers();

        // Collect epoch transitions so we can record history with local LEO.
        let mut epoch_bumps: Vec<(String, u32, u32, u64)> = Vec::new();
        let mut changed = false;
        {
            let mut asg = cluster.assignment.write();
            for ta in asg.topics.values_mut() {
                for (pid, pa) in ta.partitions.iter_mut() {
                    // Shrink ISR; restore previous if no live member remains.
                    let isr_before = pa.isr.clone();
                    pa.isr.retain(|id| live.contains(id));
                    if pa.isr.is_empty() {
                        // No live ISR — keep last known, hope for recovery.
                        pa.isr = isr_before;
                        continue;
                    }
                    if pa.isr.len() != isr_before.len() {
                        // Pure follower death must bump generation (Phase 108).
                        changed = true;
                    }
                    if pa.leader == dead_id || !live.contains(&pa.leader) {
                        if let Some(new_leader) = elect_leader(&pa.replicas, &pa.isr, &live) {
                            if pa.leader != new_leader {
                                pa.leader = new_leader;
                                let new_epoch = pa.leader_epoch.saturating_add(1);
                                pa.leader_epoch = new_epoch;
                                if !pa.isr.contains(&new_leader) {
                                    pa.isr.push(new_leader);
                                }
                                let start = self
                                    .topics
                                    .read()
                                    .get(&TopicName::new(ta.name.as_str()))
                                    .and_then(|t| t.partitions.get(&PartitionId(*pid)))
                                    .map(|p| p.leo())
                                    .unwrap_or(0);
                                epoch_bumps.push((ta.name.clone(), *pid, new_epoch, start));
                                changed = true;
                            }
                        }
                    }
                }
            }
            if changed {
                asg.generation = asg.generation.saturating_add(1);
                save_assignment(&cluster.data_dir, &asg)?;
            }
        }
        for (topic, pid, new_epoch, start) in epoch_bumps {
            self.record_epoch_start(&topic, pid, new_epoch, start);
        }
        if changed {
            self.apply_local_assignment()?;
        }
        Ok(())
    }

    /// List (topic, partition, leader_id, local_leo) for partitions we follow.
    pub fn follower_targets(&self) -> Vec<(String, u32, u32, u64)> {
        let mut out = Vec::new();
        let topics = self.topics.read();
        for (name, t) in topics.iter() {
            for (pid, p) in &t.partitions {
                if p.is_replica(self.node_id) && !p.is_leader(self.node_id) {
                    out.push((name.as_str().to_owned(), pid.0, p.leader, p.leo()));
                }
            }
        }
        out
    }

    /// Whether this node is leader for the partition.
    pub fn is_partition_leader(&self, topic: &TopicName, partition: PartitionId) -> bool {
        let topics = self.topics.read();
        topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| p.is_leader(self.node_id))
            .unwrap_or(false)
    }

    /// Set the leader epoch for a partition (tests / controlled epoch bumps).
    ///
    /// Does not change the leader node id — only the epoch counter used for
    /// fencing (`FencedLeaderEpoch` / KIP-951 CurrentLeader).
    ///
    /// When `epoch` advances past the current value, records durable leader-epoch
    /// history with start offset = current LEO (Phase 87).
    pub fn set_partition_leader_epoch(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        epoch: u32,
    ) -> Result<()> {
        let (old_epoch, leo) = {
            let mut topics = self.topics.write();
            let t = topics
                .get_mut(topic)
                .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
            let part = t
                .partitions
                .get_mut(&partition)
                .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
            let old = part.leader_epoch;
            let leo = part.leo();
            part.leader_epoch = epoch;
            (old, leo)
        };
        // Ensure prior epoch is in history, then record the new epoch start.
        if epoch > old_epoch {
            self.ensure_epoch_entry(topic.as_str(), partition.0, old_epoch, 0);
            self.record_epoch_start(topic.as_str(), partition.0, epoch, leo);
        } else if epoch == old_epoch {
            self.ensure_epoch_entry(topic.as_str(), partition.0, epoch, 0);
        } else {
            // Epoch regression (unusual): keep history, just set live epoch.
            self.ensure_epoch_entry(topic.as_str(), partition.0, epoch, 0);
        }
        // Keep cluster assignment in sync when present.
        if let Some(cluster) = &self.cluster {
            let mut asg = cluster.assignment.write();
            if let Some(ta) = asg.topics.get_mut(topic.as_str()) {
                if let Some(pa) = ta.partitions.get_mut(&partition.0) {
                    pa.leader_epoch = epoch;
                }
            }
        }
        Ok(())
    }

    /// Resolve OffsetForLeaderEpoch end offset from durable history (Phase 87).
    ///
    /// Returns `(found_epoch, end_offset)` or `None` when the requested epoch is
    /// strictly greater than the current partition epoch.
    pub fn offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: u32,
        requested_epoch: i32,
    ) -> Option<(i32, i64)> {
        let (current_epoch, hwm) = {
            let topics = self.topics.read();
            let t = topics.get(&TopicName::new(topic))?;
            let part = t.partitions.get(&PartitionId(partition))?;
            let hwm = if self.cluster.is_none() {
                part.committed_hwm.max(part.leo())
            } else {
                part.committed_hwm
            };
            (part.leader_epoch, hwm)
        };
        // Ensure at least epoch-0 seed so lookups work on pre-Phase-87 data dirs.
        self.ensure_epoch_entry(topic, partition, 0, 0);
        let epochs = self.leader_epochs.read();
        let entries = epochs
            .get(&(topic.to_owned(), partition))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        end_offset_for(entries, current_epoch, hwm, requested_epoch)
    }

    pub(super) fn load_leader_epochs(&self) {
        let Ok(file) = self.leader_epoch_store.load() else {
            return;
        };
        let mut map = self.leader_epochs.write();
        for (key, entries) in file.partitions {
            if let Some((topic, part)) = crate::producer_state::parse_partition_key(&key) {
                let mut sorted = entries;
                sorted.sort_by_key(|e| e.epoch);
                map.insert((topic, part), sorted);
            }
        }
    }

    pub(super) fn persist_leader_epochs(&self) {
        let epochs = self.leader_epochs.read();
        let mut file = LeaderEpochsFile::default();
        for ((topic, part), entries) in epochs.iter() {
            file.partitions
                .insert(leader_epoch::partition_key(topic, *part), entries.clone());
        }
        let _ = self.leader_epoch_store.save(&file);
    }

    /// Seed epoch 0 @ 0 for any live partition missing history, and restore
    /// live `Partition.leader_epoch` from the highest stored history entry
    /// (single-node has no assignment file for epochs).
    pub(super) fn seed_missing_leader_epochs(&self) {
        let mut dirty = false;
        {
            let mut topics = self.topics.write();
            let mut epochs = self.leader_epochs.write();
            for (name, t) in topics.iter_mut() {
                for (pid, part) in t.partitions.iter_mut() {
                    let key = (name.as_str().to_owned(), pid.0);
                    let e = epochs.entry(key).or_default();
                    if e.is_empty() {
                        ensure_entry(e, 0, 0);
                        dirty = true;
                    }
                    // Restore live epoch from durable history when history is ahead
                    // (e.g. single-node restart after set_partition_leader_epoch).
                    if let Some(max) = e.iter().map(|x| x.epoch).max() {
                        if max > part.leader_epoch {
                            part.leader_epoch = max;
                        } else if part.leader_epoch > max {
                            // Live epoch ahead of history (shouldn't happen after
                            // set_partition_leader_epoch) — ensure an entry.
                            ensure_entry(e, part.leader_epoch, part.leo());
                            dirty = true;
                        }
                    }
                }
            }
        }
        if dirty {
            self.persist_leader_epochs();
        }
    }

    pub(super) fn ensure_epoch_entry(
        &self,
        topic: &str,
        partition: u32,
        epoch: u32,
        start_offset: u64,
    ) {
        let mut epochs = self.leader_epochs.write();
        let e = epochs.entry((topic.to_owned(), partition)).or_default();
        let before = e.len();
        ensure_entry(e, epoch, start_offset);
        let changed = e.len() != before;
        drop(epochs);
        if changed {
            self.persist_leader_epochs();
        }
    }

    pub(super) fn record_epoch_start(
        &self,
        topic: &str,
        partition: u32,
        epoch: u32,
        start_offset: u64,
    ) {
        let mut epochs = self.leader_epochs.write();
        let e = epochs.entry((topic.to_owned(), partition)).or_default();
        // Always ensure prior epoch 0 exists for a continuous chain.
        ensure_entry(e, 0, 0);
        let before_len = e.len();
        let had = e.iter().any(|x| x.epoch == epoch);
        if !had {
            ensure_entry(e, epoch, start_offset);
        }
        let changed = e.len() != before_len || !had;
        drop(epochs);
        if changed {
            self.persist_leader_epochs();
        }
    }

    /// Whether a local partition log exists.
    pub fn topics_has_partition(&self, topic: &TopicName, partition: PartitionId) -> bool {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| t.partitions.contains_key(&partition))
            .unwrap_or(false)
    }

    /// Whether `|ISR| >= min_isr` for the partition (true in single-node).
    pub fn isr_sufficient(&self, topic: &TopicName, partition: PartitionId, min_isr: u32) -> bool {
        if self.cluster.is_none() {
            return true;
        }
        let topics = self.topics.read();
        topics
            .get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| (p.isr.len() as u32) >= min_isr)
            .unwrap_or(false)
    }

    /// Simulate marking a broker dead (for tests) and run failover if controller.
    pub fn test_kill_broker(&self, dead_id: u32) -> Result<()> {
        self.on_broker_death(dead_id)
    }

    /// Test hook: fail outbound inter-broker RPC immediately (no connect).
    ///
    /// Used to isolate a still-alive process that cannot heartbeat or
    /// ReplicaFetch out. Default is unblocked.
    pub fn test_set_inter_broker_blocked(&self, blocked: bool) {
        self.inter_broker_blocked
            .store(blocked, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether outbound inter-broker RPC is blocked (test hook).
    pub fn test_inter_broker_blocked(&self) -> bool {
        self.inter_broker_blocked
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Block outbound inter-broker RPC to `peer_id` only (asymmetric isolate).
    ///
    /// Reverse direction and other peers stay open. Default is unblocked.
    pub fn test_block_inter_broker_peer(&self, peer_id: u32, blocked: bool) {
        let mut set = self.inter_broker_blocked_peers.write();
        if blocked {
            set.insert(peer_id);
        } else {
            set.remove(&peer_id);
        }
    }

    /// Whether outbound RPC to `addr` is dest-blocked (test hook).
    pub fn test_inter_broker_blocked_to(&self, addr: &str) -> bool {
        let peers = self.inter_broker_blocked_peers.read();
        if peers.is_empty() {
            return false;
        }
        peers
            .iter()
            .any(|id| self.broker_addr(*id).as_deref() == Some(addr))
    }

    /// Force-set follower LEO and recompute HWM (unit tests).
    ///
    /// Also stamps last-caught-up when lag ≤ `replica_lag_max_messages` so Phase
    /// 125 time-lag tests can control the clock via sleep after a fresh stamp.
    pub fn test_set_follower_leo(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        replica_id: u32,
        leo: u64,
    ) -> Result<()> {
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        part.follower_leo.insert(replica_id, leo);
        let max_lag = self
            .cluster
            .as_ref()
            .map(|c| c.config.read().replica_lag_max_messages)
            .unwrap_or(u64::MAX);
        let leader_leo = part.leo();
        if leader_leo.saturating_sub(leo) <= max_lag {
            part.follower_caught_up_at
                .insert(replica_id, Instant::now());
        }
        part.recompute_hwm(self.node_id);
        self.hwm_cvar.notify_all();
        Ok(())
    }

    /// Force last-caught-up timestamp age for tests (Phase 125).
    ///
    /// Sets `follower_caught_up_at[replica_id] = now - age_ms`.
    pub fn test_set_follower_caught_up_age_ms(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        replica_id: u32,
        age_ms: u64,
    ) -> Result<()> {
        let mut topics = self.topics.write();
        let t = topics
            .get_mut(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        let at = Instant::now()
            .checked_sub(Duration::from_millis(age_ms))
            .unwrap_or_else(Instant::now);
        part.follower_caught_up_at.insert(replica_id, at);
        Ok(())
    }

    /// Expire sessions / membership (called periodically).
    ///
    /// Phase 110: **every** observer (not only the controller) runs
    /// [`Self::on_broker_death`] for newly expired peers so local ISR shrink +
    /// HWM recompute happen without waiting for a ClusterState pull. The
    /// controller path inside `on_broker_death` still owns durable assignment
    /// updates / generation bumps.
    pub fn tick_cluster(&self) {
        let Some(cluster) = &self.cluster else {
            return;
        };
        cluster.membership.write().touch_self();
        let dead = cluster.membership.write().expire();
        for d in dead {
            let _ = self.on_broker_death(d);
        }
    }

    /// Record a peer as live (e.g. after successful heartbeat response).
    pub fn note_peer_live(&self, peer_id: u32) {
        if let Some(cluster) = &self.cluster {
            cluster.membership.write().heartbeat(peer_id);
        }
    }

    /// Live broker ids from local membership (sorted). Empty when single-node.
    pub fn live_brokers(&self) -> Vec<u32> {
        match &self.cluster {
            None => vec![self.node_id],
            Some(c) => c.membership.read().live_brokers(),
        }
    }

    /// Local partition ISR (may differ from assignment until ClusterState pull).
    pub fn local_partition_isr(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Result<Vec<u32>> {
        let topics = self.topics.read();
        let t = topics
            .get(topic)
            .ok_or_else(|| Error::NotFound(format!("topic {}", topic.as_str())))?;
        let part = t
            .partitions
            .get(&partition)
            .ok_or_else(|| Error::NotFound(format!("partition {partition}")))?;
        Ok(part.isr.clone())
    }

    /// Queue a Phase 142 IsrUpdate report (coalesced by topic/partition).
    pub(super) fn enqueue_isr_report(&self, report: PendingIsrReport) {
        let mut q = self.pending_isr_reports.lock();
        if let Some(existing) = q
            .iter_mut()
            .find(|r| r.topic == report.topic && r.partition == report.partition)
        {
            *existing = report;
        } else {
            q.push(report);
        }
    }

    /// Enqueue IsrUpdate for every partition this node currently leads.
    ///
    /// Used after non-controller death shrink so controller assignment catches up.
    pub(super) fn enqueue_isr_reports_for_local_leaders(&self) {
        let Some(cluster) = &self.cluster else {
            return;
        };
        let gen_hint = cluster.assignment.read().generation;
        let topics = self.topics.read();
        let mut reports = Vec::new();
        for (name, t) in topics.iter() {
            for (pid, part) in &t.partitions {
                if !part.is_leader(self.node_id) {
                    continue;
                }
                reports.push(PendingIsrReport {
                    topic: name.as_str().to_owned(),
                    partition: pid.0,
                    leader_id: part.leader,
                    leader_epoch: part.leader_epoch,
                    isr: part.isr.clone(),
                    generation_hint: gen_hint,
                });
            }
        }
        drop(topics);
        // Mirror local ISR into assignment without bumping generation.
        {
            let mut asg = cluster.assignment.write();
            for r in &reports {
                if let Some(ta) = asg.topics.get_mut(&r.topic) {
                    if let Some(pa) = ta.partitions.get_mut(&r.partition) {
                        if pa.isr != r.isr {
                            pa.isr = r.isr.clone();
                        }
                    }
                }
            }
        }
        for r in reports {
            self.enqueue_isr_report(r);
        }
    }

    /// Drain pending IsrUpdate reports (Phase 142).
    pub fn drain_pending_isr_reports(&self) -> Vec<PendingIsrReport> {
        std::mem::take(&mut *self.pending_isr_reports.lock())
    }

    /// Whether any IsrUpdate reports are queued.
    pub fn has_pending_isr_reports(&self) -> bool {
        !self.pending_isr_reports.lock().is_empty()
    }

    /// Align local assignment generation to controller SoT after a successful
    /// IsrUpdate report (Phase 142). Avoids permanent gen divergence that would
    /// reject later ClusterState pulls.
    pub fn align_assignment_generation(&self, controller_generation: u32) {
        let Some(cluster) = &self.cluster else {
            return;
        };
        let mut asg = cluster.assignment.write();
        if asg.generation != controller_generation {
            asg.generation = controller_generation;
            let _ = save_assignment(&cluster.data_dir, &asg);
        }
    }

    /// Controller: apply a leader-reported ISR update (Phase 142).
    ///
    /// Accepts only when this node is controller, the topic/partition exists,
    /// `leader_id` matches the assignment leader, and `leader_epoch` is not
    /// stale (`local > requested` → fenced). On success updates assignment ISR,
    /// bumps generation, and persists.
    ///
    /// Returns `(error_code, assignment_generation)`.
    pub fn apply_leader_isr_update(
        &self,
        topic: &str,
        partition: u32,
        leader_id: u32,
        leader_epoch: u32,
        isr: &[u32],
        _generation_hint: u32,
    ) -> (u16, u32) {
        let Some(cluster) = &self.cluster else {
            // Single-node: no-op success.
            return (0, 0);
        };
        if !self.is_controller() {
            return (ErrorCode::NotController as u16, self.generation());
        }
        if topic.is_empty() {
            return (ErrorCode::InvalidArg as u16, self.generation());
        }
        if isr.is_empty() || !isr.contains(&leader_id) {
            return (ErrorCode::InvalidArg as u16, self.generation());
        }

        let mut asg = cluster.assignment.write();
        let Some(ta) = asg.topics.get_mut(topic) else {
            return (ErrorCode::NotFound as u16, asg.generation);
        };
        let Some(pa) = ta.partitions.get_mut(&partition) else {
            return (ErrorCode::NotFound as u16, asg.generation);
        };
        if pa.leader != leader_id {
            return (ErrorCode::NotLeaderForPartition as u16, asg.generation);
        }
        if pa.leader_epoch > leader_epoch {
            return (ErrorCode::InvalidProducerEpoch as u16, asg.generation);
        }
        // Keep only replica-set members; always include leader.
        let mut new_isr: Vec<u32> = Vec::with_capacity(isr.len());
        new_isr.push(leader_id);
        for &id in isr {
            if id != leader_id && pa.replicas.contains(&id) && !new_isr.contains(&id) {
                new_isr.push(id);
            }
        }
        if pa.isr != new_isr {
            pa.isr = new_isr;
            asg.generation = asg.generation.saturating_add(1);
            let _ = save_assignment(&cluster.data_dir, &asg);
        }
        let gen = asg.generation;
        (0, gen)
    }

    /// Reconcile local membership against the controller's `alive_brokers`
    /// set from a HeartbeatBroker response (Phase 110).
    ///
    /// Brokers previously considered live but **missing** from `alive` are
    /// treated as dead via [`Self::on_broker_death`] (local ISR shrink + HWM
    /// on every observer; durable assignment only if this node is controller).
    /// Peers listed in `alive` are marked live.
    ///
    /// Non-controllers call this on every successful controller heartbeat so
    /// they do not wait for a generation-bumped ClusterState pull to drop a
    /// dead follower from local ISR (unblocks `acks=all`).
    pub fn apply_controller_alive_set(&self, alive: &[u32]) -> Result<()> {
        let Some(cluster) = &self.cluster else {
            return Ok(());
        };
        let alive_set: std::collections::HashSet<u32> = alive.iter().copied().collect();
        let prev_live = cluster.membership.read().live_brokers();
        let missing: Vec<u32> = prev_live
            .into_iter()
            .filter(|id| *id != self.node_id && !alive_set.contains(id))
            .collect();
        for dead_id in missing {
            self.on_broker_death(dead_id)?;
        }
        {
            let mut m = cluster.membership.write();
            for &id in alive {
                m.heartbeat(id);
            }
            m.touch_self();
        }
        Ok(())
    }

    /// Shared cluster state for background tasks.
    pub fn cluster_state(&self) -> Option<Arc<ClusterState>> {
        self.cluster.clone()
    }
}
