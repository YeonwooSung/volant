//! DeleteRecords, truncate-journal, and assignment/metadata-raft accessors.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tracing::warn;
use volant_core::{Error, Offset, PartitionId, Result, TopicName};
use volant_protocol::{ClusterTopicState, ErrorCode};

use crate::cluster::{AssignmentConsensus, MetadataCommand, MetadataLogEntry, MetadataRaftState};
use crate::cluster_admin::{ClusterAdminFile, ClusterAdminStore};
use crate::delete_records_outbox::DeleteRecordsOutbox;

use super::*;
use super::{fence_leader_epoch, EpochFenceMode};

impl Broker {
    // --- Phase 113 cluster admin generations (fan-out behavior lands in later PRs) ---

    /// Controller (or local) BROKER-config generation.
    pub fn config_generation(&self) -> u64 {
        self.config_generation.load(Ordering::Relaxed)
    }

    /// Last applied BROKER-config generation on this node.
    pub fn applied_config_generation(&self) -> u64 {
        self.applied_config_generation.load(Ordering::Relaxed)
    }

    /// Controller (or local) ACL generation.
    pub fn acl_generation(&self) -> u64 {
        self.acl_generation.load(Ordering::Relaxed)
    }

    /// Last applied ACL generation on this node.
    pub fn applied_acl_generation(&self) -> u64 {
        self.applied_acl_generation.load(Ordering::Relaxed)
    }

    /// DeleteRecords fan-out error counter (Phase 113).
    pub fn delete_records_fanout_errors_total(&self) -> u64 {
        self.delete_records_fanout_errors_total
            .load(Ordering::Relaxed)
    }

    /// Increment DeleteRecords fan-out error counter (Phase 113).
    pub fn note_delete_records_fanout_error(&self) {
        self.delete_records_fanout_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Phase 135: whether DeleteRecords client path waits for journal majority.
    ///
    /// Default **false** (best-effort; client success independent of majority).
    /// Env `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` = `1`/`true`/`yes` enables at
    /// construct time; use [`Self::set_delete_records_wait_majority`] in tests.
    pub fn delete_records_wait_majority(&self) -> bool {
        self.delete_records_wait_majority.load(Ordering::Relaxed)
    }

    /// Phase 135: runtime toggle for tests / operator tooling.
    pub fn set_delete_records_wait_majority(&self, wait: bool) {
        self.delete_records_wait_majority
            .store(wait, Ordering::Relaxed);
    }

    /// Phase 137: merge request trailer with broker default wait knob.
    /// * 0 / other → `delete_records_wait_majority()`
    /// * 1 → true
    /// * 2 → false
    pub fn effective_delete_records_wait_majority(&self, request_flag: u8) -> bool {
        match request_flag {
            1 => true,
            2 => false,
            _ => self.delete_records_wait_majority(),
        }
    }

    /// Phase 135: wait-mode majority success counter.
    pub fn delete_records_majority_wait_success_total(&self) -> u64 {
        self.delete_records_majority_wait_success_total
            .load(Ordering::Relaxed)
    }

    /// Phase 135: wait-mode majority failure counter.
    pub fn delete_records_majority_wait_fail_total(&self) -> u64 {
        self.delete_records_majority_wait_fail_total
            .load(Ordering::Relaxed)
    }

    /// Increment wait-mode majority success (Phase 135/148; only when wait is on).
    pub fn note_delete_records_majority_wait_success(&self) {
        self.delete_records_majority_wait_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment wait-mode majority failure (Phase 135/148; only when wait is on).
    pub fn note_delete_records_majority_wait_fail(&self) {
        self.delete_records_majority_wait_fail_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Phase 148: majority-first path success counter.
    pub fn delete_records_majority_first_success_total(&self) -> u64 {
        self.delete_records_majority_first_success_total
            .load(Ordering::Relaxed)
    }

    /// Phase 148: majority-first path failure counter (no local truncate).
    pub fn delete_records_majority_first_fail_total(&self) -> u64 {
        self.delete_records_majority_first_fail_total
            .load(Ordering::Relaxed)
    }

    /// Increment majority-first success (Phase 148 wait path after journal + local).
    pub fn note_delete_records_majority_first_success(&self) {
        self.delete_records_majority_first_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment majority-first fail (Phase 148 wait path; local log unchanged).
    pub fn note_delete_records_majority_first_fail(&self) {
        self.delete_records_majority_first_fail_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Phase 148: preflight DeleteRecords without mutating the log.
    ///
    /// Returns `(log_start, error_code)`. `error_code == 0` means this node may
    /// proceed (leader or single-node). Not-leader returns `(0, NotLeader)`.
    /// Missing topic/partition → [`Error::NotFound`].
    pub fn delete_records_leader_log_start(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<(u64, u16)> {
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let t = topics
            .get(&name)
            .ok_or_else(|| Error::NotFound(format!("topic {topic}")))?;
        let part = t
            .partitions
            .get(&PartitionId(partition))
            .ok_or_else(|| Error::NotFound(format!("partition {topic}/{partition}")))?;
        if self.cluster.is_some() && !part.is_leader(self.node_id) {
            return Ok((0, ErrorCode::NotLeaderForPartition as u16));
        }
        Ok((part.log.log_start_offset().raw(), 0))
    }

    /// Phase 148: journal note offset for majority-first — `min(before, LEO)`.
    ///
    /// Whole-segment clamp may still land local low below this; max-merge journal
    /// (watermark ≥ local) remains honest.
    pub fn delete_records_note_offset(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> u64 {
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let Some(part) = topics
            .get(&name)
            .and_then(|t| t.partitions.get(&PartitionId(partition)))
        else {
            return before_offset;
        };
        let leo = part.log.log_end_offset().raw();
        before_offset.min(leo)
    }

    /// Durable DeleteRecords outbox (Phase 116).
    pub fn delete_records_outbox(&self) -> &DeleteRecordsOutbox {
        &self.delete_records_outbox
    }

    /// Pending outbox depth (Phase 116).
    pub fn delete_records_outbox_depth(&self) -> u64 {
        self.delete_records_outbox.depth()
    }

    /// Outbox enqueue counter (Phase 116).
    pub fn delete_records_outbox_enqueued_total(&self) -> u64 {
        self.delete_records_outbox.enqueued_total()
    }

    /// Outbox retry success counter (Phase 116).
    pub fn delete_records_outbox_retry_success_total(&self) -> u64 {
        self.delete_records_outbox.retry_success_total()
    }

    /// Outbox retry error counter (Phase 116).
    pub fn delete_records_outbox_retry_errors_total(&self) -> u64 {
        self.delete_records_outbox.retry_errors_total()
    }

    /// Outbox capacity-drop counter (Phase 116).
    pub fn delete_records_outbox_drops_total(&self) -> u64 {
        self.delete_records_outbox.drops_total()
    }

    /// Enqueue a pending peer truncate after fan-out failure (Phase 116).
    pub fn enqueue_delete_records_outbox(
        &self,
        replica_id: u32,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) {
        let _ = self.delete_records_outbox.enqueue(
            replica_id,
            topic,
            partition,
            before_offset,
            leader_epoch,
        );
    }

    /// Pending outbox entries for currently live peers (Phase 116 drain).
    pub fn delete_records_outbox_pending_live(
        &self,
    ) -> Vec<crate::delete_records_outbox::OutboxEntry> {
        let live = self.live_brokers();
        self.delete_records_outbox.pending_for_replicas(&live)
    }

    /// Phase 123: partition reconcile passes that advanced last_reconcile.
    pub fn delete_records_outbox_reconcile_total(&self) -> u64 {
        self.delete_records_outbox_reconcile_total
            .load(Ordering::Relaxed)
    }

    /// Phase 129 truncate journal accessor.
    pub fn truncate_journal(&self) -> &TruncateJournal {
        &self.truncate_journal
    }

    /// Phase 129/130: journal generation (local after note/push).
    pub fn truncate_journal_generation(&self) -> u64 {
        self.truncate_journal.generation()
    }

    /// Phase 129/130: last applied journal push generation.
    pub fn truncate_journal_applied_generation(&self) -> u64 {
        self.truncate_journal.applied_generation()
    }

    /// Phase 130: majority consensus success count.
    pub fn truncate_journal_consensus_success_total(&self) -> u64 {
        self.truncate_journal.consensus_success_total()
    }

    /// Phase 150: assignment consensus state.
    pub fn assignment_consensus(&self) -> &AssignmentConsensus {
        &self.assignment_consensus
    }

    /// Phase 150: last majority-committed assignment generation.
    pub fn assignment_committed_generation(&self) -> u32 {
        self.assignment_consensus.committed_generation()
    }

    /// Phase 150: majority assignment commit success count.
    pub fn assignment_consensus_success_total(&self) -> u64 {
        self.assignment_consensus.success_total()
    }

    /// Phase 150: majority assignment commit failure count.
    pub fn assignment_consensus_fail_total(&self) -> u64 {
        self.assignment_consensus.fail_total()
    }

    /// Phase 150: whether admin paths fan out assignment consensus notes.
    pub fn assignment_consensus_enabled(&self) -> bool {
        self.assignment_consensus_enabled.load(Ordering::Relaxed)
    }

    /// Phase 150: runtime toggle for tests / ops.
    pub fn set_assignment_consensus_enabled(&self, enabled: bool) {
        self.assignment_consensus_enabled
            .store(enabled, Ordering::Relaxed);
    }

    /// Phase 150: whether CreateTopic/etc. wait for majority before client ok.
    pub fn assignment_consensus_wait(&self) -> bool {
        self.assignment_consensus_wait.load(Ordering::Relaxed)
    }

    /// Phase 150: runtime toggle for tests (`set_assignment_consensus_wait(true)`).
    pub fn set_assignment_consensus_wait(&self, wait: bool) {
        self.assignment_consensus_wait
            .store(wait, Ordering::Relaxed);
    }

    /// Phase 152: Metadata serves majority-committed assignment when consensus
    /// is enabled (default **false**; `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY=1`
    /// serves the committed snapshot).
    pub fn assignment_metadata_committed_only(&self) -> bool {
        self.assignment_metadata_committed_only
            .load(Ordering::Relaxed)
    }

    /// Phase 152: runtime toggle for tests / ops (`0` env restores lead Metadata).
    pub fn set_assignment_metadata_committed_only(&self, v: bool) {
        self.assignment_metadata_committed_only
            .store(v, Ordering::Relaxed);
    }

    /// Phase 152: `max(0, live_generation - committed_generation)`.
    pub fn assignment_generation_lag(&self) -> u32 {
        let live = self.generation();
        let committed = self.assignment_committed_generation();
        live.saturating_sub(committed)
    }

    /// Phase 150: handle peer `AssignmentConsensusNote` — apply snapshot when
    /// `generation >= local`, return acked generation.
    pub fn handle_assignment_consensus_note(
        &self,
        generation: u32,
        controller_id: u32,
        topics: &[ClusterTopicState],
    ) -> (u16, u32) {
        if self.cluster.is_none() {
            // Single-node: accept and commit locally.
            self.assignment_consensus.commit(generation);
            return (0, generation);
        }
        match self.apply_cluster_state(generation, controller_id, topics) {
            Ok(()) => {
                // Peer applied (or ignored stale). Ack the proposed generation
                // so the controller can count majority; durable commit is
                // controller-side only after majority.
                //
                // Phase 152: also install committed snapshot from the applied
                // live assignment so peer Metadata (committed-only) can serve
                // the generation once applied. Residual: peer may advertise a
                // gen that the controller later fails to majority-commit when
                // other peers are down (static-N honesty).
                if let Some(cluster) = &self.cluster {
                    let snap = cluster.assignment.read().clone();
                    if snap.generation > 0 {
                        self.assignment_consensus
                            .note_committed_snapshot(snap.generation, &snap);
                    }
                }
                let local = self.generation();
                (0, local.max(generation))
            }
            Err(e) => {
                warn!(
                    error = %e,
                    generation,
                    "assignment consensus note apply failed"
                );
                (ErrorCode::Unknown as u16, self.generation())
            }
        }
    }

    /// Phase 154: metadata Raft log state.
    pub fn metadata_raft(&self) -> &MetadataRaftState {
        &self.metadata_raft
    }

    /// Phase 154: whether admin paths use the metadata Raft log.
    pub fn metadata_raft_enabled(&self) -> bool {
        self.metadata_raft_enabled.load(Ordering::Relaxed)
    }

    /// Phase 154: runtime toggle for tests / ops.
    pub fn set_metadata_raft_enabled(&self, enabled: bool) {
        self.metadata_raft_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Phase 154: current metadata Raft term.
    pub fn metadata_raft_term(&self) -> u64 {
        self.metadata_raft.current_term()
    }

    /// Phase 154: metadata Raft commit index.
    pub fn metadata_raft_commit_index(&self) -> u64 {
        self.metadata_raft.commit_index()
    }

    /// Phase 154: metadata Raft last applied index.
    pub fn metadata_raft_last_applied(&self) -> u64 {
        self.metadata_raft.last_applied()
    }

    /// Phase 154: append success total.
    pub fn metadata_raft_append_success_total(&self) -> u64 {
        self.metadata_raft.append_success_total()
    }

    /// Phase 154: append fail total.
    pub fn metadata_raft_append_fail_total(&self) -> u64 {
        self.metadata_raft.append_fail_total()
    }

    /// Phase 154: handle peer `MetadataRaftAppend` (simplified AppendEntries).
    ///
    /// On success, advances peer log/commit and applies committed
    /// `SetAssignment` entries to live assignment + Phase 152 committed snap.
    pub fn handle_metadata_raft_append(
        &self,
        leader_id: u32,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[MetadataLogEntry],
        leader_commit: u64,
    ) -> (u64, bool, u64) {
        let _ = leader_id;
        let r = self.metadata_raft.append_entries(
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        );
        if r.success {
            self.apply_committed_metadata_entries();
        }
        (r.term, r.success, r.match_index)
    }

    /// Phase 154: apply committed-but-unapplied log entries to assignment.
    ///
    /// Also bumps Phase 152 `assignment_consensus` committed snapshot for
    /// metrics/Metadata compatibility.
    pub fn apply_committed_metadata_entries(&self) {
        let controller_id = self.controller_id();
        for entry in self.metadata_raft.take_entries_to_apply() {
            match &entry.payload {
                MetadataCommand::SetAssignment { generation, topics } => {
                    if self.cluster.is_some() {
                        if let Err(e) = self.apply_cluster_state(*generation, controller_id, topics)
                        {
                            warn!(
                                error = %e,
                                index = entry.index,
                                generation,
                                "metadata raft apply SetAssignment failed"
                            );
                            continue;
                        }
                    }
                    // Phase 152 compatibility: committed snapshot + gen.
                    if let Some(cluster) = &self.cluster {
                        let snap = cluster.assignment.read().clone();
                        self.assignment_consensus
                            .note_committed_snapshot(*generation, &snap);
                    } else {
                        // Single-node: still advance consensus gens for metrics.
                        self.assignment_consensus.commit(*generation);
                    }
                }
                MetadataCommand::Noop => {}
            }
        }
    }

    /// Phase 154: leader helper — append current live assignment as a log entry.
    pub fn append_assignment_to_metadata_log(&self) -> MetadataLogEntry {
        let (generation, topics) = if let Some(cluster) = &self.cluster {
            let asg = cluster.assignment.read();
            (asg.generation, asg.to_wire_topics())
        } else {
            (0, vec![])
        };
        self.metadata_raft
            .append_command(MetadataCommand::SetAssignment { generation, topics })
    }

    /// Phase 130: majority consensus failure count.
    pub fn truncate_journal_consensus_fail_total(&self) -> u64 {
        self.truncate_journal.consensus_fail_total()
    }

    /// Phase 131: successful truncate-journal rejoin catch-up pushes.
    pub fn journal_catchup_success_total(&self) -> u64 {
        self.truncate_journal.journal_catchup_success_total()
    }

    /// Phase 131: failed truncate-journal rejoin catch-up pushes.
    pub fn journal_catchup_errors_total(&self) -> u64 {
        self.truncate_journal.journal_catchup_errors_total()
    }

    /// Whether a peer's applied journal generation lags local journal SoT (Phase 131).
    ///
    /// True when this node has a newer journal generation (`local > peer_applied`)
    /// and local state is non-empty (`local_gen > 0` or at least one watermark).
    pub fn peer_journal_gen_lags(&self, peer_applied_journal: u64) -> bool {
        let local_gen = self.truncate_journal_generation();
        if local_gen <= peer_applied_journal {
            return false;
        }
        local_gen > 0 || self.truncate_journal.entry_count() > 0
    }

    /// Phase 132: try to claim a journal catch-up slot for `peer_id`.
    ///
    /// Returns `true` when the caller should spawn a catch-up task. Returns
    /// `false` (and increments [`Self::journal_catchup_skipped_total`]) when:
    /// - a catch-up is already in-flight for this peer (single-flight), or
    /// - a prior start was within the min-interval throttle window.
    ///
    /// On `true`, the caller **must** eventually call
    /// [`Self::finish_journal_catchup`] (normally via a `Drop` guard or task
    /// finally block) so single-flight is released.
    pub fn try_begin_journal_catchup(&self, peer_id: u32) -> bool {
        let min_ms = journal_catchup_min_interval_ms();
        let mut st = self.journal_catchup.lock();
        if st.in_flight.contains(&peer_id) {
            self.journal_catchup_skipped_total
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if min_ms > 0 {
            if let Some(started) = st.last_start.get(&peer_id) {
                if started.elapsed() < Duration::from_millis(min_ms) {
                    self.journal_catchup_skipped_total
                        .fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
        }
        st.in_flight.insert(peer_id);
        st.last_start.insert(peer_id, Instant::now());
        true
    }

    /// Phase 132: release the per-peer catch-up single-flight claim.
    pub fn finish_journal_catchup(&self, peer_id: u32) {
        self.journal_catchup.lock().in_flight.remove(&peer_id);
    }

    /// Phase 132: schedule skips due to single-flight or min-interval.
    pub fn journal_catchup_skipped_total(&self) -> u64 {
        self.journal_catchup_skipped_total.load(Ordering::Relaxed)
    }

    /// Phase 132: force-reset catch-up scheduler state (integration tests).
    pub fn reset_journal_catchup_scheduler_for_test(&self) {
        let mut st = self.journal_catchup.lock();
        st.in_flight.clear();
        st.last_start.clear();
        self.journal_catchup_skipped_total
            .store(0, Ordering::Relaxed);
    }

    /// Whether a catch-up is currently in-flight for `peer_id` (Phase 132).
    pub fn journal_catchup_in_flight(&self, peer_id: u32) -> bool {
        self.journal_catchup.lock().in_flight.contains(&peer_id)
    }

    /// Phase 136: try to claim an admin (ACL/config) catch-up slot for `peer_id`.
    ///
    /// Returns `true` when the caller should spawn a catch-up task. Returns
    /// `false` (and increments [`Self::admin_catchup_skipped_total`]) when:
    /// - a catch-up is already in-flight for this peer (single-flight), or
    /// - a prior start was within the min-interval throttle window.
    ///
    /// On `true`, the caller **must** eventually call
    /// [`Self::finish_admin_catchup`].
    pub fn try_begin_admin_catchup(&self, peer_id: u32) -> bool {
        let min_ms = admin_catchup_min_interval_ms();
        let mut st = self.admin_catchup.lock();
        if st.in_flight.contains(&peer_id) {
            self.admin_catchup_skipped_total
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if min_ms > 0 {
            if let Some(started) = st.last_start.get(&peer_id) {
                if started.elapsed() < Duration::from_millis(min_ms) {
                    self.admin_catchup_skipped_total
                        .fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
        }
        st.in_flight.insert(peer_id);
        st.last_start.insert(peer_id, Instant::now());
        true
    }

    /// Phase 136: release the per-peer admin catch-up single-flight claim.
    pub fn finish_admin_catchup(&self, peer_id: u32) {
        self.admin_catchup.lock().in_flight.remove(&peer_id);
    }

    /// Phase 136: schedule skips due to single-flight or min-interval.
    pub fn admin_catchup_skipped_total(&self) -> u64 {
        self.admin_catchup_skipped_total.load(Ordering::Relaxed)
    }

    /// Phase 136: force-reset admin catch-up scheduler state (integration tests).
    pub fn reset_admin_catchup_scheduler_for_test(&self) {
        let mut st = self.admin_catchup.lock();
        st.in_flight.clear();
        st.last_start.clear();
        self.admin_catchup_skipped_total.store(0, Ordering::Relaxed);
    }

    /// Whether an admin catch-up is currently in-flight for `peer_id` (Phase 136).
    pub fn admin_catchup_in_flight(&self, peer_id: u32) -> bool {
        self.admin_catchup.lock().in_flight.contains(&peer_id)
    }

    /// Configured cluster size for majority (effective overlay or toml N).
    pub fn cluster_member_count(&self) -> usize {
        self.cluster
            .as_ref()
            .map(|c| c.config.read().brokers.len().max(1))
            .unwrap_or(1)
    }

    /// Configured membership size as `u64` (Phase 141 ops gauges).
    ///
    /// Single-node (no cluster config) is **1**. Same as
    /// [`Self::cluster_member_count`] for multi-node.
    pub fn configured_broker_count(&self) -> u64 {
        self.cluster_member_count() as u64
    }

    /// Live membership size as `u64` (Phase 141 ops gauges).
    ///
    /// Single-node is **1**. Multi-node uses local membership
    /// ([`Self::live_brokers`]).
    pub fn live_broker_count(&self) -> u64 {
        self.live_brokers().len() as u64
    }

    /// Journal-majority quorum size `floor(N/2)+1` for **configured** N (Phase 141).
    ///
    /// Matches [`TruncateJournal::majority`] / Phase 130 note fan-out. Single-node
    /// is **1**.
    pub fn majority_quorum_size(&self) -> u64 {
        TruncateJournal::majority(self.cluster_member_count()) as u64
    }

    /// Whether journal majority cannot succeed given live vs configured N (Phase 141).
    ///
    /// `true` when `live_broker_count() < majority_quorum_size()`. Single-node is
    /// always `false`. Classic sharp edge: **N=2** with one peer down → majority
    /// need 2 but live is 1.
    pub fn majority_impossible(&self) -> bool {
        self.live_broker_count() < self.majority_quorum_size()
    }

    /// Durable local note + generation bump (any broker; Phase 130 multi-controller).
    ///
    /// Returns new local generation. Prefer
    /// [`crate::net::fanout_truncate_journal_note`] for majority consensus.
    pub fn controller_note_truncate_journal(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) -> u64 {
        self.truncate_journal
            .note(topic, partition, before_offset, leader_epoch, true)
    }

    /// Alias: durable local note on any node (Phase 130).
    pub fn local_note_truncate_journal(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) -> u64 {
        self.controller_note_truncate_journal(topic, partition, before_offset, leader_epoch)
    }

    /// Known topic names for Phase 137 journal push anti-resurrection.
    ///
    /// Cluster: assignment topic keys. Single-node: local topics map.
    pub(super) fn journal_known_topics(&self) -> HashSet<String> {
        if let Some(cluster) = &self.cluster {
            cluster.assignment.read().topics.keys().cloned().collect()
        } else {
            self.topics
                .read()
                .keys()
                .map(|n| n.as_str().to_owned())
                .collect()
        }
    }

    /// Phase 129/130: apply journal snapshot push (max-merge).
    ///
    /// Phase 137: filters to known topics so deleted topics cannot resurrect.
    pub fn apply_truncate_journal_push(&self, generation: u64, snapshot: &[u8]) -> Result<()> {
        let known = self.journal_known_topics();
        self.truncate_journal
            .apply_push_filtered(generation, snapshot, Some(&known))
            .map_err(Error::InvalidArgument)
    }

    /// Phase 130: handle inter-broker TruncateJournalNote on **any** broker
    /// (multi-controller durable replicate). Max-merges + persists when valid.
    ///
    /// - Empty `topic` → [`ErrorCode::InvalidArg`], generation unchanged.
    /// - `before_offset == 0` → no-op success (journal ignores zero watermarks).
    /// - Unknown topic/partition → [`ErrorCode::NotFound`] (no orphan SoT keys).
    /// - Epoch fence via [`EpochFenceMode::RequireStamped`] (negative epoch and
    ///   stale local>req rejected; future epochs accepted for multi-controller lag).
    /// - Receivers need not be leaders (Phase 130 multi-controller).
    ///
    /// Proposer path uses [`Self::local_note_truncate_journal`] directly (trusted
    /// local leadership). Production should still enable ACL/auth on 86/88 —
    /// equal-epoch forge with a huge offset remains open under weak auth.
    pub fn handle_truncate_journal_note(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) -> (u16, u64) {
        if topic.is_empty() {
            return (
                ErrorCode::InvalidArg as u16,
                self.truncate_journal.generation(),
            );
        }
        // before_offset == 0: journal ignores zero watermarks; no existence check.
        if before_offset == 0 {
            return (0, self.truncate_journal.generation());
        }

        // Ingress fence before durable max-merge (journal is cluster SoT).
        {
            let name = TopicName::new(topic);
            let topics = self.topics.read();
            let Some(t) = topics.get(&name) else {
                return (
                    ErrorCode::NotFound as u16,
                    self.truncate_journal.generation(),
                );
            };
            let Some(part) = t.partitions.get(&PartitionId(partition)) else {
                return (
                    ErrorCode::NotFound as u16,
                    self.truncate_journal.generation(),
                );
            };
            if let Err(code) = fence_leader_epoch(
                part.leader_epoch,
                leader_epoch,
                EpochFenceMode::RequireStamped,
            ) {
                return (code as u16, self.truncate_journal.generation());
            }
        }

        let gen = self.local_note_truncate_journal(topic, partition, before_offset, leader_epoch);
        (0, gen)
    }

    /// Phase 129/130: handle TruncateJournalPush on peer.
    pub fn handle_truncate_journal_push(&self, generation: u64, snapshot: &[u8]) -> u16 {
        match self.apply_truncate_journal_push(generation, snapshot) {
            Ok(()) => 0,
            Err(_) => ErrorCode::Storage as u16,
        }
    }

    /// Rebuild pending DeleteRecords outbox entries for partitions this node
    /// leads (Phase 123 + 129/130 journal).
    ///
    /// Target = `max(local log_start, journal watermark)`. When the journal
    /// is ahead of the local log, apply a local truncate first so the leader
    /// does not serve below the committed watermark while driving peers.
    /// Peers are always enqueued at the desired journal `target` (SoT).
    ///
    /// **Progress / freeze fix:** `last_reconcile` is set to `(epoch, target)`
    /// only when local log is known `>= target` after the attempt. Partial
    /// segment advances (`low < target`), errors, or epoch fence skips leave
    /// `last_reconcile` unchanged so the next tick retries local truncate.
    /// The return value counts partitions processed this pass (not skipped by
    /// the fully-reconciled early-continue); `delete_records_outbox_reconcile_total`
    /// increments only when `last_reconcile` advances.
    ///
    /// **Epoch fencing:** if the journal entry has `leader_epoch >= 0` and this
    /// node's local leader epoch is **strictly less**, skip local truncate this
    /// tick (stale leader view) but still enqueue peers with `target`.
    /// Only partitions this node leads are considered (local apply is
    /// leader-only via [`Self::delete_records`]).
    ///
    /// Idempotent once local is at target; no-op in single-node mode.
    pub fn reconcile_delete_records_outbox(&self) -> u64 {
        if self.cluster.is_none() {
            return 0;
        }
        // Journal epoch stamps for local-apply fencing (topic, partition) → epoch.
        let journal_epochs: HashMap<(String, u32), i32> = self
            .truncate_journal
            .list()
            .into_iter()
            .map(|e| ((e.topic, e.partition), e.leader_epoch))
            .collect();

        // Collect led partitions + targets without holding the outbox lock.
        let targets: Vec<(String, u32, u64, u64, i32, Vec<u32>)> = {
            let topics = self.topics.read();
            let mut out = Vec::new();
            for (name, t) in topics.iter() {
                for (pid, part) in &t.partitions {
                    if !part.is_leader(self.node_id) {
                        continue;
                    }
                    let log_start = part.log.log_start_offset().raw();
                    let journal_wm = self
                        .truncate_journal
                        .watermark(name.as_str(), pid.0)
                        .unwrap_or(0);
                    let target = log_start.max(journal_wm);
                    if target == 0 {
                        continue;
                    }
                    let epoch = part.leader_epoch;
                    let peers: Vec<u32> = part
                        .replicas
                        .iter()
                        .copied()
                        .filter(|id| *id != self.node_id && self.broker_addr(*id).is_some())
                        .collect();
                    // Still reconcile when journal > local even if no peers
                    // (apply local truncate).
                    out.push((
                        name.as_str().to_owned(),
                        pid.0,
                        log_start,
                        target,
                        epoch as i32,
                        peers,
                    ));
                }
            }
            out
        };

        let mut advanced = 0u64;
        let mut last = self.delete_records_outbox_last_reconcile.lock();
        for (topic, partition, log_start, target, epoch, peers) in targets {
            let key = (topic.clone(), partition);
            let epoch_u = epoch as u32;
            // Fully reconciled to this (epoch, target) — skip until target/epoch changes.
            if last.get(&key) == Some(&(epoch_u, target)) {
                continue;
            }

            // Local apply only when journal/desired watermark is ahead of log_start.
            // `local_at_target` starts true when we already meet the watermark.
            let mut local_at_target = log_start >= target;
            if target > log_start {
                let journal_epoch = journal_epochs
                    .get(&(topic.clone(), partition))
                    .copied()
                    .unwrap_or(-1);
                // Journal stamped by a newer leader than our local view: do not
                // truncate locally this tick (stale leader), but still drive peers.
                let fenced = journal_epoch >= 0 && epoch_u < journal_epoch as u32;
                if fenced {
                    warn!(
                        topic,
                        partition,
                        target,
                        local_epoch = epoch_u,
                        journal_epoch,
                        "reconcile skip local truncate: journal epoch fences local leader"
                    );
                } else {
                    match self.delete_records(&topic, partition, target) {
                        Ok((low, err)) => {
                            if err != 0 {
                                warn!(
                                    topic,
                                    partition,
                                    target,
                                    error_code = err,
                                    "reconcile local truncate failed"
                                );
                                // Leave local_at_target false — retry next tick.
                            } else {
                                // Achieved local watermark (segment-boundary clamp).
                                let achieved = low.max(log_start);
                                local_at_target = achieved >= target;
                                if !local_at_target {
                                    // Partial advance — peers still get full target SoT;
                                    // do not mark last_reconcile so we retry locally.
                                    warn!(
                                        topic,
                                        partition,
                                        target,
                                        achieved,
                                        "reconcile local truncate partial; will retry"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                topic,
                                partition,
                                target,
                                error = %e,
                                "reconcile local truncate error"
                            );
                        }
                    }
                }
            }

            // Always enqueue the desired journal watermark for peers (SoT).
            // While local lags, leader keeps retrying; outbox max-merges duplicates.
            for peer in peers {
                let _ = self
                    .delete_records_outbox
                    .enqueue(peer, &topic, partition, target, epoch);
            }

            // Only freeze progress (last_reconcile) when local log is at target.
            if local_at_target {
                last.insert(key, (epoch_u, target));
                self.delete_records_outbox_reconcile_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            advanced += 1;
        }
        advanced
    }

    /// Current leader epoch for a partition if this node leads it (Phase 123 drain).
    ///
    /// Returns `None` when the partition is unknown or this node is not leader.
    pub fn led_partition_epoch(&self, topic: &str, partition: u32) -> Option<i32> {
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let part = topics.get(&name)?.partitions.get(&PartitionId(partition))?;
        if part.is_leader(self.node_id) {
            Some(part.leader_epoch as i32)
        } else {
            None
        }
    }

    /// BROKER config push error counter (Phase 113).
    pub fn cluster_config_push_errors_total(&self) -> u64 {
        self.cluster_config_push_errors_total
            .load(Ordering::Relaxed)
    }

    /// ACL snapshot push error counter (Phase 113).
    pub fn cluster_acl_push_errors_total(&self) -> u64 {
        self.cluster_acl_push_errors_total.load(Ordering::Relaxed)
    }

    /// Admin catch-up success counter (Phase 117).
    pub fn cluster_admin_catchup_success_total(&self) -> u64 {
        self.cluster_admin_catchup_success_total
            .load(Ordering::Relaxed)
    }

    /// Admin catch-up error counter (Phase 117).
    pub fn cluster_admin_catchup_errors_total(&self) -> u64 {
        self.cluster_admin_catchup_errors_total
            .load(Ordering::Relaxed)
    }

    /// Increment admin catch-up success counter (Phase 117).
    pub fn note_cluster_admin_catchup_success(&self) {
        self.cluster_admin_catchup_success_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment admin catch-up error counter (Phase 117).
    pub fn note_cluster_admin_catchup_error(&self) {
        self.cluster_admin_catchup_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// ISR expand counter (Phase 118).
    pub fn isr_expand_total(&self) -> u64 {
        self.isr_expand_total.load(Ordering::Relaxed)
    }

    /// ISR shrink counter (Phase 118).
    pub fn isr_shrink_total(&self) -> u64 {
        self.isr_shrink_total.load(Ordering::Relaxed)
    }

    /// ISR time-lag shrink counter (Phase 125).
    pub fn isr_time_shrink_total(&self) -> u64 {
        self.isr_time_shrink_total.load(Ordering::Relaxed)
    }

    /// Preferred-replica redirect counter (Phase 126).
    pub fn preferred_replica_redirect_total(&self) -> u64 {
        self.preferred_replica_redirect_total
            .load(Ordering::Relaxed)
    }

    /// Preferred-replica suppress counter (Phase 140): candidate existed but
    /// Fetch did not redirect (e.g. READ_COMMITTED).
    pub fn preferred_replica_suppressed_total(&self) -> u64 {
        self.preferred_replica_suppressed_total
            .load(Ordering::Relaxed)
    }

    /// Preferred-replica session suppress counter (Phase 144): candidate existed
    /// but Fetch did not redirect because the client already has a fetch session.
    pub fn preferred_replica_session_suppressed_total(&self) -> u64 {
        self.preferred_replica_session_suppressed_total
            .load(Ordering::Relaxed)
    }

    /// Rack-aware partition assignment counter (Phase 145): create-topic /
    /// create-partitions used multi-rack diversity placement.
    pub fn rack_aware_assignment_total(&self) -> u64 {
        self.rack_aware_assignment_total.load(Ordering::Relaxed)
    }

    /// Max `leader_leo − follower_leo` for preferred eligibility (Phase 140).
    /// `u64::MAX` = unlimited.
    pub fn preferred_replica_max_leo_lag(&self) -> u64 {
        self.preferred_replica_max_leo_lag.load(Ordering::Relaxed)
    }

    /// Phase 140: runtime max LEO lag for tests / operator tooling.
    pub fn set_preferred_replica_max_leo_lag(&self, max_lag: u64) {
        self.preferred_replica_max_leo_lag
            .store(max_lag, Ordering::Relaxed);
    }

    /// Optional rack for a configured broker (cluster.toml); `None` single-node or unset.
    pub fn broker_rack(&self, broker_id: u32) -> Option<String> {
        self.cluster
            .as_ref()?
            .config
            .read()
            .broker(broker_id)
            .and_then(|b| b.rack.clone())
    }

    /// Phase 126 + 133 + 140: select a preferred read replica for consumer Fetch
    /// (KIP-392 subset).
    ///
    /// Returns a **follower** broker id in the same rack as `client_rack` that is
    /// currently in the local ISR, **live**, has a usable configured address
    /// (`broker_addr` present and non-empty), has observed LEO ≥ HWM, and
    /// (Phase 140) optional `leader_leo − follower_leo ≤ max_leo_lag`, when
    /// this node is the partition leader.
    ///
    /// **Ranking (Phase 133):** among eligible peers prefer **highest follower
    /// LEO**, then **lowest broker id** as tiebreak (replaces pure min-id-only).
    ///
    /// Empty/`None` rack, single-node, non-leader, or no eligible peer → `None`
    /// (caller leaves PreferredReadReplica = -1).
    pub fn select_preferred_read_replica(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        client_rack: Option<&str>,
    ) -> Option<u32> {
        let rack = client_rack.map(str::trim).filter(|s| !s.is_empty())?;
        let cluster = self.cluster.as_ref()?;
        let topics = self.topics.read();
        let t = topics.get(topic)?;
        let part = t.partitions.get(&partition)?;
        if !part.is_leader(self.node_id) {
            return None;
        }
        let hwm = part.committed_hwm;
        let leader_leo = part.leo();
        let max_lag = self.preferred_replica_max_leo_lag.load(Ordering::Relaxed);
        let live = self.live_brokers();
        // (leo, id) — rank by leo desc, id asc (Phase 133).
        let mut best: Option<(u64, u32)> = None;
        for &id in &part.isr {
            if id == self.node_id {
                continue;
            }
            if !live.contains(&id) {
                continue;
            }
            // Usable endpoint gate (Phase 133): skip peers with no resolvable
            // configured address (missing broker, empty host, or empty addr).
            let cfg = cluster.config.read();
            let usable = cfg
                .broker(id)
                .map(|b| !b.host.trim().is_empty() && b.port != 0)
                .unwrap_or(false)
                && self
                    .broker_addr(id)
                    .map(|a| !a.trim().is_empty())
                    .unwrap_or(false);
            if !usable {
                continue;
            }
            let same_rack = cfg
                .broker(id)
                .and_then(|b| b.rack.as_deref())
                .map(|r| r.trim() == rack)
                .unwrap_or(false);
            if !same_rack {
                continue;
            }
            // Require observed LEO ≥ HWM so the follower can serve committed data.
            let Some(leo) = part.follower_leo.get(&id).copied() else {
                continue;
            };
            if leo < hwm {
                continue;
            }
            // Phase 140: optional max lag vs leader LEO (unset → unlimited).
            if leader_leo.saturating_sub(leo) > max_lag {
                continue;
            }
            match best {
                None => best = Some((leo, id)),
                Some((best_leo, best_id)) => {
                    // Highest LEO wins; lowest id breaks ties.
                    if leo > best_leo || (leo == best_leo && id < best_id) {
                        best = Some((leo, id));
                    }
                }
            }
        }
        best.map(|(_, id)| id)
    }

    pub(crate) fn note_preferred_replica_redirect(&self) {
        self.preferred_replica_redirect_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_preferred_replica_suppressed(&self) {
        self.preferred_replica_suppressed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_preferred_replica_session_suppressed(&self) {
        self.preferred_replica_session_suppressed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Effective `replica_lag_max_ms` with optional `VOLANT_REPLICA_LAG_MAX_MS` override.
    pub fn effective_replica_lag_max_ms(&self) -> u64 {
        if let Ok(s) = std::env::var("VOLANT_REPLICA_LAG_MAX_MS") {
            if let Ok(v) = s.parse::<u64>() {
                return v;
            }
        }
        self.cluster
            .as_ref()
            .map(|c| c.config.read().replica_lag_max_ms)
            .unwrap_or(0)
    }

    pub(super) fn note_isr_delta(&self, before: &[u32], after: &[u32]) {
        let mut expand = 0u64;
        let mut shrink = 0u64;
        for &id in after {
            if !before.contains(&id) {
                expand += 1;
            }
        }
        for &id in before {
            if !after.contains(&id) {
                shrink += 1;
            }
        }
        if expand > 0 {
            self.isr_expand_total.fetch_add(expand, Ordering::Relaxed);
        }
        if shrink > 0 {
            self.isr_shrink_total.fetch_add(shrink, Ordering::Relaxed);
        }
    }

    pub(super) fn note_isr_time_shrink(&self, n: u64) {
        if n > 0 {
            self.isr_time_shrink_total.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Whether a peer's applied gens lag controller SoT (Phase 117).
    pub fn peer_admin_gens_lag(
        &self,
        peer_applied_config: u64,
        peer_applied_acl: u64,
    ) -> (bool, bool) {
        let need_config =
            self.config_generation() > 0 && peer_applied_config < self.config_generation();
        let need_acl = self.acl_generation() > 0 && peer_applied_acl < self.acl_generation();
        (need_config, need_acl)
    }

    /// Load durable admin generations from `{data_dir}/__cluster_admin` (Phase 117).
    pub(super) fn load_cluster_admin_gens(&self) -> Result<()> {
        let store = ClusterAdminStore::open(&self.storage.data_dir)?;
        let file = store.load()?;
        self.config_generation
            .store(file.config_generation, Ordering::SeqCst);
        self.applied_config_generation
            .store(file.applied_config_generation, Ordering::SeqCst);
        self.acl_generation
            .store(file.acl_generation, Ordering::SeqCst);
        self.applied_acl_generation
            .store(file.applied_acl_generation, Ordering::SeqCst);
        Ok(())
    }

    /// Persist current admin generation atomics (Phase 117).
    pub fn persist_cluster_admin_gens(&self) {
        let file = ClusterAdminFile {
            version: crate::cluster_admin::CLUSTER_ADMIN_FILE_VERSION,
            config_generation: self.config_generation.load(Ordering::SeqCst),
            applied_config_generation: self.applied_config_generation.load(Ordering::SeqCst),
            acl_generation: self.acl_generation.load(Ordering::SeqCst),
            applied_acl_generation: self.applied_acl_generation.load(Ordering::SeqCst),
        };
        match ClusterAdminStore::open(&self.storage.data_dir) {
            Ok(store) => {
                if let Err(e) = store.save(&file) {
                    warn!(error = %e, "persist cluster admin generations failed");
                }
            }
            Err(e) => {
                warn!(error = %e, "open cluster admin store failed");
            }
        }
    }

    /// Peer targets for DeleteRecords fan-out: `(broker_id, addr, leader_epoch)`.
    ///
    /// Empty in single-node mode or when this node is not the partition leader /
    /// does not know the partition. Phase 113 PR2.
    pub fn delete_records_fanout_peers(
        &self,
        topic: &str,
        partition: u32,
    ) -> Vec<(u32, String, i32)> {
        if self.cluster.is_none() {
            return Vec::new();
        }
        let name = TopicName::new(topic);
        let topics = self.topics.read();
        let Some(t) = topics.get(&name) else {
            return Vec::new();
        };
        let Some(part) = t.partitions.get(&PartitionId(partition)) else {
            return Vec::new();
        };
        if !part.is_leader(self.node_id) {
            return Vec::new();
        }
        let epoch = part.leader_epoch as i32;
        let mut out = Vec::new();
        for &id in &part.replicas {
            if id == self.node_id {
                continue;
            }
            if let Some(addr) = self.broker_addr(id) {
                out.push((id, addr, epoch));
            }
        }
        out
    }

    /// Apply inter-broker `ReplicaDeleteRecords` (Phase 113 PR2).
    ///
    /// Truncates local log prefix (whole sealed segments) and runs Phase 104/111
    /// soft-marker GC/clip. Epoch fence: [`EpochFenceMode::AllowUnknown`]
    /// (`leader_epoch < 0` skips; stale local>req → InvalidProducerEpoch).
    ///
    /// Returns `(error_code, low_watermark)`.
    pub fn handle_replica_delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        leader_epoch: i32,
    ) -> (u16, u64) {
        let name = TopicName::new(topic);
        let low = {
            let mut topics = self.topics.write();
            let Some(t) = topics.get_mut(&name) else {
                return (ErrorCode::NotFound as u16, 0);
            };
            let Some(part) = t.partitions.get_mut(&PartitionId(partition)) else {
                return (ErrorCode::NotFound as u16, 0);
            };
            if let Err(code) = fence_leader_epoch(
                part.leader_epoch,
                leader_epoch,
                EpochFenceMode::AllowUnknown,
            ) {
                return (code as u16, part.log.log_start_offset().raw());
            }
            match part.log.delete_records(Offset::new(before_offset)) {
                Ok(off) => off.raw(),
                Err(e) => {
                    warn!(
                        topic,
                        partition,
                        before_offset,
                        error = %e,
                        "replica delete_records failed"
                    );
                    return (ErrorCode::Storage as u16, part.log.log_start_offset().raw());
                }
            }
        };
        self.gc_and_persist_aborted_markers(topic, partition, low);
        (0, low)
    }
}
