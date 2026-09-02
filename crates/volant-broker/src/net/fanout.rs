//! Inter-broker fan-out RPCs (assignment, metadata raft, delete records, 2PC).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, warn};
use volant_core::{Error, Result};
use volant_protocol::{ErrorCode, Request, Response};

use crate::broker::{Broker, Txn2pcFanout};
use crate::cluster::{
    AssignmentConsensus, AssignmentSnapshot, MetadataCommand, MetadataLogEntry, MetadataRaftState,
};
use crate::truncate_journal::TruncateJournal;
use volant_protocol::{metadata_raft_cmd, MetadataRaftLogEntry};

use super::rpc::{
    delete_records_fanout_budget, inter_broker_rpc, inter_broker_rpc_owned,
    inter_broker_rpc_timeout,
};

/// Phase 134: peer-to-peer heartbeat mesh (MVP).
///
/// Each tick:
/// - if self is the current controller, self-touch membership locally
/// - send [`Request::HeartbeatBroker`] to every other configured broker that
///   has an address (sequential, per-peer `inter_broker_rpc` timeout)
/// - on successful response: always [`Broker::note_peer_live`]
/// - **only** if the peer contacted is the current controller: apply the
///   controller alive-set + optional ClusterState pull (Phase 110/117 path)
///
/// Non-controller responses must never drive [`Broker::apply_controller_alive_set`]
/// — partial alive lists could shrink ISR incorrectly.
pub(super) async fn heartbeat_mesh(broker: &Broker) -> Result<()> {
    let controller = broker.controller_id();
    // Controller still self-touches membership locally (existing path).
    if controller == broker.node_id() {
        let _ = broker.handle_heartbeat_broker(broker.node_id(), controller, broker.generation());
    }

    let Some(cfg) = broker.cluster_config() else {
        return Ok(());
    };
    let self_id = broker.node_id();
    let req = Request::HeartbeatBroker {
        broker_id: self_id,
        controller_id_known: controller,
        generation: broker.generation(),
        // Phase 117: report applied admin gens so controller can catch up lag.
        applied_config_generation: broker.applied_config_generation(),
        applied_acl_generation: broker.applied_acl_generation(),
        // Phase 131: report applied truncate-journal gen for rejoin catch-up.
        applied_journal_generation: broker.truncate_journal_applied_generation(),
    };

    for peer_id in cfg.broker_ids() {
        if peer_id == self_id {
            continue;
        }
        let Some(addr) = broker.broker_addr(peer_id) else {
            continue;
        };
        if let Err(e) = heartbeat_to_peer(broker, peer_id, &addr, &req).await {
            debug!(peer_id, error = %e, "peer heartbeat failed");
        }
    }
    Ok(())
}

/// Heartbeat one peer. Alive-set / ClusterState apply only when `peer_id` is
/// the current controller.
async fn heartbeat_to_peer(broker: &Broker, peer_id: u32, addr: &str, req: &Request) -> Result<()> {
    if broker.test_inter_broker_blocked() {
        return Err(Error::Protocol("inter-broker rpc blocked".into()));
    }
    let resp = inter_broker_rpc(broker, addr, req).await?;
    match resp {
        Response::HeartbeatBroker {
            controller_id,
            generation,
            alive_brokers,
            ..
        } => {
            // Always mark the peer we reached as live (mesh liveness).
            broker.note_peer_live(peer_id);

            // Critical correctness (Phase 134): only trust alive-set / SoT
            // pull from the *current* controller. Non-controller peers may
            // return a partial local membership view.
            if peer_id == broker.controller_id() {
                // Phase 110: diff controller alive-set → on_broker_death for gaps
                // (local ISR shrink) before refreshing live peers.
                broker.apply_controller_alive_set(&alive_brokers)?;
                // Ensure the peer we reached (reported controller) stays live even
                // if a stale response omitted it from alive_brokers.
                broker.note_peer_live(controller_id);
                // Pull ClusterState if generation advanced.
                if generation > broker.generation() {
                    let cs_req = Request::ClusterState {
                        known_generation: broker.generation(),
                    };
                    if let Ok(cs_resp) = inter_broker_rpc(broker, addr, &cs_req).await {
                        if let Response::ClusterState {
                            generation: g,
                            controller_id: c,
                            topics,
                            ..
                        } = cs_resp
                        {
                            broker.apply_cluster_state(g, c, &topics)?;
                        }
                    }
                }
            }
            Ok(())
        }
        other => Err(Error::Protocol(format!(
            "unexpected heartbeat response from peer {peer_id}: {other:?}"
        ))),
    }
}

/// Phase 131: re-push full truncate journal snapshot to one lagging peer.
///
/// Any node with a newer journal may push (multi-controller). Uses opcode 88
/// `TruncateJournalPush`. Increments journal catch-up success/error metrics.
/// No-op when peer does not lag or local journal is empty.
///
/// Prefer [`schedule_catch_up_peer_truncate_journal`] from the HeartbeatBroker
/// path (Phase 132) so membership is not stalled by the RPC. This direct API
/// remains for tests and explicit callers.
pub async fn catch_up_peer_truncate_journal(
    broker: &Broker,
    peer_id: u32,
    peer_addr: &str,
    peer_applied_journal: u64,
) {
    if !broker.peer_journal_gen_lags(peer_applied_journal) {
        return;
    }
    let generation = broker.truncate_journal_generation();
    let snapshot = broker.truncate_journal().snapshot_bytes();
    let req = Request::TruncateJournalPush {
        generation,
        snapshot,
    };
    match inter_broker_rpc(broker, peer_addr, &req).await {
        Ok(Response::TruncateJournalPush { error_code: 0 }) => {
            broker.truncate_journal().note_journal_catchup_success();
            debug!(
                peer_id,
                %peer_addr,
                generation,
                peer_applied_journal,
                "truncate journal catch-up push ok"
            );
        }
        Ok(Response::TruncateJournalPush { error_code }) => {
            warn!(
                peer_id,
                %peer_addr,
                error_code,
                generation,
                peer_applied_journal,
                "truncate journal catch-up peer error"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
        Ok(other) => {
            warn!(
                peer_id,
                %peer_addr,
                ?other,
                generation,
                "truncate journal catch-up unexpected response"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
        Err(e) => {
            warn!(
                peer_id,
                %peer_addr,
                error = %e,
                generation,
                "truncate journal catch-up rpc failed"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
    }
}

/// Phase 132: schedule a non-blocking journal catch-up for a lagging peer.
///
/// Claims per-peer single-flight + min-interval throttle via
/// [`Broker::try_begin_journal_catchup`], then spawns a task that runs
/// [`catch_up_peer_truncate_journal`] and releases the claim. Returns
/// immediately so HeartbeatBroker membership is not stalled by the push RPC.
///
/// No-op when the peer does not lag or the schedule is throttled / already
/// in-flight (skipped metric increments on throttle).
pub fn schedule_catch_up_peer_truncate_journal(
    broker: Arc<Broker>,
    peer_id: u32,
    peer_addr: String,
    peer_applied_journal: u64,
) {
    if !broker.peer_journal_gen_lags(peer_applied_journal) {
        return;
    }
    if !broker.try_begin_journal_catchup(peer_id) {
        debug!(
            peer_id,
            peer_applied_journal,
            "truncate journal catch-up schedule skipped (in-flight or throttle)"
        );
        return;
    }
    tokio::spawn(async move {
        // Bound overall work; inter_broker_rpc already has its own timeout.
        // An extra outer timeout ensures finish_journal_catchup always runs.
        let timeout = inter_broker_rpc_timeout() + Duration::from_secs(1);
        let result = tokio::time::timeout(
            timeout,
            catch_up_peer_truncate_journal(&broker, peer_id, &peer_addr, peer_applied_journal),
        )
        .await;
        if result.is_err() {
            warn!(
                peer_id,
                %peer_addr,
                peer_applied_journal,
                "truncate journal catch-up timed out"
            );
            broker.truncate_journal().note_journal_catchup_error();
        }
        broker.finish_journal_catchup(peer_id);
    });
}

/// Phase 117: re-push controller ACL + BROKER config SoT to one lagging peer.
///
/// Uses Phase 113 opcodes. Increments catch-up success/error metrics. No-op when
/// this node is not the controller or when neither domain lags.
///
/// Prefer [`schedule_catch_up_peer_admin_state`] from the HeartbeatBroker path
/// (Phase 136) so membership is not stalled by up to two RPCs. This direct API
/// remains for tests and explicit callers.
pub async fn catch_up_peer_admin_state(
    broker: &Broker,
    peer_id: u32,
    peer_addr: &str,
    peer_applied_config: u64,
    peer_applied_acl: u64,
) {
    if !broker.is_controller() {
        return;
    }
    let (need_config, need_acl) = broker.peer_admin_gens_lag(peer_applied_config, peer_applied_acl);
    if !need_config && !need_acl {
        return;
    }

    if need_config {
        let generation = broker.config_generation();
        let entries = broker.describe_broker_configs();
        let req = Request::ClusterBrokerConfig {
            generation,
            entries,
        };
        match inter_broker_rpc(broker, peer_addr, &req).await {
            Ok(Response::ClusterBrokerConfig { error_code: 0, .. }) => {
                broker.note_cluster_admin_catchup_success();
            }
            Ok(Response::ClusterBrokerConfig {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error_code,
                    applied_generation,
                    generation,
                    "admin config catch-up peer error"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    ?other,
                    generation,
                    "admin config catch-up unexpected response"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error = %e,
                    generation,
                    "admin config catch-up rpc failed"
                );
                broker.note_cluster_admin_catchup_error();
            }
        }
    }

    if need_acl {
        let generation = broker.acl_generation();
        let snapshot = match broker.acl_snapshot_wire_bytes() {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    peer_id,
                    generation,
                    error = %e,
                    "admin acl catch-up encode failed"
                );
                broker.note_cluster_admin_catchup_error();
                return;
            }
        };
        let req = Request::ClusterAclSnapshot {
            generation,
            snapshot,
        };
        match inter_broker_rpc(broker, peer_addr, &req).await {
            Ok(Response::ClusterAclSnapshot { error_code: 0, .. }) => {
                broker.note_cluster_admin_catchup_success();
            }
            Ok(Response::ClusterAclSnapshot {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error_code,
                    applied_generation,
                    generation,
                    "admin acl catch-up peer error"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    ?other,
                    generation,
                    "admin acl catch-up unexpected response"
                );
                broker.note_cluster_admin_catchup_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %peer_addr,
                    error = %e,
                    generation,
                    "admin acl catch-up rpc failed"
                );
                broker.note_cluster_admin_catchup_error();
            }
        }
    }
}

/// Phase 136: schedule a non-blocking admin (ACL/config) catch-up for a lagging peer.
///
/// Claims per-peer single-flight + min-interval throttle via
/// [`Broker::try_begin_admin_catchup`], then spawns a task that runs
/// [`catch_up_peer_admin_state`] and releases the claim. Returns immediately so
/// HeartbeatBroker membership is not stalled by config/ACL re-push RPCs.
///
/// No-op when this node is not the controller, the peer does not lag, or the
/// schedule is throttled / already in-flight (skipped metric increments on
/// throttle).
pub fn schedule_catch_up_peer_admin_state(
    broker: Arc<Broker>,
    peer_id: u32,
    peer_addr: String,
    peer_applied_config: u64,
    peer_applied_acl: u64,
) {
    if !broker.is_controller() {
        return;
    }
    let (need_config, need_acl) = broker.peer_admin_gens_lag(peer_applied_config, peer_applied_acl);
    if !need_config && !need_acl {
        return;
    }
    if !broker.try_begin_admin_catchup(peer_id) {
        debug!(
            peer_id,
            peer_applied_config,
            peer_applied_acl,
            "admin catch-up schedule skipped (in-flight or throttle)"
        );
        return;
    }
    tokio::spawn(async move {
        // Admin catch-up may run up to 2 RPCs (config + ACL); outer bound is
        // 2× inter_broker timeout + 1s so finish_admin_catchup always runs.
        let timeout = inter_broker_rpc_timeout() * 2 + Duration::from_secs(1);
        let result = tokio::time::timeout(
            timeout,
            catch_up_peer_admin_state(
                &broker,
                peer_id,
                &peer_addr,
                peer_applied_config,
                peer_applied_acl,
            ),
        )
        .await;
        if result.is_err() {
            warn!(
                peer_id,
                %peer_addr,
                peer_applied_config,
                peer_applied_acl,
                "admin catch-up timed out"
            );
            broker.note_cluster_admin_catchup_error();
        }
        broker.finish_admin_catchup(peer_id);
    });
}

/// Best-effort ACL snapshot fan-out to live peers (Phase 113 PR4).
///
/// Called after a successful **controller** ACL mutate. Loads the current
/// durable snapshot from the controller and pushes it with `generation`.
/// Failures increment [`Broker::cluster_acl_push_errors_total`].
pub async fn fanout_cluster_acl_snapshot(broker: &Broker, generation: u64) {
    let peers = broker.cluster_acl_fanout_peers();
    if peers.is_empty() {
        return;
    }
    let snapshot = match broker.acl_snapshot_wire_bytes() {
        Ok(b) => b,
        Err(e) => {
            warn!(generation, error = %e, "acl snapshot encode for fan-out failed");
            broker.note_cluster_acl_push_error();
            return;
        }
    };
    let req = Request::ClusterAclSnapshot {
        generation,
        snapshot,
    };
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::ClusterAclSnapshot { error_code: 0, .. }) => {}
            Ok(Response::ClusterAclSnapshot {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    applied_generation,
                    generation,
                    "cluster acl fan-out peer error"
                );
                broker.note_cluster_acl_push_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    generation,
                    "cluster acl fan-out unexpected response"
                );
                broker.note_cluster_acl_push_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    generation,
                    "cluster acl fan-out rpc failed"
                );
                broker.note_cluster_acl_push_error();
            }
        }
    }
}

/// Best-effort BROKER config fan-out to live peers (Phase 113 PR3).
///
/// Called after a successful **controller** [`Broker::alter_broker_configs`].
/// Failures increment [`Broker::cluster_config_push_errors_total`] and never
/// fail the client path. No-op when there are no peers.
pub async fn fanout_cluster_broker_config(
    broker: &Broker,
    generation: u64,
    entries: &[(String, String)],
) {
    let peers = broker.cluster_broker_config_fanout_peers();
    if peers.is_empty() {
        return;
    }
    let req = Request::ClusterBrokerConfig {
        generation,
        entries: entries.to_vec(),
    };
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::ClusterBrokerConfig { error_code: 0, .. }) => {}
            Ok(Response::ClusterBrokerConfig {
                error_code,
                applied_generation,
            }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    applied_generation,
                    generation,
                    "cluster broker config fan-out peer error"
                );
                broker.note_cluster_config_push_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    generation,
                    "cluster broker config fan-out unexpected response"
                );
                broker.note_cluster_config_push_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    generation,
                    "cluster broker config fan-out rpc failed"
                );
                broker.note_cluster_config_push_error();
            }
        }
    }
}

/// Best-effort membership overlay push (v0.10). No majority wait.
pub async fn fanout_membership_put(broker: &Broker) {
    let Some(cfg) = broker.cluster_config() else {
        return;
    };
    let generation = broker.membership_generation();
    if generation == 0 {
        return;
    }
    let brokers: Vec<volant_protocol::MembershipBroker> = cfg
        .brokers
        .iter()
        .map(|b| volant_protocol::MembershipBroker {
            id: b.id,
            host: b.host.clone(),
            port: b.port,
            rack: b.rack.clone(),
        })
        .collect();
    let req = Request::MembershipPut {
        generation,
        brokers,
    };
    for (peer_id, addr) in broker.membership_fanout_peers() {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::MembershipPut { error_code: 0, .. }) => {}
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    generation,
                    "membership put fan-out peer error"
                );
            }
            Err(e) => {
                debug!(peer_id, %addr, error = %e, generation, "membership put fan-out rpc failed");
            }
        }
    }
}

/// Phase 129/130/135: multi-controller majority note + best-effort full-snapshot push.
///
/// 1. Durable **local** note (counts as 1 ack).
/// 2. **Parallel** `TruncateJournalNote` to all other live peers (`JoinSet`).
/// 3. If acks ≥ majority(configured N) → consensus success metric.
/// 4. Best-effort **parallel** `TruncateJournalPush` to **all** live peers
///    (full journal snapshot) so multi-key catch-up works even when a peer
///    acked the single-key note.
///
/// Returns `true` when there is **no cluster** or acks ≥ majority(configured N).
/// Not full Raft log/leader election. Client visibility of majority is gated by
/// [`Broker::delete_records_wait_majority`] (Phase 135; default off).
pub async fn fanout_truncate_journal_note(
    broker: &Broker,
    topic: &str,
    partition: u32,
    before_offset: u64,
    leader_epoch: i32,
) -> bool {
    if broker.cluster_config().is_none() {
        return true;
    }
    let n = broker.cluster_member_count();
    let need = TruncateJournal::majority(n);

    // 1) Local durable note (proposer).
    let local_gen =
        broker.local_note_truncate_journal(topic, partition, before_offset, leader_epoch);
    let mut acks = 1usize;

    // 2) Parallel note to every other live peer (multi-controller).
    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|id| *id != broker.node_id())
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    let peer_ids: Vec<u32> = peers.iter().map(|(id, _)| *id).collect();

    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (peer_id, addr) in peers {
        let req = Request::TruncateJournalNote {
            topic: topic.to_owned(),
            partition,
            before_offset,
            leader_epoch,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (peer_id, res)
        });
    }

    let mut max_gen = local_gen;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((
                _peer_id,
                Ok(Response::TruncateJournalNote {
                    error_code: 0,
                    generation,
                }),
            )) => {
                acks += 1;
                if generation > max_gen {
                    max_gen = generation;
                }
            }
            Ok((peer_id, Ok(Response::TruncateJournalNote { error_code, .. }))) => {
                warn!(
                    peer_id,
                    error_code, topic, partition, "truncate journal note peer error"
                );
            }
            Ok((peer_id, Ok(other))) => {
                warn!(
                    peer_id,
                    ?other,
                    topic,
                    partition,
                    "truncate journal note unexpected"
                );
            }
            Ok((peer_id, Err(e))) => {
                warn!(
                    peer_id,
                    error = %e,
                    topic,
                    partition,
                    "truncate journal note rpc failed"
                );
            }
            Err(e) => {
                warn!(error = %e, topic, partition, "truncate journal note join error");
            }
        }
    }

    let majority_ok = acks >= need;
    if majority_ok {
        broker.truncate_journal().note_consensus_success();
        debug!(
            acks,
            need, n, topic, partition, before_offset, "truncate journal majority consensus ok"
        );
    } else {
        broker.truncate_journal().note_consensus_fail();
        warn!(
            acks,
            need,
            n,
            topic,
            partition,
            before_offset,
            "truncate journal majority consensus failed (best-effort state retained)"
        );
    }

    // Always full-snapshot push to live peers so multi-key journal catch-up works.
    let push_peers: Vec<(u32, String)> = peer_ids
        .into_iter()
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    fanout_truncate_journal_push_to(broker, max_gen.max(local_gen), push_peers).await;
    majority_ok
}

/// Phase 150/152: majority assignment consensus note fan-out.
///
/// 1. Local ack (controller already has assignment) + set pending generation.
/// 2. Parallel `AssignmentConsensusNote` to live peers with full wire topics.
/// 3. Peer applies via `apply_cluster_state` when generation ≥ local.
/// 4. Majority of **configured N** → durable `committed_generation` + committed
///    assignment snapshot (Phase 152 Metadata SoT) + success metric; else fail
///    metric (local assignment retained here; wait/committed-only handlers
///    may restore live `assignment.json`).
///
/// Returns `true` when there is **no cluster**, consensus is **disabled**, or
/// acks ≥ majority(configured N). Client wait is gated by
/// [`Broker::assignment_consensus_wait`] or
/// [`Broker::assignment_metadata_committed_only`] (Phase 152 forces wait-like
/// admin visibility when Metadata is committed-only).
pub async fn fanout_assignment_consensus(broker: &Broker) -> bool {
    if broker.cluster_config().is_none() {
        // Single-node: trivial majority 1 — mark local gen committed.
        let gen = broker.generation();
        broker.assignment_consensus().commit(gen.max(0));
        return true;
    }
    if !broker.assignment_consensus_enabled() {
        return true;
    }

    let n = broker.cluster_member_count();
    let need = AssignmentConsensus::majority(n);
    let (error, generation, controller_id, topics) = broker.cluster_state_snapshot();
    let _ = error;
    broker.assignment_consensus().set_pending(generation);

    // Local ack (controller already holds the assignment).
    let mut acks = 1usize;

    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|id| *id != broker.node_id())
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();

    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (peer_id, addr) in peers {
        let req = Request::AssignmentConsensusNote {
            generation,
            controller_id,
            topics: topics.clone(),
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (peer_id, res)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((
                _peer_id,
                Ok(Response::AssignmentConsensusNote {
                    error_code: 0,
                    generation: _,
                }),
            )) => {
                acks += 1;
            }
            Ok((peer_id, Ok(Response::AssignmentConsensusNote { error_code, .. }))) => {
                warn!(
                    peer_id,
                    error_code, generation, "assignment consensus note peer error"
                );
            }
            Ok((peer_id, Ok(other))) => {
                warn!(
                    peer_id,
                    ?other,
                    generation,
                    "assignment consensus unexpected"
                );
            }
            Ok((peer_id, Err(e))) => {
                warn!(
                    peer_id,
                    error = %e,
                    generation,
                    "assignment consensus note rpc failed"
                );
            }
            Err(e) => {
                warn!(error = %e, generation, "assignment consensus join error");
            }
        }
    }

    let majority_ok = acks >= need;
    if majority_ok {
        broker.assignment_consensus().commit(generation);
        // Phase 152: durable committed assignment snapshot for Metadata SoT.
        if let Some(cluster) = broker.cluster_state() {
            let snap = cluster.assignment.read().clone();
            broker
                .assignment_consensus()
                .note_committed_snapshot(generation, &snap);
        }
        debug!(
            acks,
            need, n, generation, "assignment majority consensus ok"
        );
    } else {
        broker.assignment_consensus().note_fail();
        warn!(
            acks,
            need, n, generation, "assignment majority consensus failed (local assignment retained here; wait/committed-only handlers may restore live assignment.json)"
        );
    }
    majority_ok
}

/// After a successful controller assignment mutation: best-effort (or wait)
/// majority consensus. Returns `None` when consensus is disabled / not needed
/// for the client response; `Some(false)` when majority failed and
/// [`Broker::assignment_must_wait`] is on (Phase 150 wait, Phase 152
/// committed-only, or v0.40 homemade 154 wait-commit).
///
/// v0.16: when `VOLANT_OPENRAFT_METADATA` is on, prefer openraft
/// `SetAssignment` (`client_write`, opcodes 108/109) over homemade 154
/// and Phase 150 notes. Wait off → still write/apply with a timeout
/// (best-effort; client success does not depend on the result).
///
/// Phase 154: when metadata Raft is enabled (and openraft is off), prefers
/// [`fanout_metadata_raft_append`] over Phase 150 notes. v0.40 wait-commit
/// (default **on**) requires `commit_index` to cover the new entry before
/// client ok; `VOLANT_METADATA_RAFT_WAIT_COMMIT=0` keeps 154 mutate-first.
pub async fn maybe_fanout_assignment_consensus(broker: &Broker) -> Option<bool> {
    if broker.cluster_config().is_none() {
        return None;
    }
    // v0.16: prefer openraft assignment apply when the flag is on.
    if broker.openraft_metadata_enabled() {
        let ok = broker.client_write_set_assignment().await;
        let must_wait = broker.assignment_must_wait();
        return if must_wait { Some(ok) } else { None };
    }
    // Phase 154: prefer KRaft-style metadata log when enabled.
    if broker.metadata_raft_enabled() {
        let before = broker.metadata_raft_commit_index();
        let ok = fanout_metadata_raft_append(broker).await;
        let must_wait = broker.assignment_must_wait();
        if must_wait {
            // v0.40: client ok only when commit_index covers the new entry.
            let committed = ok && broker.metadata_raft_commit_index() > before;
            return Some(committed);
        }
        return None;
    }
    if !broker.assignment_consensus_enabled() {
        return None;
    }
    let ok = fanout_assignment_consensus(broker).await;
    // Phase 152: committed-only Metadata forces wait-like admin visibility so
    // create ok cannot race Metadata miss. Completed fan-out with !must_wait
    // is ignored (including 96/97 miss) so handlers do not fail the client.
    // v0.40 wait-commit is inert here (homemade raft is off in this branch).
    if broker.assignment_must_wait() {
        Some(ok)
    } else {
        None
    }
}

/// Clone live assignment when wait / committed-only / homemade wait-commit
/// will fail the client on a miss.
pub fn snapshot_if_must_wait(broker: &Broker) -> Option<AssignmentSnapshot> {
    if broker.assignment_must_wait() {
        broker.clone_live_assignment()
    } else {
        None
    }
}

/// After a successful local assignment mutate. `Ok(true)` = client success.
/// `Ok(false)` = majority miss and live assignment restored (when prev set).
pub async fn complete_assignment_mutation(
    broker: &Broker,
    prev: Option<AssignmentSnapshot>,
) -> Result<bool> {
    let expected_gen = broker.generation();
    if maybe_fanout_assignment_consensus(broker).await == Some(false) {
        if let Some(prev) = prev.as_ref() {
            broker.restore_live_assignment(prev, expected_gen)?;
        }
        return Ok(false);
    }
    Ok(true)
}

/// Convert internal log entry to wire form.
fn metadata_entry_to_wire(e: &MetadataLogEntry) -> MetadataRaftLogEntry {
    match &e.payload {
        MetadataCommand::SetAssignment { generation, topics } => MetadataRaftLogEntry {
            term: e.term,
            index: e.index,
            command_kind: metadata_raft_cmd::SET_ASSIGNMENT,
            generation: *generation,
            topics: topics.clone(),
        },
        MetadataCommand::Noop => MetadataRaftLogEntry {
            term: e.term,
            index: e.index,
            command_kind: metadata_raft_cmd::NOOP,
            generation: 0,
            topics: vec![],
        },
    }
}

/// Convert wire log entry to internal form.
pub(super) fn metadata_entry_from_wire(e: &MetadataRaftLogEntry) -> MetadataLogEntry {
    let payload = match e.command_kind {
        metadata_raft_cmd::SET_ASSIGNMENT => MetadataCommand::SetAssignment {
            generation: e.generation,
            topics: e.topics.clone(),
        },
        _ => MetadataCommand::Noop,
    };
    MetadataLogEntry {
        term: e.term,
        index: e.index,
        payload,
    }
}

/// Phase 154: append current assignment to the metadata Raft log and fan out
/// AppendEntries to live peers. Advances `commit_index` only on majority
/// match_index; then applies committed entries (and Phase 152 committed snap).
///
/// Returns `true` when majority of **configured N** matched the new index
/// (self counts as 1). Single-node / no-cluster: local append+commit.
pub async fn fanout_metadata_raft_append(broker: &Broker) -> bool {
    if broker.cluster_config().is_none() {
        // Single-node: append + commit + apply locally.
        let entry = broker.append_assignment_to_metadata_log();
        broker.metadata_raft().advance_commit(entry.index);
        broker.metadata_raft().note_append_success();
        broker.apply_committed_metadata_entries();
        return true;
    }
    if !broker.metadata_raft_enabled() {
        return true;
    }

    let n = broker.cluster_member_count();
    let need = MetadataRaftState::majority(n);

    // Append local log entry for current live assignment.
    let entry = broker.append_assignment_to_metadata_log();
    let term = entry.term;
    let index = entry.index;
    let prev_log_index = index.saturating_sub(1);
    let prev_log_term = broker.metadata_raft().term_at(prev_log_index);
    // Leader commit before this round (entry not yet committed).
    let leader_commit = broker.metadata_raft().commit_index();
    let wire_entries = vec![metadata_entry_to_wire(&entry)];

    // Local match (leader already has the entry).
    let mut matched = 1usize;
    let mut match_indexes = vec![index];

    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|id| *id != broker.node_id())
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();

    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let leader_id = broker.node_id();
    let mut set = tokio::task::JoinSet::new();
    for (peer_id, addr) in peers {
        let req = Request::MetadataRaftAppend {
            leader_id,
            term,
            prev_log_index,
            prev_log_term,
            entries: wire_entries.clone(),
            leader_commit,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (peer_id, res)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((
                _peer_id,
                Ok(Response::MetadataRaftAppend {
                    term: peer_term,
                    success,
                    match_index,
                }),
            )) => {
                if peer_term > term {
                    warn!(
                        peer_term,
                        term, "metadata raft append: peer has higher term"
                    );
                }
                if success != 0 && match_index >= index {
                    matched += 1;
                    match_indexes.push(match_index);
                }
            }
            Ok((peer_id, Ok(other))) => {
                warn!(peer_id, ?other, index, "metadata raft append unexpected");
            }
            Ok((peer_id, Err(e))) => {
                warn!(
                    peer_id,
                    error = %e,
                    index,
                    "metadata raft append rpc failed"
                );
            }
            Err(e) => {
                warn!(error = %e, index, "metadata raft append join error");
            }
        }
    }

    let majority_ok = matched >= need;
    if majority_ok {
        broker.metadata_raft().advance_commit(index);
        // Heartbeat-style second pass: push updated leader_commit so peers
        // advance commit_index and apply without waiting for the next mutation.
        let new_commit = broker.metadata_raft().commit_index();
        let peers2: Vec<(u32, String)> = broker
            .live_brokers()
            .into_iter()
            .filter(|id| *id != broker.node_id())
            .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
            .collect();
        let auth = broker.auth_token();
        let tls = broker.inter_broker_tls();
        let mut set2 = tokio::task::JoinSet::new();
        for (peer_id, addr) in peers2 {
            let req = Request::MetadataRaftAppend {
                leader_id,
                term,
                prev_log_index: index,
                prev_log_term: term,
                entries: vec![],
                leader_commit: new_commit,
            };
            let auth = auth.clone();
            let tls = tls.clone();
            set2.spawn(async move {
                let _ = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
                peer_id
            });
        }
        while set2.join_next().await.is_some() {}

        broker.metadata_raft().note_append_success();
        broker.apply_committed_metadata_entries();
        debug!(
            matched,
            need,
            n,
            index,
            commit = new_commit,
            "metadata raft majority append ok"
        );
    } else {
        broker.metadata_raft().note_append_fail();
        warn!(
            matched,
            need, n, index, "metadata raft majority append failed (uncommitted entry retained)"
        );
    }
    let _ = match_indexes;
    majority_ok
}

/// Phase 129/130: best-effort **parallel** push of full truncate journal snapshot
/// to all live peers (excluding self).
pub async fn fanout_truncate_journal_push(broker: &Broker, generation: u64) {
    if broker.cluster_config().is_none() {
        return;
    }
    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|id| *id != broker.node_id())
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    fanout_truncate_journal_push_to(broker, generation, peers).await;
}

/// Best-effort parallel push of the full truncate journal snapshot to an
/// explicit peer list.
async fn fanout_truncate_journal_push_to(
    broker: &Broker,
    generation: u64,
    peers: Vec<(u32, String)>,
) {
    if peers.is_empty() {
        return;
    }
    let snapshot = broker.truncate_journal().snapshot_bytes();
    let gen = generation.max(broker.truncate_journal_generation());
    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (peer_id, addr) in peers {
        let req = Request::TruncateJournalPush {
            generation: gen,
            snapshot: snapshot.clone(),
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (peer_id, res)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((_, Ok(Response::TruncateJournalPush { error_code: 0 }))) => {}
            Ok((peer_id, Ok(Response::TruncateJournalPush { error_code }))) => {
                warn!(peer_id, error_code, "truncate journal push peer error");
            }
            Ok((peer_id, Ok(other))) => {
                warn!(peer_id, ?other, "truncate journal push unexpected response");
            }
            Ok((peer_id, Err(e))) => {
                warn!(peer_id, error = %e, "truncate journal push rpc failed");
            }
            Err(e) => {
                warn!(error = %e, "truncate journal push join error");
            }
        }
    }
}

/// Result of [`fanout_delete_records`] (Phase 135).
///
/// `majority_ok` reflects **truncate-journal majority** only (not replica log
/// truncate / outbox). Single-node and note-skipped (not leader) paths report
/// `true` so wait-mode does not false-fail the client for those cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRecordsFanoutResult {
    /// Whether journal majority was reached (or no cluster / note skipped).
    pub majority_ok: bool,
}

/// Phase 148: journal majority note **before** local truncate (wait mode).
///
/// Uses current [`Broker::led_partition_epoch`]. On majority **fail**, rolls
/// back the provisional local journal watermark to `prev` so
/// [`Broker::reconcile_delete_records_outbox`] does not auto-apply a
/// non-majority note. Peer-side provisional notes from partial acks remain a
/// best-effort residual (max-merge).
///
/// Single-node / no cluster / not-leader → `true` (caller handles NotLeader).
pub async fn fanout_truncate_journal_note_provisional(
    broker: &Broker,
    topic: &str,
    partition: u32,
    before_offset: u64,
) -> bool {
    if broker.cluster_config().is_none() {
        return true;
    }
    let Some(epoch) = broker.led_partition_epoch(topic, partition) else {
        debug!(
            topic,
            partition, before_offset, "skip provisional journal note: not partition leader"
        );
        return true;
    };
    let prev = broker
        .truncate_journal()
        .entry(topic, partition)
        .map(|e| (e.before_offset, e.leader_epoch));
    let ok = fanout_truncate_journal_note(broker, topic, partition, before_offset, epoch).await;
    if !ok {
        // Undo provisional local watermark so reconcile will not truncate.
        broker.truncate_journal().remove_partition(topic, partition);
        if let Some((off, ep)) = prev {
            let _ = broker.local_note_truncate_journal(topic, partition, off, ep);
        }
    }
    ok
}

/// Best-effort DeleteRecords fan-out to other replicas (Phase 113/116 + 129/130/135).
///
/// After a successful **leader** local truncate: multi-controller journal note
/// (majority + full-snapshot push), then **parallel** `ReplicaDeleteRecords` to
/// peers. Peers are **pre-enqueued** on the durable outbox before the JoinSet
/// so a budget abort or join failure cannot lose retry state; successful /
/// fenced peers are `drop_entry`'d.
///
/// Returns [`DeleteRecordsFanoutResult`]; default client path still ignores
/// `majority_ok` unless wait mode is on (Phase 135/148). **Wait-off** path
/// remains local-first then this fan-out. **Wait-on** (Phase 148) uses
/// [`fanout_truncate_journal_note_provisional`] first, then local truncate, then
/// [`fanout_delete_records_replicas_only`] (journal already noted).
///
/// `truncate_to` is the **achieved** log start (whole-segment-clamped low
/// watermark), not the client-requested offset.
///
/// Journal majority is evaluated **before** the remaining budget is applied to
/// replica fan-out, so a slow peer log-truncate cannot flip `majority_ok`.
/// Overall deadline: [`delete_records_fanout_budget`] (default **20s**, or at
/// least `3 *` [`inter_broker_rpc_timeout`] `+ 2s` when env unset). Each peer
/// RPC is still bounded by [`inter_broker_rpc_timeout`] (default **5s**).
pub async fn fanout_delete_records(
    broker: &Broker,
    topic: &str,
    partition: u32,
    truncate_to: u64,
) -> DeleteRecordsFanoutResult {
    let budget = delete_records_fanout_budget();
    let start = std::time::Instant::now();

    // Phase 129/130/135: journal note first so majority_ok is known even if
    // subsequent ReplicaDeleteRecords hits the remaining budget.
    // Only stamp while we still lead — never send leader_epoch=-1 (ingress
    // rejects negative epochs for non-zero watermarks). Leadership loss after
    // local truncate skips the note; the new leader reconcile uses log_start.
    // Prefer majority_ok=true when note is skipped (not leader) — client already
    // got local success or NotLeader before fan-out in the normal path.
    let majority_ok = if broker.cluster_config().is_some() {
        match broker.led_partition_epoch(topic, partition) {
            Some(epoch) => {
                let note_budget = budget.saturating_sub(start.elapsed());
                if note_budget.is_zero() {
                    warn!(
                        topic,
                        partition,
                        truncate_to,
                        "delete records fan-out budget exhausted before journal note"
                    );
                    false
                } else {
                    match tokio::time::timeout(
                        note_budget,
                        fanout_truncate_journal_note(broker, topic, partition, truncate_to, epoch),
                    )
                    .await
                    {
                        Ok(ok) => ok,
                        Err(_) => {
                            warn!(
                                topic,
                                partition,
                                truncate_to,
                                budget_ms = budget.as_millis() as u64,
                                "delete records journal note exceeded fan-out budget"
                            );
                            false
                        }
                    }
                }
            }
            None => {
                debug!(
                    topic,
                    partition,
                    truncate_to,
                    "skip truncate journal note: not partition leader (or unknown TP)"
                );
                true
            }
        }
    } else {
        true
    };

    let remaining = budget.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        warn!(
            topic,
            partition,
            truncate_to,
            budget_ms = budget.as_millis() as u64,
            "delete records fan-out budget exhausted before replica truncate; unfinished peers remain on outbox for drain/reconcile"
        );
        return DeleteRecordsFanoutResult { majority_ok };
    }

    match tokio::time::timeout(
        remaining,
        fanout_delete_records_replica_inner(broker, topic, partition, truncate_to),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            warn!(
                topic,
                partition,
                truncate_to,
                budget_ms = budget.as_millis() as u64,
                "delete records replica fan-out overall budget exceeded; unfinished peers remain on outbox for drain/reconcile"
            );
        }
    }
    DeleteRecordsFanoutResult { majority_ok }
}

/// Phase 148: replica + outbox fan-out only (journal already majority-noted).
///
/// Used after wait-mode majority-first local truncate so we do not double-run
/// journal consensus (second note would max-merge / re-count metrics).
pub async fn fanout_delete_records_replicas_only(
    broker: &Broker,
    topic: &str,
    partition: u32,
    truncate_to: u64,
) {
    let budget = delete_records_fanout_budget();
    match tokio::time::timeout(
        budget,
        fanout_delete_records_replica_inner(broker, topic, partition, truncate_to),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            warn!(
                topic,
                partition,
                truncate_to,
                budget_ms = budget.as_millis() as u64,
                "delete records replica-only fan-out overall budget exceeded; unfinished peers remain on outbox for drain/reconcile"
            );
        }
    }
}

async fn fanout_delete_records_replica_inner(
    broker: &Broker,
    topic: &str,
    partition: u32,
    truncate_to: u64,
) {
    let peers = broker.delete_records_fanout_peers(topic, partition);
    if peers.is_empty() {
        return;
    }

    // Pre-enqueue all fan-out peers before spawning RPCs so budget abort /
    // JoinError cannot lose outbox coverage (re-enqueue on later failure is
    // idempotent).
    for (replica_id, _addr, leader_epoch) in &peers {
        broker.enqueue_delete_records_outbox(
            *replica_id,
            topic,
            partition,
            truncate_to,
            *leader_epoch,
        );
    }

    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for (replica_id, addr, leader_epoch) in peers {
        let req = Request::ReplicaDeleteRecords {
            topic: topic.to_owned(),
            partition,
            before_offset: truncate_to,
            leader_epoch,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (replica_id, addr, leader_epoch, res)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((
                replica_id,
                _addr,
                _epoch,
                Ok(Response::ReplicaDeleteRecords { error_code: 0, .. }),
            )) => {
                broker
                    .delete_records_outbox()
                    .drop_entry(replica_id, topic, partition);
            }
            Ok((
                replica_id,
                addr,
                leader_epoch,
                Ok(Response::ReplicaDeleteRecords {
                    error_code,
                    low_watermark,
                }),
            )) => {
                warn!(
                    replica_id,
                    %addr,
                    error_code,
                    low_watermark,
                    topic,
                    partition,
                    "delete records fan-out peer error"
                );
                broker.note_delete_records_fanout_error();
                if error_code == ErrorCode::InvalidProducerEpoch as u16 {
                    broker
                        .delete_records_outbox()
                        .drop_entry(replica_id, topic, partition);
                } else {
                    // Already pre-enqueued; re-enqueue is idempotent / refreshes.
                    broker.enqueue_delete_records_outbox(
                        replica_id,
                        topic,
                        partition,
                        truncate_to,
                        leader_epoch,
                    );
                }
            }
            Ok((replica_id, addr, leader_epoch, Ok(other))) => {
                warn!(
                    replica_id,
                    %addr,
                    ?other,
                    topic,
                    partition,
                    "delete records fan-out unexpected response"
                );
                broker.note_delete_records_fanout_error();
                broker.enqueue_delete_records_outbox(
                    replica_id,
                    topic,
                    partition,
                    truncate_to,
                    leader_epoch,
                );
            }
            Ok((replica_id, addr, leader_epoch, Err(e))) => {
                warn!(
                    replica_id,
                    %addr,
                    error = %e,
                    topic,
                    partition,
                    "delete records fan-out rpc failed"
                );
                broker.note_delete_records_fanout_error();
                broker.enqueue_delete_records_outbox(
                    replica_id,
                    topic,
                    partition,
                    truncate_to,
                    leader_epoch,
                );
            }
            // Pre-enqueued: JoinError has no replica_id; entry stays for drain.
            Err(e) => {
                warn!(error = %e, topic, partition, "delete records fan-out join error");
                broker.note_delete_records_fanout_error();
            }
        }
    }
}

/// Drain durable DeleteRecords outbox for currently live peers (Phase 116 + 123).
///
/// **Parallel** at-least-once retry of `ReplicaDeleteRecords`. Success removes
/// the entry; transport / peer errors leave it and increment retry-error metrics.
/// When this node still leads the partition, the RPC uses the **current**
/// local leader epoch (Phase 123) so an epoch bump does not self-fence.
/// No-op when the outbox is empty or the broker is single-node with no pending.
pub async fn drain_delete_records_outbox(broker: &Broker) {
    let pending = broker.delete_records_outbox_pending_live();
    if pending.is_empty() {
        return;
    }
    let auth = broker.auth_token();
    let tls = broker.inter_broker_tls();
    let mut set = tokio::task::JoinSet::new();
    for entry in pending {
        let Some(addr) = broker.broker_addr(entry.replica_id) else {
            continue;
        };
        // Phase 123: prefer current epoch while we still lead this TP.
        let leader_epoch = broker
            .led_partition_epoch(&entry.topic, entry.partition)
            .unwrap_or(entry.leader_epoch);
        let req = Request::ReplicaDeleteRecords {
            topic: entry.topic.clone(),
            partition: entry.partition,
            before_offset: entry.before_offset,
            leader_epoch,
        };
        let auth = auth.clone();
        let tls = tls.clone();
        let replica_id = entry.replica_id;
        let topic = entry.topic.clone();
        let partition = entry.partition;
        let before_offset = entry.before_offset;
        set.spawn(async move {
            let res = inter_broker_rpc_owned(&addr, &req, auth, tls).await;
            (
                replica_id,
                topic,
                partition,
                before_offset,
                leader_epoch,
                res,
            )
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((
                replica_id,
                topic,
                partition,
                before_offset,
                _epoch,
                Ok(Response::ReplicaDeleteRecords { error_code: 0, .. }),
            )) => {
                broker.delete_records_outbox().note_retry_success(
                    replica_id,
                    &topic,
                    partition,
                    before_offset,
                );
            }
            Ok((
                replica_id,
                topic,
                partition,
                _before,
                _epoch,
                Ok(Response::ReplicaDeleteRecords {
                    error_code,
                    low_watermark,
                }),
            )) => {
                if error_code == ErrorCode::InvalidProducerEpoch as u16 {
                    // Stale epoch — drop; Phase 123 new-leader reconcile re-creates.
                    broker
                        .delete_records_outbox()
                        .drop_entry(replica_id, &topic, partition);
                    warn!(
                        replica_id,
                        topic = %topic,
                        partition,
                        error_code,
                        low_watermark,
                        "delete records outbox drain fenced; dropping entry"
                    );
                } else {
                    warn!(
                        replica_id,
                        error_code,
                        low_watermark,
                        topic = %topic,
                        partition,
                        "delete records outbox drain peer error"
                    );
                    broker.delete_records_outbox().note_retry_error();
                }
            }
            Ok((replica_id, topic, partition, _before, _epoch, Ok(other))) => {
                warn!(
                    replica_id,
                    ?other,
                    topic = %topic,
                    partition,
                    "delete records outbox drain unexpected response"
                );
                broker.delete_records_outbox().note_retry_error();
            }
            Ok((replica_id, topic, partition, _before, _epoch, Err(e))) => {
                debug!(
                    replica_id,
                    error = %e,
                    topic = %topic,
                    partition,
                    "delete records outbox drain rpc failed"
                );
                broker.delete_records_outbox().note_retry_error();
            }
            Err(e) => {
                warn!(error = %e, "delete records outbox drain join error");
                broker.delete_records_outbox().note_retry_error();
            }
        }
    }
}

/// Run multi-broker 2PC fan-out indicated by [`Txn2pcFanout`] (Phase 114).
///
/// - **Open**: best-effort (metric++ on failure; does not fail the client).
/// - **Prepare**: strict for live peers; returns `false` if any peer fails
///   (caller should [`Broker::rollback_local_prepare`]).
/// - **Complete**: strict for live peers; returns `false` on failure (client
///   already local-finalized — metric++ and log; re-issue may be needed).
///
/// Returns `true` when all required peer RPCs succeeded (or there were no peers).
pub async fn run_txn_2pc_fanout(broker: &Broker, fanout: &Txn2pcFanout) -> bool {
    match fanout {
        Txn2pcFanout::None => true,
        Txn2pcFanout::Open {
            transactional_id,
            producer_id,
            producer_epoch,
            enable_2pc,
            coordinator_node_id,
            install_open,
        } => {
            fanout_txn_participant_open(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *enable_2pc,
                *coordinator_node_id,
                *install_open,
            )
            .await;
            true
        }
        Txn2pcFanout::Prepare {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            fanout_txn_participant_prepare(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *commit,
            )
            .await
        }
        Txn2pcFanout::Complete {
            transactional_id,
            producer_id,
            producer_epoch,
            commit,
        } => {
            fanout_txn_participant_complete(
                broker,
                transactional_id,
                *producer_id,
                *producer_epoch,
                *commit,
            )
            .await
        }
    }
}

/// Best-effort open fan-out (Phase 114 + Phase 120 coordinator trailer).
pub async fn fanout_txn_participant_open(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    enable_2pc: bool,
    coordinator_node_id: u32,
    install_open: bool,
) {
    let peers = broker.txn_2pc_fanout_peers();
    if peers.is_empty() {
        return;
    }
    let req = Request::TxnParticipantOpen {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
        enable_2pc,
        coordinator_node_id,
        install_open,
    };
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::TxnParticipantOpen { error_code: 0 }) => {}
            Ok(Response::TxnParticipantOpen { error_code }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    producer_id,
                    "txn open fan-out peer error"
                );
                broker.note_txn_2pc_fanout_error();
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    producer_id,
                    "txn open fan-out unexpected response"
                );
                broker.note_txn_2pc_fanout_error();
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    producer_id,
                    "txn open fan-out rpc failed"
                );
                broker.note_txn_2pc_fanout_error();
            }
        }
    }
}

/// Strict prepare fan-out (Phase 114). Returns `true` if all live peers OK.
pub async fn fanout_txn_participant_prepare(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    commit: bool,
) -> bool {
    let peers = broker.txn_2pc_fanout_peers();
    if peers.is_empty() {
        return true;
    }
    let req = Request::TxnParticipantPrepare {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
        commit,
    };
    let mut ok = true;
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::TxnParticipantPrepare { error_code: 0 }) => {}
            Ok(Response::TxnParticipantPrepare { error_code }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    producer_id,
                    transactional_id,
                    "txn prepare fan-out peer error"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    producer_id,
                    "txn prepare fan-out unexpected response"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    producer_id,
                    "txn prepare fan-out rpc failed"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
        }
    }
    ok
}

/// Strict complete fan-out (Phase 114). Returns `true` if all live peers OK.
pub async fn fanout_txn_participant_complete(
    broker: &Broker,
    transactional_id: &str,
    producer_id: u64,
    producer_epoch: u16,
    commit: bool,
) -> bool {
    let peers = broker.txn_2pc_fanout_peers();
    if peers.is_empty() {
        return true;
    }
    let req = Request::TxnParticipantComplete {
        transactional_id: transactional_id.to_owned(),
        producer_id,
        producer_epoch,
        commit,
    };
    let mut ok = true;
    for (peer_id, addr) in peers {
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::TxnParticipantComplete { error_code: 0 }) => {}
            Ok(Response::TxnParticipantComplete { error_code }) => {
                warn!(
                    peer_id,
                    %addr,
                    error_code,
                    producer_id,
                    transactional_id,
                    "txn complete fan-out peer error"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Ok(other) => {
                warn!(
                    peer_id,
                    %addr,
                    ?other,
                    producer_id,
                    "txn complete fan-out unexpected response"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
            Err(e) => {
                warn!(
                    peer_id,
                    %addr,
                    error = %e,
                    producer_id,
                    "txn complete fan-out rpc failed"
                );
                broker.note_txn_2pc_fanout_error();
                ok = false;
            }
        }
    }
    ok
}

/// Phase 120: peek EndTxn request body for transactional_id + producer_id.
///
/// `version` is the Kafka EndTxn API version (0–5). Returns `None` on truncated body.
pub fn peek_end_txn_ids(version: i16, body: &[u8]) -> Option<(String, u64)> {
    use crate::kafka::wire;
    use bytes::Buf;
    let flex = version >= 3;
    let mut src = body;
    let txn_id = wire::read_string(&mut src, flex).ok()?;
    if src.remaining() < 8 + 2 + 1 {
        return None;
    }
    let producer_id = src.get_i64() as u64;
    Some((txn_id, producer_id))
}

/// Phase 122: peek AddOffsetsToTxn body for transactional_id + producer_id.
///
/// Wire: txn_id, producer_id, producer_epoch, group_id (classic 0–2 / flex 3–4).
pub fn peek_add_offsets_to_txn_ids(version: i16, body: &[u8]) -> Option<(String, u64)> {
    use crate::kafka::wire;
    use bytes::Buf;
    let flex = version >= 3;
    let mut src = body;
    let txn_id = wire::read_string(&mut src, flex).ok()?;
    if src.remaining() < 8 + 2 {
        return None;
    }
    let producer_id = src.get_i64() as u64;
    Some((txn_id, producer_id))
}

/// Phase 122: peek TxnOffsetCommit body for transactional_id + producer_id.
///
/// Wire: txn_id, group_id, producer_id, producer_epoch, … (classic 0–2 / flex 3–6).
pub fn peek_txn_offset_commit_ids(version: i16, body: &[u8]) -> Option<(String, u64)> {
    use crate::kafka::wire;
    use bytes::Buf;
    let flex = version >= 3;
    let mut src = body;
    let txn_id = wire::read_string(&mut src, flex).ok()?;
    let _group_id = wire::read_string(&mut src, flex).ok()?;
    if src.remaining() < 8 + 2 {
        return None;
    }
    let producer_id = src.get_i64() as u64;
    Some((txn_id, producer_id))
}

/// Phase 120/122: minimal Kafka txn API error response bodies (no response header).
pub(super) fn put_end_txn_error_response(out: &mut bytes::BytesMut, version: i16, err: i16) {
    use crate::kafka::codec::put_empty_tag_buffer;
    use bytes::BufMut;
    let flex = version >= 3;
    out.put_i32(0); // throttle
    out.put_i16(err);
    if version >= 5 {
        out.put_i64(-1);
        out.put_i16(-1);
    }
    if flex {
        put_empty_tag_buffer(out);
    }
}

fn put_add_offsets_error_response(out: &mut bytes::BytesMut, version: i16, err: i16) {
    use crate::kafka::codec::put_empty_tag_buffer;
    use bytes::BufMut;
    let flex = version >= 3;
    out.put_i32(0); // throttle
    out.put_i16(err);
    if flex {
        put_empty_tag_buffer(out);
    }
}

fn put_txn_offset_commit_empty_response(out: &mut bytes::BytesMut, version: i16) {
    use crate::kafka::codec::{put_compact_array_len, put_empty_tag_buffer};
    use bytes::BufMut;
    let flex = version >= 3;
    out.put_i32(0); // throttle
    if flex {
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
    } else {
        out.put_i32(0); // empty topics
    }
}

/// Build an honest client-facing body when txn forward fails (peer/RPC).
fn put_txn_forward_error_body(out: &mut bytes::BytesMut, api_key: i16, api_version: i16) {
    // UnknownProducerId (59) for simple error-code APIs; TxnOffsetCommit has no
    // top-level error — empty topics (no silent local buffer).
    match api_key {
        25 => put_add_offsets_error_response(out, api_version, 59),
        28 => put_txn_offset_commit_empty_response(out, api_version),
        // 26 EndTxn (default)
        _ => put_end_txn_error_response(out, api_version, 59),
    }
}

/// Phase 120/122: if a Kafka txn API should be served by the Init-owner
/// coordinator, forward the body and return the coordinator response.
/// `None` = handle locally (no cluster, registry miss, or self is coordinator).
///
/// Supported `api_key`: 25 AddOffsetsToTxn, 26 EndTxn, 28 TxnOffsetCommit.
/// Never re-forwards on the coordinator (caller is the Kafka client path only).
pub async fn maybe_forward_kafka_txn(
    broker: &Broker,
    api_key: i16,
    api_version: i16,
    principal: &str,
    body: &[u8],
) -> Option<Bytes> {
    use bytes::BytesMut;

    if broker.cluster_config().is_none() {
        return None;
    }
    let (txn_id, producer_id) = match api_key {
        25 => peek_add_offsets_to_txn_ids(api_version, body)?,
        26 => peek_end_txn_ids(api_version, body)?,
        28 => peek_txn_offset_commit_ids(api_version, body)?,
        _ => return None,
    };
    let Some(coord) = broker.resolve_txn_coordinator(&txn_id, Some(producer_id)) else {
        return None;
    };
    if coord == broker.node_id() {
        return None;
    }
    let Some(addr) = broker.broker_addr(coord) else {
        broker.record_txn_forward_error();
        let mut out = BytesMut::new();
        put_txn_forward_error_body(&mut out, api_key, api_version);
        return Some(out.freeze());
    };

    let req = Request::KafkaTxnForward {
        api_key,
        api_version,
        principal: principal.to_owned(),
        body: Bytes::copy_from_slice(body),
    };
    match inter_broker_rpc(broker, &addr, &req).await {
        Ok(Response::KafkaTxnForward {
            error_code: 0,
            body,
        }) => {
            broker.record_txn_forward_ok();
            Some(body)
        }
        Ok(Response::KafkaTxnForward { error_code, .. }) => {
            tracing::debug!(
                coord,
                error_code,
                api_key,
                %txn_id,
                producer_id,
                "kafka txn forward peer error"
            );
            broker.record_txn_forward_error();
            let mut out = BytesMut::new();
            put_txn_forward_error_body(&mut out, api_key, api_version);
            Some(out.freeze())
        }
        Ok(other) => {
            tracing::debug!(
                coord,
                ?other,
                api_key,
                %txn_id,
                "kafka txn forward unexpected response"
            );
            broker.record_txn_forward_error();
            let mut out = BytesMut::new();
            put_txn_forward_error_body(&mut out, api_key, api_version);
            Some(out.freeze())
        }
        Err(e) => {
            tracing::debug!(
                coord,
                error = %e,
                api_key,
                %txn_id,
                "kafka txn forward rpc failed"
            );
            broker.record_txn_forward_error();
            let mut out = BytesMut::new();
            put_txn_forward_error_body(&mut out, api_key, api_version);
            Some(out.freeze())
        }
    }
}

/// Phase 120: EndTxn-only wrapper around [`maybe_forward_kafka_txn`].
pub async fn maybe_forward_kafka_end_txn(
    broker: &Broker,
    api_version: i16,
    principal: &str,
    end_txn_body: &[u8],
) -> Option<Bytes> {
    maybe_forward_kafka_txn(broker, 26, api_version, principal, end_txn_body).await
}

/// Phase 119 + 138 + 147: if this Fetch should be served by a peer session owner,
/// forward the Kafka body and return the owner's response body.
/// `None` = handle locally (primary hit, serve-from-mirror, or promote-from-mirror).
///
/// Never re-forwards on the owner (caller is the Kafka client path only).
pub async fn maybe_forward_kafka_fetch(
    broker: &Broker,
    api_version: i16,
    principal: &str,
    fetch_body: &[u8],
) -> Option<Bytes> {
    use crate::kafka::fetch_session::{decode_session_owner, INITIAL_EPOCH};
    use crate::kafka::produce_fetch::{peek_fetch_session, put_fetch_empty_response};
    use bytes::BytesMut;

    if broker.cluster_config().is_none() || api_version < 7 {
        return None;
    }
    let (session_id, session_epoch) = peek_fetch_session(api_version, fetch_body)?;
    // Create path stays local.
    if session_id == 0 || session_epoch == INITIAL_EPOCH {
        return None;
    }
    // Local primary hit → encode_fetch. Mirror-only still attempts Phase 119
    // forward while owner is reachable; on owner miss, try_owner_miss_local_serve
    // prefers serve-from-mirror without promote (Phase 147).
    if broker.fetch_sessions().contains(session_id) {
        return None;
    }
    let owner = decode_session_owner(session_id)?;
    if owner == broker.node_id() {
        // Encoded as us but primary missing: serve mirror or promote (Phase 147/138).
        if broker
            .fetch_sessions()
            .try_owner_miss_local_serve(session_id)
        {
            return None;
        }
        return None;
    }
    let Some(addr) = broker.broker_addr(owner) else {
        // Owner addr unknown — serve mirror or promote.
        if broker
            .fetch_sessions()
            .try_owner_miss_local_serve(session_id)
        {
            return None;
        }
        broker.fetch_sessions().record_forward_error();
        let mut out = BytesMut::new();
        put_fetch_empty_response(&mut out, api_version, 70, session_id);
        return Some(out.freeze());
    };

    let req = Request::KafkaFetchForward {
        api_version,
        principal: principal.to_owned(),
        body: Bytes::copy_from_slice(fetch_body),
    };
    match inter_broker_rpc(broker, &addr, &req).await {
        Ok(Response::KafkaFetchForward {
            error_code: 0,
            body,
        }) => {
            broker.fetch_sessions().record_forward_ok();
            Some(body)
        }
        Ok(Response::KafkaFetchForward { error_code, .. }) => {
            tracing::debug!(
                owner,
                error_code,
                session_id,
                "kafka fetch forward peer error"
            );
            broker.fetch_sessions().record_forward_error();
            if broker
                .fetch_sessions()
                .try_owner_miss_local_serve(session_id)
            {
                return None;
            }
            let mut out = BytesMut::new();
            put_fetch_empty_response(&mut out, api_version, 70, session_id);
            Some(out.freeze())
        }
        Ok(other) => {
            tracing::debug!(owner, ?other, session_id, "kafka fetch forward unexpected");
            broker.fetch_sessions().record_forward_error();
            if broker
                .fetch_sessions()
                .try_owner_miss_local_serve(session_id)
            {
                return None;
            }
            let mut out = BytesMut::new();
            put_fetch_empty_response(&mut out, api_version, 70, session_id);
            Some(out.freeze())
        }
        Err(e) => {
            tracing::debug!(owner, error = %e, session_id, "kafka fetch forward rpc failed");
            broker.fetch_sessions().record_forward_error();
            // Phase 147: prefer serve-from-mirror; Phase 138 promote when knobs say so.
            if broker
                .fetch_sessions()
                .try_owner_miss_local_serve(session_id)
            {
                return None;
            }
            let mut out = BytesMut::new();
            put_fetch_empty_response(&mut out, api_version, 70, session_id);
            Some(out.freeze())
        }
    }
}

/// Phase 138: best-effort fan-out of pending session mirror put/delete ops.
///
/// Does not fail the client path; fire-and-forget with per-RPC timeout.
pub async fn fanout_session_mirror_ops(broker: &Broker) {
    use crate::kafka::fetch_session::SessionMirrorOp;
    use bytes::Bytes;

    let ops = broker.fetch_sessions().drain_mirror_ops();
    if ops.is_empty() || broker.cluster_config().is_none() {
        return;
    }
    let self_id = broker.node_id();
    let peers: Vec<(u32, String)> = broker
        .live_brokers()
        .into_iter()
        .filter(|&id| id != self_id)
        .filter_map(|id| broker.broker_addr(id).map(|a| (id, a)))
        .collect();
    if peers.is_empty() {
        return;
    }

    for op in ops {
        match op {
            SessionMirrorOp::Put(session_id) => {
                // Phase 146: prefer delta vs last-exported primary; else full.
                let Some((snap, is_delta)) =
                    broker.fetch_sessions().export_mirror_put_bytes(session_id)
                else {
                    continue;
                };
                if is_delta {
                    broker.fetch_sessions().record_mirror_delta_put_sent();
                }
                // Cache exported state so subsequent Puts can delta (even if peers lag).
                broker.fetch_sessions().note_last_mirrored(session_id);
                let req = Request::FetchSessionMirrorPut {
                    session_id,
                    snapshot: Bytes::from(snap),
                };
                for (peer_id, addr) in &peers {
                    match inter_broker_rpc(broker, addr, &req).await {
                        Ok(Response::FetchSessionMirrorPut { error_code: 0 }) => {}
                        Ok(Response::FetchSessionMirrorPut { error_code }) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error_code,
                                "session mirror put peer error"
                            );
                        }
                        Ok(other) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                ?other,
                                "session mirror put unexpected"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error = %e,
                                "session mirror put rpc failed"
                            );
                        }
                    }
                }
            }
            SessionMirrorOp::Delete(session_id) => {
                let req = Request::FetchSessionMirrorDelete { session_id };
                for (peer_id, addr) in &peers {
                    match inter_broker_rpc(broker, addr, &req).await {
                        Ok(Response::FetchSessionMirrorDelete { error_code: 0 }) => {}
                        Ok(Response::FetchSessionMirrorDelete { error_code }) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error_code,
                                "session mirror delete peer error"
                            );
                        }
                        Ok(other) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                ?other,
                                "session mirror delete unexpected"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                peer_id,
                                session_id,
                                error = %e,
                                "session mirror delete rpc failed"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Phase 142: best-effort drain of pending IsrUpdate reports to the controller.
///
/// Fire-and-forget; does not block client/replica paths. On success aligns the
/// leader's local assignment generation to the controller response.
pub fn schedule_isr_update_reports(broker: &Arc<Broker>) {
    if !broker.has_pending_isr_reports() {
        return;
    }
    let b = Arc::clone(broker);
    tokio::spawn(async move {
        fanout_isr_update_reports(&b).await;
    });
}

/// Send queued IsrUpdate RPCs to the current controller (Phase 142).
pub async fn fanout_isr_update_reports(broker: &Broker) {
    let reports = broker.drain_pending_isr_reports();
    if reports.is_empty() {
        return;
    }
    let controller_id = broker.controller_id();
    if controller_id == broker.node_id() {
        // Became controller since enqueue; apply locally.
        for r in reports {
            let (err, gen) = broker.apply_leader_isr_update(
                &r.topic,
                r.partition,
                r.leader_id,
                r.leader_epoch,
                &r.isr,
                r.generation_hint,
            );
            if err == 0 {
                broker.align_assignment_generation(gen);
            }
        }
        return;
    }
    let Some(addr) = broker.broker_addr(controller_id) else {
        tracing::debug!(
            controller_id,
            "isr update: no controller addr; reports dropped"
        );
        return;
    };
    for r in reports {
        let req = Request::IsrUpdate {
            topic: r.topic.clone(),
            partition: r.partition,
            leader_id: r.leader_id,
            leader_epoch: r.leader_epoch,
            isr: r.isr.clone(),
            generation_hint: r.generation_hint,
        };
        match inter_broker_rpc(broker, &addr, &req).await {
            Ok(Response::IsrUpdate {
                error_code: 0,
                generation,
            }) => {
                broker.align_assignment_generation(generation);
            }
            Ok(Response::IsrUpdate {
                error_code,
                generation: _,
            }) => {
                tracing::debug!(
                    topic = %r.topic,
                    partition = r.partition,
                    error_code,
                    "isr update rejected by controller"
                );
            }
            Ok(other) => {
                tracing::debug!(
                    topic = %r.topic,
                    partition = r.partition,
                    ?other,
                    "isr update unexpected response"
                );
            }
            Err(e) => {
                tracing::debug!(
                    topic = %r.topic,
                    partition = r.partition,
                    error = %e,
                    "isr update rpc failed"
                );
            }
        }
    }
}

/// Schedule [`fanout_session_mirror_ops`] after local Kafka Fetch session mutations.
///
/// Phase 139: **Deletes** flush immediately. **Puts** are single-flight debounced
/// by `mirror_put_min_interval_ms` (default 50; `0` = immediate after coalesce).
/// Does not block the client Fetch response path.
pub fn schedule_session_mirror_fanout(broker: &Arc<Broker>) {
    if broker.cluster_config().is_none() {
        return;
    }
    let sessions = broker.fetch_sessions();
    if !sessions.has_pending_mirror_ops() {
        return;
    }

    // Delete pending → flush now (no debounce wait).
    if sessions.has_pending_mirror_delete() {
        let b = Arc::clone(broker);
        tokio::spawn(async move {
            fanout_session_mirror_ops(b.as_ref()).await;
        });
        return;
    }

    let interval_ms = sessions.mirror_put_min_interval_ms();
    if interval_ms == 0 {
        let b = Arc::clone(broker);
        tokio::spawn(async move {
            fanout_session_mirror_ops(b.as_ref()).await;
        });
        return;
    }

    // Puts only: single-flight debounce. Further schedules are no-ops until flush.
    if !sessions.try_arm_mirror_put_debounce() {
        return;
    }
    let b = Arc::clone(broker);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        b.fetch_sessions().clear_mirror_put_debounce_armed();
        fanout_session_mirror_ops(b.as_ref()).await;
    });
}
