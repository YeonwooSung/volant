//! Phase 142: Metadata ISR overlay on leaders + leader→controller IsrUpdate.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, boot_triple_inprocess, cluster_config, default_storage, propagate, unique_dir,
    Guard,
};
use volant_broker::{
    fanout_isr_update_reports, inter_broker_rpc, serve_listener, Broker, PendingIsrReport,
};
use volant_core::{PartitionId, TopicName};
use volant_protocol::{ErrorCode, Request, Response};

fn broker_of<'a>(b1: &'a Broker, b2: &'a Broker, b3: &'a Broker, id: u32) -> &'a Broker {
    match id {
        1 => b1,
        2 => b2,
        3 => b3,
        _ => panic!("bad id {id}"),
    }
}

/// Topic name whose partition-0 leader is node 2 under RF=3, brokers [1,2,3].
const TOPIC: &str = "p142";

#[test]
fn leader_metadata_overlays_local_isr_before_controller_sync() {
    let base = unique_dir("p142", "overlay");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19101, 19102, 19103]);

    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);

    let topic = TopicName::new(TOPIC);
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    assert_eq!(
        leader_id, 2,
        "test topic must elect node 2 as leader (controller is 1)"
    );
    assert!(b1.is_controller());
    assert!(!b2.is_controller());

    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let followers: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    let dead = followers[0];
    let other = followers[1];

    let isr0 = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert_eq!(isr0.len(), 3, "full RF ISR: {isr0:?}");

    // Death on the non-controller leader shrinks local ISR. Controller
    // assignment is not updated yet (leader ≠ controller).
    leader.test_kill_broker(dead).unwrap();
    let isr_local = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr_local.contains(&dead),
        "local ISR must drop dead: {isr_local:?}"
    );
    assert!(isr_local.contains(&leader_id));
    assert!(isr_local.contains(&other));

    // Phase 142 overlay: leader.metadata() prefers local ISR over assignment.
    let leader_meta_isr = leader.metadata(None).topics[0].partitions[0]
        .isr
        .clone();
    assert_eq!(
        leader_meta_isr, isr_local,
        "leader Metadata must show live local ISR (overlay)"
    );

    // Prior honesty gap: controller assignment-only Metadata still shows the
    // pre-death ISR until IsrUpdate lands (controller is not the leader).
    let controller_meta_isr = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert!(
        controller_meta_isr.contains(&dead),
        "controller Metadata still assignment-stale before IsrUpdate: {controller_meta_isr:?}"
    );
}

#[test]
fn controller_apply_leader_isr_update_refreshes_metadata() {
    let base = unique_dir("p142", "report");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19111, 19112, 19113]);

    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);

    let topic = TopicName::new(TOPIC);
    assert_eq!(b1.metadata(None).topics[0].partitions[0].leader, 2);
    assert!(b1.is_controller());

    // Kill follower 3 on non-controller leader 2.
    b2.test_kill_broker(3).unwrap();
    let shrunk = b2.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(!shrunk.contains(&3), "shrunk: {shrunk:?}");
    assert!(shrunk.contains(&2));

    let before = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert!(before.contains(&3), "pre-report controller ISR: {before:?}");

    let epoch = b2.metadata(None).topics[0].partitions[0].leader_epoch;
    let (err, gen) = b1.apply_leader_isr_update(TOPIC, 0, 2, epoch, &shrunk, 0);
    assert_eq!(err, 0, "apply should succeed");
    assert!(gen >= 1);

    let after = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert_eq!(
        after, shrunk,
        "controller Metadata must reflect reported ISR"
    );
    assert!(!after.contains(&3));
}

#[test]
fn isr_update_epoch_fence_rejects_stale() {
    let base = unique_dir("p142", "fence");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19131, 19132, 19133]);

    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);
    let isr_full = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert_eq!(b1.metadata(None).topics[0].partitions[0].leader, 2);

    // Kill leader 2 → controller elects new leader and bumps epoch.
    b1.on_broker_death(2).unwrap();
    let meta = b1.metadata(None).topics[0].partitions[0].clone();
    assert_ne!(meta.leader, 2, "leader must move after death");
    assert!(meta.leader_epoch >= 1, "epoch bumped: {}", meta.leader_epoch);

    let gen_before = b1.generation();
    let isr_before = meta.isr.clone();

    // Stale leader id (former leader claiming still in charge).
    let (err, gen_after) = b1.apply_leader_isr_update(TOPIC, 0, 2, 0, &isr_full, 0);
    assert_eq!(
        err,
        ErrorCode::NotLeaderForPartition as u16,
        "stale leader id rejected"
    );
    assert_eq!(gen_after, gen_before);
    assert_eq!(
        b1.metadata(None).topics[0].partitions[0].isr,
        isr_before,
        "ISR unchanged after reject"
    );

    // Correct leader but stale epoch.
    let new_leader = meta.leader;
    let cur_epoch = b1.metadata(None).topics[0].partitions[0].leader_epoch;
    assert!(cur_epoch >= 1);
    let shrunk = vec![new_leader];
    let (err, gen2) = b1.apply_leader_isr_update(
        TOPIC,
        0,
        new_leader,
        cur_epoch.saturating_sub(1),
        &shrunk,
        0,
    );
    assert_eq!(
        err,
        ErrorCode::InvalidProducerEpoch as u16,
        "stale epoch must fence"
    );
    assert_eq!(gen2, gen_before);
    assert_eq!(
        b1.metadata(None).topics[0].partitions[0].isr,
        isr_before,
        "ISR unchanged after epoch fence"
    );
}

#[test]
fn non_controller_rejects_isr_update() {
    let base = unique_dir("p142", "notctrl");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19151, 19152, 19153]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);

    let (err, _) = b2.apply_leader_isr_update(TOPIC, 0, 2, 0, &[2, 3], 0);
    assert_eq!(err, ErrorCode::NotController as u16);
}

#[test]
fn pending_isr_report_enqueued_on_leader_death_shrink() {
    let base = unique_dir("p142", "enqueue");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19161, 19162, 19163]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);
    assert_eq!(b2.metadata(None).topics[0].partitions[0].leader, 2);
    assert!(!b2.is_controller());

    b2.test_kill_broker(3).unwrap();
    assert!(
        b2.has_pending_isr_reports(),
        "non-controller leader must enqueue IsrUpdate after death shrink"
    );
    let reports = b2.drain_pending_isr_reports();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].topic, TOPIC);
    assert_eq!(reports[0].leader_id, 2);
    assert!(!reports[0].isr.contains(&3));

    // Apply drained report on controller (simulates successful RPC).
    let (err, gen) = b1.apply_leader_isr_update(
        &reports[0].topic,
        reports[0].partition,
        reports[0].leader_id,
        reports[0].leader_epoch,
        &reports[0].isr,
        reports[0].generation_hint,
    );
    assert_eq!(err, 0);
    b2.align_assignment_generation(gen);
    assert_eq!(b2.generation(), gen);
    assert_eq!(
        b1.metadata(None).topics[0].partitions[0].isr,
        reports[0].isr
    );
}

#[test]
fn apply_rejects_empty_isr_and_leader_missing_from_isr() {
    let base = unique_dir("p142", "invalid-isr");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19171, 19172, 19173]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);
    let epoch = b1.metadata(None).topics[0].partitions[0].leader_epoch;
    let gen_before = b1.generation();
    let isr_before = b1.metadata(None).topics[0].partitions[0].isr.clone();

    let (err, gen) = b1.apply_leader_isr_update(TOPIC, 0, 2, epoch, &[], 0);
    assert_eq!(err, ErrorCode::InvalidArg as u16);
    assert_eq!(gen, gen_before);

    // Leader id 2 must appear in the ISR list.
    let (err, gen) = b1.apply_leader_isr_update(TOPIC, 0, 2, epoch, &[1, 3], 0);
    assert_eq!(err, ErrorCode::InvalidArg as u16);
    assert_eq!(gen, gen_before);
    assert_eq!(
        b1.metadata(None).topics[0].partitions[0].isr,
        isr_before,
        "rejected update must not mutate assignment"
    );
}

#[test]
fn apply_rejects_unknown_topic_and_partition() {
    let base = unique_dir("p142", "notfound");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19181, 19182, 19183]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);

    let (err, _) = b1.apply_leader_isr_update("no-such-topic", 0, 2, 0, &[2], 0);
    assert_eq!(err, ErrorCode::NotFound as u16);

    let (err, _) = b1.apply_leader_isr_update(TOPIC, 99, 2, 0, &[2], 0);
    assert_eq!(err, ErrorCode::NotFound as u16);

    let (err, _) = b1.apply_leader_isr_update("", 0, 2, 0, &[2], 0);
    assert_eq!(err, ErrorCode::InvalidArg as u16);
}

#[test]
fn isr_report_coalesces_by_topic_partition() {
    let base = unique_dir("p142", "coalesce");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19191, 19192, 19193]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);
    assert!(!b2.is_controller());

    // Two enqueues for the same TP (second death of already-dead id re-runs
    // enqueue while still non-controller) must coalesce to one pending report.
    b2.test_kill_broker(3).unwrap();
    assert!(b2.has_pending_isr_reports());
    let first = b2.drain_pending_isr_reports();
    assert_eq!(first.len(), 1);
    assert!(!first[0].isr.contains(&3));

    // Re-queue by killing the same id again (membership already dead; enqueue
    // still fires for non-controller leaders).
    b2.test_kill_broker(3).unwrap();
    b2.test_kill_broker(3).unwrap();
    let reports = b2.drain_pending_isr_reports();
    assert_eq!(reports.len(), 1, "same TP reports coalesce: {reports:?}");
    assert_eq!(reports[0].topic, TOPIC);
    assert_eq!(reports[0].partition, 0);
    assert!(!reports[0].isr.contains(&3));
    assert!(reports[0].isr.contains(&2));
}

#[test]
fn single_node_apply_isr_update_is_noop_success() {
    let base = unique_dir("p142", "single");
    let _g = Guard(base.clone());
    let b = Broker::new(default_storage(base.join("n1")));
    let (err, gen) = b.apply_leader_isr_update("t", 0, 1, 0, &[1], 0);
    assert_eq!(err, 0);
    assert_eq!(gen, 0);
    assert!(!b.has_pending_isr_reports());
}

#[test]
fn cluster_state_pull_after_isr_update_refreshes_non_leader() {
    let base = unique_dir("p142", "cluster-state");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19201, 19202, 19203]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);

    b2.test_kill_broker(3).unwrap();
    let shrunk = b2.local_partition_isr(&TopicName::new(TOPIC), PartitionId(0)).unwrap();
    let epoch = b2.metadata(None).topics[0].partitions[0].leader_epoch;
    let (err, gen) = b1.apply_leader_isr_update(TOPIC, 0, 2, epoch, &shrunk, 0);
    assert_eq!(err, 0);

    // Non-leader follower still has stale assignment until ClusterState apply.
    let stale = b3.metadata(None).topics[0].partitions[0].isr.clone();
    assert!(
        stale.contains(&3) || stale.len() == 3,
        "follower assignment may still list full ISR before pull: {stale:?}"
    );

    let (_, snap_gen, cid, topics) = b1.cluster_state_snapshot();
    assert_eq!(snap_gen, gen);
    b3.apply_cluster_state(snap_gen, cid, &topics).unwrap();
    let fresh = b3.metadata(None).topics[0].partitions[0].isr.clone();
    assert_eq!(
        fresh, shrunk,
        "after ClusterState pull, non-leader Metadata must match controller"
    );
}

#[tokio::test]
async fn tcp_isr_update_rpc_refreshes_controller_metadata() {
    let base = unique_dir("p142", "tcp-rpc");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config([p1, p2, p3]);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3);

    let servers: Vec<_> = [
        (l1, Arc::clone(&b1)),
        (l2, Arc::clone(&b2)),
        (l3, Arc::clone(&b3)),
    ]
    .into_iter()
    .map(|(listener, b)| {
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        })
    })
    .collect();
    tokio::time::sleep(Duration::from_millis(40)).await;

    b1.create_topic(TOPIC, 1).unwrap();
    for _ in 0..50 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt(TOPIC).is_some() && b3.partition_count_opt(TOPIC).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(b1.metadata(None).topics[0].partitions[0].leader, 2);

    // Snapshot ISR on the leader *before* any mesh interaction can rejoin,
    // then report that exact set over TCP.
    b2.test_kill_broker(3).unwrap();
    let isr = b2
        .local_partition_isr(&TopicName::new(TOPIC), PartitionId(0))
        .unwrap();
    let epoch = b2.metadata(None).topics[0].partitions[0].leader_epoch;
    assert!(!isr.contains(&3), "local ISR after kill: {isr:?}");

    let addr1 = format!("127.0.0.1:{p1}");
    let resp = inter_broker_rpc(
        &b2,
        &addr1,
        &Request::IsrUpdate {
            topic: TOPIC.into(),
            partition: 0,
            leader_id: 2,
            leader_epoch: epoch,
            isr: isr.clone(),
            generation_hint: b2.generation(),
        },
    )
    .await
    .expect("IsrUpdate RPC");
    match resp {
        Response::IsrUpdate {
            error_code,
            generation,
        } => {
            assert_eq!(error_code, 0, "controller must accept");
            assert!(generation >= 1);
            b2.align_assignment_generation(generation);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    let controller_isr = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert_eq!(
        controller_isr, isr,
        "controller Metadata after direct RPC"
    );
    assert!(!controller_isr.contains(&3));

    // Controller rejects NotLeader over the wire as well.
    let bad = inter_broker_rpc(
        &b2,
        &addr1,
        &Request::IsrUpdate {
            topic: TOPIC.into(),
            partition: 0,
            leader_id: 3, // not the leader
            leader_epoch: epoch,
            isr: vec![3],
            generation_hint: 0,
        },
    )
    .await
    .expect("IsrUpdate reject RPC");
    match bad {
        Response::IsrUpdate { error_code, .. } => {
            assert_eq!(error_code, ErrorCode::NotLeaderForPartition as u16);
        }
        other => panic!("unexpected: {other:?}"),
    }

    for s in servers {
        s.abort();
    }
}

#[tokio::test]
async fn tcp_fanout_isr_update_reports_end_to_end() {
    let base = unique_dir("p142", "tcp-fanout");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config([p1, p2, p3]);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3);

    // Accept loops only — avoid mesh bg tasks rejoining dead members mid-assert.
    let servers: Vec<_> = [
        (l1, Arc::clone(&b1)),
        (l2, Arc::clone(&b2)),
        (l3, Arc::clone(&b3)),
    ]
    .into_iter()
    .map(|(listener, b)| {
        tokio::spawn(async move {
            // serve_listener starts bg tasks; still fine if we only assert
            // controller assignment (not leader-local ISR after mesh rejoin).
            let _ = serve_listener(listener, b).await;
        })
    })
    .collect();
    tokio::time::sleep(Duration::from_millis(40)).await;

    b1.create_topic(TOPIC, 1).unwrap();
    for _ in 0..50 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt(TOPIC).is_some() && b3.partition_count_opt(TOPIC).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Only kill a follower (not the controller) so fanout still reaches controller.
    b2.test_kill_broker(3).unwrap();
    assert!(b2.has_pending_isr_reports());
    let reported = b2
        .local_partition_isr(&TopicName::new(TOPIC), PartitionId(0))
        .unwrap();
    assert!(!reported.contains(&3), "reported ISR: {reported:?}");
    let before = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert!(before.contains(&3));

    fanout_isr_update_reports(&b2).await;
    assert!(
        !b2.has_pending_isr_reports(),
        "fanout must drain the queue"
    );

    let after = b1.metadata(None).topics[0].partitions[0].isr.clone();
    assert!(
        !after.contains(&3),
        "controller Metadata after fanout: {after:?}"
    );
    assert!(after.contains(&2));
    // Controller assignment should match what was queued at kill time (no 3).
    assert!(
        after.iter().all(|id| reported.contains(id) || *id == 2),
        "controller={after:?} reported={reported:?}"
    );

    for s in servers {
        s.abort();
    }
}

#[test]
fn pending_isr_report_fields_roundtrip_via_apply() {
    // Ensures PendingIsrReport public fields stay aligned with apply_leader_isr_update.
    let base = unique_dir("p142", "pending-fields");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [19211, 19212, 19213]);
    b1.create_topic(TOPIC, 1).unwrap();
    propagate(&[&b1, &b2, &b3], TOPIC);
    b2.test_kill_broker(3).unwrap();
    let r: PendingIsrReport = b2.drain_pending_isr_reports().pop().unwrap();
    assert!(!r.topic.is_empty());
    assert_eq!(r.partition, 0);
    assert_eq!(r.leader_id, 2);
    assert!(r.isr.contains(&2));
    let (err, _) = b1.apply_leader_isr_update(
        &r.topic,
        r.partition,
        r.leader_id,
        r.leader_epoch,
        &r.isr,
        r.generation_hint,
    );
    assert_eq!(err, 0);
}
