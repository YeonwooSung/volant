//! Phase 142: Metadata ISR overlay on leaders + leader→controller IsrUpdate.

#[path = "common/mod.rs"]
mod common;

use common::cluster::{boot_triple_inprocess, propagate, unique_dir, Guard};
use volant_broker::Broker;
use volant_core::{PartitionId, TopicName};
use volant_protocol::ErrorCode;

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
