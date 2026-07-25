//! Phase 118: ISR rejoin after death + lag-based shrink of slow-but-alive followers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{Broker, BrokerEndpoint, ClusterConfig};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p118-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn cluster_config(session_timeout_ms: u32, lag_max: u64) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: lag_max,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 19181,
                rack: None,
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: 19182,
                rack: None,
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: 19183,
                rack: None,
            },
        ],
    }
}

fn boot_triple(
    base: &std::path::Path,
    session_ms: u32,
    lag_max: u64,
) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config(session_ms, lag_max);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", 19180 + id as u16);
        Arc::new(b)
    };
    (mk(1), mk(2), mk(3))
}

fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..40 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes
            .iter()
            .all(|n| n.partition_count_opt(topic).is_some())
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("assignment did not propagate for topic {topic}");
}

fn broker_of(b1: &Arc<Broker>, b2: &Arc<Broker>, b3: &Arc<Broker>, id: u32) -> Arc<Broker> {
    match id {
        1 => Arc::clone(b1),
        2 => Arc::clone(b2),
        3 => Arc::clone(b3),
        _ => panic!("bad id {id}"),
    }
}

/// Death shrinks ISR; ReplicaFetch catch-up re-expands; HWM advances with rejoin.
#[test]
fn death_shrink_then_replica_fetch_rejoin() {
    let base = unique_dir("rejoin");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000, 10_000);

    b1.create_topic("events", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "events");

    let topic = TopicName::new("events");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let followers: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    let dead_id = followers[0];
    let other = followers[1];

    // Seed + catch both followers up.
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("seed"));
    let (recs, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(recs.len(), 1);
    let leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    for fid in &followers {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), *fid, leo)
            .unwrap();
    }

    let shrink_before = leader.isr_shrink_total();
    let expand_before = leader.isr_expand_total();

    // Kill one follower → ISR shrink (Phase 108 path).
    leader.test_kill_broker(dead_id).unwrap();
    let isr_mid = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr_mid.contains(&dead_id),
        "ISR must drop dead follower: {isr_mid:?}"
    );
    assert!(isr_mid.contains(&leader_id));
    assert!(isr_mid.contains(&other));
    assert!(
        leader.isr_shrink_total() > shrink_before,
        "death must bump isr_shrink_total"
    );

    // Produce while dead so HWM can advance with remaining ISR.
    let leo_now = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), other, leo_now + 1)
        .unwrap();
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("while-dead"));
    let (recs, err) = leader
        .produce_with_acks(
            &topic,
            PartitionId(0),
            batch,
            255,
            Some(Duration::from_millis(500)),
        )
        .expect("acks=all after shrink");
    assert_eq!(err, 0);
    assert_eq!(recs.len(), 1);

    let hwm_after = leader.high_watermark(&topic, PartitionId(0)).unwrap();
    let leader_leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    assert!(leader_leo >= 2);
    assert!(hwm_after >= 2, "HWM after acks=all while dead: {hwm_after}");

    // Recovering follower still behind HWM (at seed LEO=1 while HWM advanced) → no rejoin yet.
    let behind = 1u64; // seed only
    let (err, _, _, _) = leader
        .handle_replica_fetch("events", 0, behind, 1_048_576, dead_id)
        .unwrap();
    assert_eq!(err, 0);
    let isr_behind = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr_behind.contains(&dead_id),
        "must not rejoin while LEO < HWM: {isr_behind:?}"
    );

    // Catch up to leader LEO (≥ HWM) → rejoin.
    let (err, _, _, _) = leader
        .handle_replica_fetch("events", 0, leader_leo, 1_048_576, dead_id)
        .unwrap();
    assert_eq!(err, 0);
    let isr_back = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        isr_back.contains(&dead_id),
        "caught-up follower must rejoin ISR: {isr_back:?}"
    );
    assert_eq!(isr_back.len(), 3, "full RF ISR: {isr_back:?}");
    assert!(
        leader.isr_expand_total() > expand_before,
        "rejoin must bump isr_expand_total"
    );
}

/// Slow-but-alive follower with lag > replica_lag_max_messages is dropped from ISR.
#[test]
fn lag_based_shrink_alive_slow_follower() {
    let base = unique_dir("lag-shrink");
    let _g = Guard(base.clone());
    // Tight lag so a few messages kick a slow member out.
    let (b1, b2, b3) = boot_triple(&base, 3_000, 5);

    b1.create_topic("lag", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "lag");

    let topic = TopicName::new("lag");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let followers: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    let slow = followers[0];
    let fast = followers[1];

    // Append enough that lag > 5 is easy.
    for i in 0..20 {
        let mut batch = MessageBatch::default();
        batch
            .messages
            .push(Message::from_value(format!("m{i}")));
        let (_, err) = leader
            .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
            .unwrap();
        assert_eq!(err, 0);
    }
    let leader_leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    assert!(leader_leo >= 20);

    // Both followers start caught up.
    for fid in &followers {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), *fid, leader_leo)
            .unwrap();
    }
    // Ensure full ISR via a no-op reconcile fetch from fast.
    let _ = leader
        .handle_replica_fetch("lag", 0, leader_leo, 1_048_576, fast)
        .unwrap();
    let isr0 = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert_eq!(isr0.len(), 3, "ISR before lag: {isr0:?}");

    // Slow follower stuck far behind; fast fetch triggers lag shrink of slow.
    let slow_leo = leader_leo.saturating_sub(20); // lag 20 > 5
    leader
        .test_set_follower_leo(&topic, PartitionId(0), slow, slow_leo)
        .unwrap();

    let shrink_before = leader.isr_shrink_total();
    let (err, _, _, _) = leader
        .handle_replica_fetch("lag", 0, leader_leo, 1_048_576, fast)
        .unwrap();
    assert_eq!(err, 0);

    let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr.contains(&slow),
        "slow-but-alive follower must leave ISR: {isr:?}"
    );
    assert!(isr.contains(&leader_id));
    assert!(isr.contains(&fast));
    assert!(
        leader.isr_shrink_total() > shrink_before,
        "lag shrink must bump isr_shrink_total"
    );

    // Membership still considers slow live (no death).
    leader.note_peer_live(slow);
    assert!(leader.live_brokers().contains(&slow));
}

/// ClusterState with death-shrunk assignment preserves leader-local rejoin.
#[test]
fn cluster_state_apply_preserves_local_rejoin() {
    let base = unique_dir("preserve");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000, 10_000);

    b1.create_topic("keep", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "keep");

    let topic = TopicName::new("keep");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let dead_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .unwrap();
    let other = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id && *id != dead_id)
        .unwrap();

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("seed"));
    let _ = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    let leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    for fid in [1u32, 2, 3] {
        if fid != leader_id {
            leader
                .test_set_follower_leo(&topic, PartitionId(0), fid, leo)
                .unwrap();
        }
    }

    // Death on controller (b1) shrinks durable assignment.
    b1.test_kill_broker(dead_id).unwrap();
    // Leader (may be b1) local ISR already shrunk via death.
    if leader_id != 1 {
        leader.test_kill_broker(dead_id).unwrap();
    }

    // Rejoin on leader via catch-up fetch.
    let leader_leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    // Keep other in sync so HWM is high.
    leader
        .test_set_follower_leo(&topic, PartitionId(0), other, leader_leo)
        .unwrap();
    let _ = leader
        .handle_replica_fetch("keep", 0, leader_leo, 1_048_576, dead_id)
        .unwrap();
    // Mark rejoined peer live so preserve path keeps it.
    leader.note_peer_live(dead_id);
    // Controller may still mark dead — re-live on all for apply path.
    b1.note_peer_live(dead_id);
    b2.note_peer_live(dead_id);
    b3.note_peer_live(dead_id);

    let isr_local = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        isr_local.contains(&dead_id),
        "precondition rejoin: {isr_local:?}"
    );

    // Apply controller ClusterState that still has shrunk ISR (simulate lag).
    // Build a snapshot from b1; if leader already bumped gen on rejoin and is
    // controller, force apply by re-reading and stripping dead_id from wire.
    let (err, gen, cid, mut topics_wire) = b1.cluster_state_snapshot();
    assert_eq!(err, 0);
    for tw in &mut topics_wire {
        if tw.name == "keep" {
            for pw in &mut tw.partitions {
                pw.isr.retain(|id| *id != dead_id);
            }
        }
    }
    // Use a generation at least as high as local so apply is not rejected as stale.
    let apply_gen = gen.saturating_add(1);
    leader
        .apply_cluster_state(apply_gen, cid, &topics_wire)
        .unwrap();

    let isr_after = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        isr_after.contains(&dead_id),
        "ClusterState with shrunk ISR must preserve caught-up local rejoin: {isr_after:?}"
    );
    assert!(isr_after.contains(&leader_id));
    assert!(isr_after.contains(&other));
}

/// Single-node has no ISR expand/shrink traffic.
#[test]
fn single_node_no_isr_metrics() {
    let base = unique_dir("single");
    let _g = Guard(base.clone());
    let storage = StorageConfig {
        data_dir: base.join("node"),
        flush_every_n: 1,
        ..StorageConfig::default()
    };
    let b = Broker::new(storage);
    b.create_topic("t", 1).unwrap();
    assert_eq!(b.isr_expand_total(), 0);
    assert_eq!(b.isr_shrink_total(), 0);
}
