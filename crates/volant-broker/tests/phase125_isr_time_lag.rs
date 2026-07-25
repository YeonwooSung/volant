//! Phase 125: time-based ISR lag shrink of slow-but-alive followers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{Broker, BrokerEndpoint, ClusterConfig};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p125-{label}-{}-{}",
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

fn cluster_config(session_timeout_ms: u32, lag_max: u64, lag_max_ms: u64) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: lag_max,
        replica_lag_max_ms: lag_max_ms,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 19251,
                rack: None,
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: 19252,
                rack: None,
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: 19253,
                rack: None,
            },
        ],
    }
}

fn boot_triple(
    base: &std::path::Path,
    session_ms: u32,
    lag_max: u64,
    lag_max_ms: u64,
) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config(session_ms, lag_max, lag_max_ms);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", 19250 + id as u16);
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

/// Slow-but-alive follower within message lag but past time threshold → ISR drop.
#[test]
fn time_lag_shrink_alive_slow_follower() {
    let base = unique_dir("time-shrink");
    let _g = Guard(base.clone());
    // Large message lag (won't offset-shrink); tight time lag (50ms).
    let (b1, b2, b3) = boot_triple(&base, 3_000, 10_000, 50);

    b1.create_topic("time", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "time");

    let topic = TopicName::new("time");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let followers: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    let slow = followers[0];
    let fast = followers[1];

    for i in 0..5 {
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

    // Both followers nearly caught up (lag 1 ≤ 10_000) — no offset shrink.
    let near = leader_leo.saturating_sub(1);
    for fid in &followers {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), *fid, near)
            .unwrap();
    }
    let _ = leader
        .handle_replica_fetch("time", 0, near, 1_048_576, fast)
        .unwrap();
    let isr0 = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert_eq!(isr0.len(), 3, "ISR before time lag: {isr0:?}");

    // Age slow follower's last-caught-up past threshold; keep LEO within message lag.
    leader
        .test_set_follower_caught_up_age_ms(&topic, PartitionId(0), slow, 200)
        .unwrap();
    // Keep fast fresh.
    leader
        .test_set_follower_caught_up_age_ms(&topic, PartitionId(0), fast, 0)
        .unwrap();

    let shrink_before = leader.isr_shrink_total();
    let time_before = leader.isr_time_shrink_total();
    let (err, _, _, _) = leader
        .handle_replica_fetch("time", 0, near, 1_048_576, fast)
        .unwrap();
    assert_eq!(err, 0);

    let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr.contains(&slow),
        "time-stale follower must leave ISR: {isr:?}"
    );
    assert!(isr.contains(&leader_id));
    assert!(isr.contains(&fast));
    assert!(
        leader.isr_shrink_total() > shrink_before,
        "time shrink must bump isr_shrink_total"
    );
    assert!(
        leader.isr_time_shrink_total() > time_before,
        "time shrink must bump isr_time_shrink_total"
    );

    // Membership still considers slow live.
    leader.note_peer_live(slow);
    assert!(leader.live_brokers().contains(&slow));
}

/// After time shrink, catch-up ReplicaFetch re-expands (Phase 118 rejoin).
#[test]
fn time_shrink_then_catchup_rejoin() {
    let base = unique_dir("rejoin");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000, 10_000, 50);

    b1.create_topic("rejoin", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "rejoin");

    let topic = TopicName::new("rejoin");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let followers: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    let slow = followers[0];
    let fast = followers[1];

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("seed"));
    let _ = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    let leader_leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    for fid in &followers {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), *fid, leader_leo)
            .unwrap();
    }

    // Time-shrink slow.
    leader
        .test_set_follower_caught_up_age_ms(&topic, PartitionId(0), slow, 200)
        .unwrap();
    let _ = leader
        .handle_replica_fetch("rejoin", 0, leader_leo, 1_048_576, fast)
        .unwrap();
    let isr_mid = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(!isr_mid.contains(&slow), "precondition shrink: {isr_mid:?}");

    let expand_before = leader.isr_expand_total();
    // Catch up at leader LEO (≥ HWM) → rejoin regardless of prior time lag.
    let (err, _, _, _) = leader
        .handle_replica_fetch("rejoin", 0, leader_leo, 1_048_576, slow)
        .unwrap();
    assert_eq!(err, 0);
    let isr_back = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        isr_back.contains(&slow),
        "caught-up follower must rejoin after time shrink: {isr_back:?}"
    );
    assert!(
        leader.isr_expand_total() > expand_before,
        "rejoin must bump isr_expand_total"
    );
}

/// Message-lag shrink still works with time lag enabled (Phase 118 interaction).
#[test]
fn message_lag_shrink_still_works_with_time_enabled() {
    let base = unique_dir("msg-lag");
    let _g = Guard(base.clone());
    // Tight message lag; generous time lag so only offset path fires.
    let (b1, b2, b3) = boot_triple(&base, 3_000, 5, 60_000);

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
    for fid in &followers {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), *fid, leader_leo)
            .unwrap();
    }
    let _ = leader
        .handle_replica_fetch("lag", 0, leader_leo, 1_048_576, fast)
        .unwrap();

    let slow_leo = leader_leo.saturating_sub(20); // lag 20 > 5
    leader
        .test_set_follower_leo(&topic, PartitionId(0), slow, slow_leo)
        .unwrap();
    // Fresh stamps so time path does not also fire.
    leader
        .test_set_follower_caught_up_age_ms(&topic, PartitionId(0), slow, 0)
        .unwrap();
    leader
        .test_set_follower_caught_up_age_ms(&topic, PartitionId(0), fast, 0)
        .unwrap();

    let time_before = leader.isr_time_shrink_total();
    let shrink_before = leader.isr_shrink_total();
    let _ = leader
        .handle_replica_fetch("lag", 0, leader_leo, 1_048_576, fast)
        .unwrap();
    let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr.contains(&slow),
        "offset lag must still shrink: {isr:?}"
    );
    assert!(leader.isr_shrink_total() > shrink_before);
    // Offset path does not increment time_shrink metric.
    assert_eq!(
        leader.isr_time_shrink_total(),
        time_before,
        "message-lag shrink must not bump time_shrink"
    );
}

/// `replica_lag_max_ms = 0` disables time shrink.
#[test]
fn time_lag_disabled_when_zero() {
    let base = unique_dir("disabled");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000, 10_000, 0);

    b1.create_topic("off", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "off");

    let topic = TopicName::new("off");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);
    let followers: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    let slow = followers[0];
    let fast = followers[1];

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("seed"));
    let _ = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    let leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    for fid in &followers {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), *fid, leo)
            .unwrap();
    }
    leader
        .test_set_follower_caught_up_age_ms(&topic, PartitionId(0), slow, 60_000)
        .unwrap();
    let time_before = leader.isr_time_shrink_total();
    let _ = leader
        .handle_replica_fetch("off", 0, leo, 1_048_576, fast)
        .unwrap();
    let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        isr.contains(&slow),
        "time shrink disabled: ISR must keep slow: {isr:?}"
    );
    assert_eq!(leader.isr_time_shrink_total(), time_before);
}

/// Single-node has no ISR time-shrink traffic.
#[test]
fn single_node_no_time_shrink_metrics() {
    let base = unique_dir("single");
    let _g = Guard(base.clone());
    let storage = StorageConfig {
        data_dir: base.join("node"),
        flush_every_n: 1,
        ..StorageConfig::default()
    };
    let b = Broker::new(storage);
    b.create_topic("t", 1).unwrap();
    assert_eq!(b.isr_time_shrink_total(), 0);
    assert_eq!(b.isr_expand_total(), 0);
    assert_eq!(b.isr_shrink_total(), 0);
}
