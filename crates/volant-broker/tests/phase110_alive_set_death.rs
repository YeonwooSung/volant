//! Phase 110: non-controller auto-death from heartbeat alive-set diffs.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{Broker, BrokerEndpoint, ClusterConfig};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p110-{label}-{}-{}",
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

fn cluster_config(session_timeout_ms: u32) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 19091,
                rack: None,
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: 19092,
                rack: None,
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: 19093,
                rack: None,
            },
        ],
    }
}

fn boot_triple(base: &std::path::Path, session_ms: u32) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config(session_ms);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", 19090 + id as u16);
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

/// Non-controller applies controller alive-set missing a peer → local ISR + membership drop.
#[test]
fn non_controller_alive_set_diff_shrinks_local_isr() {
    let base = unique_dir("alive-diff");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000);
    assert!(b1.is_controller());
    assert!(!b2.is_controller());

    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    let topic = TopicName::new("t");
    let isr_before = b2.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert_eq!(isr_before.len(), 3, "RF=3 ISR before death: {isr_before:?}");
    assert!(isr_before.contains(&3));
    assert!(b2.live_brokers().contains(&3));

    // Controller reports alive without broker 3 (simulates HeartbeatBroker response).
    b2.apply_controller_alive_set(&[1, 2]).unwrap();

    let isr_after = b2.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr_after.contains(&3),
        "local ISR must drop dead peer immediately: {isr_after:?}"
    );
    assert_eq!(isr_after.len(), 2, "ISR after: {isr_after:?}");
    assert!(
        !b2.live_brokers().contains(&3),
        "membership must mark 3 dead"
    );
    // Survivors stay live.
    assert!(b2.live_brokers().contains(&1));
    assert!(b2.live_brokers().contains(&2));
}

/// Alive-set death on the partition leader unblocks acks=all HWM wait (stale LEO).
#[test]
fn alive_set_death_on_leader_unblocks_acks_all() {
    let base = unique_dir("acks-all");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000);

    b1.create_topic("events", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "events");

    let topic = TopicName::new("events");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let broker_of = |id: u32| -> Arc<Broker> {
        match id {
            1 => Arc::clone(&b1),
            2 => Arc::clone(&b2),
            3 => Arc::clone(&b3),
            _ => panic!("bad id"),
        }
    };
    let leader = broker_of(leader_id);
    let dead_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .unwrap();
    let other = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id && *id != dead_id)
        .unwrap();

    // Seed one message and catch remaining live follower up so min.isr=2 works.
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("seed"));
    let (recs, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(recs.len(), 1);
    let leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    // Mark both followers caught up initially.
    for fid in [1u32, 2, 3] {
        if fid != leader_id {
            leader
                .test_set_follower_leo(&topic, PartitionId(0), fid, leo)
                .unwrap();
        }
    }

    // Dead follower stuck at LEO 0 → without ISR shrink, acks=all HWM cannot advance.
    leader
        .test_set_follower_leo(&topic, PartitionId(0), dead_id, 0)
        .unwrap();

    // Alive set from controller: everyone except dead_id.
    let alive: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != dead_id)
        .collect();
    leader.apply_controller_alive_set(&alive).unwrap();

    let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(!isr.contains(&dead_id), "leader local ISR: {isr:?}");
    assert!(isr.contains(&leader_id));
    assert!(isr.contains(&other));

    // Pre-position remaining follower LEO to cover the upcoming append so HWM
    // can advance without a live ReplicaFetch loop (unit-style).
    let leo_now = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), other, leo_now + 1)
        .unwrap();

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("after-death"));
    let (recs, err) = leader
        .produce_with_acks(
            &topic,
            PartitionId(0),
            batch,
            255,
            Some(Duration::from_millis(500)),
        )
        .expect("produce_with_acks");
    assert_eq!(err, 0, "acks=all must succeed after alive-set ISR shrink");
    assert_eq!(recs.len(), 1);

    // Sanity: without the shrink, dead_id at LEO 0 would have pinned HWM.
    // Dead id must not reappear in local ISR.
    let isr2 = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(!isr2.contains(&dead_id));
}

/// Local membership expire on a non-controller also runs on_broker_death (Phase 110).
#[test]
fn non_controller_tick_expire_shrinks_local_isr() {
    let base = unique_dir("tick-expire");
    let _g = Guard(base.clone());
    // Short session so Instant-based expire is testable without long sleeps.
    let (b1, b2, b3) = boot_triple(&base, 80);

    b1.create_topic("tick", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "tick");
    assert!(!b2.is_controller());

    let topic = TopicName::new("tick");
    assert!(b2
        .local_partition_isr(&topic, PartitionId(0))
        .unwrap()
        .contains(&3));

    // Let optimistic start heartbeats age out, then refresh only survivors.
    std::thread::sleep(Duration::from_millis(120));
    b2.note_peer_live(1);
    b2.note_peer_live(2);
    // broker 3 not refreshed → expire on tick.
    b2.tick_cluster();

    let isr = b2.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert!(
        !isr.contains(&3),
        "non-controller tick expire must shrink local ISR: {isr:?}"
    );
    assert!(!b2.live_brokers().contains(&3));
}

/// apply_controller_alive_set is idempotent when the set is unchanged.
#[test]
fn alive_set_idempotent_when_unchanged() {
    let base = unique_dir("idempotent");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, 3_000);
    b1.create_topic("idemp", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "idemp");

    let topic = TopicName::new("idemp");
    let before = b2.local_partition_isr(&topic, PartitionId(0)).unwrap();
    b2.apply_controller_alive_set(&[1, 2, 3]).unwrap();
    b2.apply_controller_alive_set(&[1, 2, 3]).unwrap();
    let after = b2.local_partition_isr(&topic, PartitionId(0)).unwrap();
    assert_eq!(before, after);
    assert_eq!(b2.live_brokers(), vec![1, 2, 3]);
}
