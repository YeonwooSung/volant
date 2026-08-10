//! Phase 133: Preferred read-replica selector ranking + usability gates.
//!
//! In-process tests of `Broker::select_preferred_read_replica` (no Kafka wire).
//! Keeps phase126 preferred + isolation suites as the wire-level regression net.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{Broker, BrokerEndpoint, ClusterConfig};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p133-{label}-{}-{}",
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

fn cluster_config_racks(ports: [u16; 3], racks: [Option<&str>; 3]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 30_000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=3)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: ports[(id - 1) as usize],
                rack: racks[(id - 1) as usize].map(|s| s.to_string()),
            })
            .collect(),
    }
}

fn boot_triple(base: &std::path::Path, ports: [u16; 3]) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    // All same rack so any leader has two same-rack followers after catch-up.
    let cfg = cluster_config_racks(ports, [Some("rack-a"), Some("rack-a"), Some("rack-a")]);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("n{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    (mk(1), mk(2), mk(3))
}

fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
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

fn catch_up_isr(leader: &Broker, topic: &str) {
    let t = TopicName::new(topic);
    let leo = leader.log_end_offset(&t, PartitionId(0)).unwrap();
    let isr = leader.local_partition_isr(&t, PartitionId(0)).unwrap();
    let lid = leader.node_id();
    for fid in isr {
        if fid != lid {
            leader
                .test_set_follower_leo(&t, PartitionId(0), fid, leo)
                .unwrap();
        }
    }
}

fn leader_of(b1: &Arc<Broker>, b2: &Arc<Broker>, b3: &Arc<Broker>, topic: &str) -> Arc<Broker> {
    let tname = TopicName::new(topic);
    let leader_id = b1
        .metadata(None)
        .topics
        .iter()
        .find(|t| t.name == tname)
        .map(|t| t.partitions[0].leader)
        .expect("topic metadata");
    match leader_id {
        1 => Arc::clone(b1),
        2 => Arc::clone(b2),
        3 => Arc::clone(b3),
        _ => panic!("bad leader {leader_id}"),
    }
}

/// Setup: 3-node same-rack cluster, one partition, produce + catch-up ISR LEOs.
fn setup_caught_up(label: &str) -> (Guard, Arc<Broker>, TopicName, Vec<u32>) {
    let base = unique_dir(label);
    let guard = Guard(base.clone());
    // Fixed high ports — no listeners needed for in-process selector tests.
    let ports = [29191, 29192, 29193];
    let (b1, b2, b3) = boot_triple(&base, ports);

    b1.create_topic(label, 1).unwrap();
    propagate(&[&b1, &b2, &b3], label);

    let topic = TopicName::new(label);
    let leader = leader_of(&b1, &b2, &b3, label);

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("p133"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, label);

    let lid = leader.node_id();
    let followers: Vec<u32> = leader
        .local_partition_isr(&topic, PartitionId(0))
        .unwrap()
        .into_iter()
        .filter(|id| *id != lid)
        .collect();
    assert_eq!(
        followers.len(),
        2,
        "expected 2 followers in ISR; leader={lid} isr={:?}",
        leader.local_partition_isr(&topic, PartitionId(0)).unwrap()
    );

    (guard, leader, topic, followers)
}

/// Test A: two same-rack followers both LEO≥HWM; higher LEO wins even if higher id.
#[test]
fn higher_leo_wins_over_lower_id() {
    let (_g, leader, topic, mut followers) = setup_caught_up("leo-rank");
    followers.sort_unstable();
    let low_id = followers[0];
    let high_id = followers[1];
    assert!(low_id < high_id);

    let hwm_base = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    // Both ≥ HWM; high_id has strictly higher LEO so it must beat pure min-id ranking.
    leader
        .test_set_follower_leo(&topic, PartitionId(0), low_id, hwm_base)
        .unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), high_id, hwm_base + 10)
        .unwrap();

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_eq!(
        pref,
        Some(high_id),
        "highest LEO must win even when id is larger; low_id={low_id} high_id={high_id}"
    );

    // Tie on LEO → lowest id still wins (regression vs pure LEO-only).
    leader
        .test_set_follower_leo(&topic, PartitionId(0), low_id, hwm_base + 10)
        .unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), high_id, hwm_base + 10)
        .unwrap();
    let pref_tie = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_eq!(pref_tie, Some(low_id), "equal LEO → lowest id tiebreak");
}

/// Test B: higher-LEO peer not live → other eligible selected (or None if sole gone).
#[test]
fn non_live_higher_leo_skipped() {
    let (_g, leader, topic, mut followers) = setup_caught_up("leo-dead");
    followers.sort_unstable();
    let low_id = followers[0];
    let high_id = followers[1];

    let hwm_base = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), low_id, hwm_base)
        .unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), high_id, hwm_base + 10)
        .unwrap();

    // Sanity: live high-LEO wins first.
    assert_eq!(
        leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a")),
        Some(high_id)
    );

    // Mark high-LEO peer dead via controller alive-set (Phase 110 path).
    let lid = leader.node_id();
    let alive: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != high_id)
        .collect();
    assert!(alive.contains(&lid));
    leader.apply_controller_alive_set(&alive).unwrap();
    assert!(
        !leader.live_brokers().contains(&high_id),
        "high-LEO peer must not be live"
    );

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    // Remaining follower should still be eligible (live + LEO≥HWM + same rack).
    // If ISR shrink dropped it too, selector returns None — still honest.
    match pref {
        Some(id) => {
            assert_eq!(id, low_id, "must select remaining live eligible peer");
            assert_ne!(id, high_id, "must not select non-live high-LEO peer");
        }
        None => {
            // Sole remaining candidate may have been removed from ISR on death.
            let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
            assert!(
                !isr.contains(&low_id) || !leader.live_brokers().contains(&low_id),
                "None only expected when no other eligible peer remains; isr={isr:?} live={:?}",
                leader.live_brokers()
            );
        }
    }

    // If we also kill the remaining follower, selector must return None.
    let alone = vec![lid];
    leader.apply_controller_alive_set(&alone).unwrap();
    assert_eq!(
        leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a")),
        None,
        "no live same-rack followers → no preferred"
    );
}

/// Usable-address gate: peer with empty host is not selected when another eligible exists.
#[test]
fn empty_addr_peer_skipped_when_other_eligible() {
    let base = unique_dir("empty-addr");
    let _g = Guard(base.clone());

    // Broker 2 has empty host → unusable endpoint; 1 and 3 are fine, all rack-a.
    let cfg = ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 30_000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 29201,
                rack: Some("rack-a".into()),
            },
            BrokerEndpoint {
                id: 2,
                host: "".into(),
                port: 29202,
                rack: Some("rack-a".into()),
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: 29203,
                rack: Some("rack-a".into()),
            },
        ],
    };

    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("n{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", 29200 + id as u16);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    b1.create_topic("ea", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "ea");
    let topic = TopicName::new("ea");
    let leader = leader_of(&b1, &b2, &b3, "ea");

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("x"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "ea");

    // Give empty-host peer a higher LEO so ranking alone would prefer it.
    let hwm = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    let lid = leader.node_id();
    for fid in leader.local_partition_isr(&topic, PartitionId(0)).unwrap() {
        if fid != lid {
            let leo = if fid == 2 { hwm + 50 } else { hwm };
            leader
                .test_set_follower_leo(&topic, PartitionId(0), fid, leo)
                .unwrap();
        }
    }

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    // Broker 2 must never be preferred (empty host). When leader is 2, self is
    // already excluded; when leader is 1 or 3, the other usable peer wins.
    assert_ne!(pref, Some(2), "empty-host peer must not be preferred; got {pref:?}");
    if lid != 2 {
        assert!(
            pref.is_some(),
            "expected another eligible peer when empty-host is skipped; leader={lid}"
        );
    }
}
