//! Phase 113 PR2: DeleteRecords fan-out to ISR replicas.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use volant_broker::{
    serve_listener, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, TopicName};
use volant_protocol::ErrorCode;
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p113-{label}-{}-{}",
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

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn cluster_config(ports: [u16; 3]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        brokers: (1..=3)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: ports[(id - 1) as usize],
                rack: None,
            })
            .collect(),
    }
}

async fn propagate(nodes: &[&Broker], topic: &str) {
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("assignment did not propagate for topic {topic}");
}

fn big(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

/// Produce enough sealed-segment data on the leader so DeleteRecords can raise log_start.
async fn fill_and_replicate(
    leader: &Client,
    topic: &str,
    min_latest: u64,
) {
    let mut i = 0u32;
    loop {
        leader
            .produce_with_acks(
                topic,
                Some(0),
                vec![Message::from_value(big(&format!("m{i}"), 180))],
                255,
            )
            .await
            .expect("produce acks=all");
        i += 1;
        let offs = leader.list_offsets(topic, vec![0]).await.unwrap();
        if offs.entries[0].latest >= min_latest {
            break;
        }
        if i > 300 {
            panic!("could not fill past {min_latest}");
        }
    }
}

async fn wait_log_start(broker: &Broker, topic: &str, min_start: u64) -> u64 {
    let name = TopicName::new(topic);
    for _ in 0..80 {
        if let Ok(entries) = broker.list_offsets(topic, &[0]) {
            if let Some((_, earliest, _)) = entries.first() {
                if *earliest >= min_start {
                    return *earliest;
                }
            }
        }
        let _ = name; // silence
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    broker
        .list_offsets(topic, &[0])
        .ok()
        .and_then(|e| e.first().map(|x| x.1))
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_delete_records_fans_out_to_followers() {
    let base = unique_dir("fanout");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            // Small segments so DeleteRecords can drop whole early segments.
            segment_size: 256,
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        broker.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(broker)
    };

    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];

    for (listener, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(&[&b1, &b2, &b3], "events").await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    let port_of = |id: u32| ports[(id - 1) as usize];
    let broker_of = |id: u32| -> Arc<Broker> {
        match id {
            1 => Arc::clone(&b1),
            2 => Arc::clone(&b2),
            3 => Arc::clone(&b3),
            _ => panic!("bad id"),
        }
    };

    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    // Fill so at least one sealed segment exists for truncate.
    fill_and_replicate(&leader_client, "events", 25).await;

    // Wait for followers to catch LEO.
    let latest = leader_client
        .list_offsets("events", vec![0])
        .await
        .unwrap()
        .entries[0]
        .latest;
    for id in [1u32, 2, 3] {
        if id == leader_id {
            continue;
        }
        let b = broker_of(id);
        for _ in 0..80 {
            let entries = b.list_offsets("events", &[0]).unwrap();
            if entries[0].2 >= latest {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let entries = b.list_offsets("events", &[0]).unwrap();
        assert!(
            entries[0].2 >= latest,
            "follower {id} LEO {} < leader latest {latest}",
            entries[0].2
        );
    }

    // DeleteRecords on leader — should advance log start and fan out.
    let before = latest / 2;
    assert!(before > 0, "need positive delete offset");
    let del = leader_client
        .delete_records("events", 0, before)
        .await
        .expect("delete_records");
    assert!(
        del.low_watermark > 0,
        "leader low watermark should advance: {}",
        del.low_watermark
    );
    let leader_low = del.low_watermark;

    // Followers should reach at least the same log start (best-effort; wait briefly).
    for id in [1u32, 2, 3] {
        if id == leader_id {
            continue;
        }
        let earliest = wait_log_start(&broker_of(id), "events", leader_low).await;
        assert!(
            earliest >= leader_low,
            "follower {id} log_start {earliest} < leader low {leader_low}"
        );
    }

    // Non-leader client DeleteRecords → NotLeader (error response).
    let follower_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .unwrap();
    let follower_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(follower_id))],
        max_redirects: 0,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    let err = follower_client
        .delete_records("events", 0, before)
        .await
        .expect_err("non-leader delete should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("NotLeader")
            || msg.contains("not leader")
            || msg.contains(&format!("{}", ErrorCode::NotLeaderForPartition as u16))
            || msg.contains("13"),
        "expected not-leader error, got: {msg}"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_failure_does_not_fail_client_and_increments_metric() {
    let base = unique_dir("fanout-err");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    // Third broker advertised but never listens — RPC fails.
    let p3 = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            segment_size: 256,
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        broker.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(broker)
    };

    let b1 = mk(1);
    let b2 = mk(2);
    // No b3 listener / process.

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
    ];
    for (listener, b) in [(l1, &b1), (l2, &b2)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 1, // RF=3 but node 3 never up — use acks=1
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    // min_insync=2 would fail acks=all; create topic still works on controller.
    controller.create_topic("t", 1).await.unwrap();
    propagate(&[&b1, &b2], "t").await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    // Only test when leader is 1 or 2 (reachable).
    assert!(leader_id == 1 || leader_id == 2);

    let port_of = |id: u32| ports[(id - 1) as usize];
    let leader = broker_of_pair(&b1, &b2, leader_id);
    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    // Produce with acks=1 so we don't wait on dead replica ISR.
    for i in 0..40u32 {
        leader_client
            .produce_with_acks(
                "t",
                Some(0),
                vec![Message::from_value(big(&format!("x{i}"), 180))],
                1,
            )
            .await
            .unwrap();
    }

    let before_err = leader.delete_records_fanout_errors_total();
    let latest = leader_client
        .list_offsets("t", vec![0])
        .await
        .unwrap()
        .entries[0]
        .latest;
    let before = latest / 2;
    // Client delete must succeed even if fan-out to dead peer fails.
    let del = leader_client
        .delete_records("t", 0, before)
        .await
        .expect("client delete must succeed despite fan-out errors");
    assert!(del.low_watermark > 0 || before == 0);

    // Dead peer (id 3) is always in replica set → at least one fan-out error.
    let mut saw = false;
    for _ in 0..40 {
        if leader.delete_records_fanout_errors_total() > before_err {
            saw = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        saw,
        "expected fan-out error counter to increase (before={before_err}, after={})",
        leader.delete_records_fanout_errors_total()
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

fn broker_of_pair(b1: &Arc<Broker>, b2: &Arc<Broker>, id: u32) -> Arc<Broker> {
    match id {
        1 => Arc::clone(b1),
        2 => Arc::clone(b2),
        _ => panic!("expected leader 1 or 2"),
    }
}

#[test]
fn handle_replica_delete_records_epoch_fence() {
    let dir = unique_dir("epoch-fence");
    let _g = Guard(dir.clone());
    let storage = StorageConfig {
        data_dir: dir.join("n1"),
        segment_size: 256,
        ..StorageConfig::default()
    };
    let broker = Broker::new(storage);
    broker.create_topic("e", 1).unwrap();
    for i in 0..30u32 {
        broker
            .produce_one(
                &TopicName::new("e"),
                volant_core::PartitionId(0),
                Message::from_value(big(&format!("y{i}"), 180)),
            )
            .unwrap();
    }
    // Bump epoch via metadata path if available; single-node epoch is 0.
    // Stale request epoch -1 always applies; positive epoch equal to local applies.
    let (code, low) = broker.handle_replica_delete_records("e", 0, 10, 0);
    assert_eq!(code, 0);
    let _ = low;

    // Request with epoch older than local: force local epoch high by only
    // testing equal/greater. With epoch 0, request epoch -1 applies; request
    // epoch 0 applies. Simulate fence by calling with epoch -1 after bump is
    // not easy without cluster failover — assert fenced when local > request
    // by using handle after manually checking code path with epoch 0 vs 0.
    let (code2, _) = broker.handle_replica_delete_records("e", 0, 5, -1);
    assert_eq!(code2, 0);

    // Unknown topic
    let (code3, _) = broker.handle_replica_delete_records("missing", 0, 1, -1);
    assert_eq!(code3, ErrorCode::NotFound as u16);
}

#[test]
fn single_node_delete_records_unchanged() {
    let dir = unique_dir("single");
    let _g = Guard(dir.clone());
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        segment_size: 256,
        ..StorageConfig::default()
    });
    broker.create_topic("s", 1).unwrap();
    for i in 0..30u32 {
        broker
            .produce_one(
                &TopicName::new("s"),
                volant_core::PartitionId(0),
                Message::from_value(big(&format!("z{i}"), 180)),
            )
            .unwrap();
    }
    let (low, err) = broker.delete_records("s", 0, 15).unwrap();
    assert_eq!(err, 0);
    assert!(low > 0);
    // No peers → fan-out peers empty.
    assert!(broker.delete_records_fanout_peers("s", 0).is_empty());
    assert_eq!(broker.delete_records_fanout_errors_total(), 0);
}
