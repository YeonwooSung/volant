//! Phase 8: client leader redirect + follower rolling restart.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker, BrokerEndpoint, ClusterConfig};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p8-{label}-{}-{}",
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

/// Connect to a follower and produce; client must redirect to the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_redirects_produce_to_leader() {
    let base = unique_dir("redirect");
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
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        broker.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(broker)
    };

    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    for (listener, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("redir", 1).await.unwrap();
    propagate(&[&b1, &b2, &b3], "redir").await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    let follower_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .unwrap();
    let port_of = |id: u32| ports[(id - 1) as usize];

    let client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(follower_id))],
        max_redirects: 2,
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    assert!(client
        .current_addr()
        .await
        .ends_with(&format!(":{}", port_of(follower_id))));

    let result = client
        .produce_with_acks(
            "redir",
            Some(0),
            vec![Message::from_value("via-redirect")],
            255,
        )
        .await
        .expect("produce should redirect to leader");
    assert_eq!(result.count, 1);
    assert_eq!(
        client.current_addr().await,
        format!("127.0.0.1:{}", port_of(leader_id)),
        "client should reconnect to leader host"
    );

    // acks=all waited for HWM; fetch should see the record.
    let fetched = client
        .fetch("redir", 0, Offset::ZERO, 10, 0)
        .await
        .unwrap();
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_ref(), b"via-redirect");
}

/// Kill a non-leader follower's network task, produce while down, restart accept, produce again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolling_restart_follower_preserves_data() {
    let base = unique_dir("rolling");
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
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        broker.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(broker)
    };

    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    let brokers = [Arc::clone(&b1), Arc::clone(&b2), Arc::clone(&b3)];

    let mut handles = Vec::new();
    for (listener, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        handles.push(tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        }));
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("roll", 1).await.unwrap();
    propagate(&[&b1, &b2, &b3], "roll").await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    let follower_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("need a follower");
    let port_of = |id: u32| ports[(id - 1) as usize];
    let broker_of = |id: u32| Arc::clone(&brokers[(id - 1) as usize]);

    let producer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    for i in 0..3u32 {
        producer
            .produce_with_acks(
                "roll",
                Some(0),
                vec![Message::from_value(format!("pre-{i}"))],
                255,
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Stop follower accept loop (index follower_id - 1).
    handles[(follower_id - 1) as usize].abort();
    for b in &brokers {
        let _ = b.test_kill_broker(follower_id);
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Produce while one follower is down (min.isr=2, still ok with 2 live).
    for i in 0..2u32 {
        producer
            .produce_with_acks(
                "roll",
                Some(0),
                vec![Message::from_value(format!("mid-{i}"))],
                255,
            )
            .await
            .expect("produce while follower down");
    }

    // Restart follower listen on the same port.
    let listener = loop {
        match TcpListener::bind(format!("127.0.0.1:{}", port_of(follower_id))).await {
            Ok(l) => break l,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };
    for b in &brokers {
        b.note_peer_live(follower_id);
    }
    let restarted = broker_of(follower_id);
    tokio::spawn(async move {
        let _ = serve_listener(listener, restarted).await;
    });

    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    for b in &brokers {
        let _ = b.apply_cluster_state(gen, cid, &topics);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    for i in 0..2u32 {
        producer
            .produce_with_acks(
                "roll",
                Some(0),
                vec![Message::from_value(format!("post-{i}"))],
                255,
            )
            .await
            .expect("produce after follower restart");
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let fetched = producer
        .fetch("roll", 0, Offset::ZERO, 100, 0)
        .await
        .unwrap();
    assert!(
        fetched.records.len() >= 7,
        "expected >=7 committed records, got {}",
        fetched.records.len()
    );
}
