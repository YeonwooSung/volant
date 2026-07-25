//! Phase 6: 3-node in-process cluster, acks=all produce, leader kill, fetch from new leader.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker, BrokerEndpoint, ClusterConfig};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-cluster-{label}-{}-{}",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_acks_all_survives_leader_kill() {
    let base = unique_dir("failover");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;

    let config_for = |p1: u16, p2: u16, p3: u16| ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: p1,
                rack: None,
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: p2,
                rack: None,
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: p3,
                rack: None,
            },
        ],
    };

    let mk = |id: u32, port: u16| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, id, config_for(p1, p2, p3)).unwrap();
        broker.set_advertised("127.0.0.1", port);
        Arc::new(broker)
    };

    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3);

    let h1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    let h2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_listener(l2, b).await;
        })
    };
    let h3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            let _ = serve_listener(l3, b).await;
        })
    };

    tokio::time::sleep(Duration::from_millis(150)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    controller.create_topic("events", 1).await.unwrap();

    // Propagate assignment to all nodes.
    for _ in 0..40 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("events").is_some() && b3.partition_count_opt("events").is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(b2.partition_count_opt("events").is_some());
    assert!(b3.partition_count_opt("events").is_some());

    let meta = controller.metadata().await.unwrap();
    let part = &meta.topics[0].partitions[0];
    let leader_id = part.leader;
    assert_eq!(part.replicas.len(), 3);

    let port_of = |id: u32| -> u16 {
        match id {
            1 => p1,
            2 => p2,
            3 => p3,
            _ => panic!("bad id"),
        }
    };
    let broker_of = |id: u32| -> Arc<Broker> {
        match id {
            1 => Arc::clone(&b1),
            2 => Arc::clone(&b2),
            3 => Arc::clone(&b3),
            _ => panic!("bad id"),
        }
    };

    let producer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    const N: u32 = 5;
    for i in 0..N {
        let r = producer
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(format!("msg-{i}"))],
                255,
            )
            .await
            .expect("acks=all produce");
        assert_eq!(r.count, 1);
        assert_eq!(r.base_offset, i as u64);
    }

    // Allow final replica fetches to settle.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let fetched = producer
        .fetch("events", 0, Offset::ZERO, 100, 0)
        .await
        .unwrap();
    assert_eq!(fetched.records.len() as u32, N);

    // Kill only the leader's accept loop.
    match leader_id {
        1 => h1.abort(),
        2 => h2.abort(),
        3 => h3.abort(),
        _ => unreachable!(),
    }

    // Remaining nodes run failover.
    let survivors: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    for &sid in &survivors {
        broker_of(sid).test_kill_broker(leader_id).unwrap();
    }
    // Propagate assignment from the new controller.
    let new_ctrl_id = *survivors.iter().min().unwrap();
    let ctrl = broker_of(new_ctrl_id);
    assert!(ctrl.is_controller(), "lowest live id should be controller");
    let (_, gen, cid, topics) = ctrl.cluster_state_snapshot();
    for &sid in &survivors {
        let _ = broker_of(sid).apply_cluster_state(gen, cid, &topics);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = ctrl.metadata(None);
    let new_leader_id = snap.topics[0].partitions[0].leader;
    assert_ne!(new_leader_id, leader_id);
    assert!(survivors.contains(&new_leader_id));

    let new_leader = broker_of(new_leader_id);
    let topic = TopicName::new("events");
    let leo = new_leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    assert!(
        leo >= N as u64,
        "new leader LEO {leo} missing acks=all data (N={N})"
    );

    // Recompute HWM from remaining ISR LEOs so client fetch is unblocked.
    for &sid in &survivors {
        if sid != new_leader_id {
            let other_leo = broker_of(sid)
                .log_end_offset(&topic, PartitionId(0))
                .unwrap_or(0);
            new_leader
                .test_set_follower_leo(&topic, PartitionId(0), sid, other_leo)
                .unwrap();
        }
    }
    // If only one ISR member remains after shrink, catch-up via recompute with self.
    let hwm = new_leader.committed_hwm(&topic, PartitionId(0)).unwrap();
    if hwm < leo {
        // Treat remaining survivors as caught up (acks=all already required it).
        for &sid in &survivors {
            if sid != new_leader_id {
                new_leader
                    .test_set_follower_leo(&topic, PartitionId(0), sid, leo)
                    .unwrap();
            }
        }
    }

    let consumer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(new_leader_id))],
        ..ClientConfig::default()
    })
    .await
    .expect("connect new leader");

    let mut got = Vec::new();
    for _ in 0..40 {
        let f = consumer
            .fetch("events", 0, Offset::ZERO, 100, 50)
            .await
            .unwrap();
        if f.records.len() as u32 >= N {
            got = f.records;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        got.len() as u32,
        N,
        "new leader must serve all acks=all messages; hwm may lag: got {}",
        got.len()
    );
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.value.as_ref(), format!("msg-{i}").as_bytes());
    }

    h1.abort();
    h2.abort();
    h3.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_rejects_produce() {
    let base = unique_dir("not-leader");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;

    let cfg = ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 3000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: p1,
                rack: None,
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: p2,
                rack: None,
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: p3,
                rack: None,
            },
        ],
    };

    let mk = |id: u32, port: u16| {
        let storage = StorageConfig {
            data_dir: base.join(format!("n{id}")),
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3);

    let _h1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    let _h2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_listener(l2, b).await;
        })
    };
    let _h3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            let _ = serve_listener(l3, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let c = Client::connect_addr(format!("127.0.0.1:{p1}"))
        .await
        .unwrap();
    c.create_topic("t", 1).await.unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    b2.apply_cluster_state(gen, cid, &topics).unwrap();
    b3.apply_cluster_state(gen, cid, &topics).unwrap();

    let meta = c.metadata().await.unwrap();
    let leader = meta.topics[0].partitions[0].leader;
    let follower = [1u32, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let fport = match follower {
        1 => p1,
        2 => p2,
        3 => p3,
        _ => unreachable!(),
    };

    // Disable client redirect so we observe broker-level NotLeader rejection.
    let fc = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{fport}")],
        max_redirects: 0,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    let err = fc
        .produce_with_acks("t", Some(0), vec![Message::from_value("x")], 1)
        .await;
    assert!(err.is_err(), "follower must reject produce without redirect");
}
