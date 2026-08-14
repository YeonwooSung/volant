//! v0.5 ops confidence — unwritable dir, isolate leader, in-flight acks=all.
//!
//! CI uses EACCES (chmod 0o555 / read-only `.log`), not a full ENOSPC volume;
//! the operator path is the same (append fails, produce returns an error).
//! Minority isolate is in-process (abort `serve_listener` + outbound RPC hook),
//! not chaos-mesh, and does not model an asymmetric partial mesh.
//!
//! Already covered (do not duplicate):
//! - 3-node `acks=all` leader kill → `cluster_failover`
//! - Lowest-id controller death → `v02_isr_chaos`

#[path = "common/mod.rs"]
mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, cluster_config_with_session, default_storage, propagate_async,
    unique_dir, Guard,
};
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
use volant_protocol::ErrorCode;
use volant_storage::StorageConfig;

fn make_unwritable(dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o444);
        std::fs::set_permissions(&path, perms).unwrap();
    }
    let mut perms = std::fs::metadata(dir).unwrap().permissions();
    perms.set_mode(0o555);
    std::fs::set_permissions(dir, perms).unwrap();
}

fn restore_writable(dir: &Path) {
    if !dir.exists() {
        return;
    }
    let mut perms = std::fs::metadata(dir).unwrap().permissions();
    perms.set_mode(0o755);
    let _ = std::fs::set_permissions(dir, perms);
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                restore_writable(&path);
            } else if let Ok(meta) = std::fs::metadata(&path) {
                let mut p = meta.permissions();
                p.set_mode(0o644);
                let _ = std::fs::set_permissions(&path, p);
            }
        }
    }
}

fn batch_value(s: impl Into<String>) -> MessageBatch {
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value(s.into()));
    batch
}

fn seed_hwm(leader: &Broker, others: &[&Broker], topic: &TopicName, partition: PartitionId) {
    let leo = leader.log_end_offset(topic, partition).unwrap_or(0);
    for other in others {
        let other_id = other.node_id();
        let other_leo = other.log_end_offset(topic, partition).unwrap_or(0);
        leader
            .test_set_follower_leo(topic, partition, other_id, other_leo)
            .unwrap();
    }
    if leader.committed_hwm(topic, partition).unwrap_or(0) < leo {
        for other in others {
            leader
                .test_set_follower_leo(topic, partition, other.node_id(), leo)
                .unwrap();
        }
    }
}

/// Produce after a successful flush must return an error (not panic) when the
/// partition data dir is unwritable. Fetch of already-written records still works.
///
/// CI uses EACCES, not a full disk; operator path is the same (append fails).
#[tokio::test(flavor = "multi_thread")]
async fn unwritable_data_dir_produce_errors_fetch_still_works() {
    let dir = unique_dir("v05", "eacces");
    let _g = Guard(dir.clone());
    // Tiny segment so the second produce must roll a new `.log` (chmod on an
    // already-open fd does not fail writes; roll create does → EACCES).
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        flush_every_n: 1,
        segment_size: 64,
        ..StorageConfig::default()
    });
    let topic = TopicName::new("disk");
    broker.create_topic(topic.clone(), 1).unwrap();
    broker
        .produce(&topic, PartitionId(0), batch_value("committed-0"))
        .unwrap();
    broker.flush(&topic, PartitionId(0)).unwrap();

    let part_dir = dir.join("disk").join("0");
    assert!(part_dir.is_dir(), "partition data dir {part_dir:?}");
    make_unwritable(&part_dir);

    let second = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        broker.produce(&topic, PartitionId(0), batch_value("must-fail"))
    }));
    restore_writable(&part_dir);
    restore_writable(&dir);
    match second {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("produce must error when the partition dir is unwritable"),
        Err(_) => panic!("produce must not panic on unwritable data dir (EACCES)"),
    }

    let got = broker
        .fetch(&topic, PartitionId(0), Offset::ZERO, 10)
        .expect("fetch of already-written records must still work");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].value.as_ref(), b"committed-0");
}

/// Minority isolate of the partition leader: survivors expire + elect; isolated
/// `acks=all` does not commit. In-process isolate, not chaos-mesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_isolate_leader_split_brain_honesty() {
    let base = unique_dir("v05", "isolate");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config_with_session(ports, 400);

    let mk = |id: u32| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

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
    tokio::time::sleep(Duration::from_millis(120)).await;

    let port_of = |id: u32| ports[(id - 1) as usize];
    let broker_of = |id: u32| -> Arc<Broker> {
        match id {
            1 => Arc::clone(&b1),
            2 => Arc::clone(&b2),
            3 => Arc::clone(&b3),
            _ => panic!("bad id {id}"),
        }
    };
    let handle_of = |id: u32| -> &tokio::task::JoinHandle<()> {
        match id {
            1 => &h1,
            2 => &h2,
            3 => &h3,
            _ => panic!("bad id {id}"),
        }
    };

    let admin = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 1).await.unwrap();
    propagate_async(&[&b1, &b2, &b3], "events").await;

    let meta = admin.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    assert_eq!(meta.topics[0].partitions[0].replicas.len(), 3);

    let producer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    const PRE: u32 = 3;
    for i in 0..PRE {
        producer
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(format!("pre-{i}"))],
                255,
            )
            .await
            .expect("acks=all before isolate");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Isolate the partition leader: process stays up, peers cannot ReplicaFetch,
    // and its outbound inter-broker RPC cannot heartbeat out.
    let isolated = broker_of(leader_id);
    isolated.test_set_inter_broker_blocked(true);
    handle_of(leader_id).abort();

    let survivors: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();

    // Wait past session_timeout so survivors expire() + on_broker_death.
    tokio::time::sleep(Duration::from_millis(800)).await;
    for _ in 0..20 {
        for &sid in &survivors {
            broker_of(sid).tick_cluster();
        }
        let any_dead = survivors
            .iter()
            .all(|sid| !broker_of(*sid).live_brokers().contains(&leader_id));
        let new_ctrl = survivors.iter().any(|sid| broker_of(*sid).is_controller());
        if any_dead && new_ctrl {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let ctrl_id = survivors
        .iter()
        .copied()
        .find(|id| broker_of(*id).is_controller())
        .expect("a survivor must be the new (or remaining) controller");
    let ctrl = broker_of(ctrl_id);
    let (_, gen, cid, topics) = ctrl.cluster_state_snapshot();
    for &sid in &survivors {
        let _ = broker_of(sid).apply_cluster_state(gen, cid, &topics);
    }

    let snap = ctrl.metadata(None);
    let events = snap
        .topics
        .iter()
        .find(|t| t.name.as_str() == "events")
        .expect("events on survivor");
    let new_leader_id = events.partitions[0].leader;
    assert_ne!(new_leader_id, leader_id, "ISR must elect a remaining leader");
    assert!(
        survivors.contains(&new_leader_id),
        "new leader {new_leader_id} must be a survivor"
    );

    let topic = TopicName::new("events");
    let new_leader = broker_of(new_leader_id);
    let others: Vec<Arc<Broker>> = survivors
        .iter()
        .copied()
        .filter(|id| *id != new_leader_id)
        .map(broker_of)
        .collect();
    let other_refs: Vec<&Broker> = others.iter().map(|b| b.as_ref()).collect();
    seed_hwm(&new_leader, &other_refs, &topic, PartitionId(0));

    // Isolated acks=all (10s HWM wait) in parallel with survivor produce.
    let isolated_for_wait = Arc::clone(&isolated);
    let isolated_acks_all = tokio::task::spawn_blocking(move || {
        isolated_for_wait.produce_with_acks(
            &TopicName::new("events"),
            PartitionId(0),
            batch_value("isolated-all"),
            255,
            Some(Duration::from_secs(10)),
        )
    });

    // Split-brain: isolated acks=1 may still append locally (not cluster-committed).
    let acks1 = isolated.produce(&topic, PartitionId(0), batch_value("isolated-acks1"));
    if acks1.is_ok() {
        // Documented: isolated acks=1 is local-only and is not committed cluster-wide.
    }

    let after = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(new_leader_id))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .expect("connect survivor leader");
    after
        .produce_with_acks(
            "events",
            Some(0),
            vec![Message::from_value("post-isolate")],
            255,
        )
        .await
        .expect("acks=all to survivor must succeed");

    let want = (PRE + 1) as usize;
    let mut got = Vec::new();
    for _ in 0..40 {
        let f = after
            .fetch("events", 0, Offset::ZERO, 100, 50)
            .await
            .unwrap();
        if f.records.len() >= want {
            got = f.records;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        got.len(),
        want,
        "survivor fetch must see pre-isolate committed records plus the new produce"
    );
    for i in 0..PRE {
        assert_eq!(
            got[i as usize].value.as_ref(),
            format!("pre-{i}").as_bytes()
        );
    }
    assert_eq!(got[PRE as usize].value.as_ref(), b"post-isolate");
    assert!(
        !got.iter().any(|r| r.value.as_ref() == b"isolated-acks1"
            || r.value.as_ref() == b"isolated-all"),
        "isolated local appends must not appear on the survivor"
    );

    let isolated_res = isolated_acks_all.await.expect("isolated produce task");
    match isolated_res {
        Ok((_, 0)) => panic!("isolated acks=all must not succeed within the 10s HWM wait"),
        Ok((_, code)) => {
            assert!(
                code == ErrorCode::Timeout as u16
                    || code == ErrorCode::NotEnoughReplicas as u16
                    || code == ErrorCode::NotLeaderForPartition as u16,
                "isolated acks=all expected Timeout/error, got {code}"
            );
        }
        Err(_) => {}
    }

    h1.abort();
    h2.abort();
    h3.abort();
}

/// Leader abort while an `acks=all` produce is in flight: pre-kill committed
/// records survive; the in-flight batch is not required to commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_abort_mid_inflight_acks_all() {
    let base = unique_dir("v05", "inflight");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

    let mk = |id: u32| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

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
    tokio::time::sleep(Duration::from_millis(120)).await;

    let port_of = |id: u32| ports[(id - 1) as usize];
    let broker_of = |id: u32| -> Arc<Broker> {
        match id {
            1 => Arc::clone(&b1),
            2 => Arc::clone(&b2),
            3 => Arc::clone(&b3),
            _ => panic!("bad id {id}"),
        }
    };

    let admin = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 1).await.unwrap();
    propagate_async(&[&b1, &b2, &b3], "events").await;

    let meta = admin.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;

    let producer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    const N: u32 = 4;
    for i in 0..N {
        producer
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(format!("ok-{i}"))],
                255,
            )
            .await
            .expect("pre-kill acks=all");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let inflight = tokio::spawn({
        async move {
            producer
                .produce_with_acks(
                    "events",
                    Some(0),
                    vec![Message::from_value("in-flight")],
                    255,
                )
                .await
        }
    });
    // In-flight produce may timeout or fail — that is OK.
    match leader_id {
        1 => h1.abort(),
        2 => h2.abort(),
        3 => h3.abort(),
        _ => unreachable!(),
    }
    let survivors: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader_id)
        .collect();
    for &sid in &survivors {
        broker_of(sid).test_kill_broker(leader_id).unwrap();
    }

    let new_ctrl_id = *survivors.iter().min().unwrap();
    let ctrl = broker_of(new_ctrl_id);
    assert!(ctrl.is_controller(), "lowest live id should be controller");
    let (_, gen, cid, topics) = ctrl.cluster_state_snapshot();
    for &sid in &survivors {
        let _ = broker_of(sid).apply_cluster_state(gen, cid, &topics);
    }

    let snap = ctrl.metadata(None);
    let new_leader_id = snap.topics[0].partitions[0].leader;
    assert_ne!(new_leader_id, leader_id);
    assert!(survivors.contains(&new_leader_id));

    let topic = TopicName::new("events");
    let new_leader = broker_of(new_leader_id);
    let others: Vec<Arc<Broker>> = survivors
        .iter()
        .copied()
        .filter(|id| *id != new_leader_id)
        .map(broker_of)
        .collect();
    let other_refs: Vec<&Broker> = others.iter().map(|b| b.as_ref()).collect();
    seed_hwm(&new_leader, &other_refs, &topic, PartitionId(0));

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
    assert!(
        got.len() as u32 >= N,
        "new leader must serve every successful pre-kill acks=all record; got {}",
        got.len()
    );
    for i in 0..N {
        assert_eq!(got[i as usize].value.as_ref(), format!("ok-{i}").as_bytes());
    }

    let _ = tokio::time::timeout(Duration::from_millis(200), inflight).await;

    h1.abort();
    h2.abort();
    h3.abort();
}
