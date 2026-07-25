//! Phase 123: DeleteRecords outbox leadership handoff (reconcile from log_start).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use volant_broker::{
    drain_delete_records_outbox, serve_listener, serve_listener_until, start_background_tasks,
    Broker, BrokerEndpoint, ClusterConfig,
};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p123-{label}-{}-{}",
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

fn cluster_config(ports: [u16; 3], session_timeout_ms: u32) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
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

async fn fill_and_replicate(leader: &Client, topic: &str, min_latest: u64) {
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
    for _ in 0..120 {
        if let Ok(entries) = broker.list_offsets(topic, &[0]) {
            if let Some((_, earliest, _)) = entries.first() {
                if *earliest >= min_start {
                    return *earliest;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    broker
        .list_offsets(topic, &[0])
        .ok()
        .and_then(|e| e.first().map(|x| x.1))
        .unwrap_or(0)
}

fn current_leader(nodes: &[&Broker], topic: &str) -> u32 {
    for n in nodes {
        let (_, _, _, topics) = n.cluster_state_snapshot();
        if let Some(t) = topics.iter().find(|t| t.name == topic) {
            if let Some(p) = t.partitions.iter().find(|p| p.partition_id == 0) {
                return p.leader;
            }
        }
    }
    // Fallback: who thinks they lead locally.
    for n in nodes {
        if n.is_partition_leader(&TopicName::new(topic), PartitionId(0)) {
            return n.node_id();
        }
    }
    panic!("no leader for {topic}");
}

/// Offline follower + kill old leader → new leader reconcile → peer catch-up.
///
/// Scenario:
/// 1. RF=3, produce and fully replicate
/// 2. Kill follower C (broker 3 when not leader; if 3 is leader, kill 2 as offline)
/// 3. DeleteRecords on leader → outbox for offline peer
/// 4. Kill / death old leader → elect survivor as new leader
/// 5. New leader reconcile rebuilds outbox from log_start
/// 6. Restart offline peer → drain → log_start catches up
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leadership_change_new_leader_reconciles_outbox() {
    let base = unique_dir("handoff");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports, 5_000);

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
    let b3 = mk(3);

    // Killable handles for each node (Option so we can kill twice without move errors).
    let (k1_tx, k1_rx) = oneshot::channel::<()>();
    let (k2_tx, k2_rx) = oneshot::channel::<()>();
    let (k3_tx, k3_rx) = oneshot::channel::<()>();
    let mut k1_tx = Some(k1_tx);
    let mut k2_tx = Some(k2_tx);
    let mut k3_tx = Some(k3_tx);

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];
    let mut h1 = Some({
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener_until(l1, b, async move {
                let _ = k1_rx.await;
            })
            .await;
        })
    });
    let mut h2 = Some({
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_listener_until(l2, b, async move {
                let _ = k2_rx.await;
            })
            .await;
        })
    });
    let mut h3 = Some({
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            let _ = serve_listener_until(l3, b, async move {
                let _ = k3_rx.await;
            })
            .await;
        })
    });
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

    // Offline follower: prefer 3 if not leader, else 2.
    let offline_id = if leader_id != 3 { 3u32 } else { 2u32 };
    // New leader survivor after killing offline + old leader.
    let survivor_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id && *id != offline_id)
        .expect("one survivor");

    let leader = broker_of(leader_id);
    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    fill_and_replicate(&leader_client, "events", 25).await;

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

    let before = latest / 2;
    assert!(before > 0);

    // --- Kill offline follower ---
    match offline_id {
        1 => {
            if let Some(tx) = k1_tx.take() {
                let _ = tx.send(());
            }
            if let Some(h) = h1.take() {
                let _ = h.await;
            }
            bgs.remove(0).shutdown().await;
        }
        2 => {
            if let Some(tx) = k2_tx.take() {
                let _ = tx.send(());
            }
            if let Some(h) = h2.take() {
                let _ = h.await;
            }
            bgs.remove(1).shutdown().await;
        }
        3 => {
            if let Some(tx) = k3_tx.take() {
                let _ = tx.send(());
            }
            if let Some(h) = h3.take() {
                let _ = h.await;
            }
            bgs.remove(2).shutdown().await;
        }
        _ => unreachable!(),
    }

    // Notify survivors of offline death so ISR/HWM stay healthy for delete path.
    for id in [1u32, 2, 3] {
        if id == offline_id {
            continue;
        }
        let _ = broker_of(id).on_broker_death(offline_id);
    }
    // Propagate assignment (may shrink ISR).
    for _ in 0..20 {
        let src = broker_of(if leader_id != offline_id {
            leader_id
        } else {
            survivor_id
        });
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for id in [1u32, 2, 3] {
            if id == offline_id {
                continue;
            }
            let _ = broker_of(id).apply_cluster_state(gen, cid, &topics);
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    let del = leader_client
        .delete_records("events", 0, before)
        .await
        .expect("client delete must succeed with offline follower");
    assert!(del.low_watermark > 0, "leader low={}", del.low_watermark);
    let leader_low = del.low_watermark;

    // Ensure outbox has offline peer (fan-out failure or force enqueue).
    let mut saw = false;
    for _ in 0..40 {
        if leader
            .delete_records_outbox()
            .list()
            .iter()
            .any(|e| e.replica_id == offline_id)
        {
            saw = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if !saw {
        leader.enqueue_delete_records_outbox(offline_id, "events", 0, before, 0);
    }
    assert!(
        leader
            .delete_records_outbox()
            .list()
            .iter()
            .any(|e| e.replica_id == offline_id),
        "old leader outbox must include offline {offline_id}"
    );

    // Snapshot survivor log_start before leadership change (must already be truncated).
    let survivor = broker_of(survivor_id);
    let survivor_start = survivor.list_offsets("events", &[0]).unwrap()[0].1;
    assert!(
        survivor_start >= leader_low,
        "survivor {survivor_id} must have applied truncate: {survivor_start} < {leader_low}"
    );

    // --- Kill old leader (orphans its outbox) ---
    match leader_id {
        1 => {
            if let Some(tx) = k1_tx.take() {
                let _ = tx.send(());
            }
            if let Some(h) = h1.take() {
                let _ = h.await;
            }
        }
        2 => {
            if let Some(tx) = k2_tx.take() {
                let _ = tx.send(());
            }
            if let Some(h) = h2.take() {
                let _ = h.await;
            }
        }
        3 => {
            if let Some(tx) = k3_tx.take() {
                let _ = tx.send(());
            }
            if let Some(h) = h3.take() {
                let _ = h.await;
            }
        }
        _ => unreachable!(),
    }
    // Controller election + assignment update on the survivor.
    let _ = survivor.on_broker_death(leader_id);
    let _ = survivor.on_broker_death(offline_id);

    // Apply assignment so survivor becomes leader.
    for _ in 0..40 {
        let (_, gen, cid, topics) = survivor.cluster_state_snapshot();
        let _ = survivor.apply_cluster_state(gen, cid, &topics);
        if survivor.is_partition_leader(&TopicName::new("events"), PartitionId(0)) {
            break;
        }
        // If survivor is controller, death already elected; else force via death again.
        let _ = survivor.on_broker_death(leader_id);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        survivor.is_partition_leader(&TopicName::new("events"), PartitionId(0)),
        "survivor {survivor_id} must become leader (local leader?={}, snap={})",
        survivor.is_partition_leader(&TopicName::new("events"), PartitionId(0)),
        current_leader(&[survivor.as_ref()], "events")
    );

    // Survivor should not inherit old leader outbox (different data_dir).
    // Reconcile rebuilds from local log_start.
    let advanced = survivor.reconcile_delete_records_outbox();
    assert!(
        advanced >= 1 || survivor.delete_records_outbox_depth() >= 1,
        "reconcile should enqueue (advanced={advanced}, depth={}, recon={})",
        survivor.delete_records_outbox_depth(),
        survivor.delete_records_outbox_reconcile_total()
    );
    let pending = survivor.delete_records_outbox().list();
    assert!(
        pending
            .iter()
            .any(|e| e.replica_id == offline_id && e.before_offset >= leader_low),
        "new leader outbox must include offline {offline_id} >= {leader_low}: {pending:?}"
    );

    // Idempotent second reconcile.
    let recon_before = survivor.delete_records_outbox_reconcile_total();
    assert_eq!(survivor.reconcile_delete_records_outbox(), 0);
    assert_eq!(
        survivor.delete_records_outbox_reconcile_total(),
        recon_before
    );

    // --- Restart offline peer ---
    let offline_port = port_of(offline_id);
    let l_off = TcpListener::bind(format!("127.0.0.1:{offline_port}"))
        .await
        .expect("rebind offline port");
    let b_off = {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{offline_id}")),
            flush_every_n: 1,
            segment_size: 256,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, offline_id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", offline_port);
        Arc::new(b)
    };

    // Propagate assignment to offline restart.
    for _ in 0..30 {
        let (_, gen, cid, topics) = survivor.cluster_state_snapshot();
        let _ = survivor.apply_cluster_state(gen, cid, &topics);
        let _ = b_off.apply_cluster_state(gen, cid, &topics);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let bg_off = start_background_tasks(Arc::clone(&b_off));
    bgs.push(bg_off);
    {
        let b = Arc::clone(&b_off);
        tokio::spawn(async move {
            let _ = serve_listener(l_off, b).await;
        });
    }

    survivor.note_peer_live(offline_id);
    // Ensure survivor stays "alive" to itself for membership if needed.
    survivor.note_peer_live(survivor_id);

    for _ in 0..80 {
        let _ = survivor.reconcile_delete_records_outbox();
        drain_delete_records_outbox(&survivor).await;
        let earliest = b_off
            .list_offsets("events", &[0])
            .ok()
            .and_then(|e| e.first().map(|x| x.1))
            .unwrap_or(0);
        if earliest >= leader_low {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let earliest = wait_log_start(&b_off, "events", leader_low).await;
    assert!(
        earliest >= leader_low,
        "offline peer {offline_id} log_start {earliest} < leader low {leader_low} after handoff reconcile"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Reconcile from log_start enqueues peers; second pass is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_from_log_start_enqueues_and_is_idempotent() {
    let base = unique_dir("reconcile-unit");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports, 5_000);

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
    tokio::time::sleep(Duration::from_millis(120)).await;

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
    let leader = broker_of(leader_id);
    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    fill_and_replicate(&leader_client, "events", 25).await;
    let latest = leader_client
        .list_offsets("events", vec![0])
        .await
        .unwrap()
        .entries[0]
        .latest;
    let before = latest / 2;
    let (low, err) = leader.delete_records("events", 0, before).unwrap();
    assert_eq!(err, 0);
    assert!(low > 0);

    let advanced = leader.reconcile_delete_records_outbox();
    assert!(
        advanced >= 1 || leader.delete_records_outbox_depth() >= 1,
        "reconcile should see led partition with log_start>0"
    );
    let pending = leader.delete_records_outbox().list();
    assert!(
        pending.iter().any(|e| e.before_offset >= low),
        "pending={pending:?} low={low}"
    );
    assert!(leader.delete_records_outbox_reconcile_total() >= 1);

    let before_total = leader.delete_records_outbox_reconcile_total();
    assert_eq!(leader.reconcile_delete_records_outbox(), 0);
    assert_eq!(
        leader.delete_records_outbox_reconcile_total(),
        before_total
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[test]
fn single_node_reconcile_is_noop() {
    let dir = unique_dir("single");
    let _g = Guard(dir.clone());
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        segment_size: 256,
        ..StorageConfig::default()
    });
    broker.create_topic("s", 1).unwrap();
    for i in 0..20u32 {
        broker
            .produce_one(
                &TopicName::new("s"),
                PartitionId(0),
                Message::from_value(big(&format!("z{i}"), 180)),
            )
            .unwrap();
    }
    let (low, err) = broker.delete_records("s", 0, 10).unwrap();
    assert_eq!(err, 0);
    assert!(low > 0);
    assert_eq!(broker.reconcile_delete_records_outbox(), 0);
    assert_eq!(broker.delete_records_outbox_depth(), 0);
    assert_eq!(broker.delete_records_outbox_reconcile_total(), 0);
}
