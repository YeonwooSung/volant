//! Phase 129: controller SoT DeleteRecords truncate journal.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    fanout_delete_records, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig,
    TruncateJournal,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p129-{label}-{}-{}",
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

fn cluster_config(ports: [u16; 3]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
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

fn boot_triple(base: &std::path::Path, ports: [u16; 3]) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config(ports);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
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
    panic!("assignment did not propagate for {topic}");
}

fn broker_of(b1: &Arc<Broker>, b2: &Arc<Broker>, b3: &Arc<Broker>, id: u32) -> Arc<Broker> {
    match id {
        1 => Arc::clone(b1),
        2 => Arc::clone(b2),
        3 => Arc::clone(b3),
        _ => panic!("bad id"),
    }
}

/// Controller note + push installs watermark on peers (in-process apply path).
#[tokio::test]
async fn controller_journal_push_reaches_peers() {
    let base = unique_dir("push");
    let _g = Guard(base.clone());
    // In-process cluster without TCP: exercise note/apply + reconcile only.
    let (b1, b2, b3) = boot_triple(&base, [29101, 29102, 29103]);
    b1.create_topic("tj", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "tj");

    assert!(b1.is_controller());
    let gen = b1.controller_note_truncate_journal("tj", 0, 64, 1);
    assert!(gen >= 1);
    assert_eq!(b1.truncate_journal().watermark("tj", 0), Some(64));

    // Simulate controller push apply on peers.
    let snap = b1.truncate_journal().snapshot_bytes();
    b2.apply_truncate_journal_push(gen, &snap).unwrap();
    b3.apply_truncate_journal_push(gen, &snap).unwrap();
    assert_eq!(b2.truncate_journal().watermark("tj", 0), Some(64));
    assert_eq!(b3.truncate_journal().watermark("tj", 0), Some(64));
}

/// Reconcile uses journal watermark even when local log_start is lower.
#[test]
fn reconcile_uses_journal_when_log_start_stale() {
    let base = unique_dir("recon");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [29111, 29112, 29113]);
    b1.create_topic("stale", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "stale");

    let topic = TopicName::new("stale");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = broker_of(&b1, &b2, &b3, leader_id);

    // Produce a few messages so partition exists with data.
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

    // Journal claims truncate to 3 without local delete_records on leader.
    let gen = b1.controller_note_truncate_journal("stale", 0, 3, 0);
    let snap = b1.truncate_journal().snapshot_bytes();
    leader.apply_truncate_journal_push(gen, &snap).unwrap();
    assert_eq!(leader.truncate_journal().watermark("stale", 0), Some(3));

    // Local log_start is still 0 (no local truncate) → journal drives target.
    let log_start = leader
        .metadata(Some(&[topic.clone()]))
        .topics[0]
        .partitions[0]
        .hwm; // not log_start; check via reconcile side effect
    let _ = log_start;
    let advanced = leader.reconcile_delete_records_outbox();
    assert!(advanced >= 1, "reconcile should run for journal target");
    // Outbox should have pending for peers at before_offset 3.
    let pending = leader.delete_records_outbox().list();
    assert!(
        pending.iter().any(|e| e.topic == "stale" && e.before_offset >= 3),
        "expected outbox entries at journal watermark, got {pending:?}"
    );
}

#[test]
fn journal_survives_restart() {
    let base = unique_dir("reload");
    let _g = Guard(base.clone());
    {
        let j = TruncateJournal::open(base.join("n1"));
        j.note("x", 1, 99, 2, true);
    }
    let j2 = TruncateJournal::open(base.join("n1"));
    assert_eq!(j2.watermark("x", 1), Some(99));
    assert!(j2.generation() >= 1);
}

#[tokio::test]
async fn fanout_note_on_controller_local() {
    let base = unique_dir("fanout");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [29121, 29122, 29123]);
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let _bg2 = start_background_tasks(Arc::clone(&b2));
    let _bg3 = start_background_tasks(Arc::clone(&b3));

    // Need TCP for inter-broker note; without listeners note on controller still works.
    b1.create_topic("f", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "f");
    assert!(b1.is_controller());

    // Direct controller note path used by fanout when self is controller.
    let gen = b1.controller_note_truncate_journal("f", 0, 7, 0);
    assert!(gen >= 1);
    assert_eq!(b1.truncate_journal().watermark("f", 0), Some(7));

    // fanout_delete_records will attempt RPC peers (may fail without servers)
    // but journal note for controller path runs first.
    fanout_delete_records(&b1, "f", 0, 7).await;
    assert_eq!(b1.truncate_journal().watermark("f", 0), Some(7));
}
