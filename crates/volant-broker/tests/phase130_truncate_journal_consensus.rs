//! Phase 130: multi-controller majority consensus for truncate journal.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    fanout_truncate_journal_note, start_background_tasks, serve_listener, Broker, BrokerEndpoint,
    ClusterConfig, TruncateJournal,
};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p130-{label}-{}-{}",
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

async fn bind() -> (tokio::net::TcpListener, u16) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p = l.local_addr().unwrap().port();
    (l, p)
}

fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes.iter().all(|n| n.partition_count_opt(topic).is_some()) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("propagate failed");
}

#[test]
fn majority_math() {
    assert_eq!(TruncateJournal::majority(1), 1);
    assert_eq!(TruncateJournal::majority(2), 2);
    assert_eq!(TruncateJournal::majority(3), 2);
    assert_eq!(TruncateJournal::majority(5), 3);
}

/// Any broker can durable-note (multi-controller); no NotController.
#[test]
fn non_controller_accepts_journal_note() {
    let base = unique_dir("any-note");
    let _g = Guard(base.clone());
    let cfg = cluster_config([30101, 30102, 30103]);
    let mk = |id: u32| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", 30100 + id as u16);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    // Lowest live id is controller (node 1).
    assert!(b1.is_controller());
    assert!(!b2.is_controller());
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 50, 1);
    assert_eq!(err, 0, "non-controller must accept multi-controller note");
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(50));
}

/// Full 3-node live TCP: majority consensus succeeds + best-effort push.
#[tokio::test]
async fn majority_consensus_with_best_effort_push() {
    let base = unique_dir("maj");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let cfg = cluster_config([p1, p2, p3]);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3);
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let _bg2 = start_background_tasks(Arc::clone(&b2));
    let _bg3 = start_background_tasks(Arc::clone(&b3));

    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    let s2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            serve_listener(l2, b).await.ok();
        })
    };
    let s3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            serve_listener(l3, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    b1.create_topic("c", 1).unwrap();
    for _ in 0..40 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("c").is_some() && b3.partition_count_opt("c").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Propose from non-controller if possible.
    let proposer = if !b2.is_controller() {
        Arc::clone(&b2)
    } else {
        Arc::clone(&b3)
    };
    let before = proposer.truncate_journal_consensus_success_total();
    fanout_truncate_journal_note(&proposer, "c", 0, 77, 1).await;

    assert!(
        proposer.truncate_journal_consensus_success_total() > before,
        "majority of 3 should succeed with 3 live nodes"
    );
    assert_eq!(proposer.truncate_journal().watermark("c", 0), Some(77));

    // Best-effort push: all live peers should have the watermark.
    for b in [&b1, &b2, &b3] {
        assert_eq!(
            b.truncate_journal().watermark("c", 0),
            Some(77),
            "node {} missing watermark after majority+push",
            b.node_id()
        );
    }

    s1.abort();
    s2.abort();
    s3.abort();
}

/// Offline peer: majority of 3 still succeeds with 2 acks; push is best-effort.
#[tokio::test]
async fn majority_with_one_peer_down() {
    let base = unique_dir("maj-down");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    // Node 3 port reserved but never served → always down.
    let p3 = p2.saturating_add(100).max(31000);
    let cfg = cluster_config([p1, p2, p3]);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3); // no listener
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let _bg2 = start_background_tasks(Arc::clone(&b2));

    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    let s2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            serve_listener(l2, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(40)).await;

    b1.create_topic("d", 1).unwrap();
    for _ in 0..30 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("d").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Configured N=3 → need majority 2. Local durable note + successful note
    // to b2 = 2 acks even if b3 is down / not contacted successfully.
    let before_ok = b1.truncate_journal_consensus_success_total();
    fanout_truncate_journal_note(&b1, "d", 0, 11, 0).await;

    assert!(
        b1.truncate_journal_consensus_success_total() > before_ok,
        "1 of 3 down must still reach majority with local + one live peer"
    );
    // Local + peer state retained (best-effort push).
    assert_eq!(b1.truncate_journal().watermark("d", 0), Some(11));
    assert_eq!(b2.truncate_journal().watermark("d", 0), Some(11));

    s1.abort();
    s2.abort();
}

#[test]
fn push_max_merge_does_not_shrink() {
    let base = unique_dir("merge");
    let _g = Guard(base.clone());
    let j = TruncateJournal::open(&base);
    j.note("t", 0, 100, 1, true);
    // Older/smaller snapshot must not shrink watermark.
    let smaller = TruncateJournal::open(base.join("other"));
    smaller.note("t", 0, 10, 1, true);
    let snap = smaller.snapshot_bytes();
    j.apply_push(1, &snap).unwrap();
    assert_eq!(j.watermark("t", 0), Some(100));
}
