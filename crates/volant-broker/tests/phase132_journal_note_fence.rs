//! Phase 132: TruncateJournalNote leader-epoch fence on ingress.
//!
//! `Broker::handle_truncate_journal_note` rejects stale epochs
//! (`InvalidProducerEpoch`) and negative epoch stamps (`InvalidArg`) so forged
//! high before_offset cannot become journal SoT. Multi-controller (any node
//! may accept) still holds when the epoch is current or future (req >= local).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    inter_broker_rpc, serve_listener, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_core::{PartitionId, TopicName};
use volant_protocol::{ErrorCode, Request, Response};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p132-{label}-{}-{}",
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
    panic!("propagate failed for {topic}");
}

fn boot_triple(base: &std::path::Path, ports: [u16; 3]) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config(ports);
    let mk = |id: u32| {
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
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    (mk(1), mk(2), mk(3))
}

/// Stale leader_epoch is fenced: InvalidProducerEpoch, gen/watermark unchanged.
#[test]
fn stale_epoch_note_rejected() {
    let base = unique_dir("stale");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [33201, 33202, 33203]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    // Receiver is b2 (any node; multi-controller).
    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 5)
        .unwrap();

    let gen_before = b2.truncate_journal().generation();
    assert_eq!(b2.truncate_journal().watermark("t", 0), None);

    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 100, 1);
    assert_eq!(
        err,
        ErrorCode::InvalidProducerEpoch as u16,
        "stale epoch must be fenced"
    );
    assert_eq!(gen, gen_before, "generation must not advance on fence");
    assert_eq!(
        b2.truncate_journal().generation(),
        gen_before,
        "journal generation unchanged after rejected note"
    );
    assert_eq!(
        b2.truncate_journal().watermark("t", 0),
        None,
        "watermark must not rise on stale epoch"
    );
}

/// Matching leader_epoch is accepted and max-merges the watermark.
#[test]
fn current_epoch_note_accepted() {
    let base = unique_dir("current");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [33211, 33212, 33213]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 5)
        .unwrap();

    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 50, 5);
    assert_eq!(err, 0, "current epoch note must succeed");
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(50));
}

/// Unknown topic → NotFound; no watermark side effect.
#[test]
fn unknown_topic_note_rejected() {
    let base = unique_dir("unknown-topic");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [33221, 33222, 33223]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    let gen_before = b2.truncate_journal().generation();
    let (err, gen) = b2.handle_truncate_journal_note("no-such-topic", 0, 10, 0);
    assert_eq!(err, ErrorCode::NotFound as u16);
    assert_eq!(gen, gen_before);
    assert_eq!(
        b2.truncate_journal().watermark("no-such-topic", 0),
        None
    );
}

/// leader_epoch < 0 is InvalidArg: journal SoT requires a stamped epoch.
#[test]
fn unknown_epoch_minus_one_rejected() {
    let base = unique_dir("epoch-minus-one");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [33231, 33232, 33233]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 5)
        .unwrap();

    let gen_before = b2.truncate_journal().generation();
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 20, -1);
    assert_eq!(
        err,
        ErrorCode::InvalidArg as u16,
        "leader_epoch=-1 must be InvalidArg (journal requires stamped epoch)"
    );
    assert_eq!(gen, gen_before, "generation must not advance on InvalidArg");
    assert_eq!(
        b2.truncate_journal().generation(),
        gen_before,
        "journal generation unchanged after rejected note"
    );
    assert_eq!(
        b2.truncate_journal().watermark("t", 0),
        None,
        "watermark must not rise on negative epoch"
    );
}

/// Future epochs (req >= local) are accepted so lagging multi-controller peers
/// can still ack after leadership bumps.
#[test]
fn future_epoch_note_accepted() {
    let base = unique_dir("future-epoch");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [33251, 33252, 33253]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 3)
        .unwrap();

    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 40, 5);
    assert_eq!(err, 0, "future epoch (5 > local 3) must be accepted");
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(40));
}

/// Non-controller still accepts a note with a current epoch (Phase 130 multi-controller).
#[test]
fn non_controller_accepts_fenced_valid_note() {
    let base = unique_dir("non-ctrl");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple(&base, [33241, 33242, 33243]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");

    assert!(b1.is_controller());
    assert!(!b2.is_controller());

    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 3)
        .unwrap();

    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 40, 3);
    assert_eq!(
        err, 0,
        "non-controller must still accept valid current-epoch note"
    );
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(40));
}

/// TCP TruncateJournalNote: peer with higher local epoch returns 19 and does not
/// raise watermark. (Full fanout still best-effort *pushes* snapshots without
/// re-checking epoch — this test exercises the note RPC fence only.)
#[tokio::test]
async fn tcp_stale_epoch_note_not_acked() {
    let base = unique_dir("tcp-stale");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let p3 = p2.saturating_add(50).max(33300);
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

    b1.create_topic("fence", 1).unwrap();
    for _ in 0..40 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("fence").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Peer has advanced past the note's epoch stamp.
    b2.set_partition_leader_epoch(&TopicName::new("fence"), PartitionId(0), 5)
        .unwrap();
    let gen_before = b2.truncate_journal().generation();

    // In-process handle: fence first.
    let (err, _) = b2.handle_truncate_journal_note("fence", 0, 99, 0);
    assert_eq!(err, ErrorCode::InvalidProducerEpoch as u16);
    assert_eq!(b2.truncate_journal().watermark("fence", 0), None);

    // TCP inter-broker TruncateJournalNote (not full fanout+push): peer must
    // reply InvalidProducerEpoch and leave watermark/generation unchanged.
    let peer_addr = format!("127.0.0.1:{p2}");
    let resp = inter_broker_rpc(
        &b1,
        &peer_addr,
        &Request::TruncateJournalNote {
            topic: "fence".into(),
            partition: 0,
            before_offset: 99,
            leader_epoch: 0,
        },
    )
    .await
    .expect("TruncateJournalNote rpc");
    match resp {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(
                error_code,
                ErrorCode::InvalidProducerEpoch as u16,
                "TCP note with stale epoch must be fenced"
            );
            assert_eq!(generation, gen_before);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(
        b2.truncate_journal().watermark("fence", 0),
        None,
        "peer watermark must not rise when note handle returns 19"
    );
    assert_eq!(b2.truncate_journal().generation(), gen_before);

    s1.abort();
    s2.abort();
}

/// TCP TruncateJournalNote with leader_epoch=-1 → InvalidArg; no watermark.
#[tokio::test]
async fn tcp_minus_one_epoch_note_rejected() {
    let base = unique_dir("tcp-minus-one");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let p3 = p2.saturating_add(50).max(33400);
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

    b1.create_topic("fence", 1).unwrap();
    for _ in 0..40 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("fence").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    b2.set_partition_leader_epoch(&TopicName::new("fence"), PartitionId(0), 5)
        .unwrap();
    let gen_before = b2.truncate_journal().generation();

    let peer_addr = format!("127.0.0.1:{p2}");
    let resp = inter_broker_rpc(
        &b1,
        &peer_addr,
        &Request::TruncateJournalNote {
            topic: "fence".into(),
            partition: 0,
            before_offset: 20,
            leader_epoch: -1,
        },
    )
    .await
    .expect("TruncateJournalNote rpc");
    match resp {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(
                error_code,
                ErrorCode::InvalidArg as u16,
                "TCP note with leader_epoch=-1 must be InvalidArg"
            );
            assert_eq!(generation, gen_before);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(b2.truncate_journal().watermark("fence", 0), None);
    assert_eq!(b2.truncate_journal().generation(), gen_before);

    s1.abort();
    s2.abort();
}
