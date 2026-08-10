//! Phase 132 (formal): truncate-journal catch-up hardening.
//!
//! Non-blocking HeartbeatBroker schedule, per-peer single-flight + min-interval
//! throttle, and push (88) wire depth. Residual fence/auth suites remain in
//! `phase132_journal_note_fence` / `phase133_journal_auth` (historical names).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use volant_broker::{
    catch_up_peer_truncate_journal, fanout_truncate_journal_note, inter_broker_rpc,
    schedule_catch_up_peer_truncate_journal, serve_listener, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_protocol::{Request, Response};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p132h-{label}-{}-{}",
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

/// Single-flight + min-interval throttle without network.
#[test]
fn single_flight_and_min_interval_throttle() {
    let base = unique_dir("throttle");
    let _g = Guard(base.clone());
    let b = Broker::new(StorageConfig {
        data_dir: base.join("n1"),
        ..StorageConfig::default()
    });
    b.reset_journal_catchup_scheduler_for_test();

    assert!(b.try_begin_journal_catchup(2));
    assert!(b.journal_catchup_in_flight(2));
    // Second claim while in-flight → skip.
    assert!(!b.try_begin_journal_catchup(2));
    assert_eq!(b.journal_catchup_skipped_total(), 1);

    b.finish_journal_catchup(2);
    assert!(!b.journal_catchup_in_flight(2));

    // Min-interval (default 500ms) still blocks immediate restart.
    assert!(
        !b.try_begin_journal_catchup(2),
        "min-interval should block re-start immediately after finish"
    );
    assert_eq!(b.journal_catchup_skipped_total(), 2);

    // Different peer is independent.
    assert!(b.try_begin_journal_catchup(3));
    b.finish_journal_catchup(3);

    // After interval, peer 2 can start again.
    std::thread::sleep(Duration::from_millis(550));
    assert!(b.try_begin_journal_catchup(2));
    b.finish_journal_catchup(2);
}

/// HeartbeatBroker returns promptly even when catch-up RPC would hang (black hole).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_not_blocked_by_slow_catchup() {
    let base = unique_dir("hb-noblock");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    // Peer 2: advertised port that nothing accepts → catch-up RPC fails/hangs
    // off the heartbeat path.
    let p2_blackhole = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let (l3, p3) = bind().await;
    let cfg = cluster_config([p1, p2_blackhole, p3]);

    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
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
    let _b3 = mk(3, p3);
    b1.reset_journal_catchup_scheduler_for_test();

    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    drop(l3);

    // Controller has journal; peer 2 lags at gen 0.
    b1.local_note_truncate_journal("orders", 0, 99, 1);
    assert!(b1.peer_journal_gen_lags(0));

    let hb = Request::HeartbeatBroker {
        broker_id: 2,
        controller_id_known: 1,
        generation: 0,
        applied_config_generation: 0,
        applied_acl_generation: 0,
        applied_journal_generation: 0,
    };

    let t0 = Instant::now();
    let resp = inter_broker_rpc(&b1, &format!("127.0.0.1:{p1}"), &hb)
        .await
        .expect("heartbeat rpc");
    let elapsed = t0.elapsed();

    match resp {
        Response::HeartbeatBroker { error_code, .. } => {
            assert_eq!(error_code, 0, "heartbeat should succeed");
        }
        other => panic!("unexpected {other:?}"),
    }

    // RPC timeout default is 5s; non-blocking schedule must return far sooner.
    assert!(
        elapsed < Duration::from_millis(1500),
        "HeartbeatBroker blocked too long ({elapsed:?}); catch-up must be async"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let skipped_before = b1.journal_catchup_skipped_total();
    schedule_catch_up_peer_truncate_journal(
        Arc::clone(&b1),
        2,
        format!("127.0.0.1:{p2_blackhole}"),
        0,
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        b1.journal_catchup_skipped_total() > skipped_before
            || b1.journal_catchup_in_flight(2)
            || b1.journal_catchup_errors_total() > 0,
        "expected in-flight, skip, or error after black-hole catch-up"
    );

    h1.abort();
}

/// Scheduled catch-up restores watermark on a live peer (async path).
#[tokio::test]
async fn schedule_catchup_restores_watermark() {
    let base = unique_dir("sched");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let cfg = cluster_config([p1, p2, p3]);

    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
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
    let _b3 = mk(3, p3);
    b1.reset_journal_catchup_scheduler_for_test();

    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    let h2 = tokio::spawn(serve_listener(l2, Arc::clone(&b2)));
    drop(l3);

    b1.local_note_truncate_journal("events", 0, 42, 1);
    let before_ok = b1.journal_catchup_success_total();

    schedule_catch_up_peer_truncate_journal(
        Arc::clone(&b1),
        2,
        format!("127.0.0.1:{p2}"),
        /* peer applied */ 0,
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if b2.truncate_journal().watermark("events", 0) == Some(42)
            && b1.journal_catchup_success_total() > before_ok
        {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "async catch-up failed; wm={:?} ok={} err={} skipped={}",
                b2.truncate_journal().watermark("events", 0),
                b1.journal_catchup_success_total(),
                b1.journal_catchup_errors_total(),
                b1.journal_catchup_skipped_total(),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    h1.abort();
    h2.abort();
}

/// Wire: TruncateJournalPush (88) applies max-merge snapshot on peer.
#[tokio::test]
async fn push_wire_applies_snapshot() {
    let base = unique_dir("push-wire");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let cfg = cluster_config([p1, p2, p3]);

    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
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
    let _b3 = mk(3, p3);

    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    let h2 = tokio::spawn(serve_listener(l2, Arc::clone(&b2)));
    drop(l3);

    let gen = b1.local_note_truncate_journal("wire", 0, 77, 1);
    let snapshot = b1.truncate_journal().snapshot_bytes();
    let req = Request::TruncateJournalPush {
        generation: gen,
        snapshot,
    };
    let resp = inter_broker_rpc(&b1, &format!("127.0.0.1:{p2}"), &req)
        .await
        .expect("push rpc");
    match resp {
        Response::TruncateJournalPush { error_code } => {
            assert_eq!(error_code, 0);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(b2.truncate_journal().watermark("wire", 0), Some(77));
    assert!(b2.truncate_journal_applied_generation() >= gen);

    h1.abort();
    h2.abort();
}

/// Wire: majority note fan-out (86) reaches live peers; catch-up push path works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn majority_note_and_push_depth() {
    let base = unique_dir("majority");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

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
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    let h2 = tokio::spawn(serve_listener(l2, Arc::clone(&b2)));
    let h3 = tokio::spawn(serve_listener(l3, Arc::clone(&b3)));

    // Controller creates; peers apply cluster state so note fence sees the topic.
    b1.create_topic("maj", 1).unwrap();
    for _ in 0..40 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("maj").is_some() && b3.partition_count_opt("maj").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let before = b1.truncate_journal_consensus_success_total();
    fanout_truncate_journal_note(&b1, "maj", 0, 10, 0).await;

    assert!(
        b1.truncate_journal_consensus_success_total() > before,
        "majority of 3 with 3 live should succeed"
    );
    assert_eq!(b1.truncate_journal().watermark("maj", 0), Some(10));
    // Fan-out + best-effort push should land on peers (or catch-up recovers).
    for b in [&b2, &b3] {
        if b.truncate_journal().watermark("maj", 0) != Some(10) {
            catch_up_peer_truncate_journal(
                &b1,
                b.node_id(),
                &format!("127.0.0.1:{}", ports[(b.node_id() - 1) as usize]),
                0,
            )
            .await;
        }
    }
    for _ in 0..50 {
        if b2.truncate_journal().watermark("maj", 0) == Some(10)
            && b3.truncate_journal().watermark("maj", 0) == Some(10)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(b2.truncate_journal().watermark("maj", 0), Some(10));
    assert_eq!(b3.truncate_journal().watermark("maj", 0), Some(10));

    h1.abort();
    h2.abort();
    h3.abort();
}
