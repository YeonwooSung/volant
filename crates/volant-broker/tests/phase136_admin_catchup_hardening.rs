//! Phase 136: non-blocking admin (ACL/config) catch-up hardening.
//!
//! Mirrors Phase 132 journal catch-up: single-flight + min-interval throttle,
//! HeartbeatBroker schedule so membership is not stalled by slow peers.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use volant_broker::broker_config::KEY_SWEEP_INTERVAL_MS;
use volant_broker::{
    catch_up_peer_admin_state, fanout_cluster_broker_config, inter_broker_rpc,
    schedule_catch_up_peer_admin_state, serve_listener, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_protocol::{Request, Response};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p136h-{label}-{}-{}",
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
    b.reset_admin_catchup_scheduler_for_test();

    assert!(b.try_begin_admin_catchup(2));
    assert!(b.admin_catchup_in_flight(2));
    // Second claim while in-flight → skip.
    assert!(!b.try_begin_admin_catchup(2));
    assert_eq!(b.admin_catchup_skipped_total(), 1);

    b.finish_admin_catchup(2);
    assert!(!b.admin_catchup_in_flight(2));

    // Min-interval (default 500ms) still blocks immediate restart.
    assert!(
        !b.try_begin_admin_catchup(2),
        "min-interval should block re-start immediately after finish"
    );
    assert_eq!(b.admin_catchup_skipped_total(), 2);

    // Different peer is independent.
    assert!(b.try_begin_admin_catchup(3));
    b.finish_admin_catchup(3);

    // After interval, peer 2 can start again.
    std::thread::sleep(Duration::from_millis(550));
    assert!(b.try_begin_admin_catchup(2));
    b.finish_admin_catchup(2);
}

/// HeartbeatBroker returns promptly even when admin catch-up RPC would hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_not_blocked_by_slow_admin_catchup() {
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
    assert!(b1.is_controller());
    b1.reset_admin_catchup_scheduler_for_test();

    // Bump controller config gen so peer applied=0 lags.
    let entries = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "77".to_string())];
    let gen = b1
        .alter_broker_configs(&entries)
        .unwrap()
        .expect("cluster gen");
    assert!(gen >= 1);
    assert!(b1.peer_admin_gens_lag(0, 0).0);

    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    drop(l3);

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
        "HeartbeatBroker blocked too long ({elapsed:?}); admin catch-up must be async"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let skipped_before = b1.admin_catchup_skipped_total();
    schedule_catch_up_peer_admin_state(
        Arc::clone(&b1),
        2,
        format!("127.0.0.1:{p2_blackhole}"),
        0,
        0,
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        b1.admin_catchup_skipped_total() > skipped_before
            || b1.admin_catchup_in_flight(2)
            || b1.cluster_admin_catchup_errors_total() > 0,
        "expected in-flight, skip, or error after black-hole admin catch-up"
    );

    h1.abort();
}

/// Scheduled admin catch-up restores BROKER config on a live peer (async path).
#[tokio::test]
async fn schedule_admin_catchup_restores_config() {
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
    assert!(b1.is_controller());
    b1.reset_admin_catchup_scheduler_for_test();

    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    let h2 = tokio::spawn(serve_listener(l2, Arc::clone(&b2)));
    drop(l3);

    let entries = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "88".to_string())];
    let gen = b1
        .alter_broker_configs(&entries)
        .unwrap()
        .expect("cluster gen");
    // Do not fan out live; leave b2 lagging so catch-up is the only path.
    let _ = gen;
    assert_eq!(b2.applied_config_generation(), 0);

    let before_ok = b1.cluster_admin_catchup_success_total();

    schedule_catch_up_peer_admin_state(
        Arc::clone(&b1),
        2,
        format!("127.0.0.1:{p2}"),
        /* peer applied */ 0,
        0,
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if b2.sweep_interval_ms() == 88
            && b2.applied_config_generation() >= 1
            && b1.cluster_admin_catchup_success_total() > before_ok
        {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "async admin catch-up failed; sweep={} applied={} ok={} err={} skipped={}",
                b2.sweep_interval_ms(),
                b2.applied_config_generation(),
                b1.cluster_admin_catchup_success_total(),
                b1.cluster_admin_catchup_errors_total(),
                b1.admin_catchup_skipped_total(),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    h1.abort();
    h2.abort();
}

/// Direct catch_up_peer_admin_state still works (public API for tests).
#[tokio::test]
async fn direct_catchup_api_still_works() {
    let base = unique_dir("direct");
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

    let entries = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "99".to_string())];
    let gen = b1
        .alter_broker_configs(&entries)
        .unwrap()
        .expect("cluster gen");
    // Explicit fan-out optional; direct catch-up is the assertion path.
    let _ = fanout_cluster_broker_config(&b1, gen, &entries);

    catch_up_peer_admin_state(&b1, 2, &format!("127.0.0.1:{p2}"), 0, 0).await;

    assert_eq!(b2.sweep_interval_ms(), 99);
    assert!(b2.applied_config_generation() >= 1);
    assert!(b1.cluster_admin_catchup_success_total() >= 1);

    h1.abort();
    h2.abort();
}
