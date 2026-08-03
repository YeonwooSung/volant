//! Phase 131: truncate-journal rejoin catch-up via HeartbeatBroker lag + push.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    catch_up_peer_truncate_journal, serve_listener, start_background_tasks, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p131-{label}-{}-{}",
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

fn cluster_config(ports: [u16; 3], session_timeout_ms: u32) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
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

/// Direct catch-up API: lagging peer receives full snapshot max-merge.
#[tokio::test]
async fn catch_up_push_restores_watermark() {
    let base = unique_dir("direct");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let cfg = cluster_config([p1, p2, p3], 2000);

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
    // l3 unused; drop by not serving
    drop(l3);

    // b1 has journal state; b2 is empty (missed note/push).
    b1.local_note_truncate_journal("orders", 0, 100, 1);
    assert!(b1.truncate_journal_generation() >= 1);
    assert_eq!(b2.truncate_journal().watermark("orders", 0), None);

    let before = b1.journal_catchup_success_total();
    let addr = format!("127.0.0.1:{p2}");
    catch_up_peer_truncate_journal(&b1, 2, &addr, /* peer applied */ 0).await;

    assert!(
        b1.journal_catchup_success_total() > before,
        "catch-up success metric should increment"
    );
    // Allow apply
    for _ in 0..50 {
        if b2.truncate_journal().watermark("orders", 0) == Some(100) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        b2.truncate_journal().watermark("orders", 0),
        Some(100),
        "lagging peer must max-merge catch-up snapshot"
    );
    assert!(b2.truncate_journal_applied_generation() >= 1);

    h1.abort();
    h2.abort();
}

/// Offline peer misses journal push; after rejoin + heartbeats, watermark appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_peer_journal_catchup_on_rejoin() {
    let base = unique_dir("rejoin");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let ports = [p1, p2, p3];
    // Short session → frequent heartbeats after rejoin.
    let cfg = cluster_config(ports, 1200);

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
    assert!(b1.is_controller());

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        // b3 offline initially — no bg / no listener
    ];
    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    let h2 = tokio::spawn(serve_listener(l2, Arc::clone(&b2)));
    // Hold l3 unbound until rejoin
    drop(l3);

    // Controller notes while b3 is offline.
    b1.local_note_truncate_journal("events", 0, 42, 1);
    assert_eq!(b1.truncate_journal().watermark("events", 0), Some(42));
    assert_eq!(b3.truncate_journal().watermark("events", 0), None);

    // Rejoin b3 with listener + background heartbeats.
    let l3 = tokio::net::TcpListener::bind(format!("127.0.0.1:{p3}"))
        .await
        .unwrap();
    let h3 = tokio::spawn(serve_listener(l3, Arc::clone(&b3)));
    bgs.push(start_background_tasks(Arc::clone(&b3)));

    // Wait for catch-up via heartbeat lag path (b3 → controller reports gen 0).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if b3.truncate_journal().watermark("events", 0) == Some(42) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "b3 never caught up; watermark={:?} gen={} applied={} catchup_ok={} catchup_err={}",
                b3.truncate_journal().watermark("events", 0),
                b3.truncate_journal_generation(),
                b3.truncate_journal_applied_generation(),
                b1.journal_catchup_success_total(),
                b1.journal_catchup_errors_total(),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        b1.journal_catchup_success_total() >= 1,
        "controller should have recorded at least one journal catch-up success"
    );

    for bg in bgs {
        bg.shutdown().await;
    }
    h1.abort();
    h2.abort();
    h3.abort();
}

#[test]
fn peer_journal_gen_lags_helper() {
    let base = unique_dir("lags");
    let _g = Guard(base.clone());
    let b = Broker::new(StorageConfig {
        data_dir: base.join("n1"),
        ..StorageConfig::default()
    });
    assert!(!b.peer_journal_gen_lags(0), "empty journal does not lag peers");
    b.local_note_truncate_journal("t", 0, 10, 0);
    let gen = b.truncate_journal_generation();
    assert!(gen >= 1);
    assert!(b.peer_journal_gen_lags(0));
    assert!(b.peer_journal_gen_lags(gen - 1));
    assert!(!b.peer_journal_gen_lags(gen));
    assert!(!b.peer_journal_gen_lags(gen + 5));
}
