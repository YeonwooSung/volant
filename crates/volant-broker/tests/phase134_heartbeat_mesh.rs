//! Phase 134: peer-to-peer heartbeat mesh enables non-controller → non-controller
//! truncate-journal catch-up when only a non-controller holds the watermark.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{serve_listener, Broker, BrokerEndpoint, ClusterConfig};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p134-{label}-{}-{}",
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

/// Non-controller b2 alone holds a journal watermark (no majority fan-out).
/// Empty b3 must receive the snapshot via mesh heartbeats (b3 → b2 lag path),
/// not via the controller (b1 has no watermark).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_controller_journal_catchup_via_mesh() {
    let base = unique_dir("mesh");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let ports = [p1, p2, p3];
    // Short session → frequent heartbeats (period ≈ session/3).
    let cfg = cluster_config(ports, 900);

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
    assert!(b1.is_controller(), "lowest id is controller");
    assert!(!b2.is_controller());
    assert!(!b3.is_controller());

    // Note **before** any accept loop: `serve_listener` starts mesh bg tasks
    // (single-flight), which would otherwise race catch-up into b1/b3.
    // Only b2 notes locally — no majority fan-out / push.
    b2.local_note_truncate_journal("mesh-topic", 0, 77, 1);
    assert_eq!(b2.truncate_journal().watermark("mesh-topic", 0), Some(77));
    assert_eq!(
        b1.truncate_journal().watermark("mesh-topic", 0),
        None,
        "pre-mesh: controller must not hold the watermark"
    );
    assert_eq!(
        b3.truncate_journal().watermark("mesh-topic", 0),
        None,
        "pre-mesh: b3 starts empty"
    );

    // Accept loops start mesh heartbeats (every configured peer). Under
    // controller-only HB (pre-134), b3 would only talk to empty b1 and never
    // catch up; mesh lets b3 HB → b2 schedule TruncateJournalPush.
    let h1 = tokio::spawn(serve_listener(l1, Arc::clone(&b1)));
    let h2 = tokio::spawn(serve_listener(l2, Arc::clone(&b2)));
    let h3 = tokio::spawn(serve_listener(l3, Arc::clone(&b3)));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if b3.truncate_journal().watermark("mesh-topic", 0) == Some(77) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "b3 never caught up via mesh heartbeat; \
                 b3 watermark={:?} gen={} applied={} \
                 b2 watermark={:?} gen={} catchup_ok={} catchup_err={} \
                 b1 watermark={:?} catchup_ok={} catchup_err={} \
                 live b1={:?} b2={:?} b3={:?}",
                b3.truncate_journal().watermark("mesh-topic", 0),
                b3.truncate_journal_generation(),
                b3.truncate_journal_applied_generation(),
                b2.truncate_journal().watermark("mesh-topic", 0),
                b2.truncate_journal_generation(),
                b2.journal_catchup_success_total(),
                b2.journal_catchup_errors_total(),
                b1.truncate_journal().watermark("mesh-topic", 0),
                b1.journal_catchup_success_total(),
                b1.journal_catchup_errors_total(),
                b1.live_brokers(),
                b2.live_brokers(),
                b3.live_brokers(),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        b3.truncate_journal().watermark("mesh-topic", 0),
        Some(77),
        "lagging peer must max-merge mesh catch-up snapshot"
    );
    assert!(
        b3.truncate_journal_applied_generation() >= 1,
        "b3 applied journal generation should advance"
    );
    // b2 was the sole initial holder; it must have pushed at least once
    // (to b3 and/or b1). Controller may also catch up via mesh afterward —
    // that is expected and does not invalidate the non-controller source path.
    assert!(
        b2.journal_catchup_success_total() >= 1,
        "non-controller holder should record at least one journal catch-up success"
    );

    // Controller-only membership path still healthy (no panic; controller live).
    assert!(b1.is_controller());
    assert!(
        b2.live_brokers().contains(&1) || b3.live_brokers().contains(&1),
        "peers should see controller live via mesh heartbeats"
    );

    // serve_listener owns bg tasks; abort accept loops (bg shuts down with them).
    h1.abort();
    h2.abort();
    h3.abort();
}
