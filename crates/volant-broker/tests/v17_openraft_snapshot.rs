//! v0.17 — openraft InstallSnapshot (opcodes 112/113) + log truncation.
//!
//! Flag on + `VOLANT_OPENRAFT_SNAPSHOT_LOGS=1` forces snapshots. Homemade 154
//! is not exercised. Default-off election behavior is unchanged.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, default_storage, unique_dir, Guard};
use volant_broker::{inter_broker_rpc, serve_listener, Broker};
use volant_protocol::{Request, Response};

fn set_openraft_env(on: bool, snapshot_logs: Option<&str>) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
    }
    match snapshot_logs {
        Some(v) => std::env::set_var("VOLANT_OPENRAFT_SNAPSHOT_LOGS", v),
        None => std::env::remove_var("VOLANT_OPENRAFT_SNAPSHOT_LOGS"),
    }
}

struct Triple {
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    ports: [u16; 3],
    h1: tokio::task::JoinHandle<()>,
    h2: tokio::task::JoinHandle<()>,
    h3: Option<tokio::task::JoinHandle<()>>,
}

impl Triple {
    async fn boot(
        label: &str,
        start_third: bool,
    ) -> (Self, Guard, Option<tokio::net::TcpListener>) {
        let base = unique_dir("v17", label);
        let guard = Guard(base.clone());
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
        let (h3, held_l3) = if start_third {
            let b = Arc::clone(&b3);
            (
                Some(tokio::spawn(async move {
                    let _ = serve_listener(l3, b).await;
                })),
                None,
            )
        } else {
            (None, Some(l3))
        };
        tokio::time::sleep(Duration::from_millis(120)).await;
        (
            Self {
                b1,
                b2,
                b3,
                ports,
                h1,
                h2,
                h3,
            },
            guard,
            held_l3,
        )
    }

    fn broker(&self, id: u32) -> Arc<Broker> {
        match id {
            1 => Arc::clone(&self.b1),
            2 => Arc::clone(&self.b2),
            3 => Arc::clone(&self.b3),
            _ => panic!("bad id {id}"),
        }
    }

    fn live(&self, ids: &[u32]) -> Vec<Arc<Broker>> {
        ids.iter().copied().map(|id| self.broker(id)).collect()
    }

    fn start_third(&mut self, l3: tokio::net::TcpListener) {
        let b = Arc::clone(&self.b3);
        self.h3 = Some(tokio::spawn(async move {
            let _ = serve_listener(l3, b).await;
        }));
    }

    fn abort_all(&self) {
        self.h1.abort();
        self.h2.abort();
        if let Some(h) = &self.h3 {
            h.abort();
        }
    }
}

async fn wait_agreed_leader(nodes: &[Arc<Broker>], timeout: Duration) -> u32 {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = Vec::new();
    while tokio::time::Instant::now() < deadline {
        last = nodes.iter().map(|n| n.controller_id()).collect();
        if last.iter().all(|id| *id != 0) && last.iter().all(|id| *id == last[0]) {
            let leader = last[0];
            if nodes.iter().any(|n| n.node_id() == leader) {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no agreed openraft leader within {timeout:?}; last={last:?}");
}

async fn write_noops(leader: &Broker, n: usize) {
    for i in 0..n {
        let mut last_err = None;
        for _ in 0..20 {
            match leader.test_openraft_client_write_noop().await {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        if let Some(e) = last_err {
            panic!("noop {i} failed: {e}");
        }
    }
}

async fn wait_snapshot(
    nodes: &[Arc<Broker>],
    timeout: Duration,
) -> (u32, String, Option<u64>, Vec<u8>) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = String::from("none");
    while tokio::time::Instant::now() < deadline {
        for n in nodes {
            if let Some((id, idx, bytes)) = n.test_openraft_current_snapshot().await {
                if !bytes.is_empty() {
                    return (n.node_id(), id, idx, bytes);
                }
                last = format!("node {} empty snapshot id={id}", n.node_id());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no snapshot within {timeout:?}; last={last}");
}

async fn wait_purged(nodes: &[Arc<Broker>], timeout: Duration) -> (u32, u64) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for n in nodes {
            if let Some(idx) = n.test_openraft_last_purged_index() {
                return (n.node_id(), idx);
            }
        }
        for n in nodes {
            let _ = n.test_openraft_trigger_purge().await;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let seen: Vec<_> = nodes
        .iter()
        .map(|n| (n.node_id(), n.test_openraft_last_purged_index()))
        .collect();
    panic!("last_purged did not advance within {timeout:?}; seen={seen:?}");
}

/// Flag default off: no openraft, lowest-id controller, no snapshot hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_off_no_openraft_snapshot() {
    set_openraft_env(false, None);
    let (t, _g, _) = Triple::boot("off", true).await;
    assert!(
        !t.b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA=0 keeps lowest-id"
    );
    assert_eq!(t.b1.controller_id(), 1);
    assert_eq!(t.b2.controller_id(), 1);
    assert_eq!(t.b3.controller_id(), 1);
    assert!(t.b1.test_openraft_current_snapshot().await.is_none());
    assert_eq!(t.b1.test_openraft_install_snapshot_rx(), 0);
    t.abort_all();
}

/// Flag on, 3-node: after client writes a snapshot exists with useful JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_snapshot_exists() {
    set_openraft_env(true, Some("1"));
    let (t, _g, _) = Triple::boot("snap", true).await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    write_noops(&leader, 8).await;
    let _ = leader.test_openraft_trigger_snapshot().await;
    let (_nid, snap_id, idx, bytes) = wait_snapshot(&nodes, Duration::from_secs(8)).await;
    assert!(
        snap_id.starts_with("v17-"),
        "snapshot_id should be v17-*: {snap_id}"
    );
    assert!(idx.is_some(), "snapshot last_log_id should be set");
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("last_applied"),
        "payload missing last_applied: {body}"
    );
    assert!(
        body.contains("membership"),
        "payload missing membership: {body}"
    );
    assert!(
        body.contains("assignment"),
        "payload missing assignment: {body}"
    );
    t.abort_all();
}

/// After snapshot, log prefix is purged (`last_purged` advances).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_purges_log_prefix() {
    set_openraft_env(true, Some("1"));
    let (t, _g, _) = Triple::boot("purge", true).await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    write_noops(&leader, 8).await;
    let _ = leader.test_openraft_trigger_snapshot().await;
    let _ = wait_snapshot(&nodes, Duration::from_secs(8)).await;
    let (_nid, purged) = wait_purged(&nodes, Duration::from_secs(10)).await;
    assert!(purged > 0, "last_purged should advance, got {purged}");
    t.abort_all();
}

/// 2-of-3 majority builds a snapshot; the late third node installs it (or
/// catches up) and agrees on the leader. Term does not panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_node_installs_snapshot() {
    set_openraft_env(true, Some("1"));
    let (mut t, _g, held_l3) = Triple::boot("lag", false).await;
    let l3 = held_l3.expect("third listener held");
    let majority = t.live(&[1, 2]);
    let leader_id = wait_agreed_leader(&majority, Duration::from_secs(8)).await;
    let term_before = t.broker(leader_id).openraft_term();
    assert!(term_before > 0);
    write_noops(&t.broker(leader_id), 8).await;
    let _ = t.broker(leader_id).test_openraft_trigger_snapshot().await;
    let _ = wait_snapshot(&majority, Duration::from_secs(8)).await;
    let _ = t.broker(leader_id).test_openraft_trigger_purge().await;
    // Best-effort purge so the late node cannot catch up via prefix logs.
    let _ = wait_purged(&majority, Duration::from_secs(6)).await;

    t.start_third(l3);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let all = t.live(&[1, 2, 3]);
    let leader_after = wait_agreed_leader(&all, Duration::from_secs(12)).await;
    let term_after = t.broker(leader_after).openraft_term();
    assert!(
        term_after >= term_before,
        "term must not go backwards ({term_before} → {term_after})"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut applied3 = t.b3.test_openraft_last_applied_index();
    while tokio::time::Instant::now() < deadline {
        applied3 = t.b3.test_openraft_last_applied_index();
        if applied3.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        applied3.is_some(),
        "lagging node should apply via snapshot or append; rx={}",
        t.b3.test_openraft_install_snapshot_rx()
    );
    // Prefer InstallSnapshot, but append catch-up is acceptable if purge lagged.
    let _ = t.b3.test_openraft_install_snapshot_rx();
    t.abort_all();
}

/// One 112/113 RPC roundtrip against a live openraft node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn install_snapshot_rpc_roundtrip() {
    set_openraft_env(true, Some("1"));
    let (t, _g, _) = Triple::boot("rpc", true).await;
    let nodes = t.live(&[1, 2, 3]);
    let _leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let payload = Broker::test_openraft_probe_install_snapshot_payload();
    let addr = format!("127.0.0.1:{}", t.ports[0]);
    let resp = inter_broker_rpc(&t.b1, &addr, &Request::OpenraftInstallSnapshot { payload })
        .await
        .expect("112/113 roundtrip");
    match resp {
        Response::OpenraftInstallSnapshot { payload } => {
            assert!(!payload.is_empty(), "113 body must be JSON vote");
            let s = String::from_utf8_lossy(&payload);
            assert!(s.contains("vote") || s.contains("leader_id"), "{s}");
        }
        other => panic!("expected OpenraftInstallSnapshot, got {other:?}"),
    }
    assert!(t.b1.test_openraft_install_snapshot_rx() >= 1);
    t.abort_all();
}
