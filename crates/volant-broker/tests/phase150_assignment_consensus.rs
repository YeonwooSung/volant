//! Phase 150: cluster assignment majority consensus (MVP).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    fanout_assignment_consensus, start_background_tasks, serve_listener, AssignmentConsensus,
    Broker, BrokerEndpoint, ClusterConfig,
};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p150-{label}-{}-{}",
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

fn cluster_config(ports: &[u16]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: ports.len() as u32,
        min_insync_replicas: ((ports.len() as u32) / 2).max(1),
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: ports
            .iter()
            .enumerate()
            .map(|(i, &port)| BrokerEndpoint {
                id: (i + 1) as u32,
                host: "127.0.0.1".into(),
                port,
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

#[test]
fn majority_math() {
    assert_eq!(AssignmentConsensus::majority(1), 1);
    assert_eq!(AssignmentConsensus::majority(2), 2);
    assert_eq!(AssignmentConsensus::majority(3), 2);
    assert_eq!(AssignmentConsensus::majority(5), 3);
}

/// Single-node: consensus success is trivial (majority 1).
#[test]
fn single_node_consensus_success_trivial() {
    let base = unique_dir("solo");
    let _g = Guard(base.clone());
    let b = Broker::new(StorageConfig {
        data_dir: base.join("n0"),
        ..StorageConfig::default()
    });
    b.create_topic("solo", 1).unwrap();
    // Fanout with no cluster commits local gen (0 for single-node).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let before = b.assignment_consensus_success_total();
    let ok = rt.block_on(fanout_assignment_consensus(&b));
    assert!(ok);
    assert!(
        b.assignment_consensus_success_total() > before,
        "single-node must count consensus success"
    );
}

/// Full 3-node live TCP: create_topic + consensus fanout → all peers have topic.
#[tokio::test]
async fn three_node_create_topic_majority() {
    let base = unique_dir("maj3");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let (l3, p3) = bind().await;
    let cfg = cluster_config(&[p1, p2, p3]);
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
        b.set_assignment_consensus_enabled(true);
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

    // Membership optimistically marks configured brokers live at start.
    b1.create_topic("c", 1).unwrap();
    let before = b1.assignment_consensus_success_total();
    let ok = fanout_assignment_consensus(&b1).await;
    assert!(ok, "3/3 live should reach majority");
    assert!(
        b1.assignment_consensus_success_total() > before,
        "consensus success metric must increment"
    );
    assert!(b1.assignment_committed_generation() >= 1);

    // Peers applied topic via AssignmentConsensusNote.
    for b in [&b2, &b3] {
        assert!(
            b.partition_count_opt("c").is_some(),
            "node {} missing topic after consensus fanout",
            b.node_id()
        );
    }

    s1.abort();
    s2.abort();
    s3.abort();
}

/// N=2 with one dead + wait on → consensus fail metric; local assignment retained.
#[tokio::test]
async fn n2_one_dead_wait_fails_majority() {
    let base = unique_dir("n2dead");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    // Peer never listens.
    let p2 = p1.saturating_add(100).max(33000);
    let cfg = cluster_config(&[p1, p2]);
    let b1 = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("n1"),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            1,
            cfg,
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_assignment_consensus_enabled(true);
        b.set_assignment_consensus_wait(true);
        Arc::new(b)
    };
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Configured peer is optimistically live; RPC fails (no listener).
    b1.create_topic("d", 1).unwrap();
    let before_fail = b1.assignment_consensus_fail_total();
    let before_ok = b1.assignment_consensus_success_total();
    let ok = fanout_assignment_consensus(&b1).await;
    assert!(
        !ok,
        "N=2 with dead peer must fail majority (need 2, acks=1 local)"
    );
    assert!(
        b1.assignment_consensus_fail_total() > before_fail,
        "fail metric must increment"
    );
    assert_eq!(
        b1.assignment_consensus_success_total(),
        before_ok,
        "must not count success when majority missed"
    );
    // Local assignment retained (best-effort honesty).
    assert!(b1.partition_count_opt("d").is_some());

    s1.abort();
}

/// N=1 configured cluster: majority 1 → success without peers.
#[tokio::test]
async fn single_configured_broker_majority() {
    let base = unique_dir("n1cfg");
    let _g = Guard(base.clone());
    let (l1, p1) = bind().await;
    let cfg = cluster_config(&[p1]);
    let b1 = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("n1"),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            1,
            cfg,
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_assignment_consensus_enabled(true);
        Arc::new(b)
    };
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    b1.create_topic("one", 1).unwrap();
    let before = b1.assignment_consensus_success_total();
    assert!(fanout_assignment_consensus(&b1).await);
    assert!(b1.assignment_consensus_success_total() > before);
    assert!(b1.assignment_committed_generation() >= 1);

    s1.abort();
}
