//! Phase 152: assignment consensus depth — Metadata serves committed snapshot.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    fanout_assignment_consensus, start_background_tasks, serve_listener, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p152-{label}-{}-{}",
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

fn meta_has_topic(b: &Broker, name: &str) -> bool {
    b.metadata(Some(&[TopicName::new(name)]))
        .topics
        .iter()
        .any(|t| t.name.as_str() == name)
}

/// 3-node create + consensus success → Metadata on all shows topic;
/// controller committed_gen == live gen.
#[tokio::test]
async fn three_node_metadata_serves_committed() {
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
        b.set_assignment_metadata_committed_only(true);
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
    // Before majority: committed-only Metadata must not advertise.
    assert!(
        !meta_has_topic(&b1, "c"),
        "controller Metadata must not lead committed gen"
    );
    assert!(b1.partition_count_opt("c").is_some(), "local live has topic");

    let ok = fanout_assignment_consensus(&b1).await;
    assert!(ok, "3/3 live should reach majority");
    assert_eq!(
        b1.assignment_committed_generation(),
        b1.generation(),
        "committed_gen == live gen after majority"
    );
    assert_eq!(b1.assignment_generation_lag(), 0);
    assert!(
        b1.assignment_consensus()
            .committed_snapshot()
            .is_some_and(|s| s.topics.contains_key("c")),
        "committed snapshot must include topic"
    );

    // Metadata on all nodes shows topic after consensus.
    for b in [&b1, &b2, &b3] {
        assert!(
            meta_has_topic(b, "c"),
            "node {} Metadata missing topic after consensus",
            b.node_id()
        );
    }

    s1.abort();
    s2.abort();
    s3.abort();
}

/// N=2 one dead + committed_only: majority fails; controller Metadata must not
/// advertise the new topic (local assignment retained).
#[tokio::test]
async fn n2_majority_fail_metadata_hides_uncommitted() {
    let base = unique_dir("n2dead");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
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
        b.set_assignment_metadata_committed_only(true);
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

    b1.create_topic("d", 1).unwrap();
    let before_fail = b1.assignment_consensus_fail_total();
    let ok = fanout_assignment_consensus(&b1).await;
    assert!(!ok, "N=2 with dead peer must fail majority");
    assert!(b1.assignment_consensus_fail_total() > before_fail);
    // Local assignment retained (honesty residual).
    assert!(b1.partition_count_opt("d").is_some());
    // Metadata committed-only: must not advertise uncommitted topic.
    assert!(
        !meta_has_topic(&b1, "d"),
        "committed-only Metadata must hide uncommitted topic"
    );
    assert_eq!(b1.assignment_committed_generation(), 0);

    s1.abort();
}

/// committed_only=false → Metadata can show live assignment even if committed lags.
#[tokio::test]
async fn committed_only_false_metadata_leads() {
    let base = unique_dir("lead");
    let _g = Guard(base.clone());

    let (l1, p1) = bind().await;
    let (l2, p2) = bind().await;
    let cfg = cluster_config(&[p1, p2]);
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
        b.set_assignment_metadata_committed_only(false);
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

    // Gen 1 committed.
    b1.create_topic("a", 1).unwrap();
    assert!(fanout_assignment_consensus(&b1).await);
    let committed_after_a = b1.assignment_committed_generation();
    assert!(committed_after_a >= 1);

    // Gen 2 live only (no fanout) → committed lags.
    b1.create_topic("b", 1).unwrap();
    assert!(b1.generation() > b1.assignment_committed_generation());
    assert!(b1.assignment_generation_lag() > 0);

    // Lead Metadata: both topics visible.
    assert!(meta_has_topic(&b1, "a"));
    assert!(
        meta_has_topic(&b1, "b"),
        "committed_only=false must show live uncommitted topic"
    );

    // Switch to committed-only: only a is advertised.
    b1.set_assignment_metadata_committed_only(true);
    assert!(meta_has_topic(&b1, "a"));
    assert!(
        !meta_has_topic(&b1, "b"),
        "committed-only must hide live-only topic b"
    );

    s1.abort();
    s2.abort();
}

/// Single-node (no cluster): Metadata works via local topics path.
#[test]
fn single_node_metadata_works() {
    let base = unique_dir("solo");
    let _g = Guard(base.clone());
    let b = Broker::new(StorageConfig {
        data_dir: base.join("n0"),
        ..StorageConfig::default()
    });
    b.create_topic("solo", 1).unwrap();
    assert!(meta_has_topic(&b, "solo"));
    // Fanout trivial success still works.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(rt.block_on(fanout_assignment_consensus(&b)));
}

/// Single configured broker: majority 1; Metadata tracks committed.
#[tokio::test]
async fn single_configured_broker_committed_metadata() {
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
        b.set_assignment_metadata_committed_only(true);
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
    assert!(
        !meta_has_topic(&b1, "one"),
        "before commit, committed-only hides topic"
    );
    assert!(fanout_assignment_consensus(&b1).await);
    assert_eq!(b1.assignment_committed_generation(), b1.generation());
    assert!(meta_has_topic(&b1, "one"));

    s1.abort();
}

/// maybe_fanout treats committed_only like wait (returns Some(false) on fail).
#[tokio::test]
async fn maybe_fanout_committed_only_forces_wait() {
    use volant_broker::maybe_fanout_assignment_consensus;

    let base = unique_dir("waitlike");
    let _g = Guard(base.clone());
    let (l1, p1) = bind().await;
    let p2 = p1.saturating_add(101).max(33100);
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
        b.set_assignment_consensus_wait(false);
        b.set_assignment_metadata_committed_only(true);
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

    b1.create_topic("w", 1).unwrap();
    // Wait off but committed_only on → Some(false) when majority fails.
    let res = maybe_fanout_assignment_consensus(&b1).await;
    assert_eq!(
        res,
        Some(false),
        "committed_only must force wait-like fail on majority miss"
    );

    s1.abort();
}
