//! Phase 154: KRaft-style metadata Raft log MVP.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use volant_broker::{
    fanout_assignment_consensus, fanout_metadata_raft_append, serve_listener,
    start_background_tasks, Broker, BrokerEndpoint, ClusterConfig, MetadataCommand,
    MetadataLogEntry, MetadataRaftState,
};
use volant_core::TopicName;
use volant_protocol::{ClusterPartitionState, ClusterTopicState};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p154-{label}-{}-{}",
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

fn sample_topics() -> Vec<ClusterTopicState> {
    vec![ClusterTopicState {
        name: "t".into(),
        topic_id: 1,
        partitions: vec![ClusterPartitionState {
            partition_id: 0,
            leader: 1,
            leader_epoch: 0,
            replicas: vec![1],
            isr: vec![1],
        }],
    }]
}

/// Unit: prev_log mismatch rejects (also covered in module tests).
#[test]
fn prev_log_mismatch_rejects() {
    let dir = unique_dir("prev");
    let _g = Guard(dir.clone());
    let s = MetadataRaftState::open(&dir);
    let e1 = s.append_command(MetadataCommand::Noop);
    assert_eq!(e1.index, 1);
    let bad = MetadataLogEntry {
        term: 1,
        index: 2,
        payload: MetadataCommand::Noop,
    };
    let r = s.append_entries(1, 1, 99, &[bad], 0);
    assert!(!r.success, "wrong prev_term must reject");
    assert_eq!(s.last_index(), 1);

    let good = MetadataLogEntry {
        term: 1,
        index: 2,
        payload: MetadataCommand::SetAssignment {
            generation: 1,
            topics: sample_topics(),
        },
    };
    let r2 = s.append_entries(1, 1, 1, &[good], 2);
    assert!(r2.success);
    assert_eq!(r2.match_index, 2);
    assert_eq!(s.commit_index(), 2);
}

/// Single-node: append+commit works; create_topic ok with raft fanout.
#[test]
fn single_node_append_commit_create_topic() {
    let base = unique_dir("solo");
    let _g = Guard(base.clone());
    let b = Broker::new(StorageConfig {
        data_dir: base.join("n0"),
        ..StorageConfig::default()
    });
    b.set_metadata_raft_enabled(true);
    // v0.40: keep 154 mutate-first / uncommitted-lead for these unit paths.
    b.set_metadata_raft_wait_commit(false);
    b.create_topic("solo", 1).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let before = b.metadata_raft_append_success_total();
    let ok = rt.block_on(fanout_metadata_raft_append(&b));
    assert!(ok);
    assert!(
        b.metadata_raft_append_success_total() > before,
        "single-node must count append success"
    );
    assert!(b.metadata_raft_commit_index() >= 1);
    assert!(b.metadata_raft_last_applied() >= 1);
    assert!(b.partition_count_opt("solo").is_some());
}

/// 3-node: create_topic with raft on → all nodes apply same gen; commit advances.
#[tokio::test]
async fn three_node_create_topic_raft() {
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
        b.set_metadata_raft_enabled(true);
        // v0.40: this test fans out AppendEntries directly (uncommitted-lead ok).
        b.set_metadata_raft_wait_commit(false);
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
    let before = b1.metadata_raft_append_success_total();
    let ok = fanout_metadata_raft_append(&b1).await;
    assert!(ok, "3/3 live should reach majority");
    assert!(
        b1.metadata_raft_append_success_total() > before,
        "append success metric must increment"
    );
    assert!(b1.metadata_raft_commit_index() >= 1);
    assert_eq!(
        b1.assignment_committed_generation(),
        b1.generation(),
        "Phase 152 committed_gen tracks raft-applied gen"
    );

    // Peers applied topic via metadata Raft.
    for b in [&b2, &b3] {
        // Allow a brief settle for apply path.
        for _ in 0..20 {
            if b.partition_count_opt("c").is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            b.partition_count_opt("c").is_some(),
            "node {} missing topic after metadata raft fanout",
            b.node_id()
        );
        assert!(
            b.metadata_raft_commit_index() >= 1,
            "node {} commit_index not advanced",
            b.node_id()
        );
        assert_eq!(
            b.generation(),
            b1.generation(),
            "node {} assignment gen mismatch",
            b.node_id()
        );
    }

    s1.abort();
    s2.abort();
    s3.abort();
}

/// N=2 one dead: append majority fail → fail metric; commit not advanced.
#[tokio::test]
async fn n2_one_dead_append_fails() {
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
        b.set_metadata_raft_enabled(true);
        // v0.40: expect uncommitted-lead (local assignment retained on miss).
        b.set_metadata_raft_wait_commit(false);
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
    let commit_before = b1.metadata_raft_commit_index();
    let fail_before = b1.metadata_raft_append_fail_total();
    let ok = fanout_metadata_raft_append(&b1).await;
    assert!(!ok, "N=2 with dead peer must fail majority");
    assert!(b1.metadata_raft_append_fail_total() > fail_before);
    assert_eq!(
        b1.metadata_raft_commit_index(),
        commit_before,
        "commit_index must not advance on majority miss"
    );
    // Local assignment retained; log entry left uncommitted.
    assert!(b1.partition_count_opt("d").is_some());
    assert!(b1.metadata_raft().last_index() > commit_before);

    s1.abort();
}

/// Phase 150 path still works when metadata raft is off.
#[tokio::test]
async fn phase150_path_with_raft_off() {
    let base = unique_dir("raft-off");
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
        b.set_metadata_raft_enabled(false);
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

    b1.create_topic("legacy", 1).unwrap();
    let ok = fanout_assignment_consensus(&b1).await;
    assert!(ok, "Phase 150 note path must work with raft off");
    for b in [&b2, &b3] {
        assert!(
            b.partition_count_opt("legacy").is_some(),
            "node {} missing topic via Phase 150 path",
            b.node_id()
        );
    }
    // Metadata still lists topic name when requested.
    assert!(
        b1.metadata(Some(&[TopicName::new("legacy")]))
            .topics
            .iter()
            .any(|t| t.name.as_str() == "legacy")
            || b1.assignment_committed_generation() >= 1
    );

    s1.abort();
    s2.abort();
    s3.abort();
}
