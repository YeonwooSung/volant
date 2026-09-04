//! v0.2: assignment consensus miss must not fail admin when wait/committed-only
//! are off (the shipped defaults). Raft stays off; notes stay on as best-effort.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config_n2, default_storage, rpc_seq, unique_dir, Guard};
use volant_broker::{serve_listener, start_background_tasks, Broker};
use volant_protocol::{Request, Response};

fn assignment_json_has_topic(data_dir: &std::path::Path, topic: &str) -> bool {
    let path = data_dir.join("cluster").join("assignment.json");
    if !path.is_file() {
        return false;
    }
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    raw.contains(&format!("\"{topic}\"")) || raw.contains(&format!("\"name\": \"{topic}\""))
}

/// N=2, one dead: CreateTopic / CreatePartitions / DeleteTopic succeed on
/// local `assignment.json` even when opcodes 96/97 miss majority.
#[tokio::test]
async fn n2_one_dead_admin_succeeds_on_assignment_json() {
    let base = unique_dir("v02", "miss");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    // Peer never listens — 96/97 RPC fails (connection refused).
    let p2 = p1.saturating_add(100).max(33_000);
    let cfg = cluster_config_n2([p1, p2]);
    let data_dir = base.join("n1");
    let b1 = {
        let b = Broker::with_cluster(default_storage(data_dir.clone()), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };

    // v0.2 shipped defaults (do not set flags): raft off, committed-only off,
    // wait off, consensus on.
    assert!(
        !b1.metadata_raft_enabled(),
        "VOLANT_METADATA_RAFT must default off"
    );
    assert!(
        !data_dir.join("__metadata_raft").exists(),
        "default-off broker must not create __metadata_raft"
    );
    assert!(
        !b1.assignment_metadata_committed_only(),
        "VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY must default off"
    );
    assert!(
        !b1.assignment_consensus_wait(),
        "VOLANT_ASSIGNMENT_CONSENSUS_WAIT must default off"
    );
    assert!(
        b1.assignment_consensus_enabled(),
        "VOLANT_ASSIGNMENT_CONSENSUS must stay on"
    );

    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let addr = format!("127.0.0.1:{p1}");

    let resps = rpc_seq(
        &addr,
        &[Request::CreateTopic {
            name: "t".into(),
            partitions: 1,
            configs: vec![],
        }],
    )
    .await;
    match &resps[0] {
        Response::CreateTopic {
            error_code, name, ..
        } => {
            assert_eq!(*error_code, 0, "CreateTopic must succeed on 96/97 miss");
            assert_eq!(name, "t");
        }
        other => panic!("CreateTopic expected ok, got {other:?}"),
    }
    assert!(
        assignment_json_has_topic(&data_dir, "t"),
        "assignment.json must contain topic after CreateTopic"
    );

    let resps = rpc_seq(
        &addr,
        &[Request::Metadata {
            topics: vec!["t".into()],
        }],
    )
    .await;
    match &resps[0] {
        Response::Metadata { topics, .. } => {
            assert!(
                topics.iter().any(|t| t.name == "t"),
                "Metadata must serve live assignment, got {topics:?}"
            );
        }
        other => panic!("Metadata expected, got {other:?}"),
    }

    let resps = rpc_seq(
        &addr,
        &[Request::CreatePartitions {
            topic: "t".into(),
            total_count: 2,
        }],
    )
    .await;
    match &resps[0] {
        Response::CreatePartitions {
            error_code,
            partitions,
            ..
        } => {
            assert_eq!(
                *error_code, 0,
                "CreatePartitions must succeed on 96/97 miss"
            );
            assert_eq!(*partitions, 2);
        }
        other => panic!("CreatePartitions expected ok, got {other:?}"),
    }
    assert!(
        assignment_json_has_topic(&data_dir, "t"),
        "assignment.json must still contain topic after CreatePartitions"
    );

    let resps = rpc_seq(&addr, &[Request::DeleteTopic { name: "t".into() }]).await;
    match &resps[0] {
        Response::DeleteTopic { error_code, name } => {
            assert_eq!(*error_code, 0, "DeleteTopic must succeed on 96/97 miss");
            assert_eq!(name, "t");
        }
        other => panic!("DeleteTopic expected ok, got {other:?}"),
    }
    let path = data_dir.join("cluster").join("assignment.json");
    assert!(
        path.is_file(),
        "assignment.json must exist after DeleteTopic"
    );

    s1.abort();
}
