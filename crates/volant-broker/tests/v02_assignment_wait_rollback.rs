//! v0.3: must_wait majority miss rolls back live assignment.json.
//!
//! Wait/committed-only off is unchanged (`v02_create_topic_ungate`): local
//! write stays SoT. Direct `fanout_assignment_consensus` still retains local
//! (`phase150::n2_one_dead_wait_fails_majority`).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config_n2, default_storage, rpc_seq, unique_dir, Guard};
use volant_broker::{serve_listener, start_background_tasks, Broker};
use volant_protocol::{ErrorCode, Request, Response};

fn assignment_json_has_topic(data_dir: &std::path::Path, topic: &str) -> bool {
    let path = data_dir.join("cluster").join("assignment.json");
    if !path.is_file() {
        return false;
    }
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    raw.contains(&format!("\"{topic}\"")) || raw.contains(&format!("\"name\": \"{topic}\""))
}

/// N=2 one-dead: wait-on CreateTopic/CreatePartitions/DeleteTopic return 15
/// and restore pre-mutation live assignment (disk + Metadata).
#[tokio::test]
async fn n2_one_dead_wait_rolls_back_assignment() {
    let base = unique_dir("v03", "rollback");
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

    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let addr = format!("127.0.0.1:{p1}");

    // 1. Wait on → CreateTopic "t" → 15; disk + Metadata have no "t".
    b1.set_assignment_consensus_wait(true);
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
        Response::Error { code, message } => {
            assert_eq!(
                *code,
                ErrorCode::NotEnoughReplicas as u16,
                "CreateTopic wait must surface 15, got {code} ({message})"
            );
        }
        Response::CreateTopic { error_code, .. } => {
            panic!("expected Error 15 on wait, got CreateTopic error_code={error_code}");
        }
        other => panic!("CreateTopic wait expected Error 15, got {other:?}"),
    }
    assert!(
        !assignment_json_has_topic(&data_dir, "t"),
        "wait-fail CreateTopic must not leave t on disk"
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
                !topics.iter().any(|t| t.name == "t"),
                "Metadata must not serve rolled-back t, got {topics:?}"
            );
        }
        other => panic!("Metadata expected, got {other:?}"),
    }

    // 2. Wait off → CreateTopic "t" → 0 (retry is not already-exists).
    b1.set_assignment_consensus_wait(false);
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
            assert_eq!(
                *error_code, 0,
                "retry CreateTopic after rollback must succeed"
            );
            assert_eq!(name, "t");
        }
        other => panic!("CreateTopic retry expected ok, got {other:?}"),
    }
    assert!(
        assignment_json_has_topic(&data_dir, "t"),
        "wait-off CreateTopic must write t"
    );

    // 3. Create "u" wait off → wait on → CreatePartitions total=2 → 15; still 1 partition.
    let resps = rpc_seq(
        &addr,
        &[Request::CreateTopic {
            name: "u".into(),
            partitions: 1,
            configs: vec![],
        }],
    )
    .await;
    match &resps[0] {
        Response::CreateTopic {
            error_code, name, ..
        } => {
            assert_eq!(*error_code, 0, "CreateTopic u wait-off must succeed");
            assert_eq!(name, "u");
        }
        other => panic!("CreateTopic u expected ok, got {other:?}"),
    }

    b1.set_assignment_consensus_wait(true);
    let resps = rpc_seq(
        &addr,
        &[Request::CreatePartitions {
            topic: "u".into(),
            total_count: 2,
        }],
    )
    .await;
    match &resps[0] {
        Response::CreatePartitions { error_code, .. } => {
            assert_eq!(
                *error_code,
                ErrorCode::NotEnoughReplicas as u16,
                "CreatePartitions wait must surface 15"
            );
        }
        other => panic!("CreatePartitions wait expected 15, got {other:?}"),
    }
    assert_eq!(
        b1.partition_count_opt("u"),
        Some(1),
        "wait-fail CreatePartitions must leave u at 1 partition"
    );
    let resps = rpc_seq(
        &addr,
        &[Request::Metadata {
            topics: vec!["u".into()],
        }],
    )
    .await;
    match &resps[0] {
        Response::Metadata { topics, .. } => {
            let t = topics
                .iter()
                .find(|t| t.name == "u")
                .expect("Metadata must still list u");
            assert_eq!(
                t.partitions.len(),
                1,
                "Metadata u must still have 1 partition"
            );
        }
        other => panic!("Metadata expected, got {other:?}"),
    }

    // 4. Wait on → DeleteTopic "u" → 15; "u" still in assignment.json + Metadata.
    let resps = rpc_seq(&addr, &[Request::DeleteTopic { name: "u".into() }]).await;
    match &resps[0] {
        Response::Error { code, message } => {
            assert_eq!(
                *code,
                ErrorCode::NotEnoughReplicas as u16,
                "DeleteTopic wait must surface 15, got {code} ({message})"
            );
        }
        Response::DeleteTopic { error_code, .. } => {
            panic!("expected Error 15 on wait, got DeleteTopic error_code={error_code}");
        }
        other => panic!("DeleteTopic wait expected Error 15, got {other:?}"),
    }
    assert!(
        assignment_json_has_topic(&data_dir, "u"),
        "wait-fail DeleteTopic must restore u on disk"
    );
    let resps = rpc_seq(
        &addr,
        &[Request::Metadata {
            topics: vec!["u".into()],
        }],
    )
    .await;
    match &resps[0] {
        Response::Metadata { topics, .. } => {
            assert!(
                topics.iter().any(|t| t.name == "u"),
                "Metadata must still serve u after delete wait-fail, got {topics:?}"
            );
        }
        other => panic!("Metadata expected, got {other:?}"),
    }

    s1.abort();
}
