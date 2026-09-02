//! v0.40: homemade 154 wait-commit before CreateTopic client ok.
//!
//! Default **on** when homemade raft is used: N=2 one-dead CreateTopic
//! returns 15 and rolls back live `assignment.json`. `0` restores 154
//! mutate-first (local success, uncommitted entry retained).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, cluster_config_n2, default_storage, rpc_seq, unique_dir, Guard,
};
use volant_broker::{serve_listener, start_background_tasks, Broker};
use volant_protocol::{ErrorCode, Request, Response};
use volant_storage::StorageConfig;

fn assignment_json_has_topic(data_dir: &std::path::Path, topic: &str) -> bool {
    let path = data_dir.join("cluster").join("assignment.json");
    if !path.is_file() {
        return false;
    }
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    raw.contains(&format!("\"{topic}\"")) || raw.contains(&format!("\"name\": \"{topic}\""))
}

/// Unset env: wait-commit defaults on; inert until homemade raft is enabled.
#[test]
fn wait_commit_defaults_on_inert_until_raft() {
    let dir = unique_dir("v40", "default");
    let _g = Guard(dir.clone());
    let b = Broker::new(StorageConfig {
        data_dir: dir.join("n0"),
        ..StorageConfig::default()
    });
    assert!(
        b.metadata_raft_wait_commit(),
        "VOLANT_METADATA_RAFT_WAIT_COMMIT must default on"
    );
    assert!(
        !b.metadata_raft_enabled(),
        "homemade raft still defaults off"
    );
    assert!(
        !b.assignment_must_wait(),
        "wait-commit is inert while homemade raft is off"
    );
    b.set_metadata_raft_enabled(true);
    assert!(
        b.assignment_must_wait(),
        "raft on + wait-commit on must fail the client on majority miss"
    );
}

/// Homemade raft on, wait-commit on, N=2 one dead: CreateTopic 15, no disk topic.
#[tokio::test]
async fn n2_one_dead_wait_commit_fails_and_rolls_back() {
    let base = unique_dir("v40", "wait-on");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(33_000);
    let cfg = cluster_config_n2([p1, p2]);
    let data_dir = base.join("n1");
    let b1 = {
        let b = Broker::with_cluster(default_storage(data_dir.clone()), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_metadata_raft_enabled(true);
        b.set_metadata_raft_wait_commit(true);
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
    let commit_before = b1.metadata_raft_commit_index();

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
                "wait-commit CreateTopic must surface 15, got {code} ({message})"
            );
        }
        Response::CreateTopic { error_code, .. } => {
            panic!("expected Error 15 on wait-commit, got CreateTopic error_code={error_code}");
        }
        other => panic!("CreateTopic wait-commit expected Error 15, got {other:?}"),
    }
    assert!(
        !assignment_json_has_topic(&data_dir, "t"),
        "wait-commit fail must not leave t on disk"
    );
    assert_eq!(
        b1.metadata_raft_commit_index(),
        commit_before,
        "commit_index must not advance on majority miss"
    );
    assert!(
        b1.partition_count_opt("t").is_none(),
        "rolled-back topic must not stay in live assignment"
    );

    s1.abort();
}

/// Homemade raft on, wait-commit off: CreateTopic still succeeds locally (154).
#[tokio::test]
async fn n2_one_dead_wait_commit_off_mutate_first() {
    let base = unique_dir("v40", "wait-off");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(33_000);
    let cfg = cluster_config_n2([p1, p2]);
    let data_dir = base.join("n1");
    let b1 = {
        let b = Broker::with_cluster(default_storage(data_dir.clone()), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_metadata_raft_enabled(true);
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
    let addr = format!("127.0.0.1:{p1}");
    let commit_before = b1.metadata_raft_commit_index();

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
                "wait-commit off must keep 154 mutate-first success"
            );
            assert_eq!(name, "t");
        }
        other => panic!("CreateTopic wait-off expected ok, got {other:?}"),
    }
    assert!(
        assignment_json_has_topic(&data_dir, "t"),
        "wait-commit off must write t to assignment.json"
    );
    assert_eq!(
        b1.metadata_raft_commit_index(),
        commit_before,
        "majority miss must not advance commit_index"
    );
    assert!(
        b1.metadata_raft().last_index() > commit_before,
        "uncommitted entry retained on wait-commit off"
    );

    s1.abort();
}

/// 3 live nodes, wait-commit on: CreateTopic succeeds and commit_index advances.
#[tokio::test]
async fn three_live_wait_commit_advances() {
    let base = unique_dir("v40", "maj3");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config([p1, p2, p3]);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        b.set_metadata_raft_enabled(true);
        b.set_metadata_raft_wait_commit(true);
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

    let commit_before = b1.metadata_raft_commit_index();
    let addr = format!("127.0.0.1:{p1}");
    let resps = rpc_seq(
        &addr,
        &[Request::CreateTopic {
            name: "c".into(),
            partitions: 1,
            configs: vec![],
        }],
    )
    .await;
    match &resps[0] {
        Response::CreateTopic {
            error_code, name, ..
        } => {
            assert_eq!(*error_code, 0, "3 live nodes must CreateTopic ok");
            assert_eq!(name, "c");
        }
        other => panic!("CreateTopic 3-live expected ok, got {other:?}"),
    }
    assert!(
        b1.metadata_raft_commit_index() > commit_before,
        "wait-commit success must advance commit_index ({commit_before} -> {})",
        b1.metadata_raft_commit_index()
    );
    assert!(
        assignment_json_has_topic(&base.join("n1"), "c"),
        "controller assignment.json must contain c"
    );

    s1.abort();
    s2.abort();
    s3.abort();
}
