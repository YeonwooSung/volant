//! v0.29: refuse irreversible DeleteRecords wait-off on clustered brokers.
//!
//! Cluster + effective wait-off + `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE`
//! unset/off → upgrade to wait-on (majority first; miss → 15, no truncate).
//! Env `1`/`true`/`yes`/`on` keeps today's local-first path. Single-node
//! wait-off stays allowed (no majority exists).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config_n2, default_storage, unique_dir, Guard};
use volant_broker::net::dispatch_request;
use volant_broker::{serve_listener, start_background_tasks, BackgroundTasks, Broker};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_protocol::{ErrorCode, Request, Response};
use volant_storage::StorageConfig;

fn big(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

fn small_seg_storage(data_dir: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        data_dir,
        flush_every_n: 1,
        segment_size: 256,
        ..StorageConfig::default()
    }
}

fn fill_local(broker: &Broker, topic: &str, n: u32) {
    let name = TopicName::new(topic);
    let pid = PartitionId(0);
    for i in 0..n {
        let mut batch = MessageBatch::default();
        batch
            .messages
            .push(Message::from_value(big(&format!("m{i}"), 180)));
        let (_, err) = broker
            .produce_with_acks(&name, pid, batch, 1, None)
            .expect("produce");
        assert_eq!(err, 0, "produce acks=1 should succeed on leader");
    }
}

fn assert_is_leader(broker: &Broker, topic: &str) {
    let name = TopicName::new(topic);
    assert!(
        broker.is_partition_leader(&name, PartitionId(0)),
        "node {} must lead {topic}/0",
        broker.node_id()
    );
}

fn earliest(broker: &Broker, topic: &str) -> u64 {
    broker
        .list_offsets(topic, &[0])
        .unwrap()
        .first()
        .map(|e| e.1)
        .unwrap_or(0)
}

struct EnvRestore {
    key: &'static str,
    prev: Option<String>,
}

impl EnvRestore {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prev }
    }

    fn remove(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, prev }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

/// Unset env = off; `1`/`true`/`yes`/`on` enable; cluster wait-off upgrades.
#[test]
fn allow_irreversible_default_off_cluster_upgrades() {
    let _env = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let dir = unique_dir("v29", "knob");
    let _g = Guard(dir.clone());

    let single = Broker::new(default_storage(dir.join("solo")));
    assert!(!single.delete_records_allow_irreversible());
    assert!(!single.effective_delete_records_wait_majority(2));
    assert!(!single.effective_delete_records_wait_majority(0));
    assert_eq!(single.delete_records_wait_off_upgraded_total(), 0);

    let cfg = cluster_config_n2([19_001, 19_002]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();
    assert!(!clustered.delete_records_allow_irreversible());
    assert!(
        clustered.effective_delete_records_wait_majority(2),
        "cluster flag 2 + env unset must upgrade to wait-on"
    );
    assert!(
        clustered.effective_delete_records_wait_majority(0),
        "cluster flag 0 + knob off + env unset must upgrade to wait-on"
    );
    assert!(clustered.delete_records_wait_off_upgraded_total() >= 2);

    clustered.set_delete_records_allow_irreversible(true);
    assert!(clustered.delete_records_allow_irreversible());
    assert!(
        !clustered.effective_delete_records_wait_majority(2),
        "allow=1 must keep flag 2 as wait-off"
    );
}

/// Env `1` at construct enables irreversible wait-off.
#[test]
fn allow_irreversible_env_one_at_construct() {
    let _env = EnvRestore::set("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE", "1");
    let dir = unique_dir("v29", "env1");
    let _g = Guard(dir.clone());
    let cfg = cluster_config_n2([19_011, 19_012]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();
    assert!(clustered.delete_records_allow_irreversible());
    assert!(!clustered.effective_delete_records_wait_majority(2));
}

/// Cluster N=2, one dead, force wait-off, env unset → no local truncate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_force_wait_off_env_unset_no_truncate() {
    let _env = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let base = unique_dir("v29", "upgrade");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(37_100);
    let cfg = cluster_config_n2([p1, p2]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        assert!(!b.delete_records_wait_majority());
        assert!(!b.delete_records_allow_irreversible());
        Arc::new(b)
    };
    let mut bgs: Vec<BackgroundTasks> = vec![start_background_tasks(Arc::clone(&b1))];
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(40)).await;

    // "safe" → N=2 leader broker 1.
    b1.create_topic("safe", 1).unwrap();
    assert_is_leader(&b1, "safe");
    fill_local(&b1, "safe", 40);

    let earliest_before = earliest(&b1, "safe");
    let before_upgraded = b1.delete_records_wait_off_upgraded_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "safe".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 2,
        },
    )
    .await;
    match resp {
        Response::DeleteRecords {
            error_code,
            low_watermark,
            ..
        } => {
            assert_eq!(
                error_code,
                ErrorCode::NotEnoughReplicas as u16,
                "cluster force-off + env unset must upgrade to wait-on → 15 (got {error_code})"
            );
            assert_eq!(
                low_watermark, earliest_before,
                "upgraded wait-on fail must not truncate"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(
        earliest(&b1, "safe"),
        earliest_before,
        "local log_start must stay put"
    );
    assert!(
        b1.delete_records_wait_off_upgraded_total() > before_upgraded,
        "upgrade metric must tick"
    );
    assert!(
        b1.delete_records_majority_wait_fail_total() > before_fail,
        "wait-on fail metric must tick after upgrade"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Same cluster + `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1` → old wait-off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_force_wait_off_allow_env_truncates() {
    let _env = EnvRestore::set("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE", "1");
    let base = unique_dir("v29", "allow");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(37_200);
    let cfg = cluster_config_n2([p1, p2]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        assert!(b.delete_records_allow_irreversible());
        Arc::new(b)
    };
    let mut bgs: Vec<BackgroundTasks> = vec![start_background_tasks(Arc::clone(&b1))];
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(40)).await;

    // "allow" → N=2 leader broker 1.
    b1.create_topic("allow", 1).unwrap();
    assert_is_leader(&b1, "allow");
    fill_local(&b1, "allow", 40);

    let earliest_before = earliest(&b1, "allow");
    let before_upgraded = b1.delete_records_wait_off_upgraded_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "allow".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 2,
        },
    )
    .await;
    match resp {
        Response::DeleteRecords {
            error_code,
            low_watermark,
            ..
        } => {
            assert_eq!(
                error_code, 0,
                "allow=1 must keep irreversible wait-off (got {error_code})"
            );
            assert!(
                low_watermark > earliest_before,
                "wait-off local-first must truncate; low={low_watermark} before={earliest_before}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(
        earliest(&b1, "allow") > earliest_before,
        "allow=1 must advance log_start"
    );
    assert_eq!(
        b1.delete_records_wait_off_upgraded_total(),
        before_upgraded,
        "allow=1 must not tick upgrade metric"
    );
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_fail,
        "allow=1 wait-off must not touch wait-fail metric"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Single-node force wait-off, env unset: still truncates (no cluster).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_force_wait_off_env_unset_truncates() {
    let _env = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let base = unique_dir("v29", "solo");
    let _g = Guard(base.clone());

    let b1 = {
        let b = Broker::new(small_seg_storage(base.join("n1")));
        assert!(!b.delete_records_allow_irreversible());
        assert!(!b.effective_delete_records_wait_majority(2));
        Arc::new(b)
    };

    b1.create_topic("solo", 1).unwrap();
    fill_local(&b1, "solo", 40);

    let earliest_before = earliest(&b1, "solo");
    let before_upgraded = b1.delete_records_wait_off_upgraded_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "solo".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 2,
        },
    )
    .await;
    match resp {
        Response::DeleteRecords {
            error_code,
            low_watermark,
            ..
        } => {
            assert_eq!(
                error_code, 0,
                "single-node force-off must still truncate (got {error_code})"
            );
            assert!(
                low_watermark > earliest_before,
                "single-node wait-off must truncate; low={low_watermark} before={earliest_before}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(
        earliest(&b1, "solo") > earliest_before,
        "single-node log_start must advance"
    );
    assert_eq!(
        b1.delete_records_wait_off_upgraded_total(),
        before_upgraded,
        "single-node must not tick upgrade metric"
    );
}
