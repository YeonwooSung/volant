//! v0.45: clustered DeleteRecords wait-off requires a second ACK.
//!
//! Cluster + effective wait-off stays local-first only when both
//! `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE` and
//! `VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK` are on. ALLOW alone (v0.29
//! explicit path) still upgrades to wait-on. Single-node wait-off does
//! not require ACK (no majority exists).

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

/// Default (no env): clustered wait-off still upgrades (v0.29 regression).
#[test]
fn default_clustered_wait_off_still_upgrades() {
    let _allow = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let _ack = EnvRestore::remove("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK");
    let dir = unique_dir("v45", "default");
    let _g = Guard(dir.clone());

    let single = Broker::new(default_storage(dir.join("solo")));
    assert!(!single.delete_records_allow_irreversible());
    assert!(!single.delete_records_irreversible_ack());
    assert!(!single.effective_delete_records_wait_majority(2));
    assert_eq!(single.delete_records_wait_off_upgraded_total(), 0);
    assert_eq!(single.delete_records_wait_off_ack_missing_total(), 0);

    let cfg = cluster_config_n2([19_101, 19_102]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();
    assert!(!clustered.delete_records_allow_irreversible());
    assert!(!clustered.delete_records_irreversible_ack());
    assert!(
        clustered.effective_delete_records_wait_majority(2),
        "cluster flag 2 + both envs unset must upgrade to wait-on"
    );
    assert!(
        clustered.effective_delete_records_wait_majority(0),
        "cluster flag 0 + knob off + both envs unset must upgrade to wait-on"
    );
    assert!(clustered.delete_records_wait_off_upgraded_total() >= 2);
    assert_eq!(
        clustered.delete_records_wait_off_ack_missing_total(),
        0,
        "ack-missing ticks only when ALLOW is on and ACK is off"
    );
}

/// ALLOW=1, ACK unset: clustered wait-off still upgrades (honesty close).
#[test]
fn allow_without_ack_clustered_wait_off_still_upgrades() {
    let _allow = EnvRestore::set("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE", "1");
    let _ack = EnvRestore::remove("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK");
    let dir = unique_dir("v45", "allow_only");
    let _g = Guard(dir.clone());
    let cfg = cluster_config_n2([19_111, 19_112]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();
    assert!(clustered.delete_records_allow_irreversible());
    assert!(!clustered.delete_records_irreversible_ack());
    assert!(
        clustered.effective_delete_records_wait_majority(2),
        "ALLOW=1 without ACK must still upgrade to wait-on"
    );
    assert!(clustered.delete_records_wait_off_upgraded_total() >= 1);
    assert!(
        clustered.delete_records_wait_off_ack_missing_total() >= 1,
        "ALLOW-on ACK-off must tick ack-missing"
    );
}

/// ALLOW=1, ACK=1: clustered wait-off stays wait-off (explicit double gate).
#[test]
fn allow_and_ack_clustered_wait_off_stays_off() {
    let _allow = EnvRestore::set("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE", "1");
    let _ack = EnvRestore::set("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK", "1");
    let dir = unique_dir("v45", "both");
    let _g = Guard(dir.clone());
    let cfg = cluster_config_n2([19_121, 19_122]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();
    assert!(clustered.delete_records_allow_irreversible());
    assert!(clustered.delete_records_irreversible_ack());
    assert!(
        !clustered.effective_delete_records_wait_majority(2),
        "ALLOW+ACK must keep flag 2 as wait-off"
    );
    assert!(
        !clustered.effective_delete_records_wait_majority(0),
        "ALLOW+ACK must keep flag 0 + knob off as wait-off"
    );
    assert_eq!(clustered.delete_records_wait_off_upgraded_total(), 0);
    assert_eq!(clustered.delete_records_wait_off_ack_missing_total(), 0);
}

/// ACK=1, ALLOW unset: clustered wait-off still upgrades (ACK alone is not enough).
#[test]
fn ack_without_allow_clustered_wait_off_still_upgrades() {
    let _allow = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let _ack = EnvRestore::set("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK", "1");
    let dir = unique_dir("v45", "ack_only");
    let _g = Guard(dir.clone());
    let cfg = cluster_config_n2([19_131, 19_132]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();
    assert!(!clustered.delete_records_allow_irreversible());
    assert!(clustered.delete_records_irreversible_ack());
    assert!(
        clustered.effective_delete_records_wait_majority(2),
        "ACK=1 without ALLOW must still upgrade to wait-on"
    );
    assert!(clustered.delete_records_wait_off_upgraded_total() >= 1);
    assert_eq!(
        clustered.delete_records_wait_off_ack_missing_total(),
        0,
        "ACK present: do not tick ack-missing"
    );
}

/// Runtime setters: both required; either one off upgrades.
#[test]
fn setters_require_both_gates() {
    let _allow = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let _ack = EnvRestore::remove("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK");
    let dir = unique_dir("v45", "setters");
    let _g = Guard(dir.clone());
    let cfg = cluster_config_n2([19_141, 19_142]);
    let clustered = Broker::with_cluster(default_storage(dir.join("c")), 1, cfg).unwrap();

    clustered.set_delete_records_allow_irreversible(true);
    assert!(clustered.effective_delete_records_wait_majority(2));
    let missing = clustered.delete_records_wait_off_ack_missing_total();
    assert!(missing >= 1);

    clustered.set_delete_records_irreversible_ack(true);
    assert!(!clustered.effective_delete_records_wait_majority(2));

    clustered.set_delete_records_allow_irreversible(false);
    assert!(clustered.effective_delete_records_wait_majority(2));
    assert_eq!(
        clustered.delete_records_wait_off_ack_missing_total(),
        missing,
        "ACK on + ALLOW off must not tick ack-missing"
    );
}

/// Cluster N=2, one dead, ALLOW=1 ACK unset, force wait-off → no local truncate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_allow_without_ack_no_truncate() {
    let _allow = EnvRestore::set("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE", "1");
    let _ack = EnvRestore::remove("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK");
    let base = unique_dir("v45", "allow_only_it");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(37_400);
    let cfg = cluster_config_n2([p1, p2]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        assert!(b.delete_records_allow_irreversible());
        assert!(!b.delete_records_irreversible_ack());
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

    b1.create_topic("gated", 1).unwrap();
    assert_is_leader(&b1, "gated");
    fill_local(&b1, "gated", 40);

    let earliest_before = earliest(&b1, "gated");
    let before_upgraded = b1.delete_records_wait_off_upgraded_total();
    let before_missing = b1.delete_records_wait_off_ack_missing_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "gated".into(),
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
                "ALLOW without ACK must upgrade to wait-on → 15 (got {error_code})"
            );
            assert_eq!(
                low_watermark, earliest_before,
                "upgraded wait-on fail must not truncate"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(
        earliest(&b1, "gated"),
        earliest_before,
        "local log_start must stay put"
    );
    assert!(
        b1.delete_records_wait_off_upgraded_total() > before_upgraded,
        "upgrade metric must tick"
    );
    assert!(
        b1.delete_records_wait_off_ack_missing_total() > before_missing,
        "ack-missing metric must tick"
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

/// Cluster N=2, one dead, ALLOW=1 ACK=1, force wait-off → local truncate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_allow_and_ack_truncates() {
    let _allow = EnvRestore::set("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE", "1");
    let _ack = EnvRestore::set("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK", "1");
    let base = unique_dir("v45", "both_it");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(37_500);
    let cfg = cluster_config_n2([p1, p2]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        assert!(b.delete_records_allow_irreversible());
        assert!(b.delete_records_irreversible_ack());
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

    b1.create_topic("double", 1).unwrap();
    assert_is_leader(&b1, "double");
    fill_local(&b1, "double", 40);

    let earliest_before = earliest(&b1, "double");
    let before_upgraded = b1.delete_records_wait_off_upgraded_total();
    let before_missing = b1.delete_records_wait_off_ack_missing_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "double".into(),
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
                "ALLOW+ACK must keep irreversible wait-off (got {error_code})"
            );
            assert!(
                low_watermark > earliest_before,
                "wait-off local-first must truncate; low={low_watermark} before={earliest_before}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(
        earliest(&b1, "double") > earliest_before,
        "ALLOW+ACK must advance log_start"
    );
    assert_eq!(
        b1.delete_records_wait_off_upgraded_total(),
        before_upgraded,
        "double gate must not tick upgrade metric"
    );
    assert_eq!(
        b1.delete_records_wait_off_ack_missing_total(),
        before_missing,
        "double gate must not tick ack-missing"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Single-node wait-off does not require ACK (truncate / no upgrade).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_wait_off_does_not_require_ack() {
    let _allow = EnvRestore::remove("VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE");
    let _ack = EnvRestore::remove("VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK");
    let base = unique_dir("v45", "solo");
    let _g = Guard(base.clone());

    let b1 = {
        let b = Broker::new(small_seg_storage(base.join("n1")));
        assert!(!b.delete_records_allow_irreversible());
        assert!(!b.delete_records_irreversible_ack());
        assert!(!b.effective_delete_records_wait_majority(2));
        Arc::new(b)
    };

    b1.create_topic("solo", 1).unwrap();
    fill_local(&b1, "solo", 40);

    let earliest_before = earliest(&b1, "solo");
    let before_upgraded = b1.delete_records_wait_off_upgraded_total();
    let before_missing = b1.delete_records_wait_off_ack_missing_total();

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
                "single-node force-off must still truncate without ACK (got {error_code})"
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
    assert_eq!(
        b1.delete_records_wait_off_ack_missing_total(),
        before_missing,
        "single-node must not tick ack-missing"
    );
}
