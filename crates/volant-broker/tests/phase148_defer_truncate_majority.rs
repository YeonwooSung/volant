//! Phase 148: defer local DeleteRecords truncate until journal majority (wait mode).
//!
//! Behavior matrix:
//! | effective wait | majority | local truncate | client error |
//! |----------------|----------|----------------|--------------|
//! | ON             | ok       | yes (after)    | 0            |
//! | ON             | fail     | **no**         | 15           |
//! | OFF (default)  | fail     | yes (first)    | 0            |

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, propagate_async, unique_dir, Guard};
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

/// Wait on + N=3 all live → DeleteRecords succeeds; log truncated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_on_three_live_majority_ok_truncates() {
    let base = unique_dir("p148", "wait-ok");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

    let mk = |id: u32| {
        let b = Broker::with_cluster(
            small_seg_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        b.set_delete_records_wait_majority(true);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    let mut bgs: Vec<BackgroundTasks> = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];
    for (listener, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    // "maj" → leader broker 1.
    b1.create_topic("maj", 1).unwrap();
    propagate_async(&[&b1, &b2, &b3], "maj").await;
    assert_is_leader(&b1, "maj");
    fill_local(&b1, "maj", 40);

    let earliest_before = earliest(&b1, "maj");
    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_first_ok = b1.delete_records_majority_first_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "maj".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 0,
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
                "wait-on + 3/3 live must succeed (got {error_code})"
            );
            assert!(
                low_watermark > earliest_before,
                "log must truncate: low={low_watermark} before={earliest_before}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    assert!(
        earliest(&b1, "maj") > earliest_before,
        "local log_start must advance after majority-first success"
    );
    assert!(
        b1.delete_records_majority_wait_success_total() > before_ok,
        "wait success metric"
    );
    assert!(
        b1.delete_records_majority_first_success_total() > before_first_ok,
        "majority-first success metric"
    );
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_fail,
        "success must not tick fail"
    );
    assert!(
        b1.truncate_journal().watermark("maj", 0).is_some(),
        "journal watermark after majority success"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Wait on + N=3 only proposer live → NotEnoughReplicas; log_start unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_on_majority_impossible_no_local_truncate() {
    let base = unique_dir("p148", "wait-fail");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(36_100);
    let p3 = p2.saturating_add(1);
    let cfg = cluster_config([p1, p2, p3]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_delete_records_wait_majority(true);
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

    // "a148" → leader broker 1 (topic_hash placement).
    b1.create_topic("a148", 1).unwrap();
    assert_is_leader(&b1, "a148");
    fill_local(&b1, "a148", 40);

    let earliest_before = earliest(&b1, "a148");
    let before_fail = b1.delete_records_majority_wait_fail_total();
    let before_first_fail = b1.delete_records_majority_first_fail_total();
    let before_ok = b1.delete_records_majority_wait_success_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "a148".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 0,
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
                "majority impossible must return 15 (got {error_code})"
            );
            assert_eq!(
                low_watermark, earliest_before,
                "response low must equal pre-request log_start"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    assert_eq!(
        earliest(&b1, "a148"),
        earliest_before,
        "prove no local truncate on wait-on majority fail"
    );
    // Stay unchanged across a reconcile tick so provisional note was rolled back.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        earliest(&b1, "a148"),
        earliest_before,
        "log_start must remain unchanged after reconcile tick (no provisional journal apply)"
    );
    assert!(
        b1.delete_records_majority_wait_fail_total() > before_fail,
        "wait fail metric"
    );
    assert!(
        b1.delete_records_majority_first_fail_total() > before_first_fail,
        "majority-first fail metric"
    );
    assert_eq!(
        b1.delete_records_majority_wait_success_total(),
        before_ok,
        "fail must not tick success"
    );
    assert!(
        b1.truncate_journal().watermark("a148", 0).is_none(),
        "provisional journal note rolled back"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Wait off + majority would fail → local truncate still succeeds (legacy).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_off_majority_fail_still_truncates() {
    let base = unique_dir("p148", "wait-off");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(36_200);
    let p3 = p2.saturating_add(1);
    let cfg = cluster_config([p1, p2, p3]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        assert!(!b.delete_records_wait_majority());
        // v0.29: keep this test on the irreversible wait-off path.
        // Production equivalent: VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1
        b.set_delete_records_allow_irreversible(true);
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

    // "d148" → leader broker 1.
    b1.create_topic("d148", 1).unwrap();
    assert_is_leader(&b1, "d148");
    fill_local(&b1, "d148", 40);

    let earliest_before = earliest(&b1, "d148");
    let before_wait_fail = b1.delete_records_majority_wait_fail_total();
    let before_first_fail = b1.delete_records_majority_first_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "d148".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 0,
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
                "wait-off must not surface majority fail (got {error_code})"
            );
            assert!(
                low_watermark > earliest_before,
                "wait-off local-first must truncate; low={low_watermark} before={earliest_before}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    assert!(
        earliest(&b1, "d148") > earliest_before,
        "wait-off must advance log_start even when majority impossible"
    );
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_wait_fail,
        "wait-off must not touch wait-fail metric"
    );
    assert_eq!(
        b1.delete_records_majority_first_fail_total(),
        before_first_fail,
        "wait-off must not touch majority-first fail metric"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Force-wait trailer (flag 1) with env off: same majority-first no-truncate on fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_wait_trailer_majority_fail_no_truncate() {
    let base = unique_dir("p148", "flag1");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(36_300);
    let p3 = p2.saturating_add(1);
    let cfg = cluster_config([p1, p2, p3]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        assert!(!b.delete_records_wait_majority());
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

    // "e148" → leader broker 1.
    b1.create_topic("e148", 1).unwrap();
    assert_is_leader(&b1, "e148");
    fill_local(&b1, "e148", 40);
    let earliest_before = earliest(&b1, "e148");

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "e148".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 1,
        },
    )
    .await;
    match resp {
        Response::DeleteRecords {
            error_code,
            low_watermark,
            ..
        } => {
            assert_eq!(error_code, ErrorCode::NotEnoughReplicas as u16);
            assert_eq!(low_watermark, earliest_before);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(earliest(&b1, "e148"), earliest_before);

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}
