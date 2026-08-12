//! Phase 137: native DeleteRecords per-request `wait_majority` trailer.
//!
//! * `0` — broker default (`VOLANT_DELETE_RECORDS_WAIT_MAJORITY` / AtomicBool)
//! * `1` — force wait on
//! * `2` — force wait off
//!
//! Kafka path remains broker-knob only (no wire flag).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, default_storage, propagate_async, unique_dir, Guard,
};
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

/// Helper: effective wait resolution unit tests (no network).
#[test]
fn effective_wait_flag_resolution() {
    let dir = unique_dir("p137", "eff");
    let _g = Guard(dir.clone());
    let broker = Broker::new(default_storage(dir));

    assert!(!broker.delete_records_wait_majority());
    assert!(!broker.effective_delete_records_wait_majority(0));
    assert!(broker.effective_delete_records_wait_majority(1));
    assert!(!broker.effective_delete_records_wait_majority(2));
    assert!(!broker.effective_delete_records_wait_majority(99));

    broker.set_delete_records_wait_majority(true);
    assert!(broker.effective_delete_records_wait_majority(0));
    assert!(broker.effective_delete_records_wait_majority(1));
    assert!(!broker.effective_delete_records_wait_majority(2));
}

/// Flag 1 forces wait when broker default is off; solo N=3 → NotEnoughReplicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_1_forces_wait_when_env_off() {
    let base = unique_dir("p137", "flag1");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(35_100);
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

    // Topic hash → leader broker 1 (see topic_hash placement).
    b1.create_topic("t137a", 1).unwrap();
    assert_is_leader(&b1, "t137a");
    fill_local(&b1, "t137a", 40);

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "t137a".into(),
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
            assert_eq!(
                error_code,
                ErrorCode::NotEnoughReplicas as u16,
                "flag 1 + solo majority fail must surface 15 (got {error_code})"
            );
            assert!(
                low_watermark > 0,
                "local truncate still advances low_watermark={low_watermark}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    assert_eq!(
        b1.delete_records_majority_wait_success_total(),
        before_ok,
        "fail path must not increment success"
    );
    assert!(
        b1.delete_records_majority_wait_fail_total() > before_fail,
        "flag 1 wait fail must increment fail metric"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Flag 0 with broker default off → success even without majority.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_0_uses_broker_default_off() {
    let base = unique_dir("p137", "flag0-off");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(35_200);
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

    // "t0" places partition 0 leader on broker 1.
    b1.create_topic("t0", 1).unwrap();
    assert_is_leader(&b1, "t0");
    fill_local(&b1, "t0", 40);

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "t0".into(),
            partition: 0,
            before_offset: 15,
            wait_majority: 0,
        },
    )
    .await;
    match resp {
        Response::DeleteRecords { error_code, .. } => {
            assert_eq!(
                error_code, 0,
                "flag 0 + default off must not surface majority fail"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(b1.delete_records_majority_wait_success_total(), before_ok);
    assert_eq!(b1.delete_records_majority_wait_fail_total(), before_fail);

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Flag 2 forces no-wait when broker default is on; solo N=3 → error 0, no wait metrics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_2_forces_no_wait_when_env_on() {
    let base = unique_dir("p137", "flag2");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(35_300);
    let p3 = p2.saturating_add(1);
    let cfg = cluster_config([p1, p2, p3]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_delete_records_wait_majority(true);
        assert!(b.delete_records_wait_majority());
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

    // "p0" places partition 0 leader on broker 1.
    b1.create_topic("p0", 1).unwrap();
    assert_is_leader(&b1, "p0");
    fill_local(&b1, "p0", 40);

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "p0".into(),
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
                "flag 2 must force no-wait even when env on (got {error_code})"
            );
            assert!(low_watermark > 0);
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(
        b1.delete_records_majority_wait_success_total(),
        before_ok,
        "flag 2 must not touch wait success metric"
    );
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_fail,
        "flag 2 must not touch wait fail metric"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Flag 1 + 3 live brokers → majority ok, error 0, success metric++.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_1_majority_ok_three_live() {
    let base = unique_dir("p137", "flag1-ok");
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
        // Broker default off; request flag 1 forces wait.
        assert!(!b.delete_records_wait_majority());
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

    // "maj" places partition 0 leader on broker 1 (same as phase135).
    b1.create_topic("maj", 1).unwrap();
    propagate_async(&[&b1, &b2, &b3], "maj").await;
    assert_is_leader(&b1, "maj");
    fill_local(&b1, "maj", 40);

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "maj".into(),
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
            assert_eq!(
                error_code, 0,
                "flag 1 + 3 live must succeed (got {error_code})"
            );
            assert!(low_watermark > 0);
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(
        b1.delete_records_majority_wait_success_total() > before_ok,
        "flag 1 majority success must increment metric"
    );
    assert_eq!(b1.delete_records_majority_wait_fail_total(), before_fail);

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}
