//! Phase 135: optional DeleteRecords wait on truncate-journal majority.
//!
//! Default off = best-effort (client success independent of journal majority;
//! local-first truncate).
//! Wait on (Phase 148): journal majority **first**; fail → `NotEnoughReplicas`
//! (15) with **unchanged** log_start (no local truncate).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, default_storage, propagate_async, unique_dir, Guard,
};
use volant_broker::net::dispatch_request;
use volant_broker::{
    fanout_delete_records, serve_listener, start_background_tasks, BackgroundTasks, Broker,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_protocol::{ErrorCode, Request, Response};
use volant_storage::StorageConfig;

fn big(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

/// Small segments so DeleteRecords can drop sealed prefixes.
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
        "node {} must lead {topic}/0 for DeleteRecords tests",
        broker.node_id()
    );
}

/// Default knob is off; runtime setter toggles it.
#[test]
fn wait_majority_default_off_and_setter() {
    let dir = unique_dir("p135", "knob");
    let _g = Guard(dir.clone());
    let broker = Broker::new(default_storage(dir));
    assert!(
        !broker.delete_records_wait_majority(),
        "default must be wait-majority off"
    );
    broker.set_delete_records_wait_majority(true);
    assert!(broker.delete_records_wait_majority());
    broker.set_delete_records_wait_majority(false);
    assert!(!broker.delete_records_wait_majority());
    assert_eq!(broker.delete_records_majority_wait_success_total(), 0);
    assert_eq!(broker.delete_records_majority_wait_fail_total(), 0);
}

/// N=3 configured, only proposer live → journal majority fails.
/// Wait **off** (default): client/native DeleteRecords still error_code=0;
/// wait metrics stay 0; fan-out reports `majority_ok=false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_off_succeeds_without_majority() {
    let base = unique_dir("p135", "wait-off");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    // Peers advertised but never listen → journal acks = local only < majority(3)=2.
    let p2 = p1.saturating_add(100).max(33_100);
    let p3 = p2.saturating_add(1);
    let cfg = cluster_config([p1, p2, p3]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        // Default wait off; assert explicitly.
        assert!(!b.delete_records_wait_majority());
        // v0.29/v0.45: keep this test on the irreversible wait-off path.
        // Production equivalent: ALLOW=1 and ACK=1
        b.set_delete_records_allow_irreversible(true);
        b.set_delete_records_irreversible_ack(true);
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

    // Topic "t" assigns leader = broker 1 (topic_hash placement).
    b1.create_topic("t", 1).unwrap();
    assert_is_leader(&b1, "t");
    fill_local(&b1, "t", 40);

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();
    let before_cons_fail = b1.truncate_journal_consensus_fail_total();

    // Native path: wait off → error 0 even when journal majority fails.
    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "t".into(),
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
                "wait-off must not surface majority failure (got {error_code})"
            );
            assert!(
                low_watermark > 0,
                "local truncate should advance low_watermark={low_watermark}"
            );
        }
        other => panic!("unexpected response: {other:?}"),
    }

    // Wait metrics only tick when wait mode is on.
    assert_eq!(
        b1.delete_records_majority_wait_success_total(),
        before_ok,
        "wait-off must not increment majority-wait success"
    );
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_fail,
        "wait-off must not increment majority-wait fail"
    );
    // Journal consensus should have recorded a fail (or at least not a new wait metric).
    // Solo proposer: acks=1 < need=2.
    assert!(
        b1.truncate_journal_consensus_fail_total() > before_cons_fail
            || b1.truncate_journal().watermark("t", 0).is_some(),
        "expected journal note attempt with solo proposer (fail and/or local watermark)"
    );

    // Direct fan-out API: majority_ok false, still no wait metrics.
    let fan = fanout_delete_records(
        &b1,
        "t",
        0,
        b1.truncate_journal().watermark("t", 0).unwrap_or(1),
    )
    .await;
    assert!(
        !fan.majority_ok,
        "N=3 with only proposer live must report majority_ok=false"
    );
    assert_eq!(b1.delete_records_majority_wait_success_total(), before_ok);
    assert_eq!(b1.delete_records_majority_wait_fail_total(), before_fail);

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// 3 live nodes + wait on → majority ok, client error 0, success metric++.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_on_majority_ok() {
    let base = unique_dir("p135", "wait-ok");
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
        assert!(b.delete_records_wait_majority());
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

    // Fill on leader; acks=1 is enough to get local sealed segments (majority
    // wait is about journal note acks, not produce ISR).
    fill_local(&b1, "maj", 40);

    // Give followers a brief window for replica fetch (best-effort).
    let latest = b1
        .list_offsets("maj", &[0])
        .unwrap()
        .first()
        .map(|e| e.2)
        .unwrap_or(0);
    for b in [&b2, &b3] {
        for _ in 0..40 {
            if let Ok(e) = b.list_offsets("maj", &[0]) {
                if e.first().map(|x| x.2).unwrap_or(0) >= latest {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();
    let before_cons_ok = b1.truncate_journal_consensus_success_total();

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
    let low = match resp {
        Response::DeleteRecords {
            error_code,
            low_watermark,
            ..
        } => {
            assert_eq!(
                error_code, 0,
                "wait-on + 3/3 live must succeed (got error_code={error_code})"
            );
            assert!(low_watermark > 0, "low_watermark={low_watermark}");
            low_watermark
        }
        other => panic!("unexpected: {other:?}"),
    };

    assert!(
        b1.delete_records_majority_wait_success_total() > before_ok,
        "wait-on majority success must increment metric"
    );
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_fail,
        "success path must not increment fail metric"
    );
    assert!(
        b1.truncate_journal_consensus_success_total() > before_cons_ok
            || b1.truncate_journal().watermark("maj", 0).is_some(),
        "journal majority should succeed with 3 live nodes"
    );
    // Phase 148: journal notes requested/clamped-estimate offset first; local
    // whole-segment low may be ≤ journal watermark (max-merge honest).
    let jwm = b1.truncate_journal().watermark("maj", 0);
    assert!(
        jwm.is_some() && jwm.unwrap() >= low,
        "journal watermark {jwm:?} must be ≥ achieved low {low}"
    );

    // Fan-out result API also reports majority_ok.
    let fan = fanout_delete_records(&b1, "maj", 0, low).await;
    assert!(
        fan.majority_ok,
        "fanout_delete_records with 3 live peers should report majority_ok"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Wait on + only proposer live (acks < majority of 3) → NotEnoughReplicas (15),
/// fail metric++, **log_start unchanged** (Phase 148: no local truncate on fail).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_on_majority_fail() {
    let base = unique_dir("p135", "wait-fail");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(100).max(34_100);
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

    // "p135a" → leader broker 1.
    b1.create_topic("p135a", 1).unwrap();
    assert_is_leader(&b1, "p135a");
    fill_local(&b1, "p135a", 40);

    let entries = b1.list_offsets("p135a", &[0]).unwrap();
    let earliest_before = entries.first().map(|e| e.1).unwrap_or(0);

    let before_ok = b1.delete_records_majority_wait_success_total();
    let before_fail = b1.delete_records_majority_wait_fail_total();
    let before_first_fail = b1.delete_records_majority_first_fail_total();

    let resp = dispatch_request(
        &b1,
        Request::DeleteRecords {
            topic: "p135a".into(),
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
                "wait-on + solo proposer must return NotEnoughReplicas (15), got {error_code}"
            );
            assert_eq!(
                low_watermark, earliest_before,
                "Phase 148: wait-on majority fail must not truncate; \
                 low_watermark={low_watermark} earliest_before={earliest_before}"
            );
            let earliest_after = b1
                .list_offsets("p135a", &[0])
                .unwrap()
                .first()
                .map(|e| e.1)
                .unwrap_or(0);
            assert_eq!(
                earliest_after, earliest_before,
                "local log_start must be unchanged after wait-on majority fail"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    assert!(
        b1.delete_records_majority_wait_fail_total() > before_fail,
        "wait-on majority fail must increment fail metric"
    );
    assert!(
        b1.delete_records_majority_first_fail_total() > before_first_fail,
        "Phase 148 majority-first fail metric must increment"
    );
    assert_eq!(
        b1.delete_records_majority_wait_success_total(),
        before_ok,
        "fail path must not increment success metric"
    );
    // Phase 148: provisional journal note is rolled back on majority fail so
    // outbox reconcile will not auto-truncate.
    assert!(
        b1.truncate_journal().watermark("p135a", 0).is_none(),
        "provisional journal watermark must be rolled back after wait-on majority fail"
    );

    s1.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// In-process path (no dispatch): local delete + fanout with wait on does not
/// itself change the local delete_records error_code (handler maps majority);
/// documents fanout result + metrics only fire from the request handler when wait on.
///
/// This test exercises the fanout majority_ok contract and confirms wait metrics
/// remain untouched when only fanout is called (metrics are wait-mode handler counters).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fanout_majority_ok_false_when_peers_down() {
    let base = unique_dir("p135", "fanout-only");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let p2 = p1.saturating_add(50).max(35_100);
    let p3 = p2.saturating_add(1);
    let cfg = cluster_config([p1, p2, p3]);

    let b1 = {
        let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        // Wait on for completeness; fanout itself does not read the knob for metrics.
        b.set_delete_records_wait_majority(true);
        Arc::new(b)
    };
    let _bg = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    // "x" → leader 1.
    b1.create_topic("x", 1).unwrap();
    assert_is_leader(&b1, "x");
    fill_local(&b1, "x", 30);

    let (low, err) = b1.delete_records("x", 0, 10).unwrap();
    assert_eq!(err, 0, "local delete_records ignores majority wait");
    assert!(low > 0);

    let before_wait_fail = b1.delete_records_majority_wait_fail_total();
    let fan = fanout_delete_records(&b1, "x", 0, low).await;
    assert!(!fan.majority_ok);
    // Metrics are only incremented on the client request path when wait is on —
    // bare fanout must not invent wait-success/fail counters.
    assert_eq!(
        b1.delete_records_majority_wait_fail_total(),
        before_wait_fail,
        "bare fanout_delete_records must not increment wait-fail metric"
    );

    s1.abort();
}

/// Single-node (no cluster): fanout majority_ok = true; wait on succeeds with error 0.
#[tokio::test]
async fn single_node_majority_ok_true() {
    let dir = unique_dir("p135", "single");
    let _g = Guard(dir.clone());
    let broker = Arc::new({
        let b = Broker::new(small_seg_storage(dir));
        b.set_delete_records_wait_majority(true);
        b
    });
    broker.create_topic("s", 1).unwrap();
    fill_local(&broker, "s", 30);

    let before_ok = broker.delete_records_majority_wait_success_total();
    let resp = dispatch_request(
        &broker,
        Request::DeleteRecords {
            topic: "s".into(),
            partition: 0,
            before_offset: 10,
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
            assert_eq!(error_code, 0);
            assert!(low_watermark > 0);
        }
        other => panic!("{other:?}"),
    }
    // Single-node / no cluster → majority_ok true (spec).
    let fan = fanout_delete_records(&broker, "s", 0, 1).await;
    assert!(
        fan.majority_ok,
        "single-node / no cluster must report majority_ok=true"
    );
    // Wait-on + majority ok → success metric (handler path).
    assert!(
        broker.delete_records_majority_wait_success_total() > before_ok,
        "wait-on single-node should count majority-wait success"
    );
}
