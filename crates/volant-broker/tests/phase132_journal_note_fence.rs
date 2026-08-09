//! Phase 132: TruncateJournalNote leader-epoch fence on ingress.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::cluster::{boot_triple_inprocess, propagate, unique_dir, DualServed, Guard};
use volant_broker::{inter_broker_rpc, Broker};
use volant_core::{PartitionId, TopicName};
use volant_protocol::{ErrorCode, Request, Response};

fn setup_triple(label: &str) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>, Guard) {
    let base = unique_dir("p132", label);
    let guard = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [1, 2, 3]);
    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t");
    (b1, b2, b3, guard)
}

#[test]
fn stale_epoch_note_rejected() {
    let (_b1, b2, _b3, _g) = setup_triple("stale");
    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 5)
        .unwrap();
    let gen_before = b2.truncate_journal().generation();
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 100, 1);
    assert_eq!(err, ErrorCode::InvalidProducerEpoch as u16);
    assert_eq!(gen, gen_before);
    assert_eq!(b2.truncate_journal().watermark("t", 0), None);
}

#[test]
fn current_epoch_note_accepted() {
    let (_b1, b2, _b3, _g) = setup_triple("current");
    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 5)
        .unwrap();
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 50, 5);
    assert_eq!(err, 0);
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(50));
}

#[test]
fn unknown_topic_note_rejected() {
    let (_b1, b2, _b3, _g) = setup_triple("unknown-tp");
    let gen_before = b2.truncate_journal().generation();
    let (err, gen) = b2.handle_truncate_journal_note("no-such-topic", 0, 10, 0);
    assert_eq!(err, ErrorCode::NotFound as u16);
    assert_eq!(gen, gen_before);
    assert_eq!(b2.truncate_journal().watermark("no-such-topic", 0), None);
}

#[test]
fn unknown_epoch_minus_one_rejected() {
    let (_b1, b2, _b3, _g) = setup_triple("minus-one");
    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 5)
        .unwrap();
    let gen_before = b2.truncate_journal().generation();
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 20, -1);
    assert_eq!(err, ErrorCode::InvalidArg as u16);
    assert_eq!(gen, gen_before);
    assert_eq!(b2.truncate_journal().watermark("t", 0), None);
}

#[test]
fn future_epoch_note_accepted() {
    let (_b1, b2, _b3, _g) = setup_triple("future");
    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 3)
        .unwrap();
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 40, 5);
    assert_eq!(err, 0);
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(40));
}

#[test]
fn non_controller_accepts_fenced_valid_note() {
    let (b1, b2, _b3, _g) = setup_triple("non-ctrl");
    assert!(b1.is_controller());
    assert!(!b2.is_controller());
    b2.set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 3)
        .unwrap();
    let (err, gen) = b2.handle_truncate_journal_note("t", 0, 40, 3);
    assert_eq!(err, 0);
    assert!(gen >= 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(40));
}

/// TCP: stale epoch → 19; negative epoch → 3; watermark unchanged.
#[tokio::test]
async fn tcp_note_epoch_fence() {
    let (dual, _g) = DualServed::boot("p132", "tcp-fence").await;
    dual.b1.create_topic("fence", 1).unwrap();
    for _ in 0..40 {
        let (_, gen, cid, topics) = dual.b1.cluster_state_snapshot();
        let _ = dual.b2.apply_cluster_state(gen, cid, &topics);
        if dual.b2.partition_count_opt("fence").is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }
    dual.b2
        .set_partition_leader_epoch(&TopicName::new("fence"), PartitionId(0), 5)
        .unwrap();
    let gen_before = dual.b2.truncate_journal().generation();
    let peer = dual.addr2();

    let stale = inter_broker_rpc(
        &dual.b1,
        &peer,
        &Request::TruncateJournalNote {
            topic: "fence".into(),
            partition: 0,
            before_offset: 99,
            leader_epoch: 0,
        },
    )
    .await
    .unwrap();
    match stale {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(error_code, ErrorCode::InvalidProducerEpoch as u16);
            assert_eq!(generation, gen_before);
        }
        other => panic!("{other:?}"),
    }

    let neg = inter_broker_rpc(
        &dual.b1,
        &peer,
        &Request::TruncateJournalNote {
            topic: "fence".into(),
            partition: 0,
            before_offset: 20,
            leader_epoch: -1,
        },
    )
    .await
    .unwrap();
    match neg {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(error_code, ErrorCode::InvalidArg as u16);
            assert_eq!(generation, gen_before);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(dual.b2.truncate_journal().watermark("fence", 0), None);
    assert_eq!(dual.b2.truncate_journal().generation(), gen_before);
    dual.shutdown().await;
}
