//! Phase 105: control batches for empty AddPartitions (no write-through data).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_bytes, get_string, is_txn_control_record,
    parse_txn_control_record, peek_record_batch_attributes, put_bytes, put_nullable_string,
    put_string, ControlMarkerType, RECORD_BATCH_ATTR_CONTROL, RECORD_BATCH_ATTR_TRANSACTIONAL,
    RECORD_BATCH_ATTR_TXN_CONTROL,
};
use volant_broker::Broker;
use volant_core::{Message, Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

async fn init_txn_async(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    let resp = rpc(addr, encode_request(22, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i16(), 0);
    (src.get_i64(), src.get_i16())
}

async fn add_partitions(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, topic: &str) {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    let resp = rpc(addr, encode_request(24, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
}

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

fn sample(val: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(val),
        timestamp_ms: 1,
        headers: vec![],
    }]
}

async fn end_txn(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, commit: bool) {
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, txn_id);
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(if commit { 1 } else { 0 });
    let eresp = rpc(addr, encode_request(26, 0, corr, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    assert_eq!(es.get_i16(), 0);
}

async fn fetch_v4_records(addr: &str, corr: i32, topic: &str, isolation: u8) -> Bytes {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(100);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(isolation);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_048_576);
    let resp = rpc(addr, encode_request(1, 4, corr, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4); // corr + throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    let aborted_n = src.get_i32();
    for _ in 0..aborted_n {
        let _ = src.get_i64();
        let _ = src.get_i64();
    }
    get_bytes(&mut src).unwrap().unwrap_or_default()
}

fn count_control_batches(set: &[u8], want: ControlMarkerType) -> usize {
    let mut off = 0usize;
    let mut n = 0usize;
    while off + 23 <= set.len() {
        let Some((attrs, _base)) = peek_record_batch_attributes(&set[off..]) else {
            break;
        };
        let batch_len = i32::from_be_bytes(set[off + 8..off + 12].try_into().unwrap()) as usize;
        let end = (off + 12 + batch_len).min(set.len());
        if attrs & RECORD_BATCH_ATTR_CONTROL != 0 {
            assert_eq!(attrs & RECORD_BATCH_ATTR_TXN_CONTROL, RECORD_BATCH_ATTR_TXN_CONTROL);
            assert_eq!(attrs & RECORD_BATCH_ATTR_TRANSACTIONAL, RECORD_BATCH_ATTR_TRANSACTIONAL);
            let slice = &set[off..end];
            let ty = want as i16;
            let pattern = [0u8, 0, 0, ty as u8];
            if slice.windows(4).any(|w| w == pattern) {
                n += 1;
            }
        }
        off += 12 + batch_len;
    }
    n
}

fn assert_has_control_batch(set: &[u8], want: ControlMarkerType) {
    assert!(
        count_control_batches(set, want) >= 1,
        "expected at least one {:?} control batch",
        want
    );
}

/// AddPartitions only → EndTxn abort → ABORT control on the empty partition.
#[tokio::test]
async fn add_only_end_txn_abort_control() {
    let dir = temp_dir("p105", "abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-a").await;
    add_partitions(&addr, 2, "txn-a", pid, epoch, "t").await;
    // No produce.
    end_txn(&addr, 3, "txn-a", pid, epoch, false).await;

    let rec = fetch_v4_records(&addr, 10, "t", 0).await;
    assert_has_control_batch(&rec, ControlMarkerType::Abort);
    assert_eq!(
        count_control_batches(&rec, ControlMarkerType::Abort),
        1,
        "exactly one ABORT control for empty AddPartitions abort"
    );

    // Soft aborted list should be empty (no write-through ranges).
    let aborted = broker.aborted_transactions_for_fetch("t", 0, 0, 100);
    assert!(
        aborted.is_empty(),
        "empty AddPartitions must not invent soft abort ranges: {aborted:?}"
    );

    // Unit log view: one control marker, no app data.
    let recs = broker
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    assert_eq!(recs.iter().filter(|r| is_txn_control_record(r)).count(), 1);
    assert!(recs.iter().filter(|r| !is_txn_control_record(r)).count() == 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// AddPartitions only → EndTxn commit → COMMIT control on the empty partition.
#[tokio::test]
async fn add_only_end_txn_commit_control() {
    let dir = temp_dir("p105", "commit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-c").await;
    add_partitions(&addr, 2, "txn-c", pid, epoch, "t").await;
    end_txn(&addr, 3, "txn-c", pid, epoch, true).await;

    let rec = fetch_v4_records(&addr, 10, "t", 1).await;
    assert_has_control_batch(&rec, ControlMarkerType::Commit);
    assert_eq!(count_control_batches(&rec, ControlMarkerType::Commit), 1);

    let recs = broker
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    let ctrl = recs
        .iter()
        .find(|r| is_txn_control_record(r))
        .expect("COMMIT control present");
    let p = parse_txn_control_record(ctrl).unwrap();
    assert_eq!(p.marker_type, ControlMarkerType::Commit);
    assert_eq!(p.producer_id, pid);
    assert_eq!(p.producer_epoch, epoch);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// AddPartitions only → crash reload → ABORT control (Phase 98 path extended).
#[test]
fn unit_add_only_crash_reload_abort_control() {
    let dir = temp_dir("p105", "crash");
    let topic = TopicName::new("t");
    let (pid, epoch) = {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("t", 1).unwrap();
        let (pid, epoch) = broker.init_producer_id_with_txn("txn-crash");
        assert_eq!(broker.begin_txn(pid, epoch), 0);
        assert_eq!(
            broker.record_txn_added_partitions(pid, &[("t".into(), 0)]),
            0
        );
        // No buffer_txn_produce — membership only, persisted as open_added.
        (pid, epoch)
    };

    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let recs = broker
        .fetch_kafka(&topic, PartitionId(0), Offset::ZERO, 20, false)
        .unwrap();
    let controls: Vec<_> = recs.iter().filter(|r| is_txn_control_record(r)).collect();
    assert_eq!(
        controls.len(),
        1,
        "expected ABORT control for empty AddPartitions crash promote, got {} records",
        recs.len()
    );
    let p = parse_txn_control_record(controls[0]).unwrap();
    assert_eq!(p.marker_type, ControlMarkerType::Abort);
    assert_eq!(p.producer_id, pid as i64);
    assert_eq!(p.producer_epoch, epoch as i16);

    // No soft aborted ranges invented for empty membership.
    assert!(
        broker
            .aborted_transactions_for_fetch("t", 0, 0, 100)
            .is_empty()
    );

    // Second reload must not re-append.
    drop(broker);
    let broker2 = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let recs2 = broker2
        .fetch_kafka(&topic, PartitionId(0), Offset::ZERO, 20, false)
        .unwrap();
    let n = recs2.iter().filter(|r| is_txn_control_record(r)).count();
    assert_eq!(n, 1, "second reload must not re-append control");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Wire: AddPartitions only → drop broker → reload → Fetch sees ABORT control.
#[tokio::test]
async fn wire_add_only_crash_reload_abort_control() {
    let dir = temp_dir("p105", "wire-crash");
    {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        }));
        broker.create_topic("t", 1).unwrap();
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
        let (pid, epoch) = init_txn_async(&addr, 1, "txn-w").await;
        add_partitions(&addr, 2, "txn-w", pid, epoch, "t").await;
        // No produce, no EndTxn — crash.
        server.abort();
    }

    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let rec = fetch_v4_records(&addr, 10, "t", 0).await;
    assert_has_control_batch(&rec, ControlMarkerType::Abort);
    assert_eq!(count_control_batches(&rec, ControlMarkerType::Abort), 1);

    // Soft aborted empty.
    assert!(
        broker
            .aborted_transactions_for_fetch("t", 0, 0, 100)
            .is_empty()
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// AddPartitions + produce → still one control + soft marker (regression).
#[tokio::test]
async fn add_and_produce_still_one_control() {
    let dir = temp_dir("p105", "produce-reg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-p").await;
    add_partitions(&addr, 2, "txn-p", pid, epoch, "t").await;
    let batch = encode_record_batch_idempotent(&sample(b"data"), pid, epoch, 0);
    let _ = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("t", &batch)),
    )
    .await;
    end_txn(&addr, 4, "txn-p", pid, epoch, false).await;

    let rec = fetch_v4_records(&addr, 10, "t", 0).await;
    assert_eq!(
        count_control_batches(&rec, ControlMarkerType::Abort),
        1,
        "written + added must not double-append control"
    );

    // Soft abort present for the written range.
    let aborted = broker.aborted_transactions_for_fetch("t", 0, 0, 100);
    assert!(
        !aborted.is_empty(),
        "written data abort must still record soft marker"
    );

    // READ_COMMITTED hides data, shows control.
    let rec_c = fetch_v4_records(&addr, 11, "t", 1).await;
    assert_has_control_batch(&rec_c, ControlMarkerType::Abort);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Unit regression: begin + produce (no explicit AddPartitions) still one control.
#[test]
fn unit_produce_without_explicit_add_still_control() {
    let dir = temp_dir("p105", "produce-only");
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    broker.create_topic("t", 1).unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-po");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let _ = broker.buffer_txn_produce(pid, epoch, "t", 0, 0, vec![Message::from_value("x")]);
    let (code, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(code, 0);

    let recs = broker
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    assert_eq!(recs.iter().filter(|r| is_txn_control_record(r)).count(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
