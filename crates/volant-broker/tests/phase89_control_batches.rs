//! Phase 89: Kafka control batches on the data log (COMMIT/ABORT dual-write).

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

fn assert_has_control_batch(set: &[u8], want: ControlMarkerType) {
    assert!(!set.is_empty(), "expected control batch in record set");
    let mut off = 0usize;
    let mut found = false;
    while off + 23 <= set.len() {
        let Some((attrs, _base)) = peek_record_batch_attributes(&set[off..]) else {
            break;
        };
        if attrs & RECORD_BATCH_ATTR_CONTROL != 0 {
            assert_eq!(attrs & RECORD_BATCH_ATTR_TXN_CONTROL, RECORD_BATCH_ATTR_TXN_CONTROL);
            assert_eq!(attrs & RECORD_BATCH_ATTR_TRANSACTIONAL, RECORD_BATCH_ATTR_TRANSACTIONAL);
            // batch_length at offset 8
            let batch_len = i32::from_be_bytes(set[off + 8..off + 12].try_into().unwrap()) as usize;
            // control key type sits in records after fixed header; scan for type bytes
            let end = (off + 12 + batch_len).min(set.len());
            let slice = &set[off..end];
            // type is i16 after version in control key — look for pattern 00 00 00 {0|1}
            let ty = want as i16;
            let pattern = [0u8, 0, 0, ty as u8];
            assert!(
                slice.windows(4).any(|w| w == pattern),
                "control key type {ty} not found in batch"
            );
            found = true;
            break;
        }
        let batch_len = i32::from_be_bytes(set[off + 8..off + 12].try_into().unwrap()) as usize;
        off += 12 + batch_len;
    }
    assert!(found, "no control RecordBatch (attrs & CONTROL) in set");
}

#[tokio::test]
async fn abort_writes_abort_control_batch() {
    let dir = temp_dir("p89", "abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-a").await;
    add_partitions(&addr, 2, "txn-a", pid, epoch, "t").await;
    let batch = encode_record_batch_idempotent(&sample(b"x"), pid, epoch, 0);
    let _ = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("t", &batch)),
    )
    .await;
    end_txn(&addr, 4, "txn-a", pid, epoch, false).await;

    // Soft aborted list still present under READ_COMMITTED.
    let rec_u = fetch_v4_records(&addr, 10, "t", 0).await;
    assert_has_control_batch(&rec_u, ControlMarkerType::Abort);

    let rec_c = fetch_v4_records(&addr, 11, "t", 1).await;
    assert_has_control_batch(&rec_c, ControlMarkerType::Abort);

    // Native hides control + aborted data.
    let native = broker
        .fetch(&TopicName::new("t"), PartitionId(0), Offset::ZERO, 20)
        .unwrap();
    assert!(native.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn commit_writes_commit_control_batch() {
    let dir = temp_dir("p89", "commit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-c").await;
    add_partitions(&addr, 2, "txn-c", pid, epoch, "t").await;
    let batch = encode_record_batch_idempotent(&sample(b"keep"), pid, epoch, 0);
    let _ = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("t", &batch)),
    )
    .await;
    end_txn(&addr, 4, "txn-c", pid, epoch, true).await;

    let rec = fetch_v4_records(&addr, 10, "t", 1).await;
    assert_has_control_batch(&rec, ControlMarkerType::Commit);
    // Data also present (non-control batch first).
    let (attrs, _) = peek_record_batch_attributes(&rec).unwrap();
    assert_eq!(attrs & RECORD_BATCH_ATTR_CONTROL, 0, "first batch should be data");

    let native = broker
        .fetch(&TopicName::new("t"), PartitionId(0), Offset::ZERO, 20)
        .unwrap();
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].value.as_ref(), b"keep");
    assert!(!is_txn_control_record(&native[0]));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn unit_control_message_roundtrip_headers() {
    use volant_broker::kafka::codec::txn_control_message;
    let msg = txn_control_message(ControlMarkerType::Commit, 42, 3);
    let rec = Record {
        offset: Offset::new(7),
        key: msg.key.clone(),
        value: msg.value.clone(),
        timestamp_ms: 99,
        headers: msg.headers.clone(),
    };
    let parsed = parse_txn_control_record(&rec).expect("control");
    assert_eq!(parsed.marker_type, ControlMarkerType::Commit);
    assert_eq!(parsed.producer_id, 42);
    assert_eq!(parsed.producer_epoch, 3);
}

#[test]
fn unit_end_txn_appends_control_on_log() {
    let dir = temp_dir("p89", "unit");
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    broker.create_topic("t", 1).unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("u");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let _ = broker.buffer_txn_produce(pid, epoch, "t", 0, 0, vec![Message::from_value("a")]);
    let (code, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(code, 0);

    // Raw log via uncommitted-style: use high_watermark path — fetch_kafka RU.
    let recs = broker
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    assert!(
        recs.iter().any(is_txn_control_record),
        "expected ABORT control on log, got {} records",
        recs.len()
    );
    let ctrl = recs.iter().find(|r| is_txn_control_record(r)).unwrap();
    let p = parse_txn_control_record(ctrl).unwrap();
    assert_eq!(p.marker_type, ControlMarkerType::Abort);
    assert_eq!(p.producer_id, pid as i64);

    let _ = std::fs::remove_dir_all(&dir);
}
