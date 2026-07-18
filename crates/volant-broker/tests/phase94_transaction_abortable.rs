//! Phase 94: TRANSACTION_ABORTABLE (123) honest subset after timeout auto-abort.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_string,
    put_bytes, put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_string,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

const TRANSACTION_ABORTABLE: i16 = 123;
const INVALID_TXN_STATE: i16 = 48;

fn init_v6(
    txn_id: &str,
    timeout_ms: i32,
    enable_2pc: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(timeout_ms);
    body.put_i64(-1);
    body.put_i16(-1);
    body.put_u8(if enable_2pc { 1 } else { 0 });
    body.put_u8(0); // keep_prepared
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_v6_rpc(
    addr: &str,
    corr: i32,
    txn_id: &str,
    timeout_ms: i32,
    enable_2pc: bool,
) -> (i16, i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(
            22,
            6,
            corr,
            Some("p"),
            &init_v6(txn_id, timeout_ms, enable_2pc),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    let pid = src.get_i64();
    let epoch = src.get_i16();
    let _ongoing_pid = src.get_i64();
    let _ongoing_epoch = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    (err, pid, epoch)
}

async fn add_partitions(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, topic: &str) -> i16 {
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
    src.get_i16()
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

async fn produce_txn_err(
    addr: &str,
    corr: i32,
    topic: &str,
    pid: i64,
    epoch: i16,
    seq: i32,
    val: &'static [u8],
) -> i16 {
    let batch = encode_record_batch_idempotent(&sample(val), pid, epoch, seq);
    let resp = rpc(
        addr,
        encode_request(0, 0, corr, Some("p"), &produce_body(topic, &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    src.get_i16()
}

async fn end_txn(
    addr: &str,
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    commit: bool,
) -> i16 {
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, txn_id);
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(if commit { 1 } else { 0 });
    let eresp = rpc(addr, encode_request(26, 0, corr, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    es.get_i16()
}

async fn add_offsets_v4(
    addr: &str,
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    group: &str,
) -> i16 {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_compact_string(&mut body, group);
    put_empty_tag_buffer(&mut body);
    let resp = rpc(
        addr,
        encode_request_flexible(25, 4, corr, Some("p"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    err
}

/// Open a classic write-through txn with one produce.
async fn open_write_through(
    broker: &Broker,
    addr: &str,
    txn_id: &str,
    topic: &str,
    timeout_ms: i32,
    val: &'static [u8],
) -> (i64, i16) {
    let (err, pid, epoch) = init_v6_rpc(addr, 1, txn_id, timeout_ms, false).await;
    assert_eq!(err, 0);
    assert_eq!(add_partitions(addr, 2, txn_id, pid, epoch, topic).await, 0);
    assert_eq!(
        produce_txn_err(addr, 3, topic, pid, epoch, 0, val).await,
        0
    );
    assert_eq!(
        broker.describe_transaction(txn_id).unwrap().0,
        "Ongoing"
    );
    (pid, epoch)
}

#[tokio::test]
async fn phase94_produce_after_open_timeout_is_abortable() {
    let dir = temp_dir("p94", "produce-abortable");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-prod", "events", 100, b"stale").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert!(broker.is_txn_abortable(pid as u64));

    let err = produce_txn_err(&addr, 10, "events", pid, epoch, 1, b"more").await;
    assert_eq!(
        err, TRANSACTION_ABORTABLE,
        "produce after open timeout → TRANSACTION_ABORTABLE"
    );
    // Flag remains until EndTxn.
    assert!(broker.is_txn_abortable(pid as u64));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase94_end_txn_after_open_timeout_is_abortable() {
    let dir = temp_dir("p94", "endtxn-abortable");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-end", "events", 100, b"stale").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);

    let err = end_txn(&addr, 11, "txn-end", pid, epoch, true).await;
    assert_eq!(
        err, TRANSACTION_ABORTABLE,
        "EndTxn after open timeout → TRANSACTION_ABORTABLE"
    );
    // EndTxn clears abortable so a new txn can open.
    assert!(!broker.is_txn_abortable(pid as u64));

    // Subsequent EndTxn with no open → classic InvalidTxnState (not 123).
    let err2 = end_txn(&addr, 12, "txn-end", pid, epoch, false).await;
    assert_eq!(err2, INVALID_TXN_STATE);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase94_add_offsets_after_timeout_is_abortable() {
    let dir = temp_dir("p94", "offsets-abortable");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-off", "events", 100, b"x").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);

    let err = add_offsets_v4(&addr, 20, "txn-off", pid, epoch, "cg-1").await;
    assert_eq!(
        err, TRANSACTION_ABORTABLE,
        "AddOffsetsToTxn v4 after timeout → 123"
    );

    // Clear via EndTxn, then AddOffsets succeeds (opens new txn).
    assert_eq!(
        end_txn(&addr, 21, "txn-off", pid, epoch, false).await,
        TRANSACTION_ABORTABLE
    );
    assert_eq!(
        add_offsets_v4(&addr, 22, "txn-off", pid, epoch, "cg-1").await,
        0
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase94_add_partitions_after_timeout_is_abortable() {
    let dir = temp_dir("p94", "addpart-abortable");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-ap", "events", 100, b"x").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);

    let err = add_partitions(&addr, 30, "txn-ap", pid, epoch, "events").await;
    assert_eq!(
        err, TRANSACTION_ABORTABLE,
        "AddPartitions after timeout → 123"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase94_prepared_timeout_end_txn_is_abortable() {
    let dir = temp_dir("p94", "prep-abortable");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_open_txn_timeout_ms(60_000);
    broker.set_prepared_txn_timeout_ms(100);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch) = init_v6_rpc(&addr, 1, "txn-2pc", 60_000, true).await;
    assert_eq!(err, 0);
    assert_eq!(add_partitions(&addr, 2, "txn-2pc", pid, epoch, "events").await, 0);
    assert_eq!(
        produce_txn_err(&addr, 3, "events", pid, epoch, 0, b"prep").await,
        0
    );
    // First EndTxn → prepare.
    assert_eq!(end_txn(&addr, 4, "txn-2pc", pid, epoch, true).await, 0);
    assert_eq!(
        broker.describe_transaction("txn-2pc").unwrap().0,
        "PrepareCommit"
    );

    assert!(broker.backdate_prepared_txn("txn-2pc", 5_000));
    assert_eq!(broker.expire_timed_out_prepared_txns(), 1);
    assert!(broker.is_txn_abortable(pid as u64));

    let err = end_txn(&addr, 5, "txn-2pc", pid, epoch, true).await;
    assert_eq!(
        err, TRANSACTION_ABORTABLE,
        "EndTxn after prepared timeout → 123"
    );
    assert!(!broker.is_txn_abortable(pid as u64));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase94_success_paths_still_zero() {
    let dir = temp_dir("p94", "success-zero");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch) = init_v6_rpc(&addr, 1, "txn-ok", 60_000, false).await;
    assert_eq!(err, 0);
    assert_eq!(add_partitions(&addr, 2, "txn-ok", pid, epoch, "events").await, 0);
    assert_eq!(
        produce_txn_err(&addr, 3, "events", pid, epoch, 0, b"live").await,
        0
    );
    assert_eq!(
        add_offsets_v4(&addr, 4, "txn-ok", pid, epoch, "cg-ok").await,
        0
    );
    assert_eq!(end_txn(&addr, 5, "txn-ok", pid, epoch, true).await, 0);

    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().any(|r| r.value.as_ref() == b"live"),
        "committed payload visible: {native:?}"
    );
    assert!(!broker.is_txn_abortable(pid as u64));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase94_never_opened_is_invalid_txn_state_not_abortable() {
    let dir = temp_dir("p94", "never-open");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Init only — never AddPartitions / open.
    let (err, pid, epoch) = init_v6_rpc(&addr, 1, "txn-empty", 60_000, false).await;
    assert_eq!(err, 0);

    let perr = produce_txn_err(&addr, 2, "events", pid, epoch, 0, b"nope").await;
    assert_eq!(
        perr, INVALID_TXN_STATE,
        "produce without open → InvalidTxnState, not 123"
    );
    assert_ne!(perr, TRANSACTION_ABORTABLE);

    let eerr = end_txn(&addr, 3, "txn-empty", pid, epoch, true).await;
    assert_eq!(eerr, INVALID_TXN_STATE);
    assert_ne!(eerr, TRANSACTION_ABORTABLE);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
