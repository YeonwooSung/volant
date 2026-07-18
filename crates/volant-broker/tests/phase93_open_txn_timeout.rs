//! Phase 93: Open transaction timeout / auto-abort MVP.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn init_v6(
    txn_id: &str,
    timeout_ms: i32,
    resume_pid: i64,
    resume_epoch: i16,
    enable_2pc: bool,
    keep_prepared: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(timeout_ms);
    body.put_i64(resume_pid);
    body.put_i16(resume_epoch);
    body.put_u8(if enable_2pc { 1 } else { 0 });
    body.put_u8(if keep_prepared { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_v6_rpc(
    addr: &str,
    corr: i32,
    txn_id: &str,
    timeout_ms: i32,
    enable_2pc: bool,
    keep_prepared: bool,
) -> (i16, i64, i16, i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(
            22,
            6,
            corr,
            Some("p"),
            &init_v6(txn_id, timeout_ms, -1, -1, enable_2pc, keep_prepared),
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
    let ongoing_pid = src.get_i64();
    let ongoing_epoch = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    (err, pid, epoch, ongoing_pid, ongoing_epoch)
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

async fn produce_txn(
    addr: &str,
    corr: i32,
    topic: &str,
    pid: i64,
    epoch: i16,
    seq: i32,
    val: &'static [u8],
) {
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
    assert_eq!(src.get_i16(), 0);
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

/// Open a classic (non-2PC) write-through txn with one produce.
async fn open_write_through(
    broker: &Broker,
    addr: &str,
    txn_id: &str,
    topic: &str,
    timeout_ms: i32,
    val: &'static [u8],
) -> (i64, i16) {
    let (err, pid, epoch, _, _) =
        init_v6_rpc(addr, 1, txn_id, timeout_ms, false, false).await;
    assert_eq!(err, 0);
    add_partitions(addr, 2, txn_id, pid, epoch, topic).await;
    produce_txn(addr, 3, topic, pid, epoch, 0, val).await;
    assert_eq!(
        broker.describe_transaction(txn_id).unwrap().0,
        "Ongoing"
    );
    (pid, epoch)
}

#[tokio::test]
async fn phase93_open_txn_times_out_and_aborts() {
    let dir = temp_dir("p93", "timeout-abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Client-supplied timeout; backdate past it (no flaky wall-clock sleep).
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-open", "events", 100, b"stale").await;

    let desc = broker.describe_transaction("txn-open").unwrap();
    assert_eq!(desc.0, "Ongoing");
    assert_eq!(desc.1, 100, "Describe reports client open timeout");
    assert!(desc.2 > 0, "Describe reports opened_at_ms as start time");

    let lso_before = broker.last_stable_offset("events", 0);
    let hwm_before = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert!(hwm_before > lso_before, "open txn holds LSO before timeout");

    assert!(
        broker.backdate_open_txn(pid as u64, 5_000),
        "must age open entry"
    );
    let n = broker.expire_timed_out_open_txns();
    assert_eq!(n, 1, "exactly one open txn auto-aborted");

    let desc = broker.describe_transaction("txn-open").unwrap();
    assert_eq!(desc.0, "Empty", "timed-out open becomes Empty");

    let lso2 = broker.last_stable_offset("events", 0);
    let hwm2 = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm2, lso2, "LSO released after open auto-abort");

    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().all(|r| r.value.as_ref() != b"stale"),
        "auto-aborted payload hidden: {native:?}"
    );

    // EndTxn after timeout no longer commits.
    assert_ne!(
        end_txn(&addr, 11, "txn-open", pid, epoch, true).await,
        0,
        "cannot commit after open timeout abort"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase93_end_txn_before_timeout_still_works() {
    let dir = temp_dir("p93", "still-ok");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-ok", "events", 60_000, b"live").await;

    // Commit before timeout.
    assert_eq!(end_txn(&addr, 20, "txn-ok", pid, epoch, true).await, 0);
    assert_eq!(broker.describe_transaction("txn-ok").unwrap().0, "Empty");

    let lso = broker.last_stable_offset("events", 0);
    let hwm = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm, lso);

    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().any(|r| r.value.as_ref() == b"live"),
        "committed payload visible: {native:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase93_open_timeout_zero_disables_auto_abort() {
    let dir = temp_dir("p93", "disabled");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Client timeout 0 → broker default; set broker default to 0 (disabled).
    broker.set_open_txn_timeout_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        open_write_through(&broker, &addr, "txn-hold", "events", 0, b"held").await;
    assert!(broker.backdate_open_txn(pid as u64, 1_000_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 0);
    assert_eq!(
        broker.describe_transaction("txn-hold").unwrap().0,
        "Ongoing",
        "effective timeout=0 never auto-aborts"
    );

    assert_eq!(
        end_txn(&addr, 30, "txn-hold", pid, epoch, true).await,
        0
    );
    assert_eq!(
        broker.describe_transaction("txn-hold").unwrap().0,
        "Empty"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase93_lazy_expire_on_list_and_lso() {
    let dir = temp_dir("p93", "lazy");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, _) =
        open_write_through(&broker, &addr, "txn-lazy", "events", 100, b"x").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));

    // Lazy via ListTransactions path.
    let listed = broker.list_open_transactions();
    assert!(
        listed
            .iter()
            .all(|(id, _, st)| id != "txn-lazy" || st != "Ongoing"),
        "list should not show timed-out open: {listed:?}"
    );
    assert_eq!(broker.describe_transaction("txn-lazy").unwrap().0, "Empty");

    // LSO path also expires (already empty here; smoke that it is callable).
    let lso = broker.last_stable_offset("events", 0);
    let hwm = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm, lso);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase93_prepared_path_regression_smoke() {
    let dir = temp_dir("p93", "prepared-smoke");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Generous timeouts so open path does not interfere with 2PC.
    broker.set_open_txn_timeout_ms(60_000);
    broker.set_prepared_txn_timeout_ms(60_000);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) =
        init_v6_rpc(&addr, 1, "txn-2pc", 60_000, true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-2pc", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"prep").await;

    // First EndTxn → prepare (not open timeout).
    assert_eq!(end_txn(&addr, 4, "txn-2pc", pid, epoch, true).await, 0);
    assert_eq!(
        broker.describe_transaction("txn-2pc").unwrap().0,
        "PrepareCommit"
    );

    // Open map should be empty; open expiry must not touch prepared.
    assert_eq!(broker.expire_timed_out_open_txns(), 0);
    assert_eq!(
        broker.describe_transaction("txn-2pc").unwrap().0,
        "PrepareCommit"
    );

    // Complete prepare.
    assert_eq!(end_txn(&addr, 5, "txn-2pc", pid, epoch, true).await, 0);
    assert_eq!(broker.describe_transaction("txn-2pc").unwrap().0, "Empty");

    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().any(|r| r.value.as_ref() == b"prep"),
        "prepared-then-commit payload visible: {native:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase93_broker_default_timeout_when_client_zero() {
    let dir = temp_dir("p93", "broker-default");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_open_txn_timeout_ms(250);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Client timeout 0 → broker default 250.
    let (pid, _) =
        open_write_through(&broker, &addr, "txn-def", "events", 0, b"d").await;
    let desc = broker.describe_transaction("txn-def").unwrap();
    assert_eq!(desc.0, "Ongoing");
    assert_eq!(desc.1, 250, "Describe reports broker-default open timeout");

    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert_eq!(broker.describe_transaction("txn-def").unwrap().0, "Empty");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
