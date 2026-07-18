//! Phase 92: Prepared transaction timeout / auto-abort MVP.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_bytes, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn init_v6(
    txn_id: &str,
    resume_pid: i64,
    resume_epoch: i16,
    enable_2pc: bool,
    keep_prepared: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
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
            &init_v6(txn_id, -1, -1, enable_2pc, keep_prepared),
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

async fn fetch_v4_lso(
    addr: &str,
    corr: i32,
    topic: &str,
    isolation: u8,
) -> (i64, i64, Bytes) {
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
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let aborted_n = src.get_i32();
    for _ in 0..aborted_n {
        let _ = src.get_i64();
        let _ = src.get_i64();
    }
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    (hwm, lso, records)
}

async fn prepare_commit(
    broker: &Broker,
    addr: &str,
    txn_id: &str,
    topic: &str,
    val: &'static [u8],
) -> (i64, i16) {
    let (err, pid, epoch, _, _) = init_v6_rpc(addr, 1, txn_id, true, false).await;
    assert_eq!(err, 0);
    add_partitions(addr, 2, txn_id, pid, epoch, topic).await;
    produce_txn(addr, 3, topic, pid, epoch, 0, val).await;
    assert_eq!(end_txn(addr, 4, txn_id, pid, epoch, true).await, 0);
    assert_eq!(
        broker.describe_transaction(txn_id).unwrap().0,
        "PrepareCommit"
    );
    (pid, epoch)
}

#[tokio::test]
async fn phase92_timeout_auto_aborts_prepared_commit() {
    let dir = temp_dir("p92", "timeout-abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Short timeout; backdate past it (no flaky wall-clock sleep).
    broker.set_prepared_txn_timeout_ms(100);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        prepare_commit(&broker, &addr, "txn-to", "events", b"stale").await;

    // Still prepared before ageing.
    assert_eq!(
        broker.describe_transaction("txn-to").unwrap().0,
        "PrepareCommit"
    );
    let (hwm, lso, _) = fetch_v4_lso(&addr, 10, "events", 1).await;
    assert!(hwm > lso, "prepared still holds LSO before timeout");

    assert!(
        broker.backdate_prepared_txn("txn-to", 5_000),
        "must age prepared entry"
    );
    let n = broker.expire_timed_out_prepared_txns();
    assert_eq!(n, 1, "exactly one prepared txn auto-aborted");

    let desc = broker.describe_transaction("txn-to").unwrap();
    assert_eq!(desc.0, "Empty", "timed-out prepare becomes Empty");

    // LSO catches HWM; aborted payload hidden under native committed-only.
    let lso2 = broker.last_stable_offset("events", 0);
    let hwm2 = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm2, lso2, "LSO released after auto-abort");

    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().all(|r| r.value.as_ref() != b"stale"),
        "auto-aborted payload hidden: {native:?}"
    );

    // Second EndTxn(commit) no longer finalizes a prepare.
    assert_ne!(
        end_txn(&addr, 11, "txn-to", pid, epoch, true).await,
        0,
        "cannot complete commit after timeout abort"
    );

    // KeepPreparedTxn after timeout → no OngoingTxn*.
    let (err, _, _, ong_pid, ong_epoch) =
        init_v6_rpc(&addr, 12, "txn-to", true, true).await;
    assert_eq!(err, 0);
    assert_eq!(ong_pid, -1);
    assert_eq!(ong_epoch, -1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase92_non_timeout_prepare_still_completes() {
    let dir = temp_dir("p92", "still-ok");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Generous timeout so the happy path is unaffected.
    broker.set_prepared_txn_timeout_ms(60_000);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        prepare_commit(&broker, &addr, "txn-ok", "events", b"live").await;

    let desc = broker.describe_transaction("txn-ok").unwrap();
    assert_eq!(desc.0, "PrepareCommit");
    assert_eq!(desc.1, 60_000, "Describe reports configured prepared timeout");
    assert!(desc.2 > 0, "Describe reports prepared_at_ms as start time");

    // Complete before timeout.
    assert_eq!(end_txn(&addr, 20, "txn-ok", pid, epoch, true).await, 0);
    assert_eq!(broker.describe_transaction("txn-ok").unwrap().0, "Empty");

    let (hwm, lso, records) = fetch_v4_lso(&addr, 21, "events", 1).await;
    assert_eq!(hwm, lso);
    assert!(!records.is_empty(), "committed data visible under READ_COMMITTED");

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
async fn phase92_timeout_zero_disables_auto_abort() {
    let dir = temp_dir("p92", "disabled");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_prepared_txn_timeout_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) =
        prepare_commit(&broker, &addr, "txn-hold", "events", b"held").await;
    assert!(broker.backdate_prepared_txn("txn-hold", 1_000_000));
    assert_eq!(broker.expire_timed_out_prepared_txns(), 0);
    assert_eq!(
        broker.describe_transaction("txn-hold").unwrap().0,
        "PrepareCommit",
        "timeout=0 never auto-aborts"
    );

    // Still finalizable.
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
async fn phase92_timeout_survives_restart_then_expires() {
    let dir = temp_dir("p92", "restart");
    let (pid, epoch) = {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        }));
        broker.set_prepared_txn_timeout_ms(50);
        broker.create_topic("events", 1).unwrap();
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
        let (pid, epoch) =
            prepare_commit(&broker, &addr, "txn-dur", "events", b"persist").await;
        assert!(broker.backdate_prepared_txn("txn-dur", 10_000));
        // Do not expire in-process — leave durable aged prepared for restart.
        server.abort();
        (pid, epoch)
    };

    let broker2 = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Restart re-reads env default (60s) unless overridden; force short timeout
    // then expire the aged prepared entry.
    broker2.set_prepared_txn_timeout_ms(50);
    let n = broker2.expire_timed_out_prepared_txns();
    assert_eq!(n, 1, "aged prepared from disk expires after restart");
    assert_eq!(
        broker2.describe_transaction("txn-dur").unwrap().0,
        "Empty"
    );

    let lso = broker2.last_stable_offset("events", 0);
    let hwm = broker2
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm, lso);

    // Old epoch cannot complete.
    let (addr, server) = boot_kafka(Arc::clone(&broker2)).await;
    assert_ne!(
        end_txn(&addr, 40, "txn-dur", pid, epoch, true).await,
        0
    );
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase92_lazy_expire_on_list_and_lso() {
    let dir = temp_dir("p92", "lazy");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_prepared_txn_timeout_ms(100);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = prepare_commit(&broker, &addr, "txn-lazy", "events", b"x").await;
    assert!(broker.backdate_prepared_txn("txn-lazy", 5_000));

    // Lazy via ListTransactions path.
    let listed = broker.list_open_transactions();
    assert!(
        listed.iter().all(|(id, _, st)| id != "txn-lazy" || st != "PrepareCommit"),
        "list should not show timed-out prepare: {listed:?}"
    );
    assert_eq!(broker.describe_transaction("txn-lazy").unwrap().0, "Empty");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
