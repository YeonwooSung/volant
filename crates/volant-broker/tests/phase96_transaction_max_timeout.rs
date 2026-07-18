//! Phase 96: Broker transaction.max.timeout.ms clamp (MVP).

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

/// Kafka INVALID_TRANSACTION_TIMEOUT.
const INVALID_TRANSACTION_TIMEOUT: i16 = 50;

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

/// Client timeout above max → InitProducerId returns INVALID_TRANSACTION_TIMEOUT (50).
#[tokio::test]
async fn phase96_init_rejects_client_timeout_above_max() {
    let dir = temp_dir("p96", "reject-above");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_transaction_max_timeout_ms(1_000);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, ongoing_pid, ongoing_epoch) =
        init_v6_rpc(&addr, 1, "txn-big", 5_000, false, false).await;
    assert_eq!(err, INVALID_TRANSACTION_TIMEOUT);
    assert_eq!(pid, -1);
    assert_eq!(epoch, -1);
    assert_eq!(ongoing_pid, -1);
    assert_eq!(ongoing_epoch, -1);

    // Equal to max is accepted.
    let (err2, pid2, epoch2, _, _) =
        init_v6_rpc(&addr, 2, "txn-eq", 1_000, false, false).await;
    assert_eq!(err2, 0);
    assert!(pid2 >= 0);
    assert!(epoch2 >= 0);

    // Below max accepted.
    let (err3, _, _, _, _) =
        init_v6_rpc(&addr, 3, "txn-ok", 500, false, false).await;
    assert_eq!(err3, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Client timeout below max is unchanged: Describe reports client value; expire uses it.
#[tokio::test]
async fn phase96_client_timeout_below_max_unchanged() {
    let dir = temp_dir("p96", "below-max");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Default max is 15m; client 100ms is well below.
    assert_eq!(broker.transaction_max_timeout_ms(), 900_000);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) =
        init_v6_rpc(&addr, 1, "txn-small", 100, false, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-small", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"x").await;

    let desc = broker.describe_transaction("txn-small").unwrap();
    assert_eq!(desc.0, "Ongoing");
    assert_eq!(desc.1, 100, "below-max client timeout not clamped");

    // Age past client timeout but well under max → expires.
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert_eq!(broker.describe_transaction("txn-small").unwrap().0, "Empty");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Stored client timeout above a later-lowered max is clamped for expire + Describe.
#[tokio::test]
async fn phase96_open_expire_uses_clamped_timeout() {
    let dir = temp_dir("p96", "clamp-open");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Accept a large client timeout while max is disabled.
    broker.set_transaction_max_timeout_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) =
        init_v6_rpc(&addr, 1, "txn-clamp", 5_000, false, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-clamp", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"stale").await;

    // Lower max below stored client timeout → effective becomes 1s.
    broker.set_transaction_max_timeout_ms(1_000);
    let desc = broker.describe_transaction("txn-clamp").unwrap();
    assert_eq!(desc.0, "Ongoing");
    assert_eq!(desc.1, 1_000, "Describe reports clamped open timeout");

    // Age 1.5s: would not expire under raw 5s, but clamps to 1s → aborts.
    assert!(broker.backdate_open_txn(pid as u64, 1_500));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert_eq!(
        broker.describe_transaction("txn-clamp").unwrap().0,
        "Empty"
    );

    // Payload hidden under native committed-only fetch.
    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().all(|r| r.value.as_ref() != b"stale"),
        "clamped-timeout abort hides payload: {native:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Prepared timeout above max is clamped for expire + Describe.
#[tokio::test]
async fn phase96_prepared_expire_uses_clamped_timeout() {
    let dir = temp_dir("p96", "clamp-prep");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_transaction_max_timeout_ms(900_000);
    broker.set_open_txn_timeout_ms(60_000);
    broker.set_prepared_txn_timeout_ms(5_000);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) =
        init_v6_rpc(&addr, 1, "txn-2pc", 60_000, true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-2pc", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"prep").await;
    assert_eq!(end_txn(&addr, 4, "txn-2pc", pid, epoch, true).await, 0);
    assert_eq!(
        broker.describe_transaction("txn-2pc").unwrap().0,
        "PrepareCommit"
    );

    // Lower max below prepared timeout → effective prepared = 1s.
    broker.set_transaction_max_timeout_ms(1_000);
    let desc = broker.describe_transaction("txn-2pc").unwrap();
    assert_eq!(desc.0, "PrepareCommit");
    assert_eq!(desc.1, 1_000, "Describe reports clamped prepared timeout");

    assert!(broker.backdate_prepared_txn("txn-2pc", 1_500));
    assert_eq!(broker.expire_timed_out_prepared_txns(), 1);
    assert_eq!(broker.describe_transaction("txn-2pc").unwrap().0, "Empty");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// max=0 disables clamp and Init reject.
#[tokio::test]
async fn phase96_max_zero_disables_clamp_and_reject() {
    let dir = temp_dir("p96", "max-zero");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_transaction_max_timeout_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Huge client timeout accepted when max disabled.
    let (err, pid, epoch, _, _) =
        init_v6_rpc(&addr, 1, "txn-huge", 3_600_000, false, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-huge", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"h").await;

    let desc = broker.describe_transaction("txn-huge").unwrap();
    assert_eq!(desc.0, "Ongoing");
    assert_eq!(desc.1, 3_600_000, "no clamp when max=0");

    // Age 1 hour-ish under client timeout → still open.
    assert!(broker.backdate_open_txn(pid as u64, 1_000_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 0);
    assert_eq!(
        broker.describe_transaction("txn-huge").unwrap().0,
        "Ongoing"
    );

    assert_eq!(end_txn(&addr, 10, "txn-huge", pid, epoch, true).await, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Direct broker API also returns error_code 50.
#[tokio::test]
async fn phase96_broker_api_rejects_over_max() {
    let dir = temp_dir("p96", "api-reject");
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    broker.set_transaction_max_timeout_ms(100);
    let r = broker.init_producer_id_with_opts("txn-x", false, false, 101);
    assert_eq!(r.error_code, INVALID_TRANSACTION_TIMEOUT);
    assert!(broker.describe_transaction("txn-x").is_none());

    let r2 = broker.init_producer_id_with_opts("txn-y", false, false, 100);
    assert_eq!(r2.error_code, 0);
    assert!(broker.describe_transaction("txn-y").is_some());

    let _ = std::fs::remove_dir_all(&dir);
}
