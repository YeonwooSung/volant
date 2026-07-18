//! Phase 106: graceful background task shutdown / join (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::{start_background_tasks, Broker};
use volant_core::Record;
use volant_storage::StorageConfig;

fn init_v6(txn_id: &str, timeout_ms: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(timeout_ms);
    body.put_i64(-1);
    body.put_i16(-1);
    body.put_u8(0);
    body.put_u8(0);
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_v6_rpc(addr: &str, corr: i32, txn_id: &str, timeout_ms: i32) -> (i16, i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(22, 6, corr, Some("p"), &init_v6(txn_id, timeout_ms)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    let pid = src.get_i64();
    let epoch = src.get_i16();
    let _ = src.get_i64();
    let _ = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    (err, pid, epoch)
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
        offset: volant_core::Offset::new(0),
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

async fn open_write_through(
    broker: &Broker,
    addr: &str,
    txn_id: &str,
    topic: &str,
    timeout_ms: i32,
    val: &'static [u8],
) -> (i64, i16) {
    let (err, pid, epoch) = init_v6_rpc(addr, 1, txn_id, timeout_ms).await;
    assert_eq!(err, 0);
    add_partitions(addr, 2, txn_id, pid, epoch, topic).await;
    produce_txn(addr, 3, topic, pid, epoch, 0, val).await;
    assert_eq!(broker.describe_transaction(txn_id).unwrap().0, "Ongoing");
    (pid, epoch)
}

async fn wait_until<F>(mut pred: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pred()
}

/// Interval >0: shutdown joins without hanging.
#[tokio::test]
async fn phase106_shutdown_joins_when_active() {
    let dir = temp_dir("p106", "active");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    let bg = start_background_tasks(Arc::clone(&broker));

    // Let loops tick once so tasks are definitely running.
    tokio::time::sleep(Duration::from_millis(80)).await;

    let start = Instant::now();
    bg.shutdown().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "shutdown should join promptly, took {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Interval 0 (paused sweeper): shutdown still joins promptly.
#[tokio::test]
async fn phase106_shutdown_joins_when_paused() {
    let dir = temp_dir("p106", "paused");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(0);
    let bg = start_background_tasks(Arc::clone(&broker));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = Instant::now();
    bg.shutdown().await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "paused shutdown should join promptly, took {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// After shutdown, background sweeper no longer expires open txns.
#[tokio::test]
async fn phase106_no_bg_expire_after_shutdown() {
    let dir = temp_dir("p106", "no-expire");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let bg = start_background_tasks(Arc::clone(&broker));

    // Shut down before creating the aged txn.
    bg.shutdown().await;

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-post", "events", 100, b"live").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.open_txn_count(), 1);
    let expired_before = broker.open_txns_expired_total();

    // Would have been background-expired under a live sweeper within ~1s.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        broker.open_txn_count(),
        1,
        "after shutdown, background must not expire open txn"
    );
    assert_eq!(broker.open_txns_expired_total(), expired_before);

    // Lazy path still works.
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert_eq!(broker.open_txn_count(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression: 0→>0 still enables background expire before shutdown (Phase 101).
#[tokio::test]
async fn phase106_zero_to_positive_before_shutdown() {
    let dir = temp_dir("p106", "enable");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let bg = start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-en", "events", 100, b"stale").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.open_txn_count(), 1);

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(broker.open_txn_count(), 1, "paused must not expire");

    broker.set_sweep_interval_ms(50);
    let ok = wait_until(
        || broker.open_txn_count() == 0 && broker.open_txns_expired_total() >= 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(ok, "0→>0 should enable background expire before shutdown");

    let start = Instant::now();
    bg.shutdown().await;
    assert!(start.elapsed() < Duration::from_secs(3));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
