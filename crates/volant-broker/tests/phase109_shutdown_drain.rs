//! Phase 109: accept-loop drain + single-flight background tasks (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpListener;
use tokio::sync::watch;
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::{
    serve_kafka_listener_until, serve_listener_until, start_background_tasks, Broker,
};
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

/// Double `start_background_tasks` must not double-spawn the sweeper.
#[tokio::test]
async fn phase109_single_flight_background_tasks() {
    let dir = temp_dir("p109", "single-flight");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let bg1 = start_background_tasks(Arc::clone(&broker));
    let bg2 = start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-sf", "events", 100, b"once").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.open_txn_count(), 1);

    let ok = wait_until(
        || broker.open_txn_count() == 0 && broker.open_txns_expired_total() >= 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(ok, "single sweeper should expire open txn once");
    assert_eq!(
        broker.open_txns_expired_total(),
        1,
        "double start must not double-expire"
    );

    // Second handle is a no-op; first still owns the real tasks.
    let start = Instant::now();
    bg2.shutdown().await;
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "no-op shutdown should be immediate"
    );

    // Sweeper still running after no-op second shutdown.
    let (pid2, _) =
        open_write_through(&broker, &addr, "txn-sf2", "events", 100, b"still").await;
    assert!(broker.backdate_open_txn(pid2 as u64, 5_000));
    let ok = wait_until(|| broker.open_txn_count() == 0, Duration::from_secs(2)).await;
    assert!(ok, "first-flight sweeper still alive after second no-op shutdown");

    let start = Instant::now();
    bg1.shutdown().await;
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "owner shutdown should join promptly"
    );

    // After real shutdown, no further background expire.
    let expired_before = broker.open_txns_expired_total();
    let (pid3, _) =
        open_write_through(&broker, &addr, "txn-sf3", "events", 100, b"post").await;
    assert!(broker.backdate_open_txn(pid3 as u64, 5_000));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(broker.open_txn_count(), 1);
    assert_eq!(broker.open_txns_expired_total(), expired_before);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Native accept loop stops promptly when the shutdown future completes.
#[tokio::test]
async fn phase109_native_accept_drains_promptly() {
    let dir = temp_dir("p109", "native-drain");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop_tx, stop_rx) = watch::channel(false);

    let server = tokio::spawn(async move {
        serve_listener_until(listener, broker, async move {
            let mut rx = stop_rx;
            while !*rx.borrow() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .ok();
    });

    // Let accept start.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = stop_tx.send(true);

    let start = Instant::now();
    let joined = tokio::time::timeout(Duration::from_secs(3), server).await;
    assert!(joined.is_ok(), "native serve_listener_until should finish promptly");
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "native drain took {:?}",
        start.elapsed()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Kafka accept loop stops promptly when the shutdown future completes.
#[tokio::test]
async fn phase109_kafka_accept_drains_promptly() {
    let dir = temp_dir("p109", "kafka-drain");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (stop_tx, stop_rx) = watch::channel(false);

    let server = tokio::spawn(async move {
        serve_kafka_listener_until(listener, broker, async move {
            let mut rx = stop_rx;
            while !*rx.borrow() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .ok();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = stop_tx.send(true);

    let start = Instant::now();
    let joined = tokio::time::timeout(Duration::from_secs(3), server).await;
    assert!(joined.is_ok(), "kafka serve_kafka_listener_until should finish promptly");
    assert!(start.elapsed() < Duration::from_secs(3));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression: Phase 106 join still works under single-flight (first handle).
#[tokio::test]
async fn phase109_phase106_join_regression() {
    let dir = temp_dir("p109", "p106-reg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    let bg = start_background_tasks(Arc::clone(&broker));
    // Duplicate call must not break first handle's join.
    let _bg2 = start_background_tasks(Arc::clone(&broker));

    tokio::time::sleep(Duration::from_millis(80)).await;

    let start = Instant::now();
    bg.shutdown().await;
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "phase106-style shutdown should join promptly"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
