//! Phase 101: graceful sweeper enable on 0→>0 without process restart.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::broker_config::KEY_SWEEP_INTERVAL_MS;
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_nullable_string, put_string,
    skip_tag_buffer,
};
use volant_broker::{start_background_tasks, Broker};
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn init_v6(
    txn_id: &str,
    timeout_ms: i32,
    enable_2pc: bool,
    keep_prepared: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(timeout_ms);
    body.put_i64(-1);
    body.put_i16(-1);
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
) -> (i16, i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(
            22,
            6,
            corr,
            Some("p"),
            &init_v6(txn_id, timeout_ms, false, false),
        ),
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
    assert_eq!(
        broker.describe_transaction(txn_id).unwrap().0,
        "Ongoing"
    );
    (pid, epoch)
}

fn alter_broker_body(name: &str, configs: &[(&str, Option<&str>)], validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(4); // BROKER
    put_string(&mut body, name);
    body.put_i32(configs.len() as i32);
    for (k, v) in configs {
        put_string(&mut body, k);
        put_nullable_string(&mut body, *v);
    }
    body.put_u8(if validate_only { 1 } else { 0 });
    body
}

/// Poll until `pred` is true or deadline elapses.
async fn wait_until<F>(mut pred: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pred()
}

/// Boot with sweep interval 0, start bg tasks, then setter 0→>0 enables sweep.
#[tokio::test]
async fn phase101_zero_to_positive_via_setter() {
    let dir = temp_dir("p101", "setter");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Start with interval 0 — pre-Phase 101 this skipped spawning the task.
    broker.set_sweep_interval_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let bg = start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-enable", "events", 100, b"stale").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.open_txn_count(), 1);
    assert_eq!(broker.open_txns_expired_total(), 0);

    // While paused: no background expire (poll gauges only; avoid lazy paths).
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        broker.open_txn_count(),
        1,
        "interval 0 must pause background expire"
    );
    assert_eq!(broker.open_txns_expired_total(), 0);

    // Enable without process restart / re-entry of start_background_tasks.
    broker.set_sweep_interval_ms(50);

    let ok = wait_until(
        || broker.open_txn_count() == 0 && broker.open_txns_expired_total() >= 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        ok,
        "background sweeper should abort open txn after 0→>0 via setter"
    );
    assert!(broker.is_txn_abortable(pid as u64));
    assert_eq!(
        broker.describe_transaction("txn-enable").unwrap().0,
        "Empty"
    );

    let lso = broker.last_stable_offset("events", 0);
    let hwm = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm, lso, "LSO released after background open abort");

    bg.shutdown().await;
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Boot with interval 0; AlterConfigs BROKER 0→>0 enables sweep without restart.
#[tokio::test]
async fn phase101_zero_to_positive_via_alter_configs() {
    let dir = temp_dir("p101", "alter");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let bg = start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-alter", "events", 100, b"stale").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.open_txn_count(), 1);

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(broker.open_txn_count(), 1);
    assert_eq!(broker.open_txns_expired_total(), 0);

    let body = alter_broker_body(
        "0",
        &[(KEY_SWEEP_INTERVAL_MS, Some("50"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 10, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0); // resource error
    assert_eq!(broker.sweep_interval_ms(), 50);

    let ok = wait_until(
        || broker.open_txn_count() == 0 && broker.open_txns_expired_total() >= 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        ok,
        "background sweeper should abort open txn after 0→>0 via AlterConfigs"
    );
    assert!(broker.is_txn_abortable(pid as u64));

    bg.shutdown().await;
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// >0 → 0 still pauses; lazy expire remains available.
#[tokio::test]
async fn phase101_positive_to_zero_pauses() {
    let dir = temp_dir("p101", "pause");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let bg = start_background_tasks(Arc::clone(&broker));

    // Pause before creating the aged txn so the sweeper cannot race.
    broker.set_sweep_interval_ms(0);
    // Allow any in-flight sleep/sweep to settle under paused mode.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-pause", "events", 100, b"hold").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.open_txn_count(), 1);
    let expired_before = broker.open_txns_expired_total();

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        broker.open_txn_count(),
        1,
        ">0→0 must pause background expire"
    );
    assert_eq!(broker.open_txns_expired_total(), expired_before);

    // Lazy path still works.
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert_eq!(broker.open_txn_count(), 0);
    assert_eq!(broker.open_txns_expired_total(), expired_before + 1);

    bg.shutdown().await;
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
