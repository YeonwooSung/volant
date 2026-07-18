//! Phase 97: background open/prepared/session sweeper + metrics (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
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
    enable_2pc: bool,
) -> (i16, i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(
            22,
            6,
            corr,
            Some("p"),
            &init_v6(txn_id, timeout_ms, enable_2pc, false),
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
    add_partitions(addr, 2, txn_id, pid, epoch, topic).await;
    produce_txn(addr, 3, topic, pid, epoch, 0, val).await;
    assert_eq!(
        broker.describe_transaction(txn_id).unwrap().0,
        "Ongoing"
    );
    (pid, epoch)
}

async fn prepare_commit(
    broker: &Broker,
    addr: &str,
    txn_id: &str,
    topic: &str,
    val: &'static [u8],
) -> (i64, i16) {
    let (err, pid, epoch) = init_v6_rpc(addr, 1, txn_id, 60_000, true).await;
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

#[tokio::test]
async fn phase97_background_expires_open_txn() {
    let dir = temp_dir("p97", "open-bg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-open", "events", 100, b"stale").await;
    assert_eq!(broker.open_txn_count(), 1);
    assert_eq!(broker.open_txns_expired_total(), 0);

    assert!(broker.backdate_open_txn(pid as u64, 5_000));

    // Do not call describe/LSO/txn APIs (they lazy-expire). Poll gauges only.
    let ok = wait_until(
        || broker.open_txn_count() == 0 && broker.open_txns_expired_total() >= 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(ok, "background sweeper should abort open txn");
    assert!(broker.is_txn_abortable(pid as u64));
    assert_eq!(
        broker.describe_transaction("txn-open").unwrap().0,
        "Empty"
    );

    let lso = broker.last_stable_offset("events", 0);
    let hwm = broker
        .high_watermark(&TopicName::new("events"), PartitionId(0))
        .unwrap_or(0);
    assert_eq!(hwm, lso, "LSO released after background open abort");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase97_background_expires_prepared_txn() {
    let dir = temp_dir("p97", "prep-bg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    broker.set_prepared_txn_timeout_ms(100);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        prepare_commit(&broker, &addr, "txn-prep", "events", b"stale").await;
    assert_eq!(broker.prepared_txn_count(), 1);
    assert_eq!(broker.prepared_txns_expired_total(), 0);

    assert!(broker.backdate_prepared_txn("txn-prep", 5_000));

    // Poll gauges only — describe_transaction would lazy-expire.
    let ok = wait_until(
        || {
            broker.prepared_txn_count() == 0 && broker.prepared_txns_expired_total() >= 1
        },
        Duration::from_secs(2),
    )
    .await;
    assert!(ok, "background sweeper should abort prepared txn");
    assert!(broker.is_txn_abortable(pid as u64));
    assert_eq!(
        broker.describe_transaction("txn-prep").unwrap().0,
        "Empty"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase97_background_evicts_idle_session() {
    let dir = temp_dir("p97", "sess-bg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(50);
    broker.fetch_sessions().set_idle_timeout_ms(100);
    start_background_tasks(Arc::clone(&broker));

    // Create with old activity so it is immediately idle past TTL.
    let id = broker
        .fetch_sessions()
        .create_at(HashMap::new(), 1_000);
    assert_eq!(broker.fetch_sessions().active_count(), 1);
    let idle_before = broker.fetch_sessions().idle_evicted_total();

    let ok = wait_until(
        || broker.fetch_sessions().active_count() == 0,
        Duration::from_secs(2),
    )
    .await;
    assert!(ok, "background sweeper should idle-evict session {id}");
    assert!(
        broker.fetch_sessions().idle_evicted_total() > idle_before,
        "idle counter should increment"
    );
    assert!(broker.fetch_sessions().evicted_total() >= 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase97_sweep_disabled_when_interval_zero() {
    let dir = temp_dir("p97", "disabled");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    // Interval 0: sweeper task still runs (Phase 101) but pauses work.
    start_background_tasks(Arc::clone(&broker));

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-hold", "events", 100, b"live").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));

    // Wait longer than a normal short sweep interval; must still be open.
    // Avoid describe_transaction (lazy expire); use open_txn_count only.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        broker.open_txn_count(),
        1,
        "interval 0 must not background-expire"
    );
    assert_eq!(broker.open_txns_expired_total(), 0);

    // Lazy path still works.
    assert_eq!(broker.expire_timed_out_open_txns(), 1);
    assert_eq!(broker.open_txn_count(), 0);
    assert_eq!(broker.open_txns_expired_total(), 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase97_lazy_path_still_counts() {
    let dir = temp_dir("p97", "lazy");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    // Keep background off so only lazy counts.
    broker.set_sweep_interval_ms(0);
    broker.set_prepared_txn_timeout_ms(100);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, _epoch) =
        open_write_through(&broker, &addr, "txn-lazy", "events", 100, b"x").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    let (o, p, s) = broker.sweep_timeouts();
    assert_eq!((o, p, s), (1, 0, 0));
    assert_eq!(broker.open_txns_expired_total(), 1);

    let (_pid2, _) = prepare_commit(&broker, &addr, "txn-p", "events", b"y").await;
    assert!(broker.backdate_prepared_txn("txn-p", 5_000));
    assert_eq!(broker.expire_timed_out_prepared_txns(), 1);
    assert_eq!(broker.prepared_txns_expired_total(), 1);

    // Direct sweep entry for idle sessions.
    broker.fetch_sessions().set_idle_timeout_ms(50);
    let _ = broker.fetch_sessions().create_at(HashMap::new(), 1);
    let idle = broker.fetch_sessions().evict_idle_at(10_000);
    assert_eq!(idle, 1);
    assert_eq!(broker.fetch_sessions().idle_evicted_total(), 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn phase97_metrics_series_present() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use volant_broker::run_metrics_server;

    let dir = temp_dir("p97", "metrics");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_sweep_interval_ms(0);
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, _) =
        open_write_through(&broker, &addr, "txn-m", "events", 100, b"m").await;
    assert!(broker.backdate_open_txn(pid as u64, 5_000));
    assert_eq!(broker.expire_timed_out_open_txns(), 1);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let maddr = listener.local_addr().unwrap();
    drop(listener);
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = run_metrics_server(maddr, b).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(maddr).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    for needle in [
        "volant_open_txns ",
        "volant_prepared_txns ",
        "volant_open_txns_expired_total ",
        "volant_prepared_txns_expired_total ",
        "volant_fetch_sessions_idle_evicted_total ",
        "volant_fetch_sessions_active ",
    ] {
        assert!(
            text.contains(needle),
            "metrics missing {needle}:\n{text}"
        );
    }
    assert!(text.contains("volant_open_txns_expired_total 1"));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
