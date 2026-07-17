//! Phase 48: Kafka Produce classic v0–8 (log_start_offset, record_errors).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_nullable_string, get_string, put_bytes,
    put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

/// Produce body for classic v3–8 (transactional_id + acks + timeout + one partition).
fn produce_body_v3(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, None); // transactional_id
    body.put_i16(1); // acks
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

#[tokio::test]
async fn api_versions_produce_max_v13() {
    let dir = temp_dir("p48", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut produce = None;
    let mut fetch = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 0 {
            produce = Some((min, max));
        }
        if key == 1 {
            fetch = Some((min, max));
        }
    }
    assert_eq!(produce, Some((0, 13))); // Phase 71 TopicId
    assert_eq!(fetch, Some((0, 13))); // Phase 68 Fetch TopicId

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v5_log_start_offset() {
    let dir = temp_dir("p48", "v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"hello-v5"));
    let resp = rpc(
        &addr,
        encode_request(0, 5, 2, Some("c"), &produce_body_v3("orders", &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2); // corr
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i32(), 0); // index
    assert_eq!(src.get_i16(), 0); // error
    let base = src.get_i64();
    assert!(base >= 0);
    assert_eq!(src.get_i64(), -1); // log_append_time
    let log_start = src.get_i64();
    assert!(log_start >= 0, "log_start_offset should be known, got {log_start}");
    assert_eq!(src.get_i32(), 0); // trailing throttle

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v8_record_errors_and_error_message() {
    let dir = temp_dir("p48", "v8");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"hello-v8"));
    let resp = rpc(
        &addr,
        encode_request(0, 8, 3, Some("c"), &produce_body_v3("events", &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let base = src.get_i64();
    assert!(base >= 0);
    assert_eq!(src.get_i64(), -1); // log_append_time
    let log_start = src.get_i64();
    assert!(log_start >= 0);
    assert_eq!(src.get_i32(), 0); // record_errors empty
    assert_eq!(get_nullable_string(&mut src).unwrap(), None); // error_message
    assert_eq!(src.get_i32(), 0); // throttle

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v0_still_works() {
    let dir = temp_dir("p48", "v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v0 body: no transactional_id
    let batch = encode_record_batch(&sample_records(b"v0"));
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, "t");
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));

    let resp = rpc(&addr, encode_request(0, 0, 4, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert!(src.get_i64() >= 0);
    // v0: no log_append_time, no throttle

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// Produce v9 flexible support: phase53_flexible_produce.
// Produce v13 TopicId: phase71_produce_topic_id.
