//! Phase 68: Fetch TopicId v13 (KIP-516 UUID topics).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_bytes, get_uuid, put_bytes, put_compact_array_len, put_compact_string,
    put_empty_tag_buffer, put_nullable_string, put_string, put_uuid, skip_tag_buffer,
    volant_topic_uuid, KAFKA_UUID_ZERO,
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

fn produce_body_v3(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

/// Fetch v13 body: TopicId UUID instead of topic name.
fn fetch_v13_body(topic_uuid: &[u8; 16], fetch_offset: i64, session_id: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(session_id);
    body.put_i32(-1); // session_epoch
    put_compact_array_len(&mut body, 1); // topics
    put_uuid(&mut body, topic_uuid);
    put_compact_array_len(&mut body, 1); // partitions
    body.put_i32(0); // partition
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(fetch_offset);
    body.put_i32(-1); // last_fetched_epoch
    body.put_i64(-1); // log_start_offset
    body.put_i32(1_000_000); // partition_max_bytes
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_compact_array_len(&mut body, 0); // forgotten
    put_compact_string(&mut body, ""); // rack_id
    put_empty_tag_buffer(&mut body); // top-level
    body
}

#[tokio::test]
async fn api_versions_fetch_max_13() {
    let dir = temp_dir("p68", "api");
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
    let mut fetch = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 1 {
            fetch = Some((min, max));
        }
    }
    assert_eq!(fetch, Some((0, 13)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v13_by_topic_id() {
    let dir = temp_dir("p68", "by-id");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let numeric_id = (0u32..64)
        .find(|&id| broker.topic_name_by_id(id).as_deref() == Some("orders"))
        .expect("orders topic id");
    let uuid = volant_topic_uuid(numeric_id);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"topic-id-fetch"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("orders", &batch)),
    )
    .await;

    let body = fetch_v13_body(&uuid, 0, 9);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 13, 42, Some("flex-tid"), &body),
    )
    .await;
    let mut src = resp.freeze();
    // Response header v1
    assert_eq!(src.get_i32(), 42);
    skip_tag_buffer(&mut src).unwrap();

    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top error
    assert_eq!(src.get_i32(), 9); // session echo
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    let resp_uuid = get_uuid(&mut src).unwrap();
    assert_eq!(resp_uuid, uuid);
    let n_parts = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let hwm = src.get_i64();
    assert!(hwm >= 1, "hwm {hwm}");
    let lso = src.get_i64();
    assert_eq!(lso, hwm);
    let _log_start = src.get_i64();
    let n_aborted = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_aborted, 0);
    assert_eq!(src.get_i32(), -1); // preferred_read_replica
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty(), "expected record batch bytes");
    skip_tag_buffer(&mut src).unwrap(); // partition tags
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    skip_tag_buffer(&mut src).unwrap(); // top-level
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v13_unknown_topic_id() {
    let dir = temp_dir("p68", "unknown");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Non-volant UUID → UnknownTopicId (100).
    let mut bad = [0u8; 16];
    bad[0] = 0xde;
    bad[1] = 0xad;
    let body = fetch_v13_body(&bad, 0, 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 13, 7, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), bad);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 100); // UNKNOWN_TOPIC_ID

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v13_zero_uuid_unknown() {
    let dir = temp_dir("p68", "zero");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = fetch_v13_body(&KAFKA_UUID_ZERO, 0, 0);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 13, 3, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    skip_tag_buffer(&mut src).unwrap();
    let _ = src.get_i32();
    let _ = src.get_i16();
    let _ = src.get_i32();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), KAFKA_UUID_ZERO);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 100);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v12_still_name_based() {
    let dir = temp_dir("p68", "v12");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("named", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"name"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("named", &batch)),
    )
    .await;

    // v12 still uses compact topic name (phase54 path).
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(0);
    body.put_i32(-1);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "named");
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i64(0);
    body.put_i32(-1);
    body.put_i64(-1);
    body.put_i32(1_000_000);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 0);
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);

    let resp = rpc(
        &addr,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    // Still a name, not a UUID.
    use volant_broker::kafka::codec::get_compact_string;
    assert_eq!(get_compact_string(&mut src).unwrap(), "named");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
