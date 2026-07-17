//! Phase 71: Produce TopicId v13 (+ v10–12 flexible name path).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_nullable_string, get_compact_string, get_uuid, put_compact_array_len,
    put_compact_bytes, put_compact_nullable_string, put_compact_string, put_empty_tag_buffer,
    put_uuid, skip_tag_buffer, volant_topic_uuid, KAFKA_UUID_ZERO,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
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

fn produce_v13_body(topic_uuid: &[u8; 16], batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, topic_uuid);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    put_compact_bytes(&mut body, Some(batch));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn produce_v9_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    put_compact_bytes(&mut body, Some(batch));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_produce_max_13() {
    let dir = temp_dir("p71", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut produce = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 0 {
            produce = Some((min, max));
        }
    }
    assert_eq!(produce, Some((0, 13)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v13_by_topic_id() {
    let dir = temp_dir("p71", "by-id");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let id = broker
        .metadata(Some(&[TopicName::new("orders")]))
        .topics[0]
        .topic_id
        .0;
    let uuid = volant_topic_uuid(id);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"tid-prod"));
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 13, 99, Some("p"), &produce_v13_body(&uuid, &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let base = src.get_i64();
    assert!(base >= 0);
    assert_eq!(src.get_i64(), -1); // log_append_time
    let _log_start = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    skip_tag_buffer(&mut src).unwrap();

    let fetched = broker
        .fetch(
            &TopicName::new("orders"),
            PartitionId(0),
            Offset::new(base as u64),
            1024,
        )
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].value.as_ref(), b"tid-prod");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v13_unknown_topic_id() {
    let dir = temp_dir("p71", "unk");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut bad = [0u8; 16];
    bad[0] = 0xbe;
    let batch = encode_record_batch(&sample_records(b"x"));
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 13, 7, Some("p"), &produce_v13_body(&bad, &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), bad);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 100); // UNKNOWN_TOPIC_ID

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v13_zero_uuid_unknown() {
    let dir = temp_dir("p71", "zero");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let batch = encode_record_batch(&sample_records(b"z"));
    let resp = rpc(
        &addr,
        encode_request_flexible(
            0,
            13,
            3,
            Some("p"),
            &produce_v13_body(&KAFKA_UUID_ZERO, &batch),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), KAFKA_UUID_ZERO);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 100);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v10_name_based_still_works() {
    let dir = temp_dir("p71", "v10");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("named", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v10"));
    // v10 same wire as v9 (name-based flexible).
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 10, 11, Some("p"), &produce_v9_body("named", &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "named");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let base = src.get_i64();
    assert!(base >= 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_v9_still_name_based() {
    let dir = temp_dir("p71", "v9");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("legacy", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v9"));
    let resp = rpc(
        &addr,
        encode_request_flexible(0, 9, 5, Some("p"), &produce_v9_body("legacy", &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    // Name, not UUID.
    assert_eq!(get_compact_string(&mut src).unwrap(), "legacy");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
