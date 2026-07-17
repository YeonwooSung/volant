//! Phase 84: Fetch v14–18 (Kafka max wire ratchet).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_bytes, get_compact_nullable_string, get_compact_string, get_uuid, put_bytes,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_nullable_string,
    put_string, put_unsigned_varint, put_uuid, read_unsigned_varint, skip_tag_buffer,
    volant_topic_uuid,
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

/// Fetch v13/v14 body: TopicId + top-level ReplicaId (≤v14).
fn fetch_topic_id_with_replica(
    topic_uuid: &[u8; 16],
    fetch_offset: i64,
    session_id: i32,
    current_leader_epoch: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id (v0–14)
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(session_id);
    body.put_i32(-1); // session_epoch
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, topic_uuid);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0); // partition
    body.put_i32(current_leader_epoch);
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

/// Fetch v15+ body: no top-level ReplicaId; optional ReplicaState in tags.
fn fetch_v15_plus_body(
    topic_uuid: &[u8; 16],
    fetch_offset: i64,
    session_id: i32,
    current_leader_epoch: i32,
    with_replica_state: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    // No replica_id — starts at MaxWaitMs (KIP-903).
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation
    body.put_i32(session_id);
    body.put_i32(-1); // session_epoch
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, topic_uuid);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0); // partition
    body.put_i32(current_leader_epoch);
    body.put_i64(fetch_offset);
    body.put_i32(-1); // last_fetched_epoch
    body.put_i64(-1); // log_start_offset
    body.put_i32(1_000_000); // partition_max_bytes
    put_empty_tag_buffer(&mut body); // partition tags (v17+/v18 tags ignored when empty)
    put_empty_tag_buffer(&mut body); // topic tags
    put_compact_array_len(&mut body, 0); // forgotten
    put_compact_string(&mut body, ""); // rack_id
    if with_replica_state {
        // Tag 1 = ReplicaState { ReplicaId int32, ReplicaEpoch int64, tags }
        let mut value = BytesMut::new();
        value.put_i32(-1); // replica_id
        value.put_i64(-1); // replica_epoch
        put_empty_tag_buffer(&mut value);
        put_unsigned_varint(&mut body, 1); // one tag
        put_unsigned_varint(&mut body, 1); // tag 1
        put_unsigned_varint(&mut body, value.len() as u32);
        body.extend_from_slice(&value);
    } else {
        put_empty_tag_buffer(&mut body);
    }
    body
}

fn assert_fetch_success_header(src: &mut Bytes, corr: i32, session: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top error
    assert_eq!(src.get_i32(), session);
}

#[tokio::test]
async fn api_versions_fetch_max_18() {
    let dir = temp_dir("p84", "api");
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
    assert_eq!(fetch, Some((0, 18)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v14_topic_id_round_trip() {
    let dir = temp_dir("p84", "v14");
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

    let batch = encode_record_batch(&sample_records(b"v14-fetch"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("orders", &batch)),
    )
    .await;

    let body = fetch_topic_id_with_replica(&uuid, 0, 11, -1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 14, 42, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_fetch_success_header(&mut src, 42, 11);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    assert!(hwm >= 1);
    let lso = src.get_i64();
    assert_eq!(lso, hwm);
    let _log_start = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i32(), -1);
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty());
    skip_tag_buffer(&mut src).unwrap(); // partition tags empty
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    skip_tag_buffer(&mut src).unwrap(); // top-level (no NodeEndpoints on v14)
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v15_no_top_level_replica_id() {
    let dir = temp_dir("p84", "v15");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let tid = broker.metadata(None).topics[0].topic_id.0;
    let uuid = volant_topic_uuid(tid);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v15"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;

    let body = fetch_v15_plus_body(&uuid, 0, 5, -1, true);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 15, 7, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_fetch_success_header(&mut src, 7, 5);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _hwm = src.get_i64();
    let _lso = src.get_i64();
    let _ls = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i32(), -1);
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty());
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // empty top tags (v15 < 16)
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v16_success_empty_node_endpoints() {
    let dir = temp_dir("p84", "v16ok");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let tid = broker.metadata(None).topics[0].topic_id.0;
    let uuid = volant_topic_uuid(tid);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v16ok"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;

    let body = fetch_v15_plus_body(&uuid, 0, 0, -1, false);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 16, 8, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_fetch_success_header(&mut src, 8, 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    let _ = get_uuid(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    src.advance(8 + 8 + 8); // hwm, lso, log_start
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i32(), -1);
    let _ = get_compact_bytes(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // partition
    skip_tag_buffer(&mut src).unwrap(); // topic
    // Success → empty top-level tags (no NodeEndpoints).
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v16_fenced_includes_node_endpoints() {
    let dir = temp_dir("p84", "v16fence");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let node = broker.node_id() as i32;
    broker
        .set_partition_leader_epoch(&TopicName::new("t"), PartitionId(0), 3)
        .unwrap();
    let tid = broker.metadata(None).topics[0].topic_id.0;
    let uuid = volant_topic_uuid(tid);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Client epoch 0 < current 3 → FencedLeaderEpoch + CurrentLeader + NodeEndpoints.
    let body = fetch_v15_plus_body(&uuid, 0, 0, 0, false);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 16, 9, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    let _ = get_uuid(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 74); // FencedLeaderEpoch
    let _ = src.get_i64(); // hwm
    let _ = src.get_i64(); // lso
    let _ = src.get_i64(); // log_start
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i32(), -1);
    let _ = get_compact_bytes(&mut src).unwrap();
    // Partition tags: CurrentLeader tag 1
    let n = read_unsigned_varint(&mut src).unwrap();
    assert_eq!(n, 1);
    let tag = read_unsigned_varint(&mut src).unwrap();
    assert_eq!(tag, 1);
    let len = read_unsigned_varint(&mut src).unwrap() as usize;
    let mut cl = src.copy_to_bytes(len);
    assert_eq!(cl.get_i32(), node);
    assert_eq!(cl.get_i32(), 3);
    skip_tag_buffer(&mut cl).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    // Top-level: NodeEndpoints tag 0
    let n = read_unsigned_varint(&mut src).unwrap();
    assert_eq!(n, 1, "one top-level tag");
    let tag = read_unsigned_varint(&mut src).unwrap();
    assert_eq!(tag, 0, "NodeEndpoints is tag 0");
    let len = read_unsigned_varint(&mut src).unwrap() as usize;
    let mut ep = src.copy_to_bytes(len);
    assert_eq!(get_compact_array_len(&mut ep).unwrap().unwrap(), 1);
    assert_eq!(ep.get_i32(), node);
    let host = get_compact_string(&mut ep).unwrap();
    assert!(!host.is_empty());
    let port = ep.get_i32();
    assert!(port > 0);
    assert!(get_compact_nullable_string(&mut ep).unwrap().is_none()); // rack
    skip_tag_buffer(&mut ep).unwrap();
    assert_eq!(ep.remaining(), 0);
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v18_kafka_max_round_trip() {
    let dir = temp_dir("p84", "v18");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let tid = broker.metadata(None).topics[0].topic_id.0;
    let uuid = volant_topic_uuid(tid);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v18"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;

    // v18 request: same as v15+ plus optional partition HighWatermark tag (empty here).
    let body = fetch_v15_plus_body(&uuid, 0, 3, -1, false);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 18, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_fetch_success_header(&mut src, 10, 3);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    assert!(hwm >= 1);
    let lso = src.get_i64();
    assert_eq!(lso, hwm);
    let _ = src.get_i64();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 0);
    assert_eq!(src.get_i32(), -1);
    let records = get_compact_bytes(&mut src).unwrap().unwrap();
    assert!(!records.is_empty());
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap(); // empty NodeEndpoints on success
    assert_eq!(src.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v13_still_works() {
    let dir = temp_dir("p84", "v13");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let tid = broker.metadata(None).topics[0].topic_id.0;
    let uuid = volant_topic_uuid(tid);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&sample_records(b"v13"));
    let _ = rpc(
        &addr,
        encode_request(0, 5, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;

    let body = fetch_topic_id_with_replica(&uuid, 0, 0, -1);
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 13, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_fetch_success_header(&mut src, 11, 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v19_unsupported_header_v1() {
    let dir = temp_dir("p84", "v19");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // v19 not handled; flexible response header v1 (version ≥12).
    let resp = rpc(
        &addr,
        encode_request_flexible(1, 19, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
