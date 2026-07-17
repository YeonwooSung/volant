//! Phase 74: ListOffsets v7–11 special timestamps (max / local / tiered).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_string, put_bytes, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

/// Flexible ListOffsets body for v6–9 / v11 (no TimeoutMs).
fn list_offsets_flex(topic: &str, partition: i32, timestamp: i64) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_u8(0); // isolation
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(timestamp);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

/// Flexible ListOffsets v10+ with TimeoutMs.
fn list_offsets_v10(topic: &str, partition: i32, timestamp: i64, timeout_ms: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_u8(0);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i32(-1);
    body.put_i64(timestamp);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body.put_i32(timeout_ms);
    put_empty_tag_buffer(&mut body);
    body
}

async fn produce_ts(addr: &str, topic: &str, timestamp_ms: i64, value: &'static [u8]) {
    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms,
        headers: vec![],
    }]);
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(1000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));
    let resp = rpc(addr, encode_request(0, 0, 1, Some("p"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 1);
    let _ = volant_broker::kafka::codec::get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
}

fn parse_one_partition(src: &mut impl Buf) -> (i16, i64, i64) {
    assert_eq!(get_compact_array_len(src).unwrap(), Some(1));
    let _topic = get_compact_string(src).unwrap();
    assert_eq!(get_compact_array_len(src).unwrap(), Some(1));
    let _part = src.get_i32();
    let err = src.get_i16();
    let ts = src.get_i64();
    let off = src.get_i64();
    let _epoch = src.get_i32();
    skip_tag_buffer(src).unwrap();
    skip_tag_buffer(src).unwrap();
    skip_tag_buffer(src).unwrap();
    (err, ts, off)
}

#[tokio::test]
async fn api_versions_list_offsets_max_11() {
    let dir = temp_dir("p74", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut found = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        if key == 2 {
            found = Some((min_v, max_v));
        }
    }
    assert_eq!(found, Some((0, 11)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v7_max_timestamp() {
    let dir = temp_dir("p74", "max-ts");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    produce_ts(&addr, "orders", 1_000, b"a").await;
    produce_ts(&addr, "orders", 5_000, b"b").await; // max ts at offset 1
    produce_ts(&addr, "orders", 2_000, b"c").await; // lower ts later offset

    let resp = rpc(
        &addr,
        encode_request_flexible(2, 7, 10, Some("c"), &list_offsets_flex("orders", 0, -3)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, ts, off) = parse_one_partition(&mut src);
    assert_eq!(err, 0);
    assert_eq!(ts, 5_000); // actual max timestamp
    assert_eq!(off, 1); // offset of max-ts record
    assert_eq!(src.remaining(), 0);

    // Empty partition
    broker.create_topic("empty", 1).unwrap();
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 7, 11, Some("c"), &list_offsets_flex("empty", 0, -3)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, ts, off) = parse_one_partition(&mut src);
    assert_eq!(err, 0);
    assert_eq!(ts, -1);
    assert_eq!(off, -1);

    // v6 still rejects -3 as InvalidTimestamp
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 6, 12, Some("c"), &list_offsets_flex("orders", 0, -3)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, _, _) = parse_one_partition(&mut src);
    assert_eq!(err, 32); // InvalidTimestamp

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v8_earliest_local() {
    let dir = temp_dir("p74", "local");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_ts(&addr, "t", 100, b"x").await;
    produce_ts(&addr, "t", 200, b"y").await;

    let resp = rpc(
        &addr,
        encode_request_flexible(2, 8, 20, Some("c"), &list_offsets_flex("t", 0, -4)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, ts, off) = parse_one_partition(&mut src);
    assert_eq!(err, 0);
    assert_eq!(ts, -4);
    assert_eq!(off, 0); // earliest

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v9_tiered_and_v11_pending_empty() {
    let dir = temp_dir("p74", "tiered");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_ts(&addr, "t", 1, b"z").await;

    // LATEST_TIERED (-5) → no remote → -1/-1
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 9, 30, Some("c"), &list_offsets_flex("t", 0, -5)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, ts, off) = parse_one_partition(&mut src);
    assert_eq!(err, 0);
    assert_eq!(ts, -1);
    assert_eq!(off, -1);

    // EARLIEST_PENDING_UPLOAD (-6) on v11
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 11, 31, Some("c"), &list_offsets_flex("t", 0, -6)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 31);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, ts, off) = parse_one_partition(&mut src);
    assert_eq!(err, 0);
    assert_eq!(ts, -1);
    assert_eq!(off, -1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v10_timeout_ms_ignored() {
    let dir = temp_dir("p74", "timeout");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_ts(&addr, "t", 50, b"a").await;
    produce_ts(&addr, "t", 60, b"b").await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            2,
            10,
            40,
            Some("c"),
            &list_offsets_v10("t", 0, -1, 5_000),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 40);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let (err, ts, off) = parse_one_partition(&mut src);
    assert_eq!(err, 0);
    assert_eq!(ts, -1);
    assert_eq!(off, 2); // latest

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v12_unsupported_header_v1() {
    let dir = temp_dir("p74", "v12");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(2, 12, 50, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 50);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
