//! Phase 63: Flexible ListOffsets v6 + OffsetForLeaderEpoch v4.

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

fn list_offsets_v6(topic: &str, partition: i32, timestamp: i64) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_u8(0); // isolation READ_UNCOMMITTED
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(timestamp);
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn ofle_v4(topic: &str, partition: i32, current_epoch: i32, leader_epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id (consumer)
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i32(current_epoch);
    body.put_i32(leader_epoch);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

async fn produce_one(addr: &str, topic: &str) {
    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"x"),
        timestamp_ms: 1,
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
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(volant_broker::kafka::codec::get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
}

#[tokio::test]
async fn api_versions_list_offsets_ofle_flex_maxes() {
    let dir = temp_dir("p63", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        found.insert(key, (min_v, max_v));
    }
    assert_eq!(found.get(&2), Some(&(0, 11))); // Phase 74 special timestamps
    assert_eq!(found.get(&23), Some(&(0, 4))); // OffsetForLeaderEpoch
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v6_flexible_earliest_latest() {
    let dir = temp_dir("p63", "lo");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    produce_one(&addr, "orders").await;
    produce_one(&addr, "orders").await;
    produce_one(&addr, "orders").await;

    // latest (-1)
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 6, 10, Some("c"), &list_offsets_v6("orders", 0, -1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(src.get_i64(), -1); // timestamp echo
    assert_eq!(src.get_i64(), 3); // latest offset (next offset / HWM)
    let _epoch = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // earliest (-2)
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 6, 11, Some("c"), &list_offsets_v6("orders", 0, -2)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i64(), -2);
    assert_eq!(src.get_i64(), 0); // earliest
    let _ = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_v4_flexible_hwm() {
    let dir = temp_dir("p63", "ofle");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "t").await;
    produce_one(&addr, "t").await;

    let resp = rpc(
        &addr,
        encode_request_flexible(23, 4, 20, Some("c"), &ofle_v4("t", 0, -1, -1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(src.get_i32(), 0); // partition
    let epoch = src.get_i32();
    assert!(epoch >= 0);
    assert_eq!(src.get_i64(), 2); // end offset = HWM
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_list_offsets_still_works() {
    let dir = temp_dir("p63", "classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("c", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_one(&addr, "c").await;

    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_u8(0);
    body.put_i32(1);
    put_string(&mut body, "c");
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(-1); // latest, no epoch field in v2
    let resp = rpc(&addr, encode_request(2, 2, 1, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(volant_broker::kafka::codec::get_string(&mut src).unwrap(), "c");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i64(), 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("p63", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // ListOffsets v12 unsupported (v7–11 closed by Phase 74)
    let resp = rpc(
        &addr,
        encode_request_flexible(2, 12, 30, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    // OFLE v5 unsupported
    let resp = rpc(
        &addr,
        encode_request_flexible(23, 5, 31, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 31);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
