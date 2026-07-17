//! Phase 40: Kafka ListOffsets classic v0–5 on the shim.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_string, put_bytes, put_string,
};
use volant_broker::Broker;
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

/// ListOffsets body for one partition.
fn list_offsets_body(
    version: i16,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    timestamp: i64,
    isolation: u8,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    if version >= 2 {
        body.put_u8(isolation);
    }
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(partition);
    if version >= 4 {
        body.put_i32(current_leader_epoch);
    }
    body.put_i64(timestamp);
    if version == 0 {
        body.put_i32(1); // max_num_offsets
    }
    body
}

async fn produce_n(addr: &str, topic: &str, n: usize) {
    for _ in 0..n {
        let batch = encode_record_batch(&[Record {
            offset: Offset::new(0),
            key: None,
            value: Bytes::from_static(b"x"),
            timestamp_ms: 1,
            headers: vec![],
        }]);
        let mut body = BytesMut::new();
        body.put_i16(1); // acks (Produce v0)
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
        let _ = get_string(&mut src).unwrap();
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i32(), 0);
        assert_eq!(src.get_i16(), 0);
    }
}

#[tokio::test]
async fn api_versions_list_offsets_max_5() {
    let dir = temp_dir("p40", "api");
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
    assert_eq!(found, Some((0, 11))); // Phase 74 special timestamps
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v2_throttle_and_isolation() {
    let dir = temp_dir("p40", "v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_n(&addr, "orders", 3).await;

    // READ_COMMITTED isolation (1) — same offsets as uncommitted under buffer-until-commit
    let body = list_offsets_body(2, "orders", 0, -1, -1, 1);
    let resp = rpc(&addr, encode_request(2, 2, 10, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(src.get_i64(), -1); // timestamp echo
    assert_eq!(src.get_i64(), 3); // latest offset

    // earliest
    let body = list_offsets_body(2, "orders", 0, -1, -2, 0);
    let resp = rpc(&addr, encode_request(2, 2, 11, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i64(), -2);
    assert_eq!(src.get_i64(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v4_leader_epoch_and_fence() {
    let dir = temp_dir("p40", "v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    produce_n(&addr, "orders", 1).await;

    // Happy path: current_leader_epoch = -1
    let body = list_offsets_body(4, "orders", 0, -1, -1, 0);
    let resp = rpc(&addr, encode_request(2, 4, 20, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i64(), 1);
    let epoch = src.get_i32();
    assert!(epoch >= 0);

    // Client ahead of broker epoch → UNKNOWN_LEADER_EPOCH (75)
    let body = list_offsets_body(5, "orders", 0, 99, -1, 0);
    let resp = rpc(&addr, encode_request(2, 5, 21, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 21);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 75);
    let _ = src.get_i64();
    assert_eq!(src.get_i64(), -1);
    let _ = src.get_i32(); // leader_epoch in error response

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v5_invalid_timestamp() {
    let dir = temp_dir("p40", "ts");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = list_offsets_body(5, "orders", 0, -1, 1_700_000_000_000, 0);
    let resp = rpc(&addr, encode_request(2, 5, 30, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 32); // INVALID_TIMESTAMP

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_unknown_topic() {
    let dir = temp_dir("p40", "missing");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = list_offsets_body(3, "nope", 0, -1, -1, 0);
    let resp = rpc(&addr, encode_request(2, 3, 40, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 40);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "nope");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
