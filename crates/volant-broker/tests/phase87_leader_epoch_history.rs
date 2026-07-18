//! Phase 87: durable OffsetForLeaderEpoch history (MVP).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_nullable_string, get_string, put_bytes, put_string,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn ofle_body(
    version: i16,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    if version >= 3 {
        body.put_i32(-1); // replica_id
    }
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(partition);
    if version >= 2 {
        body.put_i32(current_leader_epoch);
    }
    body.put_i32(leader_epoch);
    body
}

async fn produce_one_async(addr: &str, topic: &str) {
    let records = vec![Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(b"v"),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }];
    let batch = encode_record_batch(&records);
    let mut body = BytesMut::new();
    body.put_i16(-1); // nullable transactional_id (Produce v3)
    body.put_i16(1); // acks
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));
    let resp = rpc(addr, encode_request(0, 3, 1, Some("p"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1); // corr
    let topics = src.get_i32();
    assert_eq!(topics, 1);
    let _ = get_string(&mut src).unwrap();
    let parts = src.get_i32();
    assert_eq!(parts, 1);
    let _pid = src.get_i32();
    let err = src.get_i16();
    assert_eq!(err, 0, "produce failed");
}

fn parse_ofle_v2(resp: BytesMut, corr: i32) -> (i16, i32, i64) {
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1); // partitions
    let err = src.get_i16();
    let _pid = src.get_i32();
    let epoch = src.get_i32();
    let end = src.get_i64();
    (err, epoch, end)
}

#[tokio::test]
async fn ofle_prior_epoch_returns_transition_end_not_hwm() {
    let dir = temp_dir("p87", "prior");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Epoch 0: two produces → LEO/HWM = 2
    produce_one_async(&addr, "orders").await;
    produce_one_async(&addr, "orders").await;

    // Bump to epoch 1 at LEO=2
    broker
        .set_partition_leader_epoch(&TopicName::new("orders"), PartitionId(0), 1)
        .expect("bump");

    // Epoch 1: two more produces → HWM = 4
    produce_one_async(&addr, "orders").await;
    produce_one_async(&addr, "orders").await;

    // OFLE for epoch 0 must return end=2 (not current HWM=4)
    let body = ofle_body(2, "orders", 0, -1, 0);
    let resp = rpc(&addr, encode_request(23, 2, 100, Some("m"), &body)).await;
    let (err, epoch, end) = parse_ofle_v2(resp, 100);
    assert_eq!(err, 0);
    assert_eq!(epoch, 0);
    assert_eq!(end, 2, "prior epoch end must be transition LEO, not HWM");

    // Current epoch / latest → HWM=4
    let body = ofle_body(2, "orders", 0, -1, 1);
    let resp = rpc(&addr, encode_request(23, 2, 101, Some("m"), &body)).await;
    let (err, epoch, end) = parse_ofle_v2(resp, 101);
    assert_eq!(err, 0);
    assert_eq!(epoch, 1);
    assert_eq!(end, 4);

    let body = ofle_body(2, "orders", 0, -1, -1);
    let resp = rpc(&addr, encode_request(23, 2, 102, Some("m"), &body)).await;
    let (err, epoch, end) = parse_ofle_v2(resp, 102);
    assert_eq!(err, 0);
    assert_eq!(epoch, 1);
    assert_eq!(end, 4);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_history_survives_restart() {
    let dir = temp_dir("p87", "restart");
    {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        }));
        broker.create_topic("orders", 1).expect("create");
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
        produce_one_async(&addr, "orders").await;
        produce_one_async(&addr, "orders").await;
        produce_one_async(&addr, "orders").await;
        broker
            .set_partition_leader_epoch(&TopicName::new("orders"), PartitionId(0), 2)
            .expect("bump");
        produce_one_async(&addr, "orders").await;
        server.abort();
    }

    // New broker process on same data dir.
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Prior epoch 0 closed at offset 3.
    let body = ofle_body(2, "orders", 0, -1, 0);
    let resp = rpc(&addr, encode_request(23, 2, 200, Some("m"), &body)).await;
    let (err, epoch, end) = parse_ofle_v2(resp, 200);
    assert_eq!(err, 0);
    assert_eq!(epoch, 0);
    assert_eq!(end, 3, "history must survive restart");

    // Current epoch 2 → HWM 4
    let body = ofle_body(2, "orders", 0, -1, -1);
    let resp = rpc(&addr, encode_request(23, 2, 201, Some("m"), &body)).await;
    let (err, epoch, end) = parse_ofle_v2(resp, 201);
    assert_eq!(err, 0);
    assert_eq!(epoch, 2);
    assert_eq!(end, 4);

    // Direct API check.
    let (found, off) = broker
        .offset_for_leader_epoch("orders", 0, 0)
        .expect("lookup");
    assert_eq!(found, 0);
    assert_eq!(off, 3);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metadata_reports_live_leader_epoch() {
    let dir = temp_dir("p87", "meta");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    broker
        .set_partition_leader_epoch(&TopicName::new("orders"), PartitionId(0), 3)
        .expect("bump");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Metadata v7: leader_epoch field after leader id.
    let mut body = BytesMut::new();
    body.put_i32(1); // topics
    put_string(&mut body, "orders");
    body.put_u8(0); // allow_auto_topic_creation
    let resp = rpc(&addr, encode_request(3, 7, 300, Some("m"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 300);
    assert_eq!(src.get_i32(), 0); // throttle
    let n_brokers = src.get_i32();
    for _ in 0..n_brokers {
        let _ = src.get_i32();
        let _ = get_string(&mut src).unwrap();
        let _ = src.get_i32();
        let _ = get_nullable_string(&mut src).unwrap(); // rack
    }
    let _ = get_nullable_string(&mut src).unwrap(); // cluster_id
    let _ = src.get_i32(); // controller
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_u8(), 0); // is_internal
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 0); // partition
    let _leader = src.get_i32();
    let leader_epoch = src.get_i32();
    assert_eq!(leader_epoch, 3, "Metadata must advertise live epoch");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn ofle_unknown_future_epoch_unchanged() {
    let dir = temp_dir("p87", "unknown");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = ofle_body(2, "orders", 0, -1, 99);
    let resp = rpc(&addr, encode_request(23, 2, 400, Some("m"), &body)).await;
    let (err, _epoch, end) = parse_ofle_v2(resp, 400);
    assert_eq!(err, 75); // UNKNOWN_LEADER_EPOCH
    assert_eq!(end, -1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
