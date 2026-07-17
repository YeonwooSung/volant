//! Phase 25: Kafka CreateTopics / DeleteTopics / ListOffsets on the shim.

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

#[tokio::test]
async fn api_versions_includes_admin_keys() {
    let dir = temp_dir("p25", "api");
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
    let mut keys = Vec::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        keys.push(key);
        assert!(min_v <= max_v);
    }
    assert!(keys.contains(&2)); // ListOffsets
    assert!(keys.contains(&19)); // CreateTopics
    assert!(keys.contains(&20)); // DeleteTopics
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_topics_produce_list_offsets_delete() {
    let dir = temp_dir("p25", "lifecycle");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateTopics v1 (error_message, no throttle)
    let mut body = BytesMut::new();
    body.put_i32(1); // topic count
    put_string(&mut body, "orders");
    body.put_i32(2); // num_partitions
    body.put_i16(1); // replication_factor (ignored)
    body.put_i32(0); // replica assignments
    body.put_i32(0); // configs
    body.put_i32(5000); // timeout
    body.put_u8(0); // validate_only
    let resp = rpc(&addr, encode_request(19, 1, 10, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i16(), 0, "create topics error");
    assert_eq!(get_nullable_string(&mut src).unwrap(), None); // error_message

    // Topic exists on native path.
    let meta = broker.metadata(Some(&[TopicName::new("orders")]));
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(meta.topics[0].partitions.len(), 2);

    // Duplicate create → TOPIC_ALREADY_EXISTS (36)
    let mut body2 = BytesMut::new();
    body2.put_i32(1);
    put_string(&mut body2, "orders");
    body2.put_i32(1);
    body2.put_i16(1);
    body2.put_i32(0);
    body2.put_i32(0);
    body2.put_i32(5000); // timeout
    let resp2 = rpc(&addr, encode_request(19, 0, 11, Some("admin"), &body2)).await;
    let mut s2 = resp2.freeze();
    s2.advance(4); // corr
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(get_string(&mut s2).unwrap(), "orders");
    assert_eq!(s2.get_i16(), 36);

    // Produce one RecordBatch to partition 0
    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"hello"),
        timestamp_ms: 1,
        headers: vec![],
    }]);
    let mut pbody = BytesMut::new();
    pbody.put_i16(1);
    pbody.put_i32(1000);
    pbody.put_i32(1);
    put_string(&mut pbody, "orders");
    pbody.put_i32(1);
    pbody.put_i32(0);
    put_bytes(&mut pbody, Some(&batch));
    let presp = rpc(&addr, encode_request(0, 0, 12, Some("p"), &pbody)).await;
    let mut ps = presp.freeze();
    ps.advance(4 + 4);
    let _ = get_string(&mut ps).unwrap();
    ps.advance(4 + 4);
    assert_eq!(ps.get_i16(), 0);

    // ListOffsets v1: earliest + latest on p0
    let mut lbody = BytesMut::new();
    lbody.put_i32(-1); // replica
    lbody.put_i32(1); // topics
    put_string(&mut lbody, "orders");
    lbody.put_i32(2); // partitions
    lbody.put_i32(0);
    lbody.put_i64(-2); // earliest
    lbody.put_i32(0);
    lbody.put_i64(-1); // latest
    let lresp = rpc(&addr, encode_request(2, 1, 13, Some("lo"), &lbody)).await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 13);
    assert_eq!(ls.get_i32(), 1);
    assert_eq!(get_string(&mut ls).unwrap(), "orders");
    assert_eq!(ls.get_i32(), 2);
    // partition 0 earliest
    assert_eq!(ls.get_i32(), 0);
    assert_eq!(ls.get_i16(), 0);
    assert_eq!(ls.get_i64(), -2);
    let earliest = ls.get_i64();
    assert_eq!(earliest, 0);
    // partition 0 latest
    assert_eq!(ls.get_i32(), 0);
    assert_eq!(ls.get_i16(), 0);
    assert_eq!(ls.get_i64(), -1);
    let latest = ls.get_i64();
    assert_eq!(latest, 1, "LEO after one produce");

    // DeleteTopics v1 (throttle first)
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    put_string(&mut dbody, "orders");
    dbody.put_i32(5000);
    let dresp = rpc(&addr, encode_request(20, 1, 14, Some("del"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 14);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), "orders");
    assert_eq!(ds.get_i16(), 0);

    assert!(broker
        .metadata(Some(&[TopicName::new("orders")]))
        .topics
        .is_empty()
        || broker
            .fetch(&TopicName::new("orders"), PartitionId(0), Offset::new(0), 1)
            .is_err());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_v0_array_shape() {
    let dir = temp_dir("p25", "lo-v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(1);
    put_string(&mut body, "t");
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(-1); // latest
    body.put_i32(1); // max_num_offsets
    let resp = rpc(&addr, encode_request(2, 0, 1, Some("lo"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 1); // offset array len
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i64(), 0); // empty log LEO

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
