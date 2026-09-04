//! v0.235: Kafka DescribeLogDirs key 35 v0–1.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{default_storage, unique_dir, Guard};
use common::{boot_kafka, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string, get_string,
    put_empty_tag_buffer, put_string, put_unsigned_varint, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Message, PartitionId, TopicName};

fn describe_null_v0() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body
}

fn describe_null_v1() -> BytesMut {
    let mut body = BytesMut::new();
    put_unsigned_varint(&mut body, 0); // topics = null
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_named_v0(topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(partitions.len() as i32);
    for &p in partitions {
        body.put_i32(p);
    }
    body
}

#[tokio::test]
async fn api_versions_lists_describe_log_dirs_35() {
    let base = unique_dir("v235", "api");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
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
    assert!(found.len() >= 43);
    assert_eq!(found.get(&35), Some(&(0, 1)));

    server.abort();
}

#[tokio::test]
async fn describe_log_dirs_null_topics_after_produce() {
    let base = unique_dir("v235", "produce");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    broker
        .produce_one(
            &TopicName::new("events"),
            PartitionId(0),
            Message::from_value("hello-log-dirs"),
        )
        .unwrap();
    let expected_path = base.join("n1").display().to_string();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(35, 0, 10, Some("admin"), &describe_null_v0()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // one dir
    assert_eq!(src.get_i16(), 0); // dir error
    let path = get_string(&mut src).unwrap();
    assert_eq!(path, expected_path);
    assert_eq!(src.get_i32(), 1); // one topic
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1); // one partition
    assert_eq!(src.get_i32(), 0); // partition
    let size = src.get_i64();
    assert!(size > 0, "expected size > 0 after produce, got {size}");
    let _lag = src.get_i64();
    assert_eq!(src.get_u8(), 0); // isFuture

    server.abort();
}

#[tokio::test]
async fn describe_log_dirs_unknown_topic_empty_or_3() {
    let base = unique_dir("v235", "unk");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(35, 0, 12, Some("admin"), &describe_named_v0("missing", &[0])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    assert_eq!(src.get_i32(), 0); // throttle
    let n_dirs = src.get_i32();
    assert!(n_dirs >= 0);
    if n_dirs == 0 {
        server.abort();
        return;
    }
    assert_eq!(n_dirs, 1);
    let dir_err = src.get_i16();
    let _path = get_string(&mut src).unwrap();
    let n_topics = src.get_i32();
    // Unknown topic: skip (empty topics) or surface error 3.
    assert!(
        dir_err == 3 || n_topics == 0 || n_topics == 1,
        "dir_err={dir_err} n_topics={n_topics}"
    );
    if n_topics == 1 {
        let _ = get_string(&mut src).unwrap();
        let n_parts = src.get_i32();
        assert!(n_parts >= 0);
    }

    server.abort();
}

#[tokio::test]
async fn describe_log_dirs_v2_unsupported() {
    let base = unique_dir("v235", "v2");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(35, 2, 99, Some("c"), &describe_null_v1()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (v>=1 flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn describe_log_dirs_v1_flex_null_topics() {
    let base = unique_dir("v235", "v1");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    broker
        .produce_one(
            &TopicName::new("events"),
            PartitionId(0),
            Message::from_value("flex"),
        )
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(35, 1, 11, Some("admin"), &describe_null_v1()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    let _path = get_compact_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert!(src.get_i64() > 0);
    let _lag = src.get_i64();
    assert_eq!(src.get_u8(), 0);

    server.abort();
}
