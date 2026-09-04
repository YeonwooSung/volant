//! v0.237: Kafka DescribeTopicPartitions key 75 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, rpc, temp_dir};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_uuid, put_compact_array_len, put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
    volant_topic_uuid,
};
use volant_broker::Broker;
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn dtp_v0(topics: &[&str], limit: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, topics.len());
    for t in topics {
        put_compact_string(&mut body, t);
        put_empty_tag_buffer(&mut body);
    }
    body.put_i32(limit);
    body.put_i8(-1); // null cursor
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn read_int_array(src: &mut impl Buf) -> Vec<i32> {
    let n = get_compact_array_len(src).unwrap().unwrap_or(0);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(src.get_i32());
    }
    out
}

struct DtpPart {
    error: i16,
    index: i32,
    leader: i32,
    epoch: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}

struct DtpTopic {
    error: i16,
    name: String,
    topic_id: [u8; 16],
    is_internal: u8,
    parts: Vec<DtpPart>,
    authorized_ops: i32,
}

fn read_dtp_body(src: &mut impl Buf) -> (i32, Vec<DtpTopic>, bool) {
    let throttle = src.get_i32();
    let n = get_compact_array_len(src).unwrap().unwrap_or(0);
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let error = src.get_i16();
        let name = get_compact_nullable_string(src)
            .unwrap()
            .unwrap_or_default();
        let topic_id = get_uuid(src).unwrap();
        let is_internal = src.get_u8();
        let pn = get_compact_array_len(src).unwrap().unwrap_or(0);
        let mut parts = Vec::with_capacity(pn);
        for _ in 0..pn {
            let perror = src.get_i16();
            let index = src.get_i32();
            let leader = src.get_i32();
            let epoch = src.get_i32();
            let replicas = read_int_array(src);
            let isr = read_int_array(src);
            let _offline = read_int_array(src);
            skip_tag_buffer(src).unwrap();
            parts.push(DtpPart {
                error: perror,
                index,
                leader,
                epoch,
                replicas,
                isr,
            });
        }
        let authorized_ops = src.get_i32();
        skip_tag_buffer(src).unwrap();
        topics.push(DtpTopic {
            error,
            name,
            topic_id,
            is_internal,
            parts,
            authorized_ops,
        });
    }
    let next_present = src.get_i8() == 0;
    if next_present {
        let _ = get_compact_nullable_string(src).unwrap();
        let _ = src.get_i32();
        skip_tag_buffer(src).unwrap();
    }
    skip_tag_buffer(src).unwrap();
    (throttle, topics, next_present)
}

fn boot_single(label: &str) -> (std::path::PathBuf, Arc<Broker>) {
    let dir = temp_dir("v237", label);
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    (dir, broker)
}

#[tokio::test]
async fn api_versions_lists_describe_topic_partitions_75() {
    let (dir, broker) = boot_single("api");
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
    assert_eq!(found.get(&75), Some(&(0, 0)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_topic_partitions_matches_metadata() {
    let (dir, broker) = boot_single("match");
    broker.create_topic(TopicName::new("events"), 3).unwrap();
    let snap = broker.metadata(Some(&[TopicName::new("events".to_string())]));
    let expected = snap.topics.first().expect("events in metadata");
    let expected_uuid = volant_topic_uuid(expected.topic_id.0);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(75, 0, 10, Some("admin"), &dtp_v0(&["events"], 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    let (throttle, topics, next_present) = read_dtp_body(&mut src);
    assert_eq!(throttle, 0);
    assert!(!next_present);
    assert_eq!(topics.len(), 1);
    let t = &topics[0];
    assert_eq!(t.error, 0);
    assert_eq!(t.name, "events");
    assert_eq!(t.topic_id, expected_uuid);
    assert_eq!(t.is_internal, 0);
    assert_eq!(t.authorized_ops, i32::MIN);
    assert_eq!(t.parts.len(), expected.partitions.len());
    for (got, want) in t.parts.iter().zip(expected.partitions.iter()) {
        assert_eq!(got.error, 0);
        assert_eq!(got.index, want.partition_id.0 as i32);
        assert_eq!(got.leader, want.leader as i32);
        assert_eq!(got.epoch, want.leader_epoch as i32);
        let replicas: Vec<i32> = want.replicas.iter().map(|&r| r as i32).collect();
        let isr: Vec<i32> = want.isr.iter().map(|&r| r as i32).collect();
        assert_eq!(got.replicas, replicas);
        assert_eq!(got.isr, isr);
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_topic_partitions_unknown_is_3() {
    let (dir, broker) = boot_single("unk");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(75, 0, 11, Some("admin"), &dtp_v0(&["missing"], 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    let (throttle, topics, next_present) = read_dtp_body(&mut src);
    assert_eq!(throttle, 0);
    assert!(!next_present);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].error, 3);
    assert_eq!(topics[0].name, "missing");
    assert!(topics[0].parts.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_topic_partitions_v1_is_35() {
    let (dir, broker) = boot_single("v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(75, 1, 12, Some("admin"), &dtp_v0(&["events"], 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 12);
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
