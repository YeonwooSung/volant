//! v0.278: Kafka ShareAcknowledge key 79 v1 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_compact_array_len,
    put_compact_nullable_string, put_empty_tag_buffer, put_uuid, read_unsigned_varint,
    skip_tag_buffer,
};

fn share_ack_v1_body(group: &str, member: &str, epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(group));
    put_compact_nullable_string(&mut body, Some(member));
    body.put_i32(epoch);
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, &[0u8; 16]);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    put_compact_array_len(&mut body, 1);
    body.put_i64(0);
    body.put_i64(10);
    put_compact_array_len(&mut body, 1);
    body.put_i8(1); // Accept
    put_empty_tag_buffer(&mut body); // batch tags
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn read_share_ack(src: &mut impl Buf) -> (i32, i16, Option<String>, u32, u32) {
    let throttle = src.get_i32();
    let error = src.get_i16();
    let err_msg = get_compact_nullable_string(src).unwrap();
    let responses = read_unsigned_varint(src).unwrap();
    let endpoints = read_unsigned_varint(src).unwrap();
    skip_tag_buffer(src).unwrap();
    (throttle, error, err_msg, responses, endpoints)
}

#[tokio::test]
async fn api_versions_lists_share_acknowledge_79() {
    let (_dir, broker) = broker_temp("v278", "api");
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
    assert!(found.len() >= 75);
    assert_eq!(found.get(&79), Some(&(1, 1)));
    assert_eq!(found.get(&75), Some(&(0, 0))); // DescribeTopicPartitions
    assert_eq!(found.get(&80), Some(&(0, 0))); // AddRaftVoter

    server.abort();
}

#[tokio::test]
async fn share_acknowledge_v1_is_42_offsets_unchanged() {
    let (_dir, broker) = broker_temp("v278", "reject");
    broker.create_topic("events", 1).unwrap();
    let committed = broker
        .groups()
        .commit_offsets("sg-v278", "", 0, &[("events".into(), 0, 7, "meta".into())])
        .unwrap();
    assert_eq!(committed.error_code, 0);
    let before = broker
        .groups()
        .fetch_offsets("sg-v278", &[("events".into(), 0)])
        .unwrap();
    assert_eq!(before.entries[0].offset, 7);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(79, 1, 10, Some("c"), &share_ack_v1_body("sg-v278", "m1", 1)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    let (throttle, error, err_msg, responses, endpoints) = read_share_ack(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42); // INVALID_REQUEST
    assert_eq!(err_msg.as_deref(), Some("not KIP-932 share acknowledge"));
    assert_eq!(responses, 1); // compact empty
    assert_eq!(endpoints, 1);
    assert_eq!(src.remaining(), 0);

    let after = broker
        .groups()
        .fetch_offsets("sg-v278", &[("events".into(), 0)])
        .unwrap();
    assert_eq!(after.entries[0].offset, before.entries[0].offset);
    assert_eq!(after.entries[0].metadata, before.entries[0].metadata);
    assert_eq!(
        after.entries[0].leader_epoch,
        before.entries[0].leader_epoch
    );

    server.abort();
}

#[tokio::test]
async fn share_acknowledge_v0_and_v2_are_35() {
    let (_dir, broker) = broker_temp("v278", "ver");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(79, 0, 98, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 98);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    let resp = rpc(&addr, encode_request_flexible(79, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
