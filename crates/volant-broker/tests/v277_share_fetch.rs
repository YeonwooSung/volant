//! v0.277: Kafka ShareFetch key 78 v1 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    put_compact_array_len, put_compact_nullable_string, put_empty_tag_buffer, put_uuid,
    skip_tag_buffer,
};
use volant_core::{Message, PartitionId, TopicName};

fn share_fetch_v1_body(group: Option<&str>) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, group);
    put_compact_nullable_string(&mut body, Some("member-1"));
    body.put_i32(0); // ShareSessionEpoch — official 0 would open a session
    body.put_i32(500); // MaxWaitMs
    body.put_i32(1); // MinBytes
    body.put_i32(i32::MAX); // MaxBytes
    body.put_i32(500); // MaxRecords (v1+)
    body.put_i32(100); // BatchSize (v1+)
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, &[0u8; 16]);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0); // PartitionIndex
    put_compact_array_len(&mut body, 1); // AcknowledgementBatches
    body.put_i64(0);
    body.put_i64(0);
    put_compact_array_len(&mut body, 1);
    body.put_i8(1); // Accept
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 0); // ForgottenTopicsData
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn read_share_fetch(
    src: &mut impl Buf,
) -> (i32, i16, Option<String>, i32, Option<usize>, Option<usize>) {
    let throttle = src.get_i32();
    let error = src.get_i16();
    let err_msg = get_compact_nullable_string(src).unwrap();
    let lock_ms = src.get_i32();
    let responses = get_compact_array_len(src).unwrap();
    let endpoints = get_compact_array_len(src).unwrap();
    skip_tag_buffer(src).unwrap();
    (throttle, error, err_msg, lock_ms, responses, endpoints)
}

#[tokio::test]
async fn api_versions_lists_share_fetch_78() {
    let (_dir, broker) = broker_temp("v277", "api");
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
    assert_eq!(found.get(&78), Some(&(1, 1)));
    assert_eq!(found.get(&1), Some(&(0, 18))); // Fetch still listed

    server.abort();
}

#[tokio::test]
async fn share_fetch_v1_is_42_no_records_written() {
    let (_dir, broker) = broker_temp("v277", "reject");
    let topic = TopicName::new("events");
    broker.create_topic(topic.clone(), 1).unwrap();
    broker
        .produce_one(&topic, PartitionId(0), Message::from_value("keep"))
        .unwrap();
    let before = broker.log_end_offset(&topic, PartitionId(0)).unwrap();
    assert!(before > 0);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(78, 1, 10, Some("c"), &share_fetch_v1_body(Some("sg-v277"))),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    let (throttle, error, err_msg, lock_ms, responses, endpoints) = read_share_fetch(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42); // INVALID_REQUEST
    assert_eq!(err_msg.as_deref(), Some("not KIP-932 share fetch"));
    assert_eq!(lock_ms, 0);
    assert_eq!(responses, Some(0));
    assert_eq!(endpoints, Some(0));
    assert_eq!(src.remaining(), 0);

    assert_eq!(
        broker.log_end_offset(&topic, PartitionId(0)).unwrap(),
        before,
        "ShareFetch must not write or acquire records"
    );

    server.abort();
}

#[tokio::test]
async fn share_fetch_v0_is_35() {
    let (_dir, broker) = broker_temp("v277", "v0");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(78, 0, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}

#[tokio::test]
async fn share_fetch_v2_is_35() {
    let (_dir, broker) = broker_temp("v277", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(78, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
