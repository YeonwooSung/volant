//! v0.275: Kafka ShareGroupHeartbeat key 76 v1 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, read_unsigned_varint,
    skip_tag_buffer,
};
use volant_broker::kafka::SUPPORTED_APIS;

fn sghb_v1_body(group: &str, member: &str, epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_string(&mut body, member);
    body.put_i32(epoch);
    put_compact_nullable_string(&mut body, None); // RackId
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "events");
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn read_sghb(src: &mut impl Buf) -> (i32, i16, Option<String>, Option<String>, i32, i32, u32) {
    let throttle = src.get_i32();
    let error = src.get_i16();
    let err_msg = get_compact_nullable_string(src).unwrap();
    let member = get_compact_nullable_string(src).unwrap();
    let epoch = src.get_i32();
    let interval = src.get_i32();
    let assignment = read_unsigned_varint(src).unwrap();
    skip_tag_buffer(src).unwrap();
    (
        throttle, error, err_msg, member, epoch, interval, assignment,
    )
}

#[tokio::test]
async fn api_versions_lists_share_group_heartbeat_76() {
    let (_dir, broker) = broker_temp("v275", "api");
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
    assert_eq!(found.get(&76), Some(&(1, 1)));
    assert_eq!(found.get(&12), Some(&(0, 4))); // classic Heartbeat still listed
    assert_eq!(found.get(&68), Some(&(0, 0))); // ConsumerGroupHeartbeat still listed
    assert!(SUPPORTED_APIS.len() >= 75);

    server.abort();
}

#[tokio::test]
async fn share_group_heartbeat_v1_is_42_membership_unchanged() {
    let (_dir, broker) = broker_temp("v275", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = broker.groups().list_group_ids();

    let resp = rpc(
        &addr,
        encode_request_flexible(
            76,
            1,
            10,
            Some("c"),
            &sghb_v1_body("sg-v275", "member-1", 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    let (throttle, error, err_msg, member, epoch, interval, assignment) = read_sghb(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42); // INVALID_REQUEST
    assert_eq!(err_msg.as_deref(), Some("not KIP-932 share group"));
    assert_eq!(member, None);
    assert_eq!(epoch, -1);
    assert_eq!(interval, 0);
    assert_eq!(assignment, 0);
    assert_eq!(src.remaining(), 0);

    assert_eq!(broker.groups().list_group_ids(), before);
    assert!(broker.groups().describe_group("sg-v275").is_none());

    server.abort();
}

#[tokio::test]
async fn share_group_heartbeat_v0_is_35() {
    let (_dir, broker) = broker_temp("v275", "v0");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(76, 0, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}

#[tokio::test]
async fn share_group_heartbeat_v2_is_35() {
    let (_dir, broker) = broker_temp("v275", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(76, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
