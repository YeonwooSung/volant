//! v0.286: Kafka StreamsGroupDescribe key 89 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    read_unsigned_varint, skip_tag_buffer,
};

fn describe_v0_body(group: &str, include_ops: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, group);
    body.put_u8(u8::from(include_ops));
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_streams_group_describe_89() {
    let (_dir, broker) = broker_temp("v286", "api");
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
    assert!(found.len() >= 85);
    assert_eq!(found.get(&89), Some(&(0, 0)));
    assert_eq!(found.get(&15), Some(&(0, 6))); // DescribeGroups still listed
    assert_eq!(found.get(&69), Some(&(0, 0))); // ConsumerGroupDescribe still listed
    assert_eq!(found.get(&77), Some(&(1, 1))); // ShareGroupDescribe still listed

    server.abort();
}

#[tokio::test]
async fn streams_group_describe_v0_is_42_empty_members_no_wrap() {
    let (_dir, broker) = broker_temp("v286", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let joined = broker
        .groups()
        .join(
            "st-v286",
            "",
            10_000,
            150,
            vec!["events".into()],
            "",
            |_| Some(1),
        )
        .unwrap();
    assert_eq!(joined.error_code, 0);
    assert_eq!(
        broker.groups().describe_group("st-v286").unwrap().members.len(),
        1
    );

    let resp = rpc(
        &addr,
        encode_request_flexible(89, 0, 10, Some("c"), &describe_v0_body("st-v286", false)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-1071 streams group")
    );
    assert_eq!(get_compact_string(&mut src).unwrap(), "st-v286");
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // groupState
    assert_eq!(src.get_i32(), -1); // groupEpoch
    assert_eq!(src.get_i32(), -1); // assignmentEpoch
    assert_eq!(read_unsigned_varint(&mut src).unwrap(), 0); // Topology null
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // empty members
    let _ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    // Does not wrap classic describe / 69 / 15 / 77.
    assert_eq!(
        broker.groups().describe_group("st-v286").unwrap().members.len(),
        1
    );

    server.abort();
}

#[tokio::test]
async fn streams_group_describe_v1_is_35() {
    let (_dir, broker) = broker_temp("v286", "ver");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(89, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn streams_group_describe_acl_deny_is_30() {
    let (_dir, broker) = broker_temp("v286", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(89, 0, 11, Some("c"), &describe_v0_body("st-v286", false)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_compact_string(&mut src).unwrap(), "st-v286");
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // groupState
    assert_eq!(src.get_i32(), -1);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(read_unsigned_varint(&mut src).unwrap(), 0); // Topology null
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // empty members
    let _ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
}
