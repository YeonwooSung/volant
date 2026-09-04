//! v0.276: Kafka ShareGroupDescribe key 77 v1 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
};

fn describe_v1_body(group: &str, include_ops: bool) -> BytesMut {
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
async fn api_versions_lists_share_group_describe_77() {
    let (_dir, broker) = broker_temp("v276", "api");
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
    assert_eq!(found.get(&77), Some(&(1, 1)));
    assert_eq!(found.get(&15), Some(&(0, 6))); // DescribeGroups still listed
    assert_eq!(found.get(&69), Some(&(0, 0))); // ConsumerGroupDescribe still listed

    server.abort();
}

#[tokio::test]
async fn share_group_describe_v1_is_42_empty_members() {
    let (_dir, broker) = broker_temp("v276", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(77, 1, 10, Some("c"), &describe_v1_body("sg-v276", false)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-932 share group")
    );
    assert_eq!(get_compact_string(&mut src).unwrap(), "sg-v276");
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // groupState
    assert_eq!(src.get_i32(), -1); // groupEpoch
    assert_eq!(src.get_i32(), -1); // assignmentEpoch
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // assignorName
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // empty members
    let _ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
}

#[tokio::test]
async fn share_group_describe_v0_and_v2_are_35() {
    let (_dir, broker) = broker_temp("v276", "ver");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(77, 0, 98, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 98);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    let resp = rpc(&addr, encode_request_flexible(77, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
