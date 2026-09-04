//! v0.284: Kafka DescribeShareGroupOffsets key 90 v0 reject.

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

fn dsgo_v0_body(group: &str, topic: &str, partition: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1); // Topics[] non-null — skipped
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // group tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_describe_share_group_offsets_90() {
    let (_dir, broker) = broker_temp("v284", "api");
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
    assert!(found.len() >= 80);
    assert_eq!(found.get(&90), Some(&(0, 0)));
    assert_eq!(found.get(&83), Some(&(0, 0))); // InitializeShareGroupState still listed

    server.abort();
}

#[tokio::test]
async fn describe_share_group_offsets_v0_is_42_nothing_persisted() {
    let (_dir, broker) = broker_temp("v284", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = broker.groups().list_group_ids();
    let before_offsets = broker.groups().fetch_offsets("sg-v284", &[]).unwrap();

    let resp = rpc(
        &addr,
        encode_request_flexible(90, 0, 10, Some("c"), &dsgo_v0_body("sg-v284", "events", 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "sg-v284");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // empty Topics[]
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST — after Topics[]
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-932 share offsets")
    );
    skip_tag_buffer(&mut src).unwrap(); // group tags
    skip_tag_buffer(&mut src).unwrap(); // top-level tags
    assert_eq!(src.remaining(), 0);

    assert_eq!(broker.groups().list_group_ids(), before);
    assert!(broker.groups().describe_group("sg-v284").is_none());
    let offsets = broker.groups().fetch_offsets("sg-v284", &[]).unwrap();
    assert_eq!(offsets.entries.len(), before_offsets.entries.len());
    assert!(
        offsets.entries.is_empty(),
        "DescribeShareGroupOffsets must not persist offsets/share state"
    );

    server.abort();
}

#[tokio::test]
async fn describe_share_group_offsets_v1_is_35() {
    let (_dir, broker) = broker_temp("v284", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(90, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn describe_share_group_offsets_acl_deny_is_30() {
    let (_dir, broker) = broker_temp("v284", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(90, 0, 11, Some("c"), &dsgo_v0_body("sg-v284", "events", 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "sg-v284");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // empty Topics[]
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
}
