//! v0.287: Kafka AlterShareGroupOffsets key 91 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_uuid, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
};

fn asgo_v0_body(group: &str, topic: &str, partition: i32, start: i64) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(start);
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_alter_share_group_offsets_91() {
    let (_dir, broker) = broker_temp("v287", "api");
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
    assert_eq!(found.get(&91), Some(&(0, 0)));
    assert_eq!(found.get(&90), Some(&(0, 0))); // DescribeShareGroupOffsets still listed

    server.abort();
}

#[tokio::test]
async fn alter_share_group_offsets_v0_is_42_offsets_unchanged() {
    let (_dir, broker) = broker_temp("v287", "reject");
    broker.create_topic("events", 1).unwrap();
    let committed = broker
        .groups()
        .commit_offsets("sg-v287", "", 0, &[("events".into(), 0, 7, "meta".into())])
        .unwrap();
    assert_eq!(committed.error_code, 0);
    let before = broker
        .groups()
        .fetch_offsets("sg-v287", &[("events".into(), 0)])
        .unwrap();
    assert_eq!(before.entries[0].offset, 7);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            91,
            0,
            10,
            Some("c"),
            &asgo_v0_body("sg-v287", "events", 0, 99),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // top-level INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-932 share offsets")
    );
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    let _topic_id = get_uuid(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42); // per-partition INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-932 share offsets")
    );
    skip_tag_buffer(&mut src).unwrap(); // partition tags
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    skip_tag_buffer(&mut src).unwrap(); // top-level tags
    assert_eq!(src.remaining(), 0);

    let after = broker
        .groups()
        .fetch_offsets("sg-v287", &[("events".into(), 0)])
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
async fn alter_share_group_offsets_v1_is_35() {
    let (_dir, broker) = broker_temp("v287", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(91, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn alter_share_group_offsets_acl_deny_is_30() {
    let (_dir, broker) = broker_temp("v287", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            91,
            0,
            11,
            Some("c"),
            &asgo_v0_body("sg-v287", "events", 0, 99),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    let _topic_id = get_uuid(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 30);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
}
