//! v0.291: Kafka StreamsGroupTopologyDescriptionUpdate key 93 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::kafka::SUPPORTED_APIS;

fn sgtdu_v0_body(group: &str, member: &str, epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_string(&mut body, member);
    body.put_i32(epoch);
    put_compact_array_len(&mut body, 0); // Subtopologies[]
    put_compact_array_len(&mut body, 0); // GlobalStores[]
    put_empty_tag_buffer(&mut body); // TopologyDescription tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_streams_group_topology_description_update_93() {
    let (_dir, broker) = broker_temp("v291", "api");
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
    assert!(found.len() >= 90);
    assert_eq!(found.get(&93), Some(&(0, 0)));
    assert_eq!(found.get(&88), Some(&(0, 0))); // StreamsGroupHeartbeat still listed
    assert_eq!(found.get(&89), Some(&(0, 0))); // StreamsGroupDescribe still listed
    assert_eq!(found.get(&94), Some(&(0, 0))); // UnregisterController still listed
    assert!(SUPPORTED_APIS.len() >= 90);

    server.abort();
}

#[tokio::test]
async fn streams_group_topology_description_update_v0_is_42_membership_unchanged() {
    let (_dir, broker) = broker_temp("v291", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = broker.groups().list_group_ids();

    let resp = rpc(
        &addr,
        encode_request_flexible(
            93,
            0,
            10,
            Some("c"),
            &sgtdu_v0_body("stg-v291", "member-1", 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-1071 streams group")
    );
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(broker.groups().list_group_ids(), before);
    assert!(broker.groups().describe_group("stg-v291").is_none());

    server.abort();
}

#[tokio::test]
async fn streams_group_topology_description_update_v1_is_35() {
    let (_dir, broker) = broker_temp("v291", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(93, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn streams_group_topology_description_update_acl_deny_is_30() {
    let (_dir, broker) = broker_temp("v291", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            93,
            0,
            11,
            Some("c"),
            &sgtdu_v0_body("stg-v291", "member-1", 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    server.abort();
}
