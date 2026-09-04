//! v0.281: Kafka WriteShareGroupState key 85 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_uuid, put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_uuid,
    skip_tag_buffer,
};

fn wsgs_v0_body(group: &str, topic_id: &[u8; 16], partition: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, topic_id);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i32(0); // StateEpoch — discarded
    body.put_i32(0); // LeaderEpoch — discarded
    body.put_i64(0); // StartOffset — discarded
    put_compact_array_len(&mut body, 1); // StateBatches[1] — discarded
    body.put_i64(0); // FirstOffset
    body.put_i64(10); // LastOffset
    body.put_i8(2); // DeliveryState
    body.put_i16(1); // DeliveryCount
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

#[tokio::test]
async fn api_versions_lists_write_share_group_state_85() {
    let (_dir, broker) = broker_temp("v281", "api");
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
    assert_eq!(found.get(&85), Some(&(0, 0)));
    assert_eq!(found.get(&83), Some(&(0, 0))); // InitializeShareGroupState still listed
    assert_eq!(found.get(&94), Some(&(0, 0))); // UnregisterController still listed

    server.abort();
}

#[tokio::test]
async fn write_share_group_state_v0_is_42_nothing_persisted() {
    let (_dir, broker) = broker_temp("v281", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = broker.groups().list_group_ids();
    let topic_id = [0x11u8; 16];

    let resp = rpc(
        &addr,
        encode_request_flexible(85, 0, 10, Some("c"), &wsgs_v0_body("sg-v281", &topic_id, 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    // Official response has no throttleTimeMs and no top-level error.
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 1);
    let echoed = get_uuid(&mut src).unwrap();
    assert_eq!(echoed, topic_id);
    let pn = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(pn, 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KIP-932 share state")
    );
    skip_tag_buffer(&mut src).unwrap(); // partition tags
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    skip_tag_buffer(&mut src).unwrap(); // top-level tags
    assert_eq!(src.remaining(), 0);

    assert_eq!(broker.groups().list_group_ids(), before);
    assert!(broker.groups().describe_group("sg-v281").is_none());
    let offsets = broker.groups().fetch_offsets("sg-v281", &[]).unwrap();
    assert!(
        offsets.entries.is_empty(),
        "WriteShareGroupState must not persist offsets/share state"
    );

    server.abort();
}

#[tokio::test]
async fn write_share_group_state_v1_is_35() {
    let (_dir, broker) = broker_temp("v281", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(85, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn write_share_group_state_acl_deny_is_30() {
    let (_dir, broker) = broker_temp("v281", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let topic_id = [0x22u8; 16];

    let resp = rpc(
        &addr,
        encode_request_flexible(85, 0, 11, Some("c"), &wsgs_v0_body("sg-v281", &topic_id, 3)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 1);
    let echoed = get_uuid(&mut src).unwrap();
    assert_eq!(echoed, topic_id);
    let pn = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(pn, 1);
    assert_eq!(src.get_i32(), 3);
    assert_eq!(src.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);
    assert!(broker
        .groups()
        .fetch_offsets("sg-v281", &[])
        .unwrap()
        .entries
        .is_empty());

    server.abort();
}
