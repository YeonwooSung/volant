//! v0.264: Kafka ConsumerGroupDescribe key 69 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, encode_request_flexible, get_bytes,
    get_compact_array_len, get_compact_nullable_string, get_compact_string, get_string, get_uuid,
    put_bytes, put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_string,
    skip_tag_buffer,
};

fn kafka_join_v1(group: &str, topic: &str, corr: i32) -> BytesMut {
    let sub = encode_consumer_subscription(&[topic]);
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(10_000); // session_timeout
    body.put_i32(150); // rebalance_timeout — avoid long parked Join
    put_string(&mut body, "");
    put_string(&mut body, "consumer");
    body.put_i32(1);
    put_string(&mut body, "range");
    put_bytes(&mut body, Some(&sub));
    encode_request(11, 1, corr, Some("c"), &body)
}

fn kafka_sync_v0(group: &str, generation: i32, member_id: &str, corr: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(generation);
    put_string(&mut body, member_id);
    body.put_i32(0); // empty assignments — keep Join assignment
    encode_request(14, 0, corr, Some("c"), &body)
}

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

fn skip_assignment(src: &mut impl Buf) {
    let n = get_compact_array_len(src).unwrap().unwrap_or(0);
    for _ in 0..n {
        let _ = get_uuid(src).unwrap();
        let _ = get_compact_string(src).unwrap();
        let pn = get_compact_array_len(src).unwrap().unwrap_or(0);
        for _ in 0..pn {
            let _ = src.get_i32();
        }
        skip_tag_buffer(src).unwrap();
    }
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_consumer_group_describe_69() {
    let (_dir, broker) = broker_temp("v264", "api");
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
    assert!(found.len() >= 61);
    assert_eq!(found.get(&69), Some(&(0, 0)));
    assert_eq!(found.get(&15), Some(&(0, 6))); // DescribeGroups still listed

    server.abort();
}

#[tokio::test]
async fn describe_joined_group_is_classic_snapshot() {
    let (_dir, broker) = broker_temp("v264", "join");
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join_v1("cg-v264", "events", 10)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _protocol = get_string(&mut js).unwrap();
    let _leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert!(!member_id.is_empty());

    let sresp = rpc(&addr, kafka_sync_v0("cg-v264", generation, &member_id, 11)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i16(), 0);
    let _ = get_bytes(&mut ss).unwrap();

    let resp = rpc(
        &addr,
        encode_request_flexible(69, 0, 12, Some("c"), &describe_v0_body("cg-v264", false)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 12);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_compact_string(&mut src).unwrap(), "cg-v264");
    let state = get_compact_string(&mut src).unwrap();
    assert!(
        matches!(
            state.as_str(),
            "Empty" | "Stable" | "CompletingRebalance" | "PreparingRebalance"
        ),
        "unexpected state {state}"
    );
    assert_eq!(src.get_i32(), generation); // groupEpoch
    assert_eq!(src.get_i32(), generation); // assignmentEpoch
    let assignor = get_compact_string(&mut src).unwrap();
    assert!(assignor.is_empty() || assignor == "range");
    let n_members = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n_members, 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), member_id);
    let _instance = get_compact_nullable_string(&mut src).unwrap();
    let _rack = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), -1); // memberEpoch — not KIP-848
    let _client_id = get_compact_string(&mut src).unwrap();
    let _client_host = get_compact_string(&mut src).unwrap();
    let n_topics = get_compact_array_len(&mut src).unwrap().unwrap_or(0);
    for _ in 0..n_topics {
        let _ = get_compact_string(&mut src).unwrap();
    }
    let _regex = get_compact_nullable_string(&mut src).unwrap();
    skip_assignment(&mut src);
    skip_assignment(&mut src);
    skip_tag_buffer(&mut src).unwrap(); // member tags (no GroupType / MemberType on v0)
    let _ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
}

#[tokio::test]
async fn unknown_group_is_69() {
    let (_dir, broker) = broker_temp("v264", "unknown");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            69,
            0,
            20,
            Some("c"),
            &describe_v0_body("no-such-group", false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 20);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 69); // GROUP_ID_NOT_FOUND
    let _msg = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_string(&mut src).unwrap(), "no-such-group");

    server.abort();
}

#[tokio::test]
async fn consumer_group_describe_v1_is_35() {
    let (_dir, broker) = broker_temp("v264", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(69, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
