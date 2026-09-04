//! v0.269: Kafka ConsumerGroupHeartbeat key 68 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, encode_request_flexible, get_bytes,
    get_compact_nullable_string, get_string, put_bytes, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_string, put_uuid,
    read_unsigned_varint, skip_tag_buffer,
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

fn kafka_heartbeat_v0(group: &str, generation: i32, member_id: &str, corr: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(generation);
    put_string(&mut body, member_id);
    encode_request(12, 0, corr, Some("c"), &body)
}

fn cghb_v0_body(group: &str, member: &str, epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_string(&mut body, member);
    body.put_i32(epoch);
    put_compact_nullable_string(&mut body, None); // InstanceId
    put_compact_nullable_string(&mut body, None); // RackId
    body.put_i32(150); // RebalanceTimeoutMs
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "events");
    put_compact_nullable_string(&mut body, None); // ServerAssignor
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, &[0u8; 16]);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    put_empty_tag_buffer(&mut body); // TopicPartitions tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn read_cghb(src: &mut impl Buf) -> (i32, i16, Option<String>, Option<String>, i32, i32, u32) {
    let throttle = src.get_i32();
    let error = src.get_i16();
    let err_msg = get_compact_nullable_string(src).unwrap();
    let member = get_compact_nullable_string(src).unwrap();
    let epoch = src.get_i32();
    let interval = src.get_i32();
    let assignment = read_unsigned_varint(src).unwrap();
    skip_tag_buffer(src).unwrap();
    (throttle, error, err_msg, member, epoch, interval, assignment)
}

#[tokio::test]
async fn api_versions_lists_consumer_group_heartbeat_68() {
    let (_dir, broker) = broker_temp("v269", "api");
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
    assert!(found.len() >= 65);
    assert_eq!(found.get(&68), Some(&(0, 0)));
    assert_eq!(found.get(&12), Some(&(0, 4))); // classic Heartbeat still listed
    assert_eq!(found.get(&69), Some(&(0, 0))); // ConsumerGroupDescribe still listed

    server.abort();
}

#[tokio::test]
async fn consumer_group_heartbeat_v0_is_42_membership_unchanged() {
    let (_dir, broker) = broker_temp("v269", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = broker.groups().list_group_ids();

    let resp = rpc(
        &addr,
        encode_request_flexible(
            68,
            0,
            10,
            Some("c"),
            &cghb_v0_body("cg-v269", "member-1", 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    let (throttle, error, err_msg, member, epoch, interval, assignment) = read_cghb(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42); // INVALID_REQUEST
    assert_eq!(err_msg.as_deref(), Some("not KIP-848 consumer protocol"));
    assert_eq!(member, None);
    assert_eq!(epoch, -1);
    assert_eq!(interval, 0);
    assert_eq!(assignment, 0);
    assert_eq!(src.remaining(), 0);

    assert_eq!(broker.groups().list_group_ids(), before);
    assert!(broker.groups().describe_group("cg-v269").is_none());

    server.abort();
}

#[tokio::test]
async fn join_sync_then_key_68_is_42_classic_heartbeat_still_0() {
    let (_dir, broker) = broker_temp("v269", "join");
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join_v1("cg-v269", "events", 10)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _protocol = get_string(&mut js).unwrap();
    let _leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert!(!member_id.is_empty());

    let sresp = rpc(&addr, kafka_sync_v0("cg-v269", generation, &member_id, 11)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i16(), 0);
    let _ = get_bytes(&mut ss).unwrap();

    let before = broker.groups().describe_group("cg-v269").unwrap();
    assert_eq!(before.members.len(), 1);

    let resp = rpc(
        &addr,
        encode_request_flexible(
            68,
            0,
            12,
            Some("c"),
            &cghb_v0_body("cg-v269", &member_id, generation),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 12);
    let (throttle, error, _, member, epoch, interval, assignment) = read_cghb(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42);
    assert_eq!(member, None);
    assert_eq!(epoch, -1);
    assert_eq!(interval, 0);
    assert_eq!(assignment, 0);

    let after = broker.groups().describe_group("cg-v269").unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.members.len(), 1);
    assert_eq!(after.members[0].member_id, member_id);

    let hresp = rpc(
        &addr,
        kafka_heartbeat_v0("cg-v269", generation, &member_id, 13),
    )
    .await;
    let mut hs = hresp.freeze();
    assert_eq!(hs.get_i32(), 13);
    assert_eq!(hs.get_i16(), 0); // classic Heartbeat 12 still works

    server.abort();
}

#[tokio::test]
async fn consumer_group_heartbeat_v1_is_35() {
    let (_dir, broker) = broker_temp("v269", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(68, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
