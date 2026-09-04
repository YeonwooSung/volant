//! v0.285: Kafka StreamsGroupHeartbeat key 88 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, encode_request_flexible, get_bytes,
    get_compact_nullable_string, get_string, put_bytes, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_string, put_unsigned_varint,
    read_unsigned_varint, skip_tag_buffer,
};
use volant_broker::kafka::SUPPORTED_APIS;

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

fn sghb_v0_body(group: &str, member: &str, epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_string(&mut body, member);
    body.put_i32(epoch);
    body.put_i32(0); // EndpointInformationEpoch
    put_compact_nullable_string(&mut body, None); // InstanceId
    put_compact_nullable_string(&mut body, None); // RackId
    body.put_i32(-1); // RebalanceTimeoutMs
    put_unsigned_varint(&mut body, 0); // Topology null
    put_unsigned_varint(&mut body, 0); // ActiveTasks null
    put_unsigned_varint(&mut body, 0); // StandbyTasks null
    put_unsigned_varint(&mut body, 0); // WarmupTasks null
    put_compact_nullable_string(&mut body, None); // ProcessId
    put_unsigned_varint(&mut body, 0); // UserEndpoint null
    put_unsigned_varint(&mut body, 0); // ClientTags null
    put_unsigned_varint(&mut body, 0); // TaskOffsets null
    put_unsigned_varint(&mut body, 0); // TaskEndOffsets null
    body.put_u8(0); // ShutdownApplication
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn read_stghb(
    src: &mut impl Buf,
) -> (
    i32,
    i16,
    Option<String>,
    Option<String>,
    i32,
    i32,
    i32,
    i32,
    u32,
    u32,
    u32,
    u32,
    i32,
    u32,
) {
    let throttle = src.get_i32();
    let error = src.get_i16();
    let err_msg = get_compact_nullable_string(src).unwrap();
    let member = get_compact_nullable_string(src).unwrap();
    let epoch = src.get_i32();
    let interval = src.get_i32();
    let lag_legacy = src.get_i32();
    let task_offset_interval = src.get_i32();
    let status = read_unsigned_varint(src).unwrap();
    let active = read_unsigned_varint(src).unwrap();
    let standby = read_unsigned_varint(src).unwrap();
    let warmup = read_unsigned_varint(src).unwrap();
    let endpoint_epoch = src.get_i32();
    let partitions = read_unsigned_varint(src).unwrap();
    skip_tag_buffer(src).unwrap();
    (
        throttle,
        error,
        err_msg,
        member,
        epoch,
        interval,
        lag_legacy,
        task_offset_interval,
        status,
        active,
        standby,
        warmup,
        endpoint_epoch,
        partitions,
    )
}

#[tokio::test]
async fn api_versions_lists_streams_group_heartbeat_88() {
    let (_dir, broker) = broker_temp("v285", "api");
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
    assert_eq!(found.get(&88), Some(&(0, 0)));
    assert_eq!(found.get(&12), Some(&(0, 4))); // classic Heartbeat still listed
    assert_eq!(found.get(&68), Some(&(0, 0))); // ConsumerGroupHeartbeat still listed
    assert_eq!(found.get(&76), Some(&(1, 1))); // ShareGroupHeartbeat still listed
    assert!(SUPPORTED_APIS.len() >= 85);

    server.abort();
}

#[tokio::test]
async fn streams_group_heartbeat_v0_is_42_membership_unchanged() {
    let (_dir, broker) = broker_temp("v285", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = broker.groups().list_group_ids();

    let resp = rpc(
        &addr,
        encode_request_flexible(
            88,
            0,
            10,
            Some("c"),
            &sghb_v0_body("stg-v285", "member-1", 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    let (
        throttle,
        error,
        err_msg,
        member,
        epoch,
        interval,
        lag_legacy,
        task_offset_interval,
        status,
        active,
        standby,
        warmup,
        endpoint_epoch,
        partitions,
    ) = read_stghb(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42); // INVALID_REQUEST
    assert_eq!(err_msg.as_deref(), Some("not KIP-1071 streams group"));
    assert_eq!(member, None);
    assert_eq!(epoch, -1);
    assert_eq!(interval, 0);
    assert_eq!(lag_legacy, 0);
    assert_eq!(task_offset_interval, 0);
    assert_eq!(status, 0);
    assert_eq!(active, 0);
    assert_eq!(standby, 0);
    assert_eq!(warmup, 0);
    assert_eq!(endpoint_epoch, 0);
    assert_eq!(partitions, 0);
    assert_eq!(src.remaining(), 0);

    assert_eq!(broker.groups().list_group_ids(), before);
    assert!(broker.groups().describe_group("stg-v285").is_none());

    server.abort();
}

#[tokio::test]
async fn join_sync_then_key_88_is_42_classic_heartbeat_still_0() {
    let (_dir, broker) = broker_temp("v285", "join");
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join_v1("stg-v285", "events", 10)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _protocol = get_string(&mut js).unwrap();
    let _leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert!(!member_id.is_empty());

    let sresp = rpc(&addr, kafka_sync_v0("stg-v285", generation, &member_id, 11)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i16(), 0);
    let _ = get_bytes(&mut ss).unwrap();

    let before = broker.groups().describe_group("stg-v285").unwrap();
    assert_eq!(before.members.len(), 1);

    let resp = rpc(
        &addr,
        encode_request_flexible(
            88,
            0,
            12,
            Some("c"),
            &sghb_v0_body("stg-v285", &member_id, generation),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 12);
    let (throttle, error, _, member, epoch, interval, ..) = read_stghb(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 42);
    assert_eq!(member, None);
    assert_eq!(epoch, -1);
    assert_eq!(interval, 0);

    let after = broker.groups().describe_group("stg-v285").unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.members.len(), 1);
    assert_eq!(after.members[0].member_id, member_id);

    let hresp = rpc(
        &addr,
        kafka_heartbeat_v0("stg-v285", generation, &member_id, 13),
    )
    .await;
    let mut hs = hresp.freeze();
    assert_eq!(hs.get_i32(), 13);
    assert_eq!(hs.get_i16(), 0); // classic Heartbeat 12 still works

    server.abort();
}

#[tokio::test]
async fn streams_group_heartbeat_v1_is_35() {
    let (_dir, broker) = broker_temp("v285", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(88, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35);

    server.abort();
}

#[tokio::test]
async fn streams_group_heartbeat_acl_deny_is_30() {
    let (_dir, broker) = broker_temp("v285", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            88,
            0,
            11,
            Some("c"),
            &sghb_v0_body("stg-v285", "member-1", 1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    let (throttle, error, err_msg, member, epoch, interval, ..) = read_stghb(&mut src);
    assert_eq!(throttle, 0);
    assert_eq!(error, 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(err_msg, None);
    assert_eq!(member, None);
    assert_eq!(epoch, -1);
    assert_eq!(interval, 0);
    assert_eq!(src.remaining(), 0);

    server.abort();
}
