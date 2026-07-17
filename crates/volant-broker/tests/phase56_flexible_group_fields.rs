//! Phase 56: JoinGroup v7–9 / SyncGroup v5 / LeaveGroup v5 field completeness.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    decode_consumer_assignment, encode_consumer_subscription, encode_request,
    encode_request_flexible, get_compact_array_len, get_compact_bytes,
    get_compact_nullable_string, get_compact_string, put_compact_array_len, put_compact_bytes,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// JoinGroup flexible body; `version` selects Reason (v8+) presence.
fn join_flex(version: i16, group: &str, member_id: &str, topics: &[&str]) -> BytesMut {
    let sub = encode_consumer_subscription(topics);
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(10_000);
    body.put_i32(10_000);
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None);
    put_compact_string(&mut body, "consumer");
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "range");
    put_compact_bytes(&mut body, Some(&sub));
    put_empty_tag_buffer(&mut body);
    if version >= 8 {
        put_compact_nullable_string(&mut body, Some("rebalance"));
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn sync_v5(group: &str, generation: i32, member_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(generation);
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None);
    put_compact_nullable_string(&mut body, Some("consumer")); // ProtocolType
    put_compact_nullable_string(&mut body, Some("range")); // ProtocolName
    put_compact_array_len(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body
}

fn leave_v5(group: &str, member_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None);
    put_compact_nullable_string(&mut body, Some("shutdown")); // Reason
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_advertises_group_field_maxes() {
    let dir = temp_dir("p56", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
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
    assert_eq!(found.get(&11), Some(&(0, 9)));
    assert_eq!(found.get(&12), Some(&(0, 4)));
    assert_eq!(found.get(&13), Some(&(0, 5)));
    assert_eq!(found.get(&14), Some(&(0, 5)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_v9_protocol_type_skip_assignment_sync_v5_leave_v5() {
    let dir = temp_dir("p56", "lifecycle");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // JoinGroup v9
    let jresp = rpc(
        &addr,
        encode_request_flexible(
            11,
            9,
            10,
            Some("c"),
            &join_flex(9, "cg-p56", "", &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    skip_tag_buffer(&mut js).unwrap();
    assert_eq!(js.get_i32(), 0); // throttle
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    assert!(generation > 0);
    // ProtocolType (v7+)
    assert_eq!(
        get_compact_nullable_string(&mut js).unwrap().as_deref(),
        Some("consumer")
    );
    // ProtocolName (nullable v7+)
    assert_eq!(
        get_compact_nullable_string(&mut js).unwrap().as_deref(),
        Some("range")
    );
    let leader = get_compact_string(&mut js).unwrap();
    // SkipAssignment (v9+)
    assert_eq!(js.get_u8(), 0);
    let member_id = get_compact_string(&mut js).unwrap();
    assert_eq!(leader, member_id);
    let n = get_compact_array_len(&mut js).unwrap().unwrap();
    assert_eq!(n, 1);
    assert_eq!(get_compact_string(&mut js).unwrap(), member_id);
    let _ = get_compact_nullable_string(&mut js).unwrap();
    let _ = get_compact_bytes(&mut js).unwrap();
    skip_tag_buffer(&mut js).unwrap();
    skip_tag_buffer(&mut js).unwrap();
    assert_eq!(js.remaining(), 0);

    // SyncGroup v5 with ProtocolType/Name
    let sresp = rpc(
        &addr,
        encode_request_flexible(
            14,
            5,
            11,
            Some("c"),
            &sync_v5("cg-p56", generation, &member_id),
        ),
    )
    .await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    skip_tag_buffer(&mut ss).unwrap();
    assert_eq!(ss.get_i32(), 0);
    assert_eq!(ss.get_i16(), 0);
    assert_eq!(
        get_compact_nullable_string(&mut ss).unwrap().as_deref(),
        Some("consumer")
    );
    assert_eq!(
        get_compact_nullable_string(&mut ss).unwrap().as_deref(),
        Some("range")
    );
    let assign = get_compact_bytes(&mut ss).unwrap().unwrap_or_default();
    let assignment = decode_consumer_assignment(&assign).unwrap();
    assert_eq!(assignment.len(), 2);
    skip_tag_buffer(&mut ss).unwrap();
    assert_eq!(ss.remaining(), 0);

    // LeaveGroup v5 with Reason
    let lresp = rpc(
        &addr,
        encode_request_flexible(13, 5, 13, Some("c"), &leave_v5("cg-p56", &member_id)),
    )
    .await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 13);
    skip_tag_buffer(&mut ls).unwrap();
    assert_eq!(ls.get_i32(), 0);
    assert_eq!(ls.get_i16(), 0);
    let n = get_compact_array_len(&mut ls).unwrap().unwrap();
    assert_eq!(n, 1);
    assert_eq!(get_compact_string(&mut ls).unwrap(), member_id);
    let _ = get_compact_nullable_string(&mut ls).unwrap();
    assert_eq!(ls.get_i16(), 0);
    skip_tag_buffer(&mut ls).unwrap();
    skip_tag_buffer(&mut ls).unwrap();
    assert_eq!(ls.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_v7_protocol_type_no_skip_assignment() {
    let dir = temp_dir("p56", "v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(11, 7, 20, Some("c"), &join_flex(7, "cg-v7", "", &["t"])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _gen = src.get_i32();
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("consumer")
    );
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("range")
    );
    let leader = get_compact_string(&mut src).unwrap();
    // No SkipAssignment on v7 — next field is MemberId.
    let mid = get_compact_string(&mut src).unwrap();
    assert_eq!(leader, mid);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_v6_still_no_protocol_type() {
    let dir = temp_dir("p56", "v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(11, 6, 30, Some("c"), &join_flex(6, "cg-v6", "", &["t"])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _gen = src.get_i32();
    // v6: ProtocolName is non-nullable compact string (not ProtocolType).
    assert_eq!(get_compact_string(&mut src).unwrap(), "range");
    let leader = get_compact_string(&mut src).unwrap();
    let mid = get_compact_string(&mut src).unwrap();
    assert_eq!(leader, mid);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_v10_unsupported_uses_header_v1() {
    let dir = temp_dir("p56", "v10");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(11, 10, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sync_v6_unsupported_uses_header_v1() {
    let dir = temp_dir("p56", "sync6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(14, 6, 2, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
