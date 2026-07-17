//! Phase 55: Flexible JoinGroup v6 / SyncGroup v4 / Heartbeat v4 / LeaveGroup v4.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    decode_consumer_assignment, encode_consumer_subscription, encode_request,
    encode_request_flexible, get_compact_array_len, get_compact_bytes, get_compact_string,
    get_string, put_compact_array_len, put_compact_bytes, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// JoinGroup v6 flexible body.
fn join_v6(group: &str, member_id: &str, topics: &[&str]) -> BytesMut {
    let sub = encode_consumer_subscription(topics);
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(10_000); // session_timeout
    body.put_i32(10_000); // rebalance_timeout
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None); // group_instance_id
    put_compact_string(&mut body, "consumer");
    put_compact_array_len(&mut body, 1); // protocols
    put_compact_string(&mut body, "range");
    put_compact_bytes(&mut body, Some(&sub));
    put_empty_tag_buffer(&mut body); // protocol tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

fn sync_v4(group: &str, generation: i32, member_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(generation);
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None); // group_instance_id
    put_compact_array_len(&mut body, 0); // assignments
    put_empty_tag_buffer(&mut body);
    body
}

fn heartbeat_v4(group: &str, generation: i32, member_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(generation);
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None);
    put_empty_tag_buffer(&mut body);
    body
}

fn leave_v4(group: &str, member_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, member_id);
    put_compact_nullable_string(&mut body, None);
    put_empty_tag_buffer(&mut body); // member tags
    put_empty_tag_buffer(&mut body); // top-level
    body
}

#[tokio::test]
async fn api_versions_advertises_flex_group_maxes() {
    let dir = temp_dir("p55", "api");
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
    assert_eq!(found.get(&11), Some(&(0, 9))); // JoinGroup (Phase 56)
    assert_eq!(found.get(&12), Some(&(0, 4))); // Heartbeat
    assert_eq!(found.get(&13), Some(&(0, 5))); // LeaveGroup (Phase 56)
    assert_eq!(found.get(&14), Some(&(0, 5))); // SyncGroup (Phase 56)
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_sync_heartbeat_leave_flexible() {
    let dir = temp_dir("p55", "lifecycle");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // JoinGroup v6
    let jresp = rpc(
        &addr,
        encode_request_flexible(11, 6, 10, Some("c"), &join_v6("cg-flex", "", &["events"])),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10); // corr
    skip_tag_buffer(&mut js).unwrap(); // response header v1 tags
    assert_eq!(js.get_i32(), 0); // throttle
    let jerr = js.get_i16();
    assert_eq!(jerr, 0, "join error {jerr}");
    let generation = js.get_i32();
    assert!(generation > 0);
    let protocol = get_compact_string(&mut js).unwrap();
    assert_eq!(protocol, "range");
    let leader = get_compact_string(&mut js).unwrap();
    let member_id = get_compact_string(&mut js).unwrap();
    assert!(!member_id.is_empty());
    assert_eq!(leader, member_id);
    let member_count = get_compact_array_len(&mut js).unwrap().unwrap();
    assert_eq!(member_count, 1);
    assert_eq!(get_compact_string(&mut js).unwrap(), member_id);
    // group_instance_id nullable
    let _inst = volant_broker::kafka::codec::get_compact_nullable_string(&mut js).unwrap();
    let _meta = get_compact_bytes(&mut js).unwrap();
    skip_tag_buffer(&mut js).unwrap(); // member tags
    skip_tag_buffer(&mut js).unwrap(); // top-level
    assert_eq!(js.remaining(), 0);

    // SyncGroup v4
    let sresp = rpc(
        &addr,
        encode_request_flexible(
            14,
            4,
            11,
            Some("c"),
            &sync_v4("cg-flex", generation, &member_id),
        ),
    )
    .await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    skip_tag_buffer(&mut ss).unwrap();
    assert_eq!(ss.get_i32(), 0); // throttle
    assert_eq!(ss.get_i16(), 0);
    let assign_bytes = get_compact_bytes(&mut ss).unwrap().unwrap_or_default();
    let assignment = decode_consumer_assignment(&assign_bytes).unwrap();
    assert_eq!(assignment.len(), 2, "both partitions: {assignment:?}");
    skip_tag_buffer(&mut ss).unwrap();
    assert_eq!(ss.remaining(), 0);

    // Heartbeat v4
    let hresp = rpc(
        &addr,
        encode_request_flexible(
            12,
            4,
            12,
            Some("c"),
            &heartbeat_v4("cg-flex", generation, &member_id),
        ),
    )
    .await;
    let mut hs = hresp.freeze();
    assert_eq!(hs.get_i32(), 12);
    skip_tag_buffer(&mut hs).unwrap();
    assert_eq!(hs.get_i32(), 0);
    assert_eq!(hs.get_i16(), 0);
    skip_tag_buffer(&mut hs).unwrap();
    assert_eq!(hs.remaining(), 0);

    // LeaveGroup v4
    let lresp = rpc(
        &addr,
        encode_request_flexible(13, 4, 13, Some("c"), &leave_v4("cg-flex", &member_id)),
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
    let _ = volant_broker::kafka::codec::get_compact_nullable_string(&mut ls).unwrap();
    assert_eq!(ls.get_i16(), 0); // member error
    skip_tag_buffer(&mut ls).unwrap(); // member tags
    skip_tag_buffer(&mut ls).unwrap(); // top-level
    assert_eq!(ls.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_v5_still_classic() {
    let dir = temp_dir("p55", "classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let sub = encode_consumer_subscription(&["t"]);
    let mut body = BytesMut::new();
    put_string(&mut body, "cg-classic");
    body.put_i32(10_000);
    body.put_i32(10_000);
    put_string(&mut body, "");
    // group_instance_id nullable classic
    body.put_i16(-1);
    put_string(&mut body, "consumer");
    body.put_i32(1);
    put_string(&mut body, "range");
    volant_broker::kafka::codec::put_bytes(&mut body, Some(&sub));

    let resp = rpc(&addr, encode_request(11, 5, 20, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20); // header v0 (no tags)
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    let _gen = src.get_i32();
    assert_eq!(get_string(&mut src).unwrap(), "range"); // classic string
    let leader = get_string(&mut src).unwrap();
    let mid = get_string(&mut src).unwrap();
    assert_eq!(leader, mid);
    assert_eq!(src.get_i32(), 1); // classic member count

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// JoinGroup v7+ / SyncGroup v5 support: phase56_flexible_group_fields.
