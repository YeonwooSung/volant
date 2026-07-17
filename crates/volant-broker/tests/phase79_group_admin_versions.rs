//! Phase 79: ListGroups v4–5, DescribeGroups v6, DeleteGroups v3.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_bytes, get_compact_nullable_string, get_compact_string, get_string, put_bytes,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_nullable_string,
    put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn join_v5(group: &str, member_id: &str, instance: Option<&str>, topics: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(10_000);
    body.put_i32(10_000);
    put_string(&mut body, member_id);
    put_nullable_string(&mut body, instance);
    put_string(&mut body, "consumer");
    body.put_i32(1);
    put_string(&mut body, "range");
    let sub = encode_consumer_subscription(topics);
    put_bytes(&mut body, Some(&sub));
    body
}

fn list_v4_body(states: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, states.len());
    for s in states {
        put_compact_string(&mut body, s);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn list_v5_body(states: &[&str], types: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, states.len());
    for s in states {
        put_compact_string(&mut body, s);
    }
    put_compact_array_len(&mut body, types.len());
    for t in types {
        put_compact_string(&mut body, t);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_v6_body(group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, group);
    body.put_u8(0); // include_ops = false
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_v3_body(group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, group);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_group_admin_p79_maxes() {
    let dir = temp_dir("p79", "api");
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
    assert_eq!(found.get(&15), Some(&(0, 6))); // DescribeGroups
    assert_eq!(found.get(&16), Some(&(0, 5))); // ListGroups
    assert_eq!(found.get(&42), Some(&(0, 3))); // DeleteGroups
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_groups_v4_state_and_filter() {
    let dir = temp_dir("p79", "list-v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(
        &addr,
        encode_request(
            11,
            5,
            10,
            Some("c"),
            &join_v5("p79stable", "", Some("pod-a"), &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i32(), 0);
    assert_eq!(js.get_i16(), 0);

    // v4 all states: expect GroupState "Stable"
    let lresp = rpc(
        &addr,
        encode_request_flexible(16, 4, 2, Some("c"), &list_v4_body(&[])),
    )
    .await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 2);
    skip_tag_buffer(&mut ls).unwrap();
    assert_eq!(ls.get_i32(), 0);
    assert_eq!(ls.get_i16(), 0);
    let n = get_compact_array_len(&mut ls).unwrap().unwrap();
    let mut found = false;
    for _ in 0..n {
        let gid = get_compact_string(&mut ls).unwrap();
        let ptype = get_compact_string(&mut ls).unwrap();
        let state = get_compact_string(&mut ls).unwrap();
        skip_tag_buffer(&mut ls).unwrap();
        if gid == "p79stable" {
            found = true;
            assert_eq!(ptype, "consumer");
            assert_eq!(state, "Stable");
        }
    }
    assert!(found);
    skip_tag_buffer(&mut ls).unwrap();

    // Filter Empty only → stable group excluded
    let lresp2 = rpc(
        &addr,
        encode_request_flexible(16, 4, 3, Some("c"), &list_v4_body(&["Empty"])),
    )
    .await;
    let mut ls2 = lresp2.freeze();
    assert_eq!(ls2.get_i32(), 3);
    skip_tag_buffer(&mut ls2).unwrap();
    assert_eq!(ls2.get_i32(), 0);
    assert_eq!(ls2.get_i16(), 0);
    let n2 = get_compact_array_len(&mut ls2).unwrap().unwrap();
    for _ in 0..n2 {
        let gid = get_compact_string(&mut ls2).unwrap();
        let _ = get_compact_string(&mut ls2).unwrap();
        let _ = get_compact_string(&mut ls2).unwrap();
        skip_tag_buffer(&mut ls2).unwrap();
        assert_ne!(gid, "p79stable");
    }

    // Filter Stable → included
    let lresp3 = rpc(
        &addr,
        encode_request_flexible(16, 4, 4, Some("c"), &list_v4_body(&["Stable"])),
    )
    .await;
    let mut ls3 = lresp3.freeze();
    assert_eq!(ls3.get_i32(), 4);
    skip_tag_buffer(&mut ls3).unwrap();
    assert_eq!(ls3.get_i32(), 0);
    assert_eq!(ls3.get_i16(), 0);
    let n3 = get_compact_array_len(&mut ls3).unwrap().unwrap();
    let mut found3 = false;
    for _ in 0..n3 {
        let gid = get_compact_string(&mut ls3).unwrap();
        let _ = get_compact_string(&mut ls3).unwrap();
        let state = get_compact_string(&mut ls3).unwrap();
        skip_tag_buffer(&mut ls3).unwrap();
        if gid == "p79stable" {
            found3 = true;
            assert_eq!(state, "Stable");
        }
    }
    assert!(found3);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_groups_v5_type_and_filter() {
    let dir = temp_dir("p79", "list-v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request(
            11,
            5,
            10,
            Some("c"),
            &join_v5("p79type", "", Some("pod-b"), &["events"]),
        ),
    )
    .await;

    // v5: GroupType "classic"
    let lresp = rpc(
        &addr,
        encode_request_flexible(16, 5, 2, Some("c"), &list_v5_body(&[], &[])),
    )
    .await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 2);
    skip_tag_buffer(&mut ls).unwrap();
    assert_eq!(ls.get_i32(), 0);
    assert_eq!(ls.get_i16(), 0);
    let n = get_compact_array_len(&mut ls).unwrap().unwrap();
    let mut found = false;
    for _ in 0..n {
        let gid = get_compact_string(&mut ls).unwrap();
        let ptype = get_compact_string(&mut ls).unwrap();
        let state = get_compact_string(&mut ls).unwrap();
        let gtype = get_compact_string(&mut ls).unwrap();
        skip_tag_buffer(&mut ls).unwrap();
        if gid == "p79type" {
            found = true;
            assert_eq!(ptype, "consumer");
            assert_eq!(state, "Stable");
            assert_eq!(gtype, "classic");
        }
    }
    assert!(found);
    skip_tag_buffer(&mut ls).unwrap();

    // TypesFilter "share" → empty match for our classic groups
    let lresp2 = rpc(
        &addr,
        encode_request_flexible(16, 5, 3, Some("c"), &list_v5_body(&[], &["share"])),
    )
    .await;
    let mut ls2 = lresp2.freeze();
    assert_eq!(ls2.get_i32(), 3);
    skip_tag_buffer(&mut ls2).unwrap();
    assert_eq!(ls2.get_i32(), 0);
    assert_eq!(ls2.get_i16(), 0);
    let n2 = get_compact_array_len(&mut ls2).unwrap().unwrap();
    for _ in 0..n2 {
        let gid = get_compact_string(&mut ls2).unwrap();
        let _ = get_compact_string(&mut ls2).unwrap();
        let _ = get_compact_string(&mut ls2).unwrap();
        let _ = get_compact_string(&mut ls2).unwrap();
        skip_tag_buffer(&mut ls2).unwrap();
        assert_ne!(gid, "p79type");
    }

    // TypesFilter "classic" → included
    let lresp3 = rpc(
        &addr,
        encode_request_flexible(16, 5, 4, Some("c"), &list_v5_body(&[], &["classic"])),
    )
    .await;
    let mut ls3 = lresp3.freeze();
    assert_eq!(ls3.get_i32(), 4);
    skip_tag_buffer(&mut ls3).unwrap();
    assert_eq!(ls3.get_i32(), 0);
    assert_eq!(ls3.get_i16(), 0);
    let n3 = get_compact_array_len(&mut ls3).unwrap().unwrap();
    let mut found3 = false;
    for _ in 0..n3 {
        let gid = get_compact_string(&mut ls3).unwrap();
        let _ = get_compact_string(&mut ls3).unwrap();
        let _ = get_compact_string(&mut ls3).unwrap();
        let gtype = get_compact_string(&mut ls3).unwrap();
        skip_tag_buffer(&mut ls3).unwrap();
        if gid == "p79type" {
            found3 = true;
            assert_eq!(gtype, "classic");
        }
    }
    assert!(found3);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_v6_error_message() {
    let dir = temp_dir("p79", "desc-v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(
        &addr,
        encode_request(
            11,
            5,
            10,
            Some("c"),
            &join_v5("p79d", "", Some("pod-c"), &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i32(), 0);
    assert_eq!(js.get_i16(), 0);

    // Success → null ErrorMessage
    let dresp = rpc(
        &addr,
        encode_request_flexible(15, 6, 2, Some("c"), &describe_v6_body("p79d")),
    )
    .await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 2);
    skip_tag_buffer(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_compact_string(&mut ds).unwrap(), "p79d");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "Stable");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "consumer");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "range");
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    let _ = get_compact_string(&mut ds).unwrap(); // member_id
    let _ = get_compact_nullable_string(&mut ds).unwrap(); // instance
    let _ = get_compact_string(&mut ds).unwrap(); // client_id
    let _ = get_compact_string(&mut ds).unwrap(); // client_host
    let _ = get_compact_bytes(&mut ds).unwrap(); // metadata
    let _ = get_compact_bytes(&mut ds).unwrap(); // assignment
    skip_tag_buffer(&mut ds).unwrap(); // member tags
    let _auth = ds.get_i32(); // authorized ops (INT32_MIN when include=false)
    assert!(get_compact_nullable_string(&mut ds).unwrap().is_none()); // ErrorMessage null
    skip_tag_buffer(&mut ds).unwrap(); // group tags
    skip_tag_buffer(&mut ds).unwrap(); // top tags

    // Unknown group → GroupIdNotFound + ErrorMessage
    let dresp2 = rpc(
        &addr,
        encode_request_flexible(15, 6, 3, Some("c"), &describe_v6_body("no-such-group")),
    )
    .await;
    let mut ds2 = dresp2.freeze();
    assert_eq!(ds2.get_i32(), 3);
    skip_tag_buffer(&mut ds2).unwrap();
    assert_eq!(ds2.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut ds2).unwrap(), Some(1));
    assert_eq!(ds2.get_i16(), 69); // GroupIdNotFound
    assert_eq!(get_compact_string(&mut ds2).unwrap(), "no-such-group");
    assert_eq!(get_compact_string(&mut ds2).unwrap(), "Dead");
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_string(&mut ds2).unwrap();
    assert_eq!(get_compact_array_len(&mut ds2).unwrap(), Some(0));
    let _auth2 = ds2.get_i32();
    let msg = get_compact_nullable_string(&mut ds2).unwrap();
    assert_eq!(msg.as_deref(), Some("Group id not found"));
    skip_tag_buffer(&mut ds2).unwrap();
    skip_tag_buffer(&mut ds2).unwrap();

    // v5 still has no ErrorMessage field
    let dresp3 = rpc(
        &addr,
        encode_request_flexible(15, 5, 4, Some("c"), &describe_v6_body("p79d")),
    )
    .await;
    let mut ds3 = dresp3.freeze();
    assert_eq!(ds3.get_i32(), 4);
    skip_tag_buffer(&mut ds3).unwrap();
    assert_eq!(ds3.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut ds3).unwrap(), Some(1));
    assert_eq!(ds3.get_i16(), 0);
    // drain to authorized ops then tags — no ErrorMessage between
    let _ = get_compact_string(&mut ds3).unwrap();
    let _ = get_compact_string(&mut ds3).unwrap();
    let _ = get_compact_string(&mut ds3).unwrap();
    let _ = get_compact_string(&mut ds3).unwrap();
    assert_eq!(get_compact_array_len(&mut ds3).unwrap(), Some(1));
    let _ = get_compact_string(&mut ds3).unwrap();
    let _ = get_compact_nullable_string(&mut ds3).unwrap();
    let _ = get_compact_string(&mut ds3).unwrap();
    let _ = get_compact_string(&mut ds3).unwrap();
    let _ = get_compact_bytes(&mut ds3).unwrap();
    let _ = get_compact_bytes(&mut ds3).unwrap();
    skip_tag_buffer(&mut ds3).unwrap();
    let _ = ds3.get_i32();
    skip_tag_buffer(&mut ds3).unwrap(); // group tags immediately after auth ops
    skip_tag_buffer(&mut ds3).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_v3_error_message() {
    let dir = temp_dir("p79", "del-v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(
        &addr,
        encode_request(
            11,
            5,
            10,
            Some("c"),
            &join_v5("p79del", "", Some("pod-d"), &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i32(), 0);
    assert_eq!(js.get_i16(), 0);
    let _gen = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();

    // Non-empty → 68 + ErrorMessage
    let delr = rpc(
        &addr,
        encode_request_flexible(42, 3, 11, Some("c"), &delete_v3_body("p79del")),
    )
    .await;
    let mut del = delr.freeze();
    assert_eq!(del.get_i32(), 11);
    skip_tag_buffer(&mut del).unwrap();
    assert_eq!(del.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut del).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut del).unwrap(), "p79del");
    assert_eq!(del.get_i16(), 68);
    assert_eq!(
        get_compact_nullable_string(&mut del).unwrap().as_deref(),
        Some("Group is not empty")
    );
    skip_tag_buffer(&mut del).unwrap();
    skip_tag_buffer(&mut del).unwrap();

    // Unknown → 69 + ErrorMessage
    let delr2 = rpc(
        &addr,
        encode_request_flexible(42, 3, 12, Some("c"), &delete_v3_body("ghost-group")),
    )
    .await;
    let mut del2 = delr2.freeze();
    assert_eq!(del2.get_i32(), 12);
    skip_tag_buffer(&mut del2).unwrap();
    assert_eq!(del2.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut del2).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut del2).unwrap(), "ghost-group");
    assert_eq!(del2.get_i16(), 69);
    assert_eq!(
        get_compact_nullable_string(&mut del2).unwrap().as_deref(),
        Some("Group id not found")
    );
    skip_tag_buffer(&mut del2).unwrap();
    skip_tag_buffer(&mut del2).unwrap();

    // Leave then delete: 0 (had residual state) or 69 (fully gone) — both valid.
    let mut leave = BytesMut::new();
    put_string(&mut leave, "p79del");
    leave.put_i32(1);
    put_string(&mut leave, &member_id);
    put_nullable_string(&mut leave, Some("pod-d"));
    let _ = rpc(&addr, encode_request(13, 3, 13, Some("c"), &leave)).await;

    let delr3 = rpc(
        &addr,
        encode_request_flexible(42, 3, 14, Some("c"), &delete_v3_body("p79del")),
    )
    .await;
    let mut del3 = delr3.freeze();
    assert_eq!(del3.get_i32(), 14);
    skip_tag_buffer(&mut del3).unwrap();
    assert_eq!(del3.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut del3).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut del3).unwrap(), "p79del");
    let err = del3.get_i16();
    assert!(err == 0 || err == 69, "delete after leave err={err}");
    let msg = get_compact_nullable_string(&mut del3).unwrap();
    if err == 0 {
        assert!(msg.is_none());
    } else {
        assert_eq!(msg.as_deref(), Some("Group id not found"));
    }
    skip_tag_buffer(&mut del3).unwrap();
    skip_tag_buffer(&mut del3).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("p79", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // ListGroups v6
    let r = rpc(
        &addr,
        encode_request_flexible(16, 6, 1, Some("c"), &list_v5_body(&[], &[])),
    )
    .await;
    let mut s = r.freeze();
    assert_eq!(s.get_i32(), 1);
    skip_tag_buffer(&mut s).unwrap(); // header v1
    assert_eq!(s.get_i16(), 35); // UnsupportedVersion

    // DescribeGroups v7
    let r2 = rpc(
        &addr,
        encode_request_flexible(15, 7, 2, Some("c"), &describe_v6_body("g")),
    )
    .await;
    let mut s2 = r2.freeze();
    assert_eq!(s2.get_i32(), 2);
    skip_tag_buffer(&mut s2).unwrap();
    assert_eq!(s2.get_i16(), 35);

    // DeleteGroups v4
    let r3 = rpc(
        &addr,
        encode_request_flexible(42, 4, 3, Some("c"), &delete_v3_body("g")),
    )
    .await;
    let mut s3 = r3.freeze();
    assert_eq!(s3.get_i32(), 3);
    skip_tag_buffer(&mut s3).unwrap();
    assert_eq!(s3.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
