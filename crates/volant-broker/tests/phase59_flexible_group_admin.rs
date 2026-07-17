//! Phase 59: Flexible DescribeGroups v5 / ListGroups v3 / DeleteGroups v2.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    decode_consumer_assignment, encode_consumer_subscription, encode_request,
    encode_request_flexible, get_compact_array_len, get_compact_bytes,
    get_compact_nullable_string, get_compact_string, get_string, put_bytes, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
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

fn describe_v5_body(group: &str, include_ops: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, group);
    body.put_u8(if include_ops { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn list_v3_body() -> BytesMut {
    let mut body = BytesMut::new();
    put_empty_tag_buffer(&mut body);
    body
}

fn delete_v2_body(group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, group);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_group_admin_flex_maxes() {
    let dir = temp_dir("p59", "api");
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
    assert_eq!(found.get(&15), Some(&(0, 6))); // DescribeGroups (Phase 79)
    assert_eq!(found.get(&16), Some(&(0, 5))); // ListGroups (Phase 79)
    assert_eq!(found.get(&42), Some(&(0, 3))); // DeleteGroups (Phase 79)
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_describe_delete_flexible_roundtrip() {
    let dir = temp_dir("p59", "roundtrip");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Join with static instance so Describe can surface group_instance_id.
    let jresp = rpc(
        &addr,
        encode_request(
            11,
            5,
            10,
            Some("c"),
            &join_v5("p59g", "", Some("pod-a"), &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i32(), 0); // throttle
    assert_eq!(js.get_i16(), 0);
    let gen = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert_eq!(member_id, "static:pod-a");
    assert!(gen >= 0);

    // ListGroups v3 flexible
    let lresp = rpc(
        &addr,
        encode_request_flexible(16, 3, 2, Some("c"), &list_v3_body()),
    )
    .await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 2);
    skip_tag_buffer(&mut ls).unwrap(); // response header v1
    assert_eq!(ls.get_i32(), 0); // throttle
    assert_eq!(ls.get_i16(), 0); // error
    let n = get_compact_array_len(&mut ls).unwrap().unwrap();
    assert!(n >= 1);
    let mut listed = false;
    for _ in 0..n {
        let gid = get_compact_string(&mut ls).unwrap();
        let ptype = get_compact_string(&mut ls).unwrap();
        skip_tag_buffer(&mut ls).unwrap();
        if gid == "p59g" {
            listed = true;
            assert_eq!(ptype, "consumer");
        }
    }
    assert!(listed, "p59g not listed");
    skip_tag_buffer(&mut ls).unwrap();

    // DescribeGroups v5 flexible with authorized ops
    let dresp = rpc(
        &addr,
        encode_request_flexible(15, 5, 3, Some("c"), &describe_v5_body("p59g", true)),
    )
    .await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 3);
    skip_tag_buffer(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(ds.get_i16(), 0); // error
    assert_eq!(get_compact_string(&mut ds).unwrap(), "p59g");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "Stable");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "consumer");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "range");
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut ds).unwrap(), "static:pod-a");
    assert_eq!(
        get_compact_nullable_string(&mut ds).unwrap().as_deref(),
        Some("pod-a")
    );
    assert_eq!(get_compact_string(&mut ds).unwrap(), "volant-kafka");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "/");
    let meta = get_compact_bytes(&mut ds).unwrap().unwrap();
    assert!(!meta.is_empty());
    let asg = get_compact_bytes(&mut ds).unwrap().unwrap();
    let _ = decode_consumer_assignment(&asg).unwrap();
    skip_tag_buffer(&mut ds).unwrap(); // member tags
    let auth_ops = ds.get_i32();
    assert_ne!(auth_ops, i32::MIN);
    assert_ne!(auth_ops & (1 << 8), 0); // Describe
    skip_tag_buffer(&mut ds).unwrap(); // group tags
    skip_tag_buffer(&mut ds).unwrap(); // top tags

    // include_ops=false → INT32_MIN
    let dresp2 = rpc(
        &addr,
        encode_request_flexible(15, 5, 4, Some("c"), &describe_v5_body("p59g", false)),
    )
    .await;
    let mut ds2 = dresp2.freeze();
    assert_eq!(ds2.get_i32(), 4);
    skip_tag_buffer(&mut ds2).unwrap();
    assert_eq!(ds2.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut ds2).unwrap(), Some(1));
    assert_eq!(ds2.get_i16(), 0);
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_string(&mut ds2).unwrap();
    assert_eq!(get_compact_array_len(&mut ds2).unwrap(), Some(1));
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_nullable_string(&mut ds2).unwrap();
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_string(&mut ds2).unwrap();
    let _ = get_compact_bytes(&mut ds2).unwrap();
    let _ = get_compact_bytes(&mut ds2).unwrap();
    skip_tag_buffer(&mut ds2).unwrap();
    assert_eq!(ds2.get_i32(), i32::MIN);
    skip_tag_buffer(&mut ds2).unwrap();
    skip_tag_buffer(&mut ds2).unwrap();

    // DeleteGroups v2 while non-empty → NON_EMPTY_GROUP (68)
    let delr = rpc(
        &addr,
        encode_request_flexible(42, 2, 11, Some("c"), &delete_v2_body("p59g")),
    )
    .await;
    let mut del = delr.freeze();
    assert_eq!(del.get_i32(), 11);
    skip_tag_buffer(&mut del).unwrap();
    assert_eq!(del.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut del).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut del).unwrap(), "p59g");
    assert_eq!(del.get_i16(), 68);
    skip_tag_buffer(&mut del).unwrap();
    skip_tag_buffer(&mut del).unwrap();

    // Leave then delete succeeds
    let mut leave = BytesMut::new();
    put_string(&mut leave, "p59g");
    leave.put_i32(1);
    put_string(&mut leave, &member_id);
    put_nullable_string(&mut leave, Some("pod-a"));
    let _ = rpc(&addr, encode_request(13, 3, 12, Some("c"), &leave)).await;

    let delr2 = rpc(
        &addr,
        encode_request_flexible(42, 2, 13, Some("c"), &delete_v2_body("p59g")),
    )
    .await;
    let mut d2 = delr2.freeze();
    assert_eq!(d2.get_i32(), 13);
    skip_tag_buffer(&mut d2).unwrap();
    assert_eq!(d2.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut d2).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut d2).unwrap(), "p59g");
    let err = d2.get_i16();
    assert!(err == 0 || err == 69, "delete after leave err={err}");
    skip_tag_buffer(&mut d2).unwrap();
    skip_tag_buffer(&mut d2).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_group_admin_still_works() {
    let dir = temp_dir("p59", "classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request(11, 5, 10, Some("c"), &join_v5("cg", "", Some("i1"), &["t"])),
    )
    .await;

    // ListGroups v2 classic
    let resp = rpc(&addr, encode_request(16, 2, 2, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i32(), 0); // throttle, no header tags
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    assert!(n >= 1);
    let mut found = false;
    for _ in 0..n {
        let gid = get_string(&mut src).unwrap();
        let _ptype = get_string(&mut src).unwrap();
        if gid == "cg" {
            found = true;
        }
    }
    assert!(found);

    // DescribeGroups v4 classic
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    put_string(&mut dbody, "cg");
    dbody.put_u8(0);
    let dresp = rpc(&addr, encode_request(15, 4, 3, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 3);
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_string(&mut ds).unwrap(), "cg");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("p59", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // DescribeGroups v7 unsupported (v6 ErrorMessage closed by Phase 79)
    let resp = rpc(
        &addr,
        encode_request_flexible(15, 7, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    // ListGroups v6 unsupported (v4–5 closed by Phase 79)
    let resp = rpc(
        &addr,
        encode_request_flexible(16, 6, 2, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    // DeleteGroups v4 unsupported (v3 ErrorMessage closed by Phase 79)
    let resp = rpc(
        &addr,
        encode_request_flexible(42, 4, 3, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_unknown_group_v5() {
    let dir = temp_dir("p59", "unknown");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let dresp = rpc(
        &addr,
        encode_request_flexible(15, 5, 1, Some("c"), &describe_v5_body("nope", false)),
    )
    .await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 1);
    skip_tag_buffer(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(1));
    assert_eq!(ds.get_i16(), 69); // GROUP_ID_NOT_FOUND
    assert_eq!(get_compact_string(&mut ds).unwrap(), "nope");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "Dead");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "");
    assert_eq!(get_compact_string(&mut ds).unwrap(), "");
    assert_eq!(get_compact_array_len(&mut ds).unwrap(), Some(0));
    assert_eq!(ds.get_i32(), i32::MIN); // ops omitted
    skip_tag_buffer(&mut ds).unwrap();
    skip_tag_buffer(&mut ds).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
