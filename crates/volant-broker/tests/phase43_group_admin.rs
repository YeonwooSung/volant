//! Phase 43: Kafka group-admin classic versions (Describe/List/DeleteGroups).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, get_bytes, get_nullable_string, get_string,
    put_bytes, put_nullable_string, put_string,
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

#[tokio::test]
async fn api_versions_group_admin_classic_max() {
    let dir = temp_dir("p43", "api");
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
    assert_eq!(found.get(&15), Some(&(0, 5))); // DescribeGroups (Phase 59 flex v5)
    assert_eq!(found.get(&16), Some(&(0, 3))); // ListGroups (Phase 59 flex v3)
    assert_eq!(found.get(&42), Some(&(0, 2))); // DeleteGroups (Phase 59 flex v2)
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_groups_v2_throttle() {
    let dir = temp_dir("p43", "list");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Create a live group
    let _ = rpc(
        &addr,
        encode_request(11, 5, 10, Some("c"), &join_v5("lg", "", Some("i1"), &["t"])),
    )
    .await;

    let resp = rpc(&addr, encode_request(16, 2, 2, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2); // correlation
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let n = src.get_i32();
    assert!(n >= 1);
    let mut found = false;
    for _ in 0..n {
        let gid = get_string(&mut src).unwrap();
        let ptype = get_string(&mut src).unwrap();
        if gid == "lg" {
            found = true;
            assert_eq!(ptype, "consumer");
        }
    }
    assert!(found, "lg not listed");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_groups_v4_static_and_auth_ops() {
    let dir = temp_dir("p43", "describe");
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
            &join_v5("dg", "", Some("pod-a"), &["events"]),
        ),
    )
    .await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i32(), 0); // throttle
    assert_eq!(js.get_i16(), 0);
    let _ = js.get_i32(); // gen
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert_eq!(member_id, "static:pod-a");

    // DescribeGroups v4 with include_authorized_operations = true
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    put_string(&mut dbody, "dg");
    dbody.put_u8(1); // include_authorized_operations
    let dresp = rpc(&addr, encode_request(15, 4, 3, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 3);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1); // groups
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_string(&mut ds).unwrap(), "dg");
    assert_eq!(get_string(&mut ds).unwrap(), "Stable");
    assert_eq!(get_string(&mut ds).unwrap(), "consumer");
    let _proto = get_string(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 1); // members
    assert_eq!(get_string(&mut ds).unwrap(), "static:pod-a");
    assert_eq!(
        get_nullable_string(&mut ds).unwrap().as_deref(),
        Some("pod-a")
    );
    let _ = get_string(&mut ds).unwrap(); // client_id
    let _ = get_string(&mut ds).unwrap(); // host
    let _ = get_bytes(&mut ds).unwrap();
    let _ = get_bytes(&mut ds).unwrap();
    let auth_ops = ds.get_i32();
    assert_ne!(auth_ops, i32::MIN);
    // Describe bit (Kafka code 8)
    assert_ne!(auth_ops & (1 << 8), 0);
    // Read bit (Kafka code 3)
    assert_ne!(auth_ops & (1 << 3), 0);

    // include=false → INT32_MIN
    let mut dbody2 = BytesMut::new();
    dbody2.put_i32(1);
    put_string(&mut dbody2, "dg");
    dbody2.put_u8(0);
    let dresp2 = rpc(&addr, encode_request(15, 4, 4, Some("c"), &dbody2)).await;
    let mut ds2 = dresp2.freeze();
    assert_eq!(ds2.get_i32(), 4);
    assert_eq!(ds2.get_i32(), 0);
    assert_eq!(ds2.get_i32(), 1);
    assert_eq!(ds2.get_i16(), 0);
    let _ = get_string(&mut ds2).unwrap();
    let _ = get_string(&mut ds2).unwrap();
    let _ = get_string(&mut ds2).unwrap();
    let _ = get_string(&mut ds2).unwrap();
    assert_eq!(ds2.get_i32(), 1);
    let _ = get_string(&mut ds2).unwrap();
    let _ = get_nullable_string(&mut ds2).unwrap();
    let _ = get_string(&mut ds2).unwrap();
    let _ = get_string(&mut ds2).unwrap();
    let _ = get_bytes(&mut ds2).unwrap();
    let _ = get_bytes(&mut ds2).unwrap();
    assert_eq!(ds2.get_i32(), i32::MIN);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_groups_v1_throttle_and_non_empty() {
    let dir = temp_dir("p43", "delete");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(
        &addr,
        encode_request(11, 5, 10, Some("c"), &join_v5("del-g", "", Some("x"), &["t"])),
    )
    .await;
    let mut js = jresp.freeze();
    js.advance(4 + 4 + 2 + 4); // corr, throttle, err, gen
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();

    // Non-empty → NON_EMPTY_GROUP (68)
    let mut del = BytesMut::new();
    del.put_i32(1);
    put_string(&mut del, "del-g");
    let delr = rpc(&addr, encode_request(42, 1, 11, Some("c"), &del)).await;
    let mut ds = delr.freeze();
    assert_eq!(ds.get_i32(), 11);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), "del-g");
    assert_eq!(ds.get_i16(), 68);

    // Leave then delete succeeds
    let mut leave = BytesMut::new();
    put_string(&mut leave, "del-g");
    leave.put_i32(1);
    put_string(&mut leave, &member_id);
    put_nullable_string(&mut leave, Some("x"));
    let _ = rpc(&addr, encode_request(13, 3, 12, Some("c"), &leave)).await;

    let delr2 = rpc(&addr, encode_request(42, 1, 13, Some("c"), &del)).await;
    let mut d2 = delr2.freeze();
    assert_eq!(d2.get_i32(), 13);
    assert_eq!(d2.get_i32(), 0);
    assert_eq!(d2.get_i32(), 1);
    assert_eq!(get_string(&mut d2).unwrap(), "del-g");
    let err = d2.get_i16();
    assert!(err == 0 || err == 69, "delete after leave err={err}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_groups_v0_still_works() {
    let dir = temp_dir("p43", "v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let sub = encode_consumer_subscription(&["t"]);
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, "v0g");
    jbody.put_i32(10_000);
    put_string(&mut jbody, "");
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1);
    put_string(&mut jbody, "range");
    put_bytes(&mut jbody, Some(&sub));
    let jresp = rpc(&addr, encode_request(11, 0, 1, Some("c"), &jbody)).await;
    let mut js = jresp.freeze();
    js.advance(4);
    assert_eq!(js.get_i16(), 0);
    let _ = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();

    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    put_string(&mut dbody, "v0g");
    let dresp = rpc(&addr, encode_request(15, 0, 2, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4);
    // v0: no throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_string(&mut ds).unwrap(), "v0g");
    assert_eq!(get_string(&mut ds).unwrap(), "Stable");
    let _ = get_string(&mut ds).unwrap();
    let _ = get_string(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), member_id);
    // no group_instance_id on v0
    let _ = get_string(&mut ds).unwrap();
    let _ = get_string(&mut ds).unwrap();
    let _ = get_bytes(&mut ds).unwrap();
    let _ = get_bytes(&mut ds).unwrap();
    // no authorized_ops on v0
    assert_eq!(ds.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
