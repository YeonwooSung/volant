//! Phase 42: Kafka consumer group classic versions + static membership.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    decode_consumer_assignment, encode_consumer_subscription, encode_request, get_bytes,
    get_nullable_string, get_string, put_bytes, put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// JoinGroup v5 body with optional group.instance.id.
fn join_v5(group: &str, member_id: &str, instance: Option<&str>, topics: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(10_000); // session
    body.put_i32(10_000); // rebalance
    put_string(&mut body, member_id);
    put_nullable_string(&mut body, instance);
    put_string(&mut body, "consumer");
    body.put_i32(1); // protocols
    put_string(&mut body, "range");
    let sub = encode_consumer_subscription(topics);
    put_bytes(&mut body, Some(&sub));
    body
}

#[tokio::test]
async fn api_versions_group_classic_max() {
    let dir = temp_dir("p42", "api");
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
    assert_eq!(found.get(&11), Some(&(0, 9))); // JoinGroup (Phase 56 flex v6–9)
    assert_eq!(found.get(&12), Some(&(0, 4))); // Heartbeat (Phase 55 flex v4)
    assert_eq!(found.get(&13), Some(&(0, 5))); // LeaveGroup (Phase 56 flex v4–5)
    assert_eq!(found.get(&14), Some(&(0, 5))); // SyncGroup (Phase 56 flex v4–5)
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn static_join_sync_heartbeat_leave_v3() {
    let dir = temp_dir("p42", "static");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // JoinGroup v5 with empty member_id + group.instance.id
    let resp = rpc(
        &addr,
        encode_request(11, 5, 10, Some("c"), &join_v5("cg", "", Some("inst-1"), &["events"])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let generation = src.get_i32();
    assert!(generation > 0);
    let protocol = get_string(&mut src).unwrap();
    assert_eq!(protocol, "range");
    let leader = get_string(&mut src).unwrap();
    let member_id = get_string(&mut src).unwrap();
    assert_eq!(member_id, "static:inst-1");
    assert_eq!(leader, member_id);
    let n_members = src.get_i32();
    assert_eq!(n_members, 1);
    assert_eq!(get_string(&mut src).unwrap(), "static:inst-1");
    assert_eq!(get_nullable_string(&mut src).unwrap().as_deref(), Some("inst-1"));
    let _ = get_bytes(&mut src).unwrap();

    // SyncGroup v3
    let mut sbody = BytesMut::new();
    put_string(&mut sbody, "cg");
    sbody.put_i32(generation);
    put_string(&mut sbody, &member_id);
    put_nullable_string(&mut sbody, Some("inst-1"));
    sbody.put_i32(0); // assignments empty (follower)
    let sresp = rpc(&addr, encode_request(14, 3, 11, Some("c"), &sbody)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i32(), 0); // throttle
    assert_eq!(ss.get_i16(), 0);
    let assign = get_bytes(&mut ss).unwrap().unwrap_or_default();
    let parts = decode_consumer_assignment(&assign).unwrap();
    assert_eq!(parts.len(), 2); // both partitions for single member

    // Heartbeat v3
    let mut hbody = BytesMut::new();
    put_string(&mut hbody, "cg");
    hbody.put_i32(generation);
    put_string(&mut hbody, &member_id);
    put_nullable_string(&mut hbody, Some("inst-1"));
    let hresp = rpc(&addr, encode_request(12, 3, 12, Some("c"), &hbody)).await;
    let mut hs = hresp.freeze();
    assert_eq!(hs.get_i32(), 12);
    assert_eq!(hs.get_i32(), 0); // throttle
    assert_eq!(hs.get_i16(), 0);

    // LeaveGroup v3 by instance id only
    let mut lbody = BytesMut::new();
    put_string(&mut lbody, "cg");
    lbody.put_i32(1); // members
    put_string(&mut lbody, ""); // empty member_id
    put_nullable_string(&mut lbody, Some("inst-1"));
    let lresp = rpc(&addr, encode_request(13, 3, 13, Some("c"), &lbody)).await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 13);
    assert_eq!(ls.get_i32(), 0); // throttle
    assert_eq!(ls.get_i16(), 0); // top-level
    assert_eq!(ls.get_i32(), 1); // members
    let left_mid = get_string(&mut ls).unwrap();
    assert_eq!(left_mid, "static:inst-1");
    assert_eq!(get_nullable_string(&mut ls).unwrap().as_deref(), Some("inst-1"));
    assert_eq!(ls.get_i16(), 0);

    // Heartbeat after leave → unknown member
    let mut h2 = BytesMut::new();
    put_string(&mut h2, "cg");
    h2.put_i32(generation);
    put_string(&mut h2, &member_id);
    put_nullable_string(&mut h2, Some("inst-1"));
    let h2r = rpc(&addr, encode_request(12, 3, 14, Some("c"), &h2)).await;
    let mut h2s = h2r.freeze();
    assert_eq!(h2s.get_i32(), 14);
    assert_eq!(h2s.get_i32(), 0);
    assert_eq!(h2s.get_i16(), 25); // UNKNOWN_MEMBER_ID

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn static_rejoin_same_instance_id() {
    let dir = temp_dir("p42", "rejoin");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let r1 = rpc(
        &addr,
        encode_request(11, 5, 1, Some("c"), &join_v5("g", "", Some("pod-a"), &["t"])),
    )
    .await;
    let mut s1 = r1.freeze();
    s1.advance(4 + 4 + 2 + 4); // corr, throttle, err, gen
    let _ = get_string(&mut s1).unwrap(); // protocol
    let _ = get_string(&mut s1).unwrap(); // leader
    let mid1 = get_string(&mut s1).unwrap();
    assert_eq!(mid1, "static:pod-a");

    // Re-join same instance without member_id → same static id
    let r2 = rpc(
        &addr,
        encode_request(11, 5, 2, Some("c"), &join_v5("g", "", Some("pod-a"), &["t"])),
    )
    .await;
    let mut s2 = r2.freeze();
    s2.advance(4 + 4 + 2 + 4);
    let _ = get_string(&mut s2).unwrap();
    let _ = get_string(&mut s2).unwrap();
    let mid2 = get_string(&mut s2).unwrap();
    assert_eq!(mid2, mid1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn heartbeat_v1_has_throttle() {
    let dir = temp_dir("p42", "hb");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Join v1 (no instance)
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, "g");
    jbody.put_i32(10_000);
    jbody.put_i32(10_000);
    put_string(&mut jbody, "");
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1);
    put_string(&mut jbody, "range");
    put_bytes(
        &mut jbody,
        Some(&encode_consumer_subscription(&["t"])),
    );
    let jresp = rpc(&addr, encode_request(11, 1, 1, Some("c"), &jbody)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 1);
    // v1 has no throttle
    assert_eq!(js.get_i16(), 0);
    let gen = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let mid = get_string(&mut js).unwrap();

    let mut hbody = BytesMut::new();
    put_string(&mut hbody, "g");
    hbody.put_i32(gen);
    put_string(&mut hbody, &mid);
    let hresp = rpc(&addr, encode_request(12, 1, 2, Some("c"), &hbody)).await;
    let mut hs = hresp.freeze();
    assert_eq!(hs.get_i32(), 2);
    assert_eq!(hs.get_i32(), 0); // throttle v1+
    assert_eq!(hs.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
