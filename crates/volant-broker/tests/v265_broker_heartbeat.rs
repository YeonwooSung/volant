//! v0.265: Kafka BrokerHeartbeat key 63 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;

fn heartbeat_v0(
    broker_id: i32,
    broker_epoch: i64,
    current_metadata_offset: i64,
    want_fence: bool,
    want_shut_down: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(broker_id);
    body.put_i64(broker_epoch);
    body.put_i64(current_metadata_offset);
    body.put_i8(i8::from(want_fence));
    body.put_i8(i8::from(want_shut_down));
    put_empty_tag_buffer(&mut body);
    body
}

fn overlay_ids(b: &Broker) -> Vec<u32> {
    b.list_membership().brokers.iter().map(|x| x.id).collect()
}

fn membership_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("cluster").join("membership.json")
}

#[tokio::test]
async fn api_versions_lists_broker_heartbeat_63() {
    let (_dir, broker) = broker_temp("v265", "api");
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
    assert_eq!(found.get(&63), Some(&(0, 0)));
    assert_eq!(found.get(&62), Some(&(0, 0)));
    assert_eq!(found.get(&64), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn broker_heartbeat_v0_is_42_membership_unchanged() {
    let (dir, broker) = broker_temp("v265", "hb");
    let before = overlay_ids(&broker);
    assert!(
        !membership_file(&dir).exists(),
        "single-node must not start with membership.json"
    );

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            63,
            0,
            10,
            Some("admin"),
            &heartbeat_v0(4, 7, 99, true, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(src.get_i8(), 0); // isCaughtUp = false
    assert_eq!(src.get_i8(), 1); // isFenced = true
    assert_eq!(src.get_i8(), 0); // shouldShutDown = false
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before);
    assert!(
        !membership_file(&dir).exists(),
        "BrokerHeartbeat must not create membership.json"
    );
    server.abort();

    // Existing overlay brokers stay put; a new id is not added.
    let base = unique_dir("v265", "exist");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19631, 19632]);
    let clustered =
        Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    clustered
        .add_broker(3, "127.0.0.1".into(), 19633, None)
        .unwrap();
    let before_cluster = overlay_ids(&clustered);
    assert!(before_cluster.contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&clustered)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            63,
            0,
            11,
            Some("admin"),
            &heartbeat_v0(4, 1, 0, false, true),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42);
    assert_eq!(src.get_i8(), 0);
    assert_eq!(src.get_i8(), 1);
    assert_eq!(src.get_i8(), 0);

    let after = overlay_ids(&clustered);
    assert_eq!(after, before_cluster);
    assert!(!after.contains(&4));
    server.abort();
}

#[tokio::test]
async fn broker_heartbeat_v1_unsupported() {
    let (_dir, broker) = broker_temp("v265", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(63, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
