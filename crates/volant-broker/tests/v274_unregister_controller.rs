//! v0.274: Kafka UnregisterController key 94 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;

fn unregister_v0(controller_id: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(controller_id);
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
async fn api_versions_lists_unregister_controller_94() {
    let (_dir, broker) = broker_temp("v274", "api");
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
    assert!(found.len() >= 70);
    assert_eq!(found.get(&94), Some(&(0, 0)));
    assert_eq!(found.get(&64), Some(&(0, 0)));
    assert_eq!(found.get(&70), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn unregister_controller_v0_is_42_membership_unchanged() {
    let (dir, broker) = broker_temp("v274", "unreg");
    let before = overlay_ids(&broker);
    assert!(
        !membership_file(&dir).exists(),
        "single-node must not start with membership.json"
    );

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(94, 0, 10, Some("admin"), &unregister_v0(1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KRaft controller unregister")
    );
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before);
    assert!(
        !membership_file(&dir).exists(),
        "UnregisterController must not create membership.json"
    );
    server.abort();

    // Existing overlay brokers stay put; remove_broker is not called.
    let base = unique_dir("v274", "exist");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19741, 19742]);
    let clustered =
        Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    clustered
        .add_broker(3, "127.0.0.1".into(), 19743, None)
        .unwrap();
    let before_cluster = overlay_ids(&clustered);
    assert!(before_cluster.contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&clustered)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(94, 0, 11, Some("admin"), &unregister_v0(3)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KRaft controller unregister")
    );

    let after = overlay_ids(&clustered);
    assert_eq!(after, before_cluster);
    assert!(after.contains(&3));
    server.abort();
}

#[tokio::test]
async fn unregister_controller_v1_unsupported() {
    let (_dir, broker) = broker_temp("v274", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(94, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
