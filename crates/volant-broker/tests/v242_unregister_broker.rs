//! v0.242: Kafka UnregisterBroker key 64 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;

fn unregister_v0(broker_id: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(broker_id);
    put_empty_tag_buffer(&mut body);
    body
}

fn overlay_ids(b: &Broker) -> Vec<u32> {
    b.list_membership().brokers.iter().map(|x| x.id).collect()
}

#[tokio::test]
async fn api_versions_lists_unregister_broker_64() {
    let base = unique_dir("v242", "api");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19411, 19412]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
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
    assert!(found.len() >= 46);
    assert_eq!(found.get(&64), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn unregister_extra_broker_on_controller() {
    let base = unique_dir("v242", "unreg");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19421, 19422]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    broker
        .add_broker(3, "127.0.0.1".into(), 19423, None)
        .unwrap();
    assert!(overlay_ids(&broker).contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(64, 0, 10, Some("admin"), &unregister_v0(3)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);

    assert!(
        !overlay_ids(&broker).contains(&3),
        "overlay must lose unregistered extra broker"
    );

    server.abort();
}

#[tokio::test]
async fn unregister_broker_not_controller_is_41() {
    let base = unique_dir("v242", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19431, 19432]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(64, 0, 11, Some("admin"), &unregister_v0(1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 41); // NotController

    server.abort();
}

#[tokio::test]
async fn unregister_broker_v1_unsupported() {
    let base = unique_dir("v242", "v1");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19441, 19442]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(64, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always-flex key)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
