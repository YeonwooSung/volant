//! v0.246: Kafka AllocateProducerIds key 67 v0.

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

fn allocate_v0(broker_id: i32, broker_epoch: i64) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(broker_id);
    body.put_i64(broker_epoch);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_allocate_producer_ids_67() {
    let (_dir, broker) = broker_temp("v246", "api");
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
    assert!(found.len() >= 50);
    assert_eq!(found.get(&67), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn single_node_allocate_block_of_1000() {
    let (_dir, broker) = broker_temp("v246", "alloc");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(67, 0, 10, Some("admin"), &allocate_v0(0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    let start1 = src.get_i64();
    let len1 = src.get_i32();
    assert!(start1 >= 0);
    assert_eq!(len1, 1000);

    let resp = rpc(
        &addr,
        encode_request_flexible(67, 0, 11, Some("admin"), &allocate_v0(0, 7)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let start2 = src.get_i64();
    let len2 = src.get_i32();
    assert_eq!(start2, start1 + 1000);
    assert_eq!(len2, 1000);

    server.abort();
}

#[tokio::test]
async fn allocate_producer_ids_not_controller_is_41() {
    let base = unique_dir("v246", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19471, 19472]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(67, 0, 13, Some("admin"), &allocate_v0(2, 0)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 13);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 41); // NOT_CONTROLLER

    server.abort();
}

#[tokio::test]
async fn allocate_producer_ids_v1_unsupported() {
    let (_dir, broker) = broker_temp("v246", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(67, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
