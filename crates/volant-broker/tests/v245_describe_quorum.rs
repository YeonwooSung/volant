//! v0.245: Kafka DescribeQuorum key 55 v0–1.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_array_len,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;

fn describe_quorum_empty() -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_describe_quorum_55() {
    let (_dir, broker) = broker_temp("v245", "api");
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
    assert_eq!(found.get(&55), Some(&(0, 1)));

    server.abort();
}

#[tokio::test]
async fn describe_quorum_single_node_raft_off_is_0() {
    let (_dir, broker) = broker_temp("v245", "single");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(55, 0, 10, Some("admin"), &describe_quorum_empty()),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i16(), 0); // top-level
    let n = get_compact_array_len(&mut src).unwrap();
    assert!(
        n == Some(0) || n == Some(1),
        "raft off: empty topics or synthetic partition, got {n:?}"
    );

    server.abort();
}

#[tokio::test]
async fn describe_quorum_not_controller_is_41() {
    let base = unique_dir("v245", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19471, 19472]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(55, 0, 13, Some("admin"), &describe_quorum_empty()),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 13);
    assert_eq!(src.get_i16(), 41); // NOT_CONTROLLER

    server.abort();
}

#[tokio::test]
async fn describe_quorum_v2_unsupported() {
    let (_dir, broker) = broker_temp("v245", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(55, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
