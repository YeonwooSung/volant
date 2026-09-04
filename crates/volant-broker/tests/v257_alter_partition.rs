//! v0.257: Kafka AlterPartition key 56 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::TopicName;

fn alter_v0(
    broker_id: i32,
    topic: &str,
    partition: i32,
    leader_epoch: i32,
    isr: &[i32],
    partition_epoch: i32,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(broker_id);
    body.put_i64(-1); // BrokerEpoch parsed and ignored
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i32(leader_epoch);
    put_compact_array_len(&mut body, isr.len());
    for &id in isr {
        body.put_i32(id);
    }
    body.put_i32(partition_epoch);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_alter_partition_56() {
    let (_dir, broker) = broker_temp("v257", "api");
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
    assert!(found.len() >= 57);
    assert_eq!(found.get(&56), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn alter_partition_single_node_local_isr_is_0() {
    let (_dir, broker) = broker_temp("v257", "single");
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    let local = broker.node_id() as i32;

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            56,
            0,
            10,
            Some("admin"),
            &alter_v0(local, "events", 0, 0, &[local], 0),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // per-partition

    server.abort();
}

#[tokio::test]
async fn alter_partition_cluster_not_controller_is_41() {
    let base = unique_dir("v257", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19651, 19652]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            56,
            0,
            11,
            Some("admin"),
            &alter_v0(2, "events", 0, 0, &[2], 0),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0); // throttle
    let top = src.get_i16();
    if top != 41 {
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
        let _ = get_compact_string(&mut src).unwrap();
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
        assert_eq!(src.get_i32(), 0);
        assert_eq!(src.get_i16(), 41); // NOT_CONTROLLER
    }

    server.abort();
}

#[tokio::test]
async fn alter_partition_v1_unsupported() {
    let (_dir, broker) = broker_temp("v257", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(56, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 99);
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
