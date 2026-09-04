//! v0.225: Kafka AlterPartitionReassignments key 45 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    put_unsigned_varint, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::TopicName;

fn alter_v0(topic: &str, partition: i32, replicas: Option<&[i32]>) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(5_000); // TimeoutMs ignored
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    match replicas {
        None => put_unsigned_varint(&mut body, 0),
        Some(ids) => {
            put_compact_array_len(&mut body, ids.len());
            for &id in ids {
                body.put_i32(id);
            }
        }
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn part_replicas(b: &Broker, topic: &str, pid: u32) -> Vec<u32> {
    b.clone_live_assignment()
        .and_then(|asg| {
            asg.topics
                .get(topic)
                .and_then(|t| t.partitions.get(&pid))
                .map(|p| p.replicas.clone())
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn api_versions_lists_alter_partition_reassignments_45() {
    let base = unique_dir("v225", "api");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19101, 19102]);
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
    assert_eq!(found.len(), 40);
    assert_eq!(found.get(&45), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn alter_partition_reassignments_v0_hits_native_path() {
    let base = unique_dir("v225", "reassign");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19111, 19112]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    assert_eq!(part_replicas(&broker, "events", 0), vec![1, 2]);
    let gen_before = broker.clone_live_assignment().unwrap().generation;

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(45, 0, 10, Some("admin"), &alter_v0("events", 0, Some(&[1]))),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);

    assert_eq!(part_replicas(&broker, "events", 0), vec![1]);
    assert!(
        broker.clone_live_assignment().unwrap().generation > gen_before,
        "native reassign must bump assignment generation"
    );

    server.abort();
}

#[tokio::test]
async fn alter_partition_reassignments_v1_unsupported() {
    let base = unique_dir("v225", "v1");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19121, 19122]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(45, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always-flex key)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
