//! v0.228: Kafka ListPartitionReassignments key 46 v0.

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

fn list_v0_topic(topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(5_000); // TimeoutMs ignored
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, partitions.len());
    for &p in partitions {
        body.put_i32(p);
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn list_v0_all() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(5_000);
    put_unsigned_varint(&mut body, 0); // topics = null
    put_empty_tag_buffer(&mut body);
    body
}

fn read_part(src: &mut impl Buf) -> (i32, Vec<i32>, Vec<i32>, Vec<i32>, i16) {
    let pid = src.get_i32();
    let n_rep = get_compact_array_len(src).unwrap().unwrap_or(0);
    let mut replicas = Vec::with_capacity(n_rep);
    for _ in 0..n_rep {
        replicas.push(src.get_i32());
    }
    let n_add = get_compact_array_len(src).unwrap().unwrap_or(0);
    let mut adding = Vec::with_capacity(n_add);
    for _ in 0..n_add {
        adding.push(src.get_i32());
    }
    let n_rem = get_compact_array_len(src).unwrap().unwrap_or(0);
    let mut removing = Vec::with_capacity(n_rem);
    for _ in 0..n_rem {
        removing.push(src.get_i32());
    }
    let code = src.get_i16();
    let _ = get_compact_nullable_string(src).unwrap();
    let _ = skip_tag_buffer(src);
    (pid, replicas, adding, removing, code)
}

fn part_replicas(b: &Broker, topic: &str, pid: u32) -> Vec<i32> {
    b.clone_live_assignment()
        .and_then(|asg| {
            asg.topics
                .get(topic)
                .and_then(|t| t.partitions.get(&pid))
                .map(|p| p.replicas.iter().map(|&r| r as i32).collect())
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn api_versions_lists_list_partition_reassignments_46() {
    let base = unique_dir("v228", "api");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19201, 19202]);
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
    assert_eq!(found.len(), 79);
    assert_eq!(found.get(&45), Some(&(0, 0)));
    assert_eq!(found.get(&46), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn list_partition_reassignments_v0_current_replicas() {
    let base = unique_dir("v228", "list");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19211, 19212]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    let expected = part_replicas(&broker, "events", 0);
    assert_eq!(expected, vec![1, 2]);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(46, 0, 10, Some("admin"), &list_v0_topic("events", &[0])),
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
    let (pid, replicas, adding, removing, code) = read_part(&mut src);
    assert_eq!(pid, 0);
    assert_eq!(replicas, expected);
    assert!(adding.is_empty());
    assert!(removing.is_empty());
    assert_eq!(code, 0);

    server.abort();
}

#[tokio::test]
async fn list_partition_reassignments_topics_null_lists_all() {
    let base = unique_dir("v228", "all");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19221, 19222]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    broker.create_topic(TopicName::new("logs"), 2).unwrap();

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(46, 0, 11, Some("admin"), &list_v0_all()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..n {
        let name = get_compact_string(&mut src).unwrap();
        let pc = get_compact_array_len(&mut src).unwrap().unwrap();
        for _ in 0..pc {
            let (pid, _r, adding, removing, code) = read_part(&mut src);
            assert!(adding.is_empty(), "addingReplicas must be empty");
            assert!(removing.is_empty(), "removingReplicas must be empty");
            assert_eq!(code, 0);
            seen.insert((name.clone(), pid));
        }
        skip_tag_buffer(&mut src).unwrap();
    }
    assert!(seen.contains(&("events".into(), 0)));
    assert!(seen.contains(&("logs".into(), 0)));
    assert!(seen.contains(&("logs".into(), 1)));

    server.abort();
}

#[tokio::test]
async fn list_partition_reassignments_unknown_topic_is_3() {
    let base = unique_dir("v228", "unk");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19231, 19232]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(46, 0, 12, Some("admin"), &list_v0_topic("missing", &[0])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "missing");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let (_pid, _r, adding, removing, code) = read_part(&mut src);
    assert!(adding.is_empty());
    assert!(removing.is_empty());
    assert_eq!(code, 3); // UNKNOWN_TOPIC_OR_PARTITION

    server.abort();
}

#[tokio::test]
async fn list_partition_reassignments_v1_unsupported() {
    let base = unique_dir("v228", "v1");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19241, 19242]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(46, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always-flex key)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn list_partition_reassignments_non_controller_is_41() {
    let base = unique_dir("v228", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19251, 19252]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(46, 0, 13, Some("admin"), &list_v0_all()),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 41); // NOT_CONTROLLER
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));

    server.abort();
}
