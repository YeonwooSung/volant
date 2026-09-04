//! v0.236: Kafka ElectLeaders key 43 v0–1.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_nullable_string, get_string, put_compact_array_len, put_compact_string,
    put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::TopicName;

fn elect_v0(topic: &str, partition: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(5_000); // TimeoutMs ignored
    body.put_i32(1); // one topic
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body
}

fn elect_v1(topic: &str, partition: i32, election_type: i8) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i8(election_type);
    body.put_i32(5_000); // TimeoutMs ignored
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn part_leader(b: &Broker, topic: &str, pid: u32) -> Option<u32> {
    b.clone_live_assignment().and_then(|asg| {
        asg.topics
            .get(topic)
            .and_then(|t| t.partitions.get(&pid))
            .map(|p| p.leader)
    })
}

#[tokio::test]
async fn api_versions_lists_elect_leaders_43() {
    let base = unique_dir("v236", "api");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19301, 19302]);
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
    assert!(found.len() >= 43);
    assert_eq!(found.get(&43), Some(&(0, 1)));

    server.abort();
}

#[tokio::test]
async fn elect_leaders_single_node_current_is_0() {
    let (_dir, broker) = broker_temp("v236", "single");
    broker.create_topic(TopicName::new("events"), 1).unwrap();

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request(43, 0, 10, Some("admin"), &elect_v0("events", 0)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    assert_eq!(src.get_i32(), 1); // one topic
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // per-partition
    let _ = get_nullable_string(&mut src).unwrap();

    server.abort();
}

#[tokio::test]
async fn elect_leaders_cluster_preferred_already_leader_is_0() {
    let base = unique_dir("v236", "pref");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19311, 19312]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    let leader_before = part_leader(&broker, "events", 0).unwrap();
    let gen_before = broker.clone_live_assignment().unwrap().generation;

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(43, 1, 11, Some("admin"), &elect_v1("events", 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();

    assert_eq!(part_leader(&broker, "events", 0), Some(leader_before));
    assert_eq!(
        broker.clone_live_assignment().unwrap().generation,
        gen_before
    );

    server.abort();
}

#[tokio::test]
async fn elect_leaders_unclean_type_1_is_87() {
    let base = unique_dir("v236", "unclean");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19321, 19322]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    let leader_before = part_leader(&broker, "events", 0).unwrap();
    let gen_before = broker.clone_live_assignment().unwrap().generation;

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(43, 1, 12, Some("admin"), &elect_v1("events", 0, 1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 87); // ELIGIBLE_LEADERS_NOT_AVAILABLE
    let _ = get_compact_nullable_string(&mut src).unwrap();

    assert_eq!(part_leader(&broker, "events", 0), Some(leader_before));
    assert_eq!(
        broker.clone_live_assignment().unwrap().generation,
        gen_before,
        "unclean must not change leader or generation"
    );

    server.abort();
}

#[tokio::test]
async fn elect_leaders_not_controller_is_41() {
    let base = unique_dir("v236", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19331, 19332]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(43, 1, 13, Some("admin"), &elect_v1("events", 0, 0)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 41); // NOT_CONTROLLER
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));

    server.abort();
}

#[tokio::test]
async fn elect_leaders_v2_unsupported() {
    let base = unique_dir("v236", "v2");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19341, 19342]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(43, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (v2 treated as flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
