//! v0.270: Kafka Vote key 52 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;

fn vote_v0(
    cluster_id: Option<&str>,
    topics: &[(&str, &[(i32, i32, i32, i32, i64)])],
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, cluster_id);
    put_compact_array_len(&mut body, topics.len());
    for (name, partitions) in topics {
        put_compact_string(&mut body, name);
        put_compact_array_len(&mut body, partitions.len());
        for (partition, replica_epoch, replica_id, last_offset_epoch, last_offset) in *partitions {
            body.put_i32(*partition);
            body.put_i32(*replica_epoch);
            body.put_i32(*replica_id);
            body.put_i32(*last_offset_epoch);
            body.put_i64(*last_offset);
            put_empty_tag_buffer(&mut body);
        }
        put_empty_tag_buffer(&mut body);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn overlay_ids(b: &Broker) -> Vec<u32> {
    b.list_membership().brokers.iter().map(|x| x.id).collect()
}

fn raft_state(b: &Broker) -> (bool, Option<u32>, u64) {
    (
        b.openraft_started(),
        b.openraft_leader_id(),
        b.openraft_term(),
    )
}

#[tokio::test]
async fn api_versions_lists_vote_52() {
    let (_dir, broker) = broker_temp("v270", "api");
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
    assert_eq!(found.get(&52), Some(&(0, 0)));
    assert_eq!(found.get(&55), Some(&(0, 1)));

    server.abort();
}

#[tokio::test]
async fn vote_v0_is_42_empty_topics_no_vote() {
    let (_dir, broker) = broker_temp("v270", "vote");
    let before_ids = overlay_ids(&broker);
    let before_raft = raft_state(&broker);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            52,
            0,
            10,
            Some("admin"),
            &vote_v0(Some("volant-cluster"), &[("events", &[(0, 1, 2, 0, 0)])]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST — no throttleTimeMs
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before_ids);
    assert_eq!(raft_state(&broker), before_raft, "openraft state unchanged");
    server.abort();
}

#[tokio::test]
async fn vote_v1_unsupported() {
    let (_dir, broker) = broker_temp("v270", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(52, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
