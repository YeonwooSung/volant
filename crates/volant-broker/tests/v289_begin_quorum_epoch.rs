//! v0.289: Kafka BeginQuorumEpoch key 53 v1 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_uuid,
    skip_tag_buffer,
};
use volant_broker::kafka::SUPPORTED_APIS;
use volant_broker::Broker;

fn begin_quorum_epoch_v1(
    cluster_id: Option<&str>,
    voter_id: i32,
    topics: &[(&str, &[(i32, [u8; 16], i32, i32)])],
    endpoints: &[(&str, &str, u16)],
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, cluster_id);
    body.put_i32(voter_id);
    put_compact_array_len(&mut body, topics.len());
    for (name, partitions) in topics {
        put_compact_string(&mut body, name);
        put_compact_array_len(&mut body, partitions.len());
        for (partition, directory_id, leader_id, leader_epoch) in *partitions {
            body.put_i32(*partition);
            put_uuid(&mut body, directory_id);
            body.put_i32(*leader_id);
            body.put_i32(*leader_epoch);
            put_empty_tag_buffer(&mut body);
        }
        put_empty_tag_buffer(&mut body);
    }
    put_compact_array_len(&mut body, endpoints.len());
    for (name, host, port) in endpoints {
        put_compact_string(&mut body, name);
        put_compact_string(&mut body, host);
        body.put_u16(*port);
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
async fn api_versions_lists_begin_quorum_epoch_53() {
    let (_dir, broker) = broker_temp("v289", "api");
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
    assert!(found.len() >= 85);
    assert_eq!(found.get(&53), Some(&(1, 1)));
    assert_eq!(found.get(&52), Some(&(0, 0)));
    assert_eq!(found.get(&55), Some(&(0, 1)));
    assert!(SUPPORTED_APIS.len() >= 85);

    server.abort();
}

#[tokio::test]
async fn begin_quorum_epoch_v1_is_42_membership_unchanged() {
    let (_dir, broker) = broker_temp("v289", "epoch");
    let before_ids = overlay_ids(&broker);
    let before_raft = raft_state(&broker);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut directory = [0u8; 16];
    directory[15] = 0x53;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            53,
            1,
            10,
            Some("admin"),
            &begin_quorum_epoch_v1(
                Some("volant-cluster"),
                2,
                &[("events", &[(0, directory, 1, 7)])],
                &[("CONTROLLER", "127.0.0.1", 19094)],
            ),
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
async fn begin_quorum_epoch_v0_is_35() {
    let (_dir, broker) = broker_temp("v289", "v0");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(53, 0, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn begin_quorum_epoch_v2_is_35() {
    let (_dir, broker) = broker_temp("v289", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(53, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}

#[tokio::test]
async fn begin_quorum_epoch_acl_deny_is_31() {
    let (_dir, broker) = broker_temp("v289", "acl");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let before_ids = overlay_ids(&broker);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut directory = [0u8; 16];
    directory[15] = 0x31;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            53,
            1,
            11,
            Some("admin"),
            &begin_quorum_epoch_v1(
                Some("volant-cluster"),
                2,
                &[("events", &[(0, directory, 1, 7)])],
                &[],
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 31); // CLUSTER_AUTHORIZATION_FAILED
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before_ids);
    server.abort();
}
