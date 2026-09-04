//! v0.273: Kafka UpdateRaftVoter key 82 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_uuid, skip_tag_buffer,
};
use volant_broker::Broker;

fn update_v0(
    cluster_id: Option<&str>,
    current_leader_epoch: i32,
    voter_id: i32,
    voter_directory_id: &[u8; 16],
    listeners: &[(&str, &str, u16)],
    min_supported: i16,
    max_supported: i16,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, cluster_id);
    body.put_i32(current_leader_epoch);
    body.put_i32(voter_id);
    put_uuid(&mut body, voter_directory_id);
    put_compact_array_len(&mut body, listeners.len());
    for (name, host, port) in listeners {
        put_compact_string(&mut body, name);
        put_compact_string(&mut body, host);
        body.put_u16(*port);
        put_empty_tag_buffer(&mut body);
    }
    body.put_i16(min_supported);
    body.put_i16(max_supported);
    put_empty_tag_buffer(&mut body); // KRaftVersionFeature tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn overlay_ids(b: &Broker) -> Vec<u32> {
    b.list_membership().brokers.iter().map(|x| x.id).collect()
}

fn membership_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("cluster").join("membership.json")
}

#[tokio::test]
async fn api_versions_lists_update_raft_voter_82() {
    let (_dir, broker) = broker_temp("v273", "api");
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
    assert_eq!(found.get(&82), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn update_raft_voter_v0_is_42_membership_unchanged() {
    let (dir, broker) = broker_temp("v273", "upd");
    let before = overlay_ids(&broker);
    assert!(
        !membership_file(&dir).exists(),
        "single-node must not start with membership.json"
    );

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut directory = [0u8; 16];
    directory[15] = 0x73;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            82,
            0,
            10,
            Some("admin"),
            &update_v0(
                Some("volant-cluster"),
                1,
                2,
                &directory,
                &[("CONTROLLER", "127.0.0.1", 19094)],
                0,
                1,
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before);
    assert!(
        !membership_file(&dir).exists(),
        "UpdateRaftVoter must not create membership.json"
    );
    server.abort();

    // Existing overlay brokers stay put; a new id is not added.
    let base = unique_dir("v273", "exist");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19731, 19732]);
    let clustered =
        Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    clustered
        .add_broker(3, "127.0.0.1".into(), 19733, None)
        .unwrap();
    let before_cluster = overlay_ids(&clustered);
    assert!(before_cluster.contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&clustered)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            82,
            0,
            11,
            Some("admin"),
            &update_v0(
                None,
                3,
                4,
                &directory,
                &[("CONTROLLER", "127.0.0.1", 19734)],
                1,
                1,
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42);
    skip_tag_buffer(&mut src).unwrap();

    let after = overlay_ids(&clustered);
    assert_eq!(after, before_cluster);
    assert!(!after.contains(&4));
    server.abort();
}

#[tokio::test]
async fn update_raft_voter_v1_unsupported() {
    let (_dir, broker) = broker_temp("v273", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(82, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
