//! v0.271: Kafka AddRaftVoter key 80 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_uuid,
    skip_tag_buffer,
};
use volant_broker::Broker;

fn add_voter_v0(
    cluster_id: Option<&str>,
    timeout_ms: i32,
    voter_id: i32,
    directory_id: &[u8; 16],
    listeners: &[(&str, &str, u16)],
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, cluster_id);
    body.put_i32(timeout_ms);
    body.put_i32(voter_id);
    put_uuid(&mut body, directory_id);
    put_compact_array_len(&mut body, listeners.len());
    for (name, host, port) in listeners {
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

fn membership_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("cluster").join("membership.json")
}

#[tokio::test]
async fn api_versions_lists_add_raft_voter_80() {
    let (_dir, broker) = broker_temp("v271", "api");
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
    assert_eq!(found.get(&80), Some(&(0, 0)));
    assert_eq!(found.get(&75), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn add_raft_voter_v0_is_42_membership_unchanged() {
    let (dir, broker) = broker_temp("v271", "voter");
    let before = overlay_ids(&broker);
    assert!(
        !membership_file(&dir).exists(),
        "single-node must not start with membership.json"
    );

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut directory = [0u8; 16];
    directory[15] = 0x71;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            80,
            0,
            10,
            Some("admin"),
            &add_voter_v0(
                Some("volant-cluster"),
                5_000,
                4,
                &directory,
                &[("PLAINTEXT", "127.0.0.1", 19094)],
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KRaft raft voter")
    );
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before);
    assert!(
        !membership_file(&dir).exists(),
        "AddRaftVoter must not create membership.json"
    );
    server.abort();

    // Existing overlay brokers stay put; a new id is not added.
    let base = unique_dir("v271", "exist");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19681, 19682]);
    let clustered =
        Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    clustered
        .add_broker(3, "127.0.0.1".into(), 19683, None)
        .unwrap();
    let before_cluster = overlay_ids(&clustered);
    assert!(before_cluster.contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&clustered)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            80,
            0,
            11,
            Some("admin"),
            &add_voter_v0(
                None,
                1_000,
                4,
                &directory,
                &[("CONTROLLER", "127.0.0.1", 19684)],
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KRaft raft voter")
    );

    let after = overlay_ids(&clustered);
    assert_eq!(after, before_cluster);
    assert!(!after.contains(&4));
    server.abort();
}

#[tokio::test]
async fn add_raft_voter_v1_unsupported() {
    let (_dir, broker) = broker_temp("v271", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(80, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
