//! v0.272: Kafka RemoveRaftVoter key 81 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string,
    put_compact_nullable_string, put_empty_tag_buffer, put_uuid, skip_tag_buffer,
};
use volant_broker::kafka::{ApiKey, SUPPORTED_APIS};
use volant_broker::Broker;

fn remove_v0(cluster_id: Option<&str>, voter_id: i32, directory: &[u8; 16]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, cluster_id);
    body.put_i32(voter_id);
    put_uuid(&mut body, directory);
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
async fn api_versions_lists_remove_raft_voter_81() {
    let (_dir, broker) = broker_temp("v272", "api");
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
    assert!(SUPPORTED_APIS.len() >= 70);
    assert_eq!(found.get(&81), Some(&(0, 0)));
    assert_eq!(found.get(&64), Some(&(0, 0)));
    assert_eq!(ApiKey::RemoveRaftVoter as i16, 81);

    server.abort();
}

#[tokio::test]
async fn remove_raft_voter_v0_is_42_membership_unchanged() {
    let (dir, broker) = broker_temp("v272", "rm");
    let before = overlay_ids(&broker);
    assert!(
        !membership_file(&dir).exists(),
        "single-node must not start with membership.json"
    );

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut directory = [0u8; 16];
    directory[15] = 0x72;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            81,
            0,
            10,
            Some("admin"),
            &remove_v0(Some("lkc-test"), 1, &directory),
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
        "RemoveRaftVoter must not create membership.json"
    );
    server.abort();

    // Existing overlay brokers stay put; remove_broker is not called.
    let base = unique_dir("v272", "exist");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19721, 19722]);
    let clustered =
        Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    clustered
        .add_broker(3, "127.0.0.1".into(), 19723, None)
        .unwrap();
    let before_cluster = overlay_ids(&clustered);
    assert!(before_cluster.contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&clustered)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(81, 0, 11, Some("admin"), &remove_v0(None, 3, &directory)),
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
    assert!(
        after.contains(&3),
        "existing overlay broker must not be removed"
    );
    server.abort();
}

#[tokio::test]
async fn remove_raft_voter_v1_unsupported() {
    let (_dir, broker) = broker_temp("v272", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(81, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
