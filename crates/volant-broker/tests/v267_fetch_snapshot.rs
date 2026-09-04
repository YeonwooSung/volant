//! v0.267: Kafka FetchSnapshot key 59 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;

fn fetch_snapshot_v0(
    replica_id: i32,
    max_bytes: i32,
    topics: &[(&str, &[(i32, i32, i64, i32, i64)])],
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(replica_id);
    body.put_i32(max_bytes);
    put_compact_array_len(&mut body, topics.len());
    for (name, partitions) in topics {
        put_compact_string(&mut body, name);
        put_compact_array_len(&mut body, partitions.len());
        for (partition, leader_epoch, end_offset, epoch, position) in *partitions {
            body.put_i32(*partition);
            body.put_i32(*leader_epoch);
            body.put_i64(*end_offset);
            body.put_i32(*epoch);
            body.put_i64(*position);
            put_empty_tag_buffer(&mut body);
        }
        put_empty_tag_buffer(&mut body);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn snapshot_dir(root: &std::path::Path) -> Vec<(std::path::PathBuf, u64)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(meta) = e.metadata() {
                let rel = p.strip_prefix(root).unwrap_or(&p).to_path_buf();
                out.push((rel, meta.len()));
            }
        }
    }
    out.sort();
    out
}

fn raft_state(b: &Broker) -> (bool, Option<u32>, u64) {
    (
        b.openraft_started(),
        b.openraft_leader_id(),
        b.openraft_term(),
    )
}

#[tokio::test]
async fn api_versions_lists_fetch_snapshot_59() {
    let (_dir, broker) = broker_temp("v267", "api");
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
    assert!(found.len() >= 65);
    assert_eq!(found.get(&59), Some(&(0, 0)));
    assert_eq!(found.get(&60), Some(&(0, 2)));

    server.abort();
}

#[tokio::test]
async fn fetch_snapshot_v0_is_42_empty_topics_no_snapshot() {
    let (dir, broker) = broker_temp("v267", "snap");
    let before_files = snapshot_dir(&dir);
    let before_raft = raft_state(&broker);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            59,
            0,
            10,
            Some("admin"),
            &fetch_snapshot_v0(-1, 1024, &[("__cluster_metadata", &[(0, 0, 0, 0, 0)])]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(
        snapshot_dir(&dir),
        before_files,
        "FetchSnapshot must not write snapshot files"
    );
    assert_eq!(raft_state(&broker), before_raft, "openraft state unchanged");
    server.abort();
}

#[tokio::test]
async fn fetch_snapshot_v1_unsupported() {
    let (_dir, broker) = broker_temp("v267", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(59, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
