//! v0.251: Kafka AssignReplicasToDirs key 73 v0.

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_uuid, put_compact_array_len,
    put_empty_tag_buffer, put_uuid, skip_tag_buffer, volant_topic_uuid,
};
use volant_core::{Message, PartitionId, TopicName};

fn assign_v0(
    broker_id: i32,
    broker_epoch: i64,
    dir_id: &[u8; 16],
    topic_id: &[u8; 16],
    partitions: &[i32],
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(broker_id);
    body.put_i64(broker_epoch);
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, dir_id);
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, topic_id);
    put_compact_array_len(&mut body, partitions.len());
    for &p in partitions {
        body.put_i32(p);
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn snapshot_files(root: &Path) -> BTreeMap<PathBuf, u64> {
    let mut out = BTreeMap::new();
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
                out.insert(rel, meta.len());
            }
        }
    }
    out
}

#[tokio::test]
async fn api_versions_lists_assign_replicas_to_dirs_73() {
    let (_dir, broker) = broker_temp("v251", "api");
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
    assert!(found.len() >= 53);
    assert_eq!(found.get(&73), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn assign_random_dir_is_per_partition_error_files_unmoved() {
    let (dir, broker) = broker_temp("v251", "move");
    let tid = broker.create_topic(TopicName::new("events"), 1).unwrap();
    broker
        .produce_one(
            &TopicName::new("events"),
            PartitionId(0),
            Message::from_value("stay-put"),
        )
        .unwrap();
    let topic_uuid = volant_topic_uuid(tid.0);
    let dir_id = [
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        0x01,
    ];
    let before = snapshot_files(&dir);
    assert!(!before.is_empty(), "expected log files after produce");

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            73,
            0,
            10,
            Some("admin"),
            &assign_v0(0, 0, &dir_id, &topic_uuid, &[0]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_uuid(&mut src).unwrap(), dir_id);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_uuid(&mut src).unwrap(), topic_uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST

    assert_eq!(snapshot_files(&dir), before, "replica files must stay put");

    server.abort();
}

#[tokio::test]
async fn assign_replicas_to_dirs_v1_unsupported() {
    let (_dir, broker) = broker_temp("v251", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(73, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
