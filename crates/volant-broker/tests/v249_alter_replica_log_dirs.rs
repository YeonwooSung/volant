//! v0.249: Kafka AlterReplicaLogDirs key 34 v0–1.

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string, get_string,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_core::{Message, PartitionId, TopicName};

fn alter_v0(path: &str, topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1); // one dir
    put_string(&mut body, path);
    body.put_i32(1); // one topic
    put_string(&mut body, topic);
    body.put_i32(partitions.len() as i32);
    for &p in partitions {
        body.put_i32(p);
    }
    body
}

fn alter_v1(path: &str, topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, path);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, partitions.len());
    for &p in partitions {
        body.put_i32(p);
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
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
async fn api_versions_lists_alter_replica_log_dirs_34() {
    let (_dir, broker) = broker_temp("v249", "api");
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
    assert!(found.len() >= 50);
    assert_eq!(found.get(&34), Some(&(0, 1)));

    server.abort();
}

#[tokio::test]
async fn alter_any_path_is_per_partition_error_files_unmoved() {
    let (dir, broker) = broker_temp("v249", "move");
    broker.create_topic(TopicName::new("events"), 1).unwrap();
    broker
        .produce_one(
            &TopicName::new("events"),
            PartitionId(0),
            Message::from_value("stay-put"),
        )
        .unwrap();
    let dest = dir.parent().unwrap().join(format!(
        "volant-v249-dest-{}-{}",
        std::process::id(),
        dir.file_name().unwrap().to_string_lossy()
    ));
    let before = snapshot_files(&dir);
    assert!(!before.is_empty(), "expected log files after produce");
    assert!(!dest.exists());

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let dest_s = dest.to_string_lossy();

    let resp = rpc(
        &addr,
        encode_request(34, 0, 10, Some("admin"), &alter_v0(&dest_s, "events", &[0])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // one topic
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    assert!(
        err == 42 || err == 57,
        "per-partition 42 INVALID_REQUEST or 57 LOG_DIR_NOT_FOUND, got {err}"
    );

    let resp = rpc(
        &addr,
        encode_request_flexible(34, 1, 11, Some("admin"), &alter_v1(&dest_s, "events", &[0])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    assert!(
        err == 42 || err == 57,
        "v1 per-partition 42 or 57, got {err}"
    );

    assert_eq!(snapshot_files(&dir), before, "replica files must stay put");
    assert!(!dest.exists(), "must not create a destination log dir");

    server.abort();
}

#[tokio::test]
async fn alter_replica_log_dirs_v2_unsupported() {
    let (_dir, broker) = broker_temp("v249", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(34, 2, 99, Some("c"), &alter_v1("/tmp/x", "events", &[0])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (v>=1 flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
