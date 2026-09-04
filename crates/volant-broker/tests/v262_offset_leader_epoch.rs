//! v0.262: persist OffsetCommit committed_leader_epoch.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{encode_request, get_string, put_string};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn broker_with_topic(label: &str) -> (PathBuf, Arc<Broker>) {
    let dir = temp_dir("v262", label);
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    (dir, broker)
}

/// OffsetCommit classic v6: generation + member + committed_leader_epoch.
fn commit_v6(
    group: &str,
    generation: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    epoch: i32,
    meta: &str,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(generation);
    put_string(&mut body, member_id);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(epoch);
    put_string(&mut body, meta);
    body
}

/// OffsetCommit classic v5: no committed_leader_epoch field.
fn commit_v5(
    group: &str,
    generation: i32,
    member_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    meta: &str,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(generation);
    put_string(&mut body, member_id);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    put_string(&mut body, meta);
    body
}

fn fetch_v5(group: &str, topic: &str, parts: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, group);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(parts.len() as i32);
    for p in parts {
        body.put_i32(*p);
    }
    body
}

/// Parse OffsetFetch v5 listed single-topic response → (offset, epoch, error).
fn parse_fetch_v5(mut src: bytes::Bytes, corr: i32, n_parts: usize) -> Vec<(i64, i32, i16)> {
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    let _topic = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), n_parts as i32);
    let mut out = Vec::with_capacity(n_parts);
    for _ in 0..n_parts {
        let _p = src.get_i32();
        let off = src.get_i64();
        let epoch = src.get_i32();
        let _ = get_string(&mut src).unwrap();
        let err = src.get_i16();
        out.push((off, epoch, err));
    }
    assert_eq!(src.get_i16(), 0); // top-level error
    out
}

/// Legacy on-disk file: `u64 offset LE` + `u16 meta_len LE` + UTF-8 (no epoch).
fn write_legacy_offset(
    data_dir: &std::path::Path,
    group: &str,
    topic: &str,
    partition: u32,
    offset: u64,
    meta: &str,
) {
    let path = data_dir
        .join("__consumer_offsets")
        .join(group)
        .join(topic)
        .join(partition.to_string());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let meta_bytes = meta.as_bytes();
    let mut buf = Vec::with_capacity(10 + meta_bytes.len());
    buf.extend_from_slice(&offset.to_le_bytes());
    buf.extend_from_slice(&(meta_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(meta_bytes);
    std::fs::write(path, buf).unwrap();
}

#[tokio::test]
async fn offset_commit_v6_epoch_round_trips_on_fetch() {
    let (dir, broker) = broker_with_topic("v6");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(
            8,
            6,
            1,
            Some("c"),
            &commit_v6("g-epoch", 0, "", "events", 0, 42, 3, "m"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    let resp = rpc(
        &addr,
        encode_request(9, 5, 2, Some("c"), &fetch_v5("g-epoch", "events", &[0])),
    )
    .await;
    let parts = parse_fetch_v5(resp.freeze(), 2, 1);
    assert_eq!(parts[0], (42, 3, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn legacy_file_without_epoch_trailer_fetches_minus_one() {
    let (dir, broker) = broker_with_topic("legacy");
    write_legacy_offset(&dir, "g-legacy", "events", 0, 11, "old");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(9, 5, 1, Some("c"), &fetch_v5("g-legacy", "events", &[0])),
    )
    .await;
    let parts = parse_fetch_v5(resp.freeze(), 1, 1);
    assert_eq!(parts[0], (11, -1, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v5_stores_unknown_epoch() {
    let (dir, broker) = broker_with_topic("v5");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(
            8,
            5,
            1,
            Some("c"),
            &commit_v5("g-v5", 0, "", "events", 0, 7, ""),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    let resp = rpc(
        &addr,
        encode_request(9, 5, 2, Some("c"), &fetch_v5("g-v5", "events", &[0])),
    )
    .await;
    let parts = parse_fetch_v5(resp.freeze(), 2, 1);
    assert_eq!(parts[0], (7, -1, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn native_admin_commit_stores_unknown_epoch() {
    let (dir, broker) = broker_with_topic("native");
    let r = broker
        .groups()
        .commit_offsets(
            "g-native",
            "",
            0,
            &[("events".into(), 0, 99, "admin".into())],
        )
        .unwrap();
    assert_eq!(r.error_code, 0);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request(9, 5, 1, Some("c"), &fetch_v5("g-native", "events", &[0])),
    )
    .await;
    let parts = parse_fetch_v5(resp.freeze(), 1, 1);
    assert_eq!(parts[0], (99, -1, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
