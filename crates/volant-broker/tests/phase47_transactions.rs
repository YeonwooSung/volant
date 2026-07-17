//! Phase 47: Kafka transaction APIs classic v0–2
//! (AddPartitionsToTxn / AddOffsetsToTxn / EndTxn / TxnOffsetCommit).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_string, put_bytes, put_nullable_string,
    put_string,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn init_txn_body(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    volant_broker::kafka::codec::put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body
}

fn add_partitions_body(txn_id: &str, pid: i64, epoch: i16, topic: &str, parts: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(parts.len() as i32);
    for p in parts {
        body.put_i32(*p);
    }
    body
}

fn end_txn_body(txn_id: &str, pid: i64, epoch: i16, committed: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if committed { 1 } else { 0 });
    body
}

fn add_offsets_body(txn_id: &str, pid: i64, epoch: i16, group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_string(&mut body, group);
    body
}

/// TxnOffsetCommit body; `leader_epoch: Some` writes the v2 INT32 field.
fn txn_offset_commit_body(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
    leader_epoch: Option<i32>,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    put_string(&mut body, group);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    if let Some(le) = leader_epoch {
        body.put_i32(le);
    }
    put_nullable_string(&mut body, Some(""));
    body
}

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

async fn init_txn(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let resp = rpc(
        addr,
        encode_request(22, 0, corr, Some("p"), &init_txn_body(txn_id)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let pid = src.get_i64();
    let epoch = src.get_i16();
    (pid, epoch)
}

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

#[tokio::test]
async fn api_versions_txn_classic_max_v2() {
    let dir = temp_dir("p47", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        found.insert(key, (min, max));
    }
    assert_eq!(found.get(&22), Some(&(0, 5))); // InitProducerId (Phase 75 KIP-890)
    assert_eq!(found.get(&24), Some(&(0, 5))); // AddPartitionsToTxn
    assert_eq!(found.get(&25), Some(&(0, 3))); // AddOffsetsToTxn unchanged
    assert_eq!(found.get(&26), Some(&(0, 5))); // EndTxn
    assert_eq!(found.get(&28), Some(&(0, 6))); // TxnOffsetCommit (Phase 76 TopicId)

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_partitions_end_txn_v2_commit_visible() {
    let dir = temp_dir("p47", "commit-v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn(&addr, 1, "app-v2").await;

    // AddPartitionsToTxn v2 (body identical to v0)
    let add = rpc(
        &addr,
        encode_request(
            24,
            2,
            2,
            Some("p"),
            &add_partitions_body("app-v2", pid, epoch, "events", &[0]),
        ),
    )
    .await;
    let mut asrc = add.freeze();
    assert_eq!(asrc.get_i32(), 2);
    assert_eq!(asrc.get_i32(), 0); // throttle
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(get_string(&mut asrc).unwrap(), "events");
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);

    let batch = encode_record_batch_idempotent(&sample_records(b"v2-msg"), pid, epoch, 0);
    let prod = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("events", &batch)),
    )
    .await;
    let mut ps = prod.freeze();
    assert_eq!(ps.get_i32(), 3);
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(get_string(&mut ps).unwrap(), "events");
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 0);
    assert_eq!(ps.get_i16(), 0);

    let pre = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert!(pre.is_empty());

    // EndTxn v2 commit
    let end = rpc(
        &addr,
        encode_request(
            26,
            2,
            4,
            Some("p"),
            &end_txn_body("app-v2", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 4);
    assert_eq!(es.get_i32(), 0); // throttle
    assert_eq!(es.get_i16(), 0);

    let post = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(post.len(), 1);
    assert_eq!(post[0].value.as_ref(), b"v2-msg");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_v2_leader_epoch_applies_on_commit() {
    let dir = temp_dir("p47", "toc-v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn(&addr, 1, "off-v2").await;
    let _ = rpc(
        &addr,
        encode_request(
            24,
            1,
            2,
            Some("p"),
            &add_partitions_body("off-v2", pid, epoch, "t", &[0]),
        ),
    )
    .await;

    // AddOffsetsToTxn v2
    let add_off = rpc(
        &addr,
        encode_request(
            25,
            2,
            3,
            Some("p"),
            &add_offsets_body("off-v2", pid, epoch, "cg-v2"),
        ),
    )
    .await;
    let mut ao = add_off.freeze();
    assert_eq!(ao.get_i32(), 3);
    assert_eq!(ao.get_i32(), 0);
    assert_eq!(ao.get_i16(), 0);

    // TxnOffsetCommit v2 with committed_leader_epoch
    let toc = rpc(
        &addr,
        encode_request(
            28,
            2,
            4,
            Some("p"),
            &txn_offset_commit_body("off-v2", "cg-v2", pid, epoch, "t", 0, 99, Some(7)),
        ),
    )
    .await;
    let mut ts = toc.freeze();
    assert_eq!(ts.get_i32(), 4);
    assert_eq!(ts.get_i32(), 0); // throttle
    assert_eq!(ts.get_i32(), 1);
    assert_eq!(get_string(&mut ts).unwrap(), "t");
    assert_eq!(ts.get_i32(), 1);
    assert_eq!(ts.get_i32(), 0);
    assert_eq!(ts.get_i16(), 0);

    let before = broker
        .groups()
        .fetch_offsets("cg-v2", &[("t".into(), 0)])
        .unwrap();
    assert!(before.entries.iter().all(|e| e.offset == u64::MAX));

    let end = rpc(
        &addr,
        encode_request(
            26,
            1,
            5,
            Some("p"),
            &end_txn_body("off-v2", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 5);
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);

    let after = broker
        .groups()
        .fetch_offsets("cg-v2", &[("t".into(), 0)])
        .unwrap();
    assert_eq!(after.entries.len(), 1);
    assert_eq!(after.entries[0].offset, 99);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_api_v6_unsupported_version() {
    let dir = temp_dir("p47", "v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Phase 75 max is v5; v6 remains unsupported (2PC / higher KIP-890).
    // Flexible request header → response header v1 + UnsupportedVersion.
    let resp = rpc(
        &addr,
        volant_broker::kafka::codec::encode_request_flexible(24, 6, 9, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    volant_broker::kafka::codec::skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UNSUPPORTED_VERSION

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
