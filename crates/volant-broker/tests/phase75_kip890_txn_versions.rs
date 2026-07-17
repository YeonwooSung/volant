//! Phase 75: KIP-890-era transaction API max versions
//! (InitProducerId 0–5, AddPartitionsToTxn 0–5, EndTxn 0–5; TxnOffsetCommit
//! name path through v5; TopicId max raised to 6 by Phase 76).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string,
    put_compact_array_len, put_compact_nullable_string, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// InitProducerId v5 body: compact nullable txn_id, timeout, resume pid/epoch, tags.
fn init_v5(txn_id: &str, resume_pid: i64, resume_epoch: i16) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body.put_i64(resume_pid);
    body.put_i16(resume_epoch);
    put_empty_tag_buffer(&mut body);
    body
}

/// AddPartitionsToTxn v4 batch body (single transaction).
fn add_partitions_v4_batch(
    txn_id: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    parts: &[i32],
    verify_only: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1); // Transactions
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if verify_only { 1 } else { 0 });
    put_compact_array_len(&mut body, 1); // Topics
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, parts.len());
    for p in parts {
        body.put_i32(*p);
    }
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // transaction tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn end_txn_v5(txn_id: &str, pid: i64, epoch: i16, committed: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if committed { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn txn_offset_commit_v5(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> BytesMut {
    // Wire identical to v3 (name-based).
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    put_compact_string(&mut body, group);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(-1); // generation
    put_compact_string(&mut body, ""); // member_id
    put_compact_nullable_string(&mut body, None); // group_instance_id
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1); // leader_epoch
    put_compact_nullable_string(&mut body, Some(""));
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

#[tokio::test]
async fn api_versions_kip890_txn_maxes() {
    let dir = temp_dir("p75", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2); // correlation + error
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        found.insert(key, (min_v, max_v));
    }
    assert_eq!(found.get(&22), Some(&(0, 6)), "InitProducerId (Phase 77 OngoingTxn)");
    assert_eq!(found.get(&24), Some(&(0, 5)), "AddPartitionsToTxn");
    assert_eq!(found.get(&25), Some(&(0, 4)), "AddOffsetsToTxn (Phase 82 v4)");
    assert_eq!(found.get(&26), Some(&(0, 5)), "EndTxn");
    assert_eq!(found.get(&28), Some(&(0, 6)), "TxnOffsetCommit (Phase 76 TopicId)");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn init_producer_id_v5_with_resume_fields() {
    let dir = temp_dir("p75", "init5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Resume fields present but ignored — always allocate a fresh pid.
    let resp = rpc(
        &addr,
        encode_request_flexible(
            22,
            5,
            1,
            Some("p"),
            &init_v5("app-kip890", -1, -1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let pid = src.get_i64();
    let epoch = src.get_i16();
    assert!(pid > 0, "allocated producer id");
    assert_eq!(epoch, 0);
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn end_txn_v5_response_includes_pid_epoch() {
    let dir = temp_dir("p75", "end5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Init v5
    let resp = rpc(
        &addr,
        encode_request_flexible(22, 5, 1, Some("p"), &init_v5("end-txn", -1, -1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let pid = src.get_i64();
    let epoch = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();

    // Open txn via AddPartitions v4 batch
    let add = rpc(
        &addr,
        encode_request_flexible(
            24,
            4,
            2,
            Some("p"),
            &add_partitions_v4_batch("end-txn", pid, epoch, "events", &[0], false),
        ),
    )
    .await;
    let mut asrc = add.freeze();
    assert_eq!(asrc.get_i32(), 2);
    skip_tag_buffer(&mut asrc).unwrap();
    assert_eq!(asrc.get_i32(), 0); // throttle
    assert_eq!(asrc.get_i16(), 0); // top-level error
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut asrc).unwrap(), "end-txn");
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1)); // topics
    assert_eq!(get_compact_string(&mut asrc).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1)); // partitions
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);

    // EndTxn v5 commit — response must include pid/epoch
    let end = rpc(
        &addr,
        encode_request_flexible(
            26,
            5,
            3,
            Some("p"),
            &end_txn_v5("end-txn", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 3);
    skip_tag_buffer(&mut es).unwrap();
    assert_eq!(es.get_i32(), 0); // throttle
    assert_eq!(es.get_i16(), 0); // error
    assert_eq!(es.get_i64(), pid, "v5 ProducerId echo");
    assert_eq!(es.get_i16(), epoch, "v5 ProducerEpoch echo");
    skip_tag_buffer(&mut es).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_partitions_v4_batch_single_txn() {
    let dir = temp_dir("p75", "add4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("batch-topic", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(22, 5, 1, Some("p"), &init_v5("batch-txn", -1, -1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    src.advance(4 + 2); // throttle + error
    let pid = src.get_i64();
    let epoch = src.get_i16();

    let add = rpc(
        &addr,
        encode_request_flexible(
            24,
            4,
            2,
            Some("p"),
            &add_partitions_v4_batch("batch-txn", pid, epoch, "batch-topic", &[0, 1], false),
        ),
    )
    .await;
    let mut asrc = add.freeze();
    assert_eq!(asrc.get_i32(), 2);
    skip_tag_buffer(&mut asrc).unwrap();
    assert_eq!(asrc.get_i32(), 0); // throttle
    assert_eq!(asrc.get_i16(), 0); // top-level ErrorCode
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut asrc).unwrap(), "batch-txn");
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut asrc).unwrap(), "batch-topic");
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(2));
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);
    skip_tag_buffer(&mut asrc).unwrap(); // partition tags
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(asrc.get_i16(), 0);
    skip_tag_buffer(&mut asrc).unwrap(); // partition tags
    skip_tag_buffer(&mut asrc).unwrap(); // topic tags
    skip_tag_buffer(&mut asrc).unwrap(); // transaction tags
    skip_tag_buffer(&mut asrc).unwrap(); // response tags

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_v5_name_path() {
    let dir = temp_dir("p75", "toc5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("offsets-topic", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(22, 5, 1, Some("p"), &init_v5("toc-txn", -1, -1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    src.advance(4 + 2);
    let pid = src.get_i64();
    let epoch = src.get_i16();

    // Open txn
    let _ = rpc(
        &addr,
        encode_request_flexible(
            24,
            4,
            2,
            Some("p"),
            &add_partitions_v4_batch("toc-txn", pid, epoch, "offsets-topic", &[0], false),
        ),
    )
    .await;

    // TxnOffsetCommit v5 (name path, same as v3)
    let toc = rpc(
        &addr,
        encode_request_flexible(
            28,
            5,
            3,
            Some("p"),
            &txn_offset_commit_v5("toc-txn", "cg-kip890", pid, epoch, "offsets-topic", 0, 42),
        ),
    )
    .await;
    let mut tocs = toc.freeze();
    assert_eq!(tocs.get_i32(), 3);
    skip_tag_buffer(&mut tocs).unwrap();
    assert_eq!(tocs.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut tocs).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut tocs).unwrap(), "offsets-topic");
    assert_eq!(get_compact_array_len(&mut tocs).unwrap(), Some(1));
    assert_eq!(tocs.get_i32(), 0);
    assert_eq!(tocs.get_i16(), 0); // no error
    skip_tag_buffer(&mut tocs).unwrap();
    skip_tag_buffer(&mut tocs).unwrap();
    skip_tag_buffer(&mut tocs).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_v6_txn_versions() {
    let dir = temp_dir("p75", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // InitProducerId v7 (beyond max 6) → header v1 + UnsupportedVersion (35)
    let resp = rpc(
        &addr,
        encode_request_flexible(22, 7, 10, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    // AddPartitionsToTxn v6
    let resp = rpc(
        &addr,
        encode_request_flexible(24, 6, 11, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    // EndTxn v6
    let resp = rpc(
        &addr,
        encode_request_flexible(26, 6, 12, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    // TxnOffsetCommit v7 (beyond Phase 76 max 6)
    let resp = rpc(
        &addr,
        encode_request_flexible(28, 7, 13, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 13);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
