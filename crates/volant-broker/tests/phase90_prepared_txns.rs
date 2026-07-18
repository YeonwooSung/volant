//! Phase 90: Real 2PC / prepared transactions MVP.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_bytes, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_nullable_string, put_string,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::{Offset, Record, TopicName};
use volant_storage::StorageConfig;

fn init_v6(
    txn_id: &str,
    resume_pid: i64,
    resume_epoch: i16,
    enable_2pc: bool,
    keep_prepared: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body.put_i64(resume_pid);
    body.put_i16(resume_epoch);
    body.put_u8(if enable_2pc { 1 } else { 0 });
    body.put_u8(if keep_prepared { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_v6_rpc(
    addr: &str,
    corr: i32,
    txn_id: &str,
    enable_2pc: bool,
    keep_prepared: bool,
) -> (i16, i64, i16, i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(
            22,
            6,
            corr,
            Some("p"),
            &init_v6(txn_id, -1, -1, enable_2pc, keep_prepared),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    let pid = src.get_i64();
    let epoch = src.get_i16();
    let ongoing_pid = src.get_i64();
    let ongoing_epoch = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    (err, pid, epoch, ongoing_pid, ongoing_epoch)
}

async fn add_partitions(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, topic: &str) {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    let resp = rpc(addr, encode_request(24, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
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

fn sample(val: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(val),
        timestamp_ms: 1,
        headers: vec![],
    }]
}

async fn produce_txn(
    addr: &str,
    corr: i32,
    topic: &str,
    pid: i64,
    epoch: i16,
    seq: i32,
    val: &'static [u8],
) {
    let batch = encode_record_batch_idempotent(&sample(val), pid, epoch, seq);
    let resp = rpc(
        addr,
        encode_request(0, 0, corr, Some("p"), &produce_body(topic, &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 1); // topic count
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1); // partition count
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
}

async fn end_txn(
    addr: &str,
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    commit: bool,
) -> i16 {
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, txn_id);
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(if commit { 1 } else { 0 });
    let eresp = rpc(addr, encode_request(26, 0, corr, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4); // corr + throttle
    es.get_i16()
}

async fn fetch_v4_lso_and_records(
    addr: &str,
    corr: i32,
    topic: &str,
    isolation: u8,
) -> (i64, i64, Bytes) {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(100);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(isolation);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_048_576);
    let resp = rpc(addr, encode_request(1, 4, corr, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4); // corr + throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let aborted_n = src.get_i32();
    for _ in 0..aborted_n {
        let _ = src.get_i64();
        let _ = src.get_i64();
    }
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    (hwm, lso, records)
}

#[tokio::test]
async fn enable_2pc_prepare_then_complete_commit() {
    let dir = temp_dir("p90", "prep-commit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, ongoing_pid, ongoing_epoch) =
        init_v6_rpc(&addr, 1, "txn-2pc", true, false).await;
    assert_eq!(err, 0);
    assert!(pid > 0);
    assert_eq!(ongoing_pid, -1);
    assert_eq!(ongoing_epoch, -1);

    add_partitions(&addr, 2, "txn-2pc", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"hello").await;

    // First EndTxn → prepare (data still unstable / LSO held).
    assert_eq!(end_txn(&addr, 4, "txn-2pc", pid, epoch, true).await, 0);
    let desc = broker.describe_transaction("txn-2pc").unwrap();
    assert_eq!(desc.0, "PrepareCommit");
    let (hwm, lso, _) = fetch_v4_lso_and_records(&addr, 5, "events", 1).await;
    assert!(hwm > lso, "prepared commit still holds LSO");

    // Second EndTxn → finalize commit.
    assert_eq!(end_txn(&addr, 6, "txn-2pc", pid, epoch, true).await, 0);
    let desc = broker.describe_transaction("txn-2pc").unwrap();
    assert_eq!(desc.0, "Empty");
    let (hwm2, lso2, records) = fetch_v4_lso_and_records(&addr, 7, "events", 1).await;
    assert_eq!(hwm2, lso2, "LSO catches HWM after complete commit");
    assert!(!records.is_empty(), "committed data visible under READ_COMMITTED");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn enable_2pc_prepare_abort_then_complete() {
    let dir = temp_dir("p90", "prep-abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) = init_v6_rpc(&addr, 1, "txn-abort", true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-abort", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"gone").await;

    assert_eq!(end_txn(&addr, 4, "txn-abort", pid, epoch, false).await, 0);
    assert_eq!(
        broker.describe_transaction("txn-abort").unwrap().0,
        "PrepareAbort"
    );

    // Mismatched decision rejected.
    assert_ne!(end_txn(&addr, 5, "txn-abort", pid, epoch, true).await, 0);

    // Matching abort finalizes.
    assert_eq!(end_txn(&addr, 6, "txn-abort", pid, epoch, false).await, 0);
    assert_eq!(
        broker.describe_transaction("txn-abort").unwrap().0,
        "Empty"
    );

    // READ_COMMITTED should not surface aborted app data (control markers ok).
    let (_hwm, _lso, records) = fetch_v4_lso_and_records(&addr, 7, "events", 1).await;
    // Aborted data filtered; may still have control batch. Ensure no "gone" value
    // by checking native committed-only path hides it.
    use volant_core::PartitionId;
    let native = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::ZERO, 64)
        .unwrap();
    assert!(
        native.iter().all(|r| r.value.as_ref() != b"gone"),
        "aborted payload hidden from native committed-only fetch: {native:?}"
    );
    let _ = records;

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn keep_prepared_returns_ongoing_txn() {
    let dir = temp_dir("p90", "keep");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) = init_v6_rpc(&addr, 1, "txn-keep", true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-keep", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"x").await;
    assert_eq!(end_txn(&addr, 4, "txn-keep", pid, epoch, true).await, 0);

    // Re-init with KeepPreparedTxn=true → OngoingTxn* echoes prepared identity.
    let (err2, pid2, epoch2, ong_pid, ong_epoch) =
        init_v6_rpc(&addr, 5, "txn-keep", true, true).await;
    assert_eq!(err2, 0);
    assert_eq!(pid2, pid, "same producer id when keeping prepared");
    assert_eq!(epoch2, epoch, "no fence when keeping prepared");
    assert_eq!(ong_pid, pid);
    assert_eq!(ong_epoch, epoch as i16);
    assert_eq!(
        broker.describe_transaction("txn-keep").unwrap().0,
        "PrepareCommit"
    );

    // Still finalizable.
    assert_eq!(end_txn(&addr, 6, "txn-keep", pid2, epoch2, true).await, 0);
    assert_eq!(broker.describe_transaction("txn-keep").unwrap().0, "Empty");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn drop_prepared_on_reinit_without_keep() {
    let dir = temp_dir("p90", "drop");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) = init_v6_rpc(&addr, 1, "txn-drop", true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-drop", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"y").await;
    assert_eq!(end_txn(&addr, 4, "txn-drop", pid, epoch, true).await, 0);

    // KeepPreparedTxn=false → force-abort + fence.
    let (err2, pid2, epoch2, ong_pid, ong_epoch) =
        init_v6_rpc(&addr, 5, "txn-drop", true, false).await;
    assert_eq!(err2, 0);
    assert_eq!(pid2, pid);
    assert_ne!(epoch2, epoch, "epoch fenced after drop prepared");
    assert_eq!(ong_pid, -1);
    assert_eq!(ong_epoch, -1);
    assert_eq!(broker.describe_transaction("txn-drop").unwrap().0, "Empty");

    // Old epoch cannot finalize.
    assert_ne!(end_txn(&addr, 6, "txn-drop", pid, epoch, true).await, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn prepared_survives_restart() {
    let dir = temp_dir("p90", "restart");
    {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        }));
        broker.create_topic("events", 1).unwrap();
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let (err, pid, epoch, _, _) = init_v6_rpc(&addr, 1, "txn-dur", true, false).await;
        assert_eq!(err, 0);
        add_partitions(&addr, 2, "txn-dur", pid, epoch, "events").await;
        produce_txn(&addr, 3, "events", pid, epoch, 0, b"persist").await;
        assert_eq!(end_txn(&addr, 4, "txn-dur", pid, epoch, true).await, 0);
        assert_eq!(
            broker.describe_transaction("txn-dur").unwrap().0,
            "PrepareCommit"
        );
        server.abort();
    }

    // Restart broker from same data_dir.
    let broker2 = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let desc = broker2.describe_transaction("txn-dur").unwrap();
    assert_eq!(desc.0, "PrepareCommit");
    let pid = desc.3;
    let epoch = desc.4;
    // LSO still held.
    let lso = broker2.last_stable_offset("events", 0);
    let hwm = broker2
        .high_watermark(&TopicName::new("events"), volant_core::PartitionId(0))
        .unwrap_or(0);
    assert!(hwm > lso, "prepared holds LSO across restart");

    let (addr, server) = boot_kafka(Arc::clone(&broker2)).await;
    // KeepPrepared returns OngoingTxn*.
    let (err, pid2, epoch2, ong_pid, ong_epoch) =
        init_v6_rpc(&addr, 10, "txn-dur", true, true).await;
    assert_eq!(err, 0);
    assert_eq!(pid2, pid as i64);
    assert_eq!(epoch2, epoch as i16);
    assert_eq!(ong_pid, pid as i64);
    assert_eq!(ong_epoch, epoch as i16);

    assert_eq!(
        end_txn(&addr, 11, "txn-dur", pid as i64, epoch as i16, true).await,
        0
    );
    assert_eq!(broker2.describe_transaction("txn-dur").unwrap().0, "Empty");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn non_2pc_end_txn_still_one_shot() {
    let dir = temp_dir("p90", "one-shot");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Classic InitProducerId (no 2PC).
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some("txn-classic"));
    body.put_i32(60_000);
    let resp = rpc(&addr, encode_request(22, 0, 1, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i16(), 0);
    let pid = src.get_i64();
    let epoch = src.get_i16();

    add_partitions(&addr, 2, "txn-classic", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"one").await;

    // Single EndTxn fully commits (no prepare).
    assert_eq!(end_txn(&addr, 4, "txn-classic", pid, epoch, true).await, 0);
    assert_eq!(
        broker.describe_transaction("txn-classic").unwrap().0,
        "Empty"
    );
    let (hwm, lso, records) = fetch_v4_lso_and_records(&addr, 5, "events", 1).await;
    assert_eq!(hwm, lso);
    assert!(!records.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_while_prepared_rejected() {
    let dir = temp_dir("p90", "reject");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (err, pid, epoch, _, _) = init_v6_rpc(&addr, 1, "txn-rej", true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-rej", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"a").await;
    assert_eq!(end_txn(&addr, 4, "txn-rej", pid, epoch, true).await, 0);

    // AddPartitions while prepared → partition error (InvalidTxnState mapped).
    let mut body = BytesMut::new();
    put_string(&mut body, "txn-rej");
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, "events");
    body.put_i32(1);
    body.put_i32(0);
    let resp = rpc(&addr, encode_request(24, 0, 5, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    let _ = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let part_err = src.get_i16();
    assert_ne!(part_err, 0, "AddPartitions while prepared must fail");

    // Complete to clean up.
    assert_eq!(end_txn(&addr, 6, "txn-rej", pid, epoch, true).await, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_transactions_shows_prepare_states() {
    let dir = temp_dir("p90", "list");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (_, pid, epoch, _, _) = init_v6_rpc(&addr, 1, "txn-list", true, false).await;
    add_partitions(&addr, 2, "txn-list", pid, epoch, "events").await;
    produce_txn(&addr, 3, "events", pid, epoch, 0, b"z").await;
    assert_eq!(end_txn(&addr, 4, "txn-list", pid, epoch, true).await, 0);

    let listed = broker.list_open_transactions();
    assert!(
        listed.iter().any(|(id, _, st)| id == "txn-list" && st == "PrepareCommit"),
        "list must include PrepareCommit: {listed:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
