//! Phase 86: write-through txn + true LSO / aborted soft markers / READ_COMMITTED.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_bytes, get_string, put_bytes, put_string,
    put_nullable_string,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

async fn init_txn_async(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    let resp = rpc(addr, encode_request(22, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4); // corr + throttle
    assert_eq!(src.get_i16(), 0);
    let pid = src.get_i64();
    let epoch = src.get_i16();
    (pid, epoch)
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
    body.put_i16(1); // acks
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

async fn fetch_v4(
    addr: &str,
    corr: i32,
    topic: &str,
    isolation: u8,
) -> (i64, i64, i32, Option<Bytes>) {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(100);
    body.put_i32(1);
    body.put_i32(1_000_000);
    body.put_u8(isolation);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_000_000);
    let resp = rpc(addr, encode_request(1, 4, corr, Some("f"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
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
    let records = get_bytes(&mut src).unwrap();
    (hwm, lso, aborted_n, records)
}

#[tokio::test]
async fn open_txn_lso_behind_hwm_read_committed_hides() {
    let dir = temp_dir("p86", "lso");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    // Seed a committed record so LSO has a stable prefix.
    broker
        .produce_one(
            &TopicName::new("events"),
            PartitionId(0),
            volant_core::Message::from_value("seed"),
        )
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-lso").await;
    add_partitions(&addr, 2, "txn-lso", pid, epoch, "events").await;

    let batch = encode_record_batch_idempotent(&sample(b"unstable"), pid, epoch, 0);
    let presp = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("events", &batch)),
    )
    .await;
    let mut ps = presp.freeze();
    assert_eq!(ps.get_i32(), 3);
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(get_string(&mut ps).unwrap(), "events");
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 0);
    assert_eq!(ps.get_i16(), 0);
    let base = ps.get_i64();
    assert!(base >= 1, "write-through base offset, got {base}");

    // READ_COMMITTED: LSO < HWM; only seed visible.
    let (hwm_c, lso_c, aborted_c, rec_c) = fetch_v4(&addr, 10, "events", 1).await;
    assert!(hwm_c > lso_c, "hwm={hwm_c} lso={lso_c}");
    assert_eq!(lso_c, 1, "seed is the only stable offset");
    assert_eq!(aborted_c, 0);
    assert!(rec_c.as_ref().map(|b| !b.is_empty()).unwrap_or(false));

    // READ_UNCOMMITTED: sees unstable data; LSO still reported correctly.
    let (hwm_u, lso_u, aborted_u, rec_u) = fetch_v4(&addr, 11, "events", 0).await;
    assert_eq!(hwm_u, hwm_c);
    assert_eq!(lso_u, lso_c);
    assert_eq!(aborted_u, 0);
    let len_c = rec_c.as_ref().map(|b| b.len()).unwrap_or(0);
    let len_u = rec_u.as_ref().map(|b| b.len()).unwrap_or(0);
    assert!(
        len_u > len_c,
        "READ_UNCOMMITTED record set should be larger than READ_COMMITTED ({len_u} vs {len_c})"
    );

    // Native fetch remains committed-only.
    let native = broker
        .fetch(
            &TopicName::new("events"),
            PartitionId(0),
            Offset::new(0),
            10,
        )
        .unwrap();
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].value.as_ref(), b"seed");

    // Commit → LSO catches HWM; both isolations see data.
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, "txn-lso");
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(1);
    let eresp = rpc(&addr, encode_request(26, 0, 20, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    assert_eq!(es.get_i16(), 0);

    let (hwm2, lso2, _, rec2) = fetch_v4(&addr, 21, "events", 1).await;
    assert_eq!(hwm2, lso2);
    assert!(hwm2 >= 2);
    assert!(rec2.as_ref().map(|b| !b.is_empty()).unwrap_or(false));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn abort_fills_aborted_list_and_filters_read_committed() {
    let dir = temp_dir("p86", "abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("gone", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-ab").await;
    add_partitions(&addr, 2, "txn-ab", pid, epoch, "gone").await;

    let batch = encode_record_batch_idempotent(&sample(b"drop"), pid, epoch, 0);
    let presp = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("gone", &batch)),
    )
    .await;
    let mut ps = presp.freeze();
    ps.advance(4);
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(get_string(&mut ps).unwrap(), "gone");
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 0);
    assert_eq!(ps.get_i16(), 0);

    let mut ebody = BytesMut::new();
    put_string(&mut ebody, "txn-ab");
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(0); // abort
    let eresp = rpc(&addr, encode_request(26, 0, 4, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    assert_eq!(es.get_i16(), 0);

    let (hwm_c, lso_c, aborted_c, rec_c) = fetch_v4(&addr, 10, "gone", 1).await;
    assert!(hwm_c >= 1);
    assert_eq!(lso_c, hwm_c);
    assert!(aborted_c >= 1, "expected aborted_transactions, got {aborted_c}");
    assert!(rec_c.as_ref().map(|b| b.is_empty()).unwrap_or(true));

    let (hwm_u, lso_u, aborted_u, rec_u) = fetch_v4(&addr, 11, "gone", 0).await;
    assert_eq!(hwm_u, hwm_c);
    assert_eq!(lso_u, lso_c);
    assert_eq!(aborted_u, 0, "READ_UNCOMMITTED omits aborted list");
    assert!(
        rec_u.as_ref().map(|b| !b.is_empty()).unwrap_or(false),
        "READ_UNCOMMITTED still sees aborted-on-log data"
    );

    // Native committed-only remains empty after abort.
    let native = broker
        .fetch(&TopicName::new("gone"), PartitionId(0), Offset::ZERO, 10)
        .unwrap();
    assert!(native.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unit_write_through_and_lso() {
    let dir = temp_dir("p86", "unit");
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    broker.create_topic("t", 1).unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("u");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(
        pid,
        epoch,
        "t",
        0,
        0,
        vec![volant_core::Message::from_value("x")],
    ) {
        volant_broker::IdempotentCheck::Accept { base_offset } => {
            assert_eq!(base_offset, 0);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(broker.last_stable_offset("t", 0), 0);
    assert!(broker.high_watermark(&TopicName::new("t"), PartitionId(0)).unwrap() >= 1);

    // Uncommitted not visible on native fetch.
    assert!(broker
        .fetch(&TopicName::new("t"), PartitionId(0), Offset::ZERO, 10)
        .unwrap()
        .is_empty());

    let (code, results) = broker.end_txn(pid, epoch, true, &[]).unwrap();
    assert_eq!(code, 0);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].base_offset, 0);
    assert_eq!(broker.last_stable_offset("t", 0), broker.high_watermark(&TopicName::new("t"), PartitionId(0)).unwrap());
    assert_eq!(
        broker
            .fetch(&TopicName::new("t"), PartitionId(0), Offset::ZERO, 10)
            .unwrap()
            .len(),
        1
    );

    let _ = std::fs::remove_dir_all(&dir);
}
