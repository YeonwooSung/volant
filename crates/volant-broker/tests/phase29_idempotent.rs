//! Phase 29: Kafka InitProducerId + idempotent RecordBatch produce.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_record_batch_idempotent, encode_request, get_string, put_bytes,
    put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn init_producer_body(txn_id: Option<&str>) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, txn_id);
    body.put_i32(60_000); // transaction_timeout_ms (ignored)
    body
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

fn parse_produce_base(resp: BytesMut, corr: i32, topic: &str) -> (i16, i64) {
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    let base = src.get_i64();
    (err, base)
}

#[tokio::test]
async fn api_versions_includes_init_producer_id() {
    let dir = temp_dir("p29", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let req = encode_request(18, 0, 1, Some("t"), &[]);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut found = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 22 {
            found = Some((min, max));
        }
    }
    assert_eq!(found, Some((0, 5))); // Phase 75 KIP-890 max

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn init_producer_id_allocates_pid_epoch() {
    let dir = temp_dir("p29", "init");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let req = encode_request(22, 0, 7, Some("p"), &init_producer_body(None));
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let pid = src.get_i64();
    let epoch = src.get_i16();
    assert!(pid > 0, "pid={pid}");
    assert_eq!(epoch, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn idempotent_produce_dedupes_duplicate_sequence() {
    let dir = temp_dir("p29", "dedupe");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("idem", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // InitProducerId
    let req = encode_request(22, 0, 1, Some("p"), &init_producer_body(None));
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    src.advance(4 + 4 + 2); // corr, throttle, error
    let pid = src.get_i64();
    let epoch = src.get_i16();

    let records = vec![Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(b"idempotent-v1"),
        timestamp_ms: 1_700_000_000_500,
        headers: vec![],
    }];
    let batch = encode_record_batch_idempotent(&records, pid, epoch, 0);

    let r1 = rpc(
        &addr,
        encode_request(0, 0, 10, Some("p"), &produce_body("idem", &batch)),
    )
    .await;
    let (err1, base1) = parse_produce_base(r1, 10, "idem");
    assert_eq!(err1, 0);
    assert_eq!(base1, 0);

    // Exact duplicate sequence — same base offset, no second append.
    let r2 = rpc(
        &addr,
        encode_request(0, 0, 11, Some("p"), &produce_body("idem", &batch)),
    )
    .await;
    let (err2, base2) = parse_produce_base(r2, 11, "idem");
    assert_eq!(err2, 0);
    assert_eq!(base2, 0);

    let native = broker
        .fetch(&TopicName::new("idem"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(native.len(), 1, "duplicate must not re-append");
    assert_eq!(native[0].value.as_ref(), b"idempotent-v1");

    // Next sequence advances the log.
    let records2 = vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"second"),
        timestamp_ms: 1_700_000_000_600,
        headers: vec![],
    }];
    let batch2 = encode_record_batch_idempotent(&records2, pid, epoch, 1);
    let r3 = rpc(
        &addr,
        encode_request(0, 0, 12, Some("p"), &produce_body("idem", &batch2)),
    )
    .await;
    let (err3, base3) = parse_produce_base(r3, 12, "idem");
    assert_eq!(err3, 0);
    assert_eq!(base3, 1);

    let native2 = broker
        .fetch(&TopicName::new("idem"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(native2.len(), 2);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn idempotent_unknown_pid_and_out_of_order() {
    let dir = temp_dir("p29", "err");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("errt", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let records = vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"x"),
        timestamp_ms: 1,
        headers: vec![],
    }];

    // Unknown producer id → Kafka 59
    let batch = encode_record_batch_idempotent(&records, 999_999, 0, 0);
    let r = rpc(
        &addr,
        encode_request(0, 0, 1, Some("p"), &produce_body("errt", &batch)),
    )
    .await;
    let (err, _) = parse_produce_base(r, 1, "errt");
    assert_eq!(err, 59, "UNKNOWN_PRODUCER_ID");

    // Init then out-of-order sequence → Kafka 45
    let init = rpc(
        &addr,
        encode_request(22, 0, 2, Some("p"), &init_producer_body(None)),
    )
    .await;
    let mut isrc = init.freeze();
    isrc.advance(4 + 4 + 2);
    let pid = isrc.get_i64();
    let epoch = isrc.get_i16();

    let ok_batch = encode_record_batch_idempotent(&records, pid, epoch, 0);
    let (e0, _) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 3, Some("p"), &produce_body("errt", &ok_batch)),
        )
        .await,
        3,
        "errt",
    );
    assert_eq!(e0, 0);

    let bad = encode_record_batch_idempotent(&records, pid, epoch, 5);
    let (e1, _) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 4, Some("p"), &produce_body("errt", &bad)),
        )
        .await,
        4,
        "errt",
    );
    assert_eq!(e1, 45, "OUT_OF_ORDER_SEQUENCE_NUMBER");

    // Wrong epoch → Kafka 47
    let wrong_epoch = encode_record_batch_idempotent(&records, pid, epoch + 1, 1);
    let (e2, _) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 5, Some("p"), &produce_body("errt", &wrong_epoch)),
        )
        .await,
        5,
        "errt",
    );
    assert_eq!(e2, 47, "INVALID_PRODUCER_EPOCH");

    // Non-idempotent still works.
    let plain = encode_record_batch(&records);
    let (e3, base) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 6, Some("p"), &produce_body("errt", &plain)),
        )
        .await,
        6,
        "errt",
    );
    assert_eq!(e3, 0);
    assert!(base >= 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
