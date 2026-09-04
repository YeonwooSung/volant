//! v0.256: OffsetFetch RequireStable honors LSO (error 81).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::{Broker, IdempotentCheck};
use volant_core::{Message, PartitionId, TopicName};
use volant_storage::StorageConfig;

const UNSTABLE_OFFSET_COMMIT: i16 = 81;

fn commit_v8(group: &str, topic: &str, partition: i32, offset: i64, meta: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(0); // generation (0 = no membership check)
    put_compact_string(&mut body, "");
    put_compact_nullable_string(&mut body, None);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1);
    put_compact_nullable_string(&mut body, Some(meta));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn fetch_v7(group: &str, topic: &str, parts: &[i32], require_stable: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, parts.len());
    for p in parts {
        body.put_i32(*p);
    }
    put_empty_tag_buffer(&mut body);
    body.put_u8(if require_stable { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn fetch_v6(group: &str, topic: &str, parts: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, parts.len());
    for p in parts {
        body.put_i32(*p);
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

/// Parse OffsetFetch v6/v7 single-topic listed response → (offset, error) per part.
fn parse_fetch_flex(mut src: bytes::Bytes, corr: i32, n_parts: usize) -> Vec<(i64, i16)> {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    let _topic = get_compact_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), n_parts);
    let mut out = Vec::with_capacity(n_parts);
    for _ in 0..n_parts {
        let _p = src.get_i32();
        let off = src.get_i64();
        assert_eq!(src.get_i32(), -1); // leader_epoch
        let _ = get_compact_nullable_string(&mut src).unwrap();
        let err = src.get_i16();
        skip_tag_buffer(&mut src).unwrap();
        out.push((off, err));
    }
    skip_tag_buffer(&mut src).unwrap(); // topic tags
    assert_eq!(src.get_i16(), 0); // top-level error
    skip_tag_buffer(&mut src).unwrap();
    out
}

fn broker_with_topic(label: &str) -> (std::path::PathBuf, Arc<Broker>) {
    let dir = temp_dir("v256", label);
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    (dir, broker)
}

/// Seed offset 0, open a write-through txn covering the next produce.
/// Returns the unstable produced base offset.
fn open_txn_covering_next(broker: &Broker) -> u64 {
    broker
        .produce_one(
            &TopicName::new("events"),
            PartitionId(0),
            Message::from_value("seed"),
        )
        .unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-rs");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(
        pid,
        epoch,
        "events",
        0,
        0,
        vec![Message::from_value("unstable")],
    ) {
        IdempotentCheck::Accept { base_offset } => {
            assert!(
                broker.is_unstable_offset("events", 0, base_offset),
                "produced offset {base_offset} should sit in the open txn range"
            );
            assert!(
                !broker.is_unstable_offset("events", 0, 0),
                "seed offset 0 is below LSO"
            );
            base_offset
        }
        other => panic!("unexpected txn produce {other:?}"),
    }
}

async fn commit(addr: &str, corr: i32, group: &str, topic: &str, partition: i32, offset: i64) {
    let _ = rpc(
        addr,
        encode_request_flexible(
            8,
            8,
            corr,
            Some("c"),
            &commit_v8(group, topic, partition, offset, ""),
        ),
    )
    .await;
}

#[tokio::test]
async fn require_stable_false_returns_unstable_offset() {
    let (dir, broker) = broker_with_topic("legacy");
    let unstable = open_txn_covering_next(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    commit(&addr, 1, "g", "events", 0, unstable as i64).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 7, 2, Some("c"), &fetch_v7("g", "events", &[0], false)),
    )
    .await;
    let parts = parse_fetch_flex(resp.freeze(), 2, 1);
    assert_eq!(parts[0], (unstable as i64, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn require_stable_true_unstable_returns_81() {
    let (dir, broker) = broker_with_topic("unstable");
    let unstable = open_txn_covering_next(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    commit(&addr, 1, "g", "events", 0, unstable as i64).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 7, 2, Some("c"), &fetch_v7("g", "events", &[0, 1], true)),
    )
    .await;
    let parts = parse_fetch_flex(resp.freeze(), 2, 2);
    assert_eq!(parts[0], (-1, UNSTABLE_OFFSET_COMMIT));
    // Uncommitted partition stays error 0 / offset -1.
    assert_eq!(parts[1], (-1, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn require_stable_true_below_lso_or_no_txn() {
    let (dir, broker) = broker_with_topic("stable");
    let _unstable = open_txn_covering_next(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Offset 0 is below LSO (open txn starts at the write-through produce).
    commit(&addr, 1, "g-below", "events", 0, 0).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            9,
            7,
            2,
            Some("c"),
            &fetch_v7("g-below", "events", &[0], true),
        ),
    )
    .await;
    let parts = parse_fetch_flex(resp.freeze(), 2, 1);
    assert_eq!(parts[0], (0, 0));

    // No open txn on this group: a committed offset with no covering range.
    broker.create_topic("plain", 1).unwrap();
    commit(&addr, 3, "g-plain", "plain", 0, 5).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            9,
            7,
            4,
            Some("c"),
            &fetch_v7("g-plain", "plain", &[0], true),
        ),
    )
    .await;
    let parts = parse_fetch_flex(resp.freeze(), 4, 1);
    assert_eq!(parts[0], (5, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v6_no_require_stable_field() {
    let (dir, broker) = broker_with_topic("v6");
    let unstable = open_txn_covering_next(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    commit(&addr, 1, "g", "events", 0, unstable as i64).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 6, 2, Some("c"), &fetch_v6("g", "events", &[0])),
    )
    .await;
    let parts = parse_fetch_flex(resp.freeze(), 2, 1);
    assert_eq!(parts[0], (unstable as i64, 0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
