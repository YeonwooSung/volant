//! v0.250: Kafka WriteTxnMarkers key 27 v0–1.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string, get_string,
    is_txn_control_record, parse_txn_control_record, put_compact_array_len, put_compact_string,
    put_empty_tag_buffer, put_string, skip_tag_buffer, ControlMarkerType,
};
use volant_core::{Offset, PartitionId, TopicName};

fn write_v0(pid: i64, epoch: i16, commit: bool, topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1); // one marker
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if commit { 1 } else { 0 });
    body.put_i32(1); // one topic
    put_string(&mut body, topic);
    body.put_i32(partitions.len() as i32);
    for &p in partitions {
        body.put_i32(p);
    }
    body.put_i32(0); // coordinatorEpoch (ignored)
    body
}

fn write_v1(pid: i64, epoch: i16, commit: bool, topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if commit { 1 } else { 0 });
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, partitions.len());
    for &p in partitions {
        body.put_i32(p);
    }
    put_empty_tag_buffer(&mut body); // topic tags
    body.put_i32(0); // coordinatorEpoch (ignored)
    put_empty_tag_buffer(&mut body); // marker tags
    put_empty_tag_buffer(&mut body); // top-level tags
    body
}

#[tokio::test]
async fn api_versions_lists_write_txn_markers_27() {
    let (_dir, broker) = broker_temp("v250", "api");
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
    assert_eq!(found.get(&27), Some(&(0, 1)));

    server.abort();
}

#[tokio::test]
async fn write_markers_existing_topic_is_0_and_control_on_log() {
    let (dir, broker) = broker_temp("v250", "ok");
    broker.create_topic(TopicName::new("events"), 1).unwrap();

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(
            27,
            0,
            10,
            Some("admin"),
            &write_v0(42, 1, true, "events", &[0]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // one marker
    assert_eq!(src.get_i64(), 42);
    assert_eq!(src.get_i32(), 1); // one topic
    assert_eq!(get_string(&mut src).unwrap(), "events");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    let recs = broker
        .fetch_kafka(
            &TopicName::new("events"),
            PartitionId(0),
            Offset::ZERO,
            32,
            false,
        )
        .unwrap();
    let ctrl = recs
        .iter()
        .find_map(parse_txn_control_record)
        .expect("control batch on log");
    assert!(recs.iter().any(is_txn_control_record));
    assert_eq!(ctrl.marker_type, ControlMarkerType::Commit);
    assert_eq!(ctrl.producer_id, 42);
    assert_eq!(ctrl.producer_epoch, 1);

    let resp = rpc(
        &addr,
        encode_request_flexible(
            27,
            1,
            11,
            Some("admin"),
            &write_v1(42, 1, false, "events", &[0]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i64(), 42);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    let recs = broker
        .fetch_kafka(
            &TopicName::new("events"),
            PartitionId(0),
            Offset::ZERO,
            32,
            false,
        )
        .unwrap();
    assert!(recs.iter().any(|r| {
        parse_txn_control_record(r)
            .map(|m| m.marker_type == ControlMarkerType::Abort)
            .unwrap_or(false)
    }));
    assert!(
        dir.join("__txn_markers").join("state.json").exists(),
        "__txn_markers should be persisted"
    );

    server.abort();
}

#[tokio::test]
async fn write_txn_markers_unknown_topic_is_3() {
    let (_dir, broker) = broker_temp("v250", "unk");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(
            27,
            0,
            20,
            Some("admin"),
            &write_v0(7, 0, true, "missing", &[0]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i64(), 7);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "missing");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION

    server.abort();
}

#[tokio::test]
async fn write_txn_markers_v2_unsupported() {
    let (_dir, broker) = broker_temp("v250", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(27, 2, 99, Some("c"), &write_v1(1, 0, true, "events", &[0])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (v>=1 flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
