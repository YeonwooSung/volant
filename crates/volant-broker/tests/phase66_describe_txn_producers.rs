//! Phase 66: DescribeTransactions + DescribeProducers + DescribeCluster v1 + ListTransactions v1.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_core::Message;
use volant_storage::StorageConfig;

fn describe_txns(ids: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, ids.len());
    for id in ids {
        put_compact_string(&mut body, id);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_producers(topic: &str, partitions: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, partitions.len());
    for p in partitions {
        body.put_i32(*p);
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_cluster_v1(include_ops: bool, endpoint_type: i8) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(if include_ops { 1 } else { 0 });
    body.put_i8(endpoint_type);
    put_empty_tag_buffer(&mut body);
    body
}

fn list_txns_v1(state_filters: &[&str], pid_filters: &[i64], duration: i64) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, state_filters.len());
    for s in state_filters {
        put_compact_string(&mut body, s);
    }
    put_compact_array_len(&mut body, pid_filters.len());
    for p in pid_filters {
        body.put_i64(*p);
    }
    body.put_i64(duration);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_p66_maxes() {
    let dir = temp_dir("p66", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
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
    assert_eq!(found.get(&60), Some(&(0, 2))); // DescribeCluster (Phase 70)
    assert_eq!(found.get(&61), Some(&(0, 0))); // DescribeProducers
    assert_eq!(found.get(&65), Some(&(0, 0))); // DescribeTransactions
    assert_eq!(found.get(&66), Some(&(0, 2))); // ListTransactions (Phase 70)
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_transactions_empty_ongoing_unknown() {
    let dir = temp_dir("p66", "dtxn");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-a");
    // No open txn yet → Empty
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            65,
            0,
            10,
            Some("a"),
            &describe_txns(&["txn-a", "missing-txn"]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(2));

    // txn-a Empty
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "txn-a");
    assert_eq!(get_compact_string(&mut src).unwrap(), "Empty");
    assert_eq!(src.get_i32(), 0); // timeout
    assert_eq!(src.get_i64(), 0); // start
    assert_eq!(src.get_i64(), pid as i64);
    assert_eq!(src.get_i16(), epoch as i16);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();

    // missing
    assert_eq!(src.get_i16(), 105); // TRANSACTIONAL_ID_NOT_FOUND
    assert_eq!(get_compact_string(&mut src).unwrap(), "missing-txn");
    let _ = get_compact_string(&mut src).unwrap();
    src.advance(4 + 8 + 8 + 2); // timeout, start, pid, epoch
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // Begin + buffer produce → Ongoing with topic
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(
        pid,
        epoch,
        "orders",
        0,
        0,
        vec![Message::from_value("x")],
    ) {
        volant_broker::IdempotentCheck::Accept => {}
        other => panic!("unexpected {other:?}"),
    }

    let resp = rpc(
        &addr,
        encode_request_flexible(65, 0, 11, Some("a"), &describe_txns(&["txn-a"])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_string(&mut src).unwrap(), "txn-a");
    assert_eq!(get_compact_string(&mut src).unwrap(), "Ongoing");
    src.advance(4 + 8); // timeout + start
    assert_eq!(src.get_i64(), pid as i64);
    assert_eq!(src.get_i16(), epoch as i16);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_producers_lists_active() {
    let dir = temp_dir("p66", "dprod");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-p");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(
        pid,
        epoch,
        "events",
        0,
        0,
        vec![Message::from_value("y")],
    ) {
        volant_broker::IdempotentCheck::Accept => {}
        other => panic!("unexpected {other:?}"),
    }
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            61,
            0,
            20,
            Some("a"),
            &describe_producers("events", &[0, 9]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(2));

    // partition 0 — active producer
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i64(), pid as i64);
    assert_eq!(src.get_i32(), i32::from(epoch));
    let _last_seq = src.get_i32();
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), -1);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // partition 9 — unknown
    assert_eq!(src.get_i32(), 9);
    assert_eq!(src.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_cluster_v1_and_list_txns_v1() {
    let dir = temp_dir("p66", "v1");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-b");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // DescribeCluster v1 brokers
    let resp = rpc(
        &addr,
        encode_request_flexible(60, 1, 30, Some("a"), &describe_cluster_v1(false, 1)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i8(), 1); // EndpointType
    assert_eq!(get_compact_string(&mut src).unwrap(), "volant");

    // controllers rejected
    let resp = rpc(
        &addr,
        encode_request_flexible(60, 1, 31, Some("a"), &describe_cluster_v1(false, 2)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 31);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 115); // UNSUPPORTED_ENDPOINT_TYPE

    // ListTransactions v1 with duration filter ignored
    let resp = rpc(
        &addr,
        encode_request_flexible(66, 1, 32, Some("a"), &list_txns_v1(&[], &[], 60_000)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 32);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "txn-b");
    assert_eq!(src.get_i64(), pid as i64);
    assert_eq!(get_compact_string(&mut src).unwrap(), "Ongoing");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("p66", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // DescribeCluster/ListTransactions v3 unsupported (v2 closed by Phase 70).
    for (api, ver, corr) in [
        (60i16, 3i16, 40i32),
        (61, 1, 41),
        (65, 1, 42),
        (66, 3, 43),
    ] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("a"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr, "api {api}");
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "UnsupportedVersion api {api}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
