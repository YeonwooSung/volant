//! Phase 70: DescribeCluster v2 (IsFenced) + ListTransactions v2 (TransactionalIdPattern).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn describe_cluster_v2(include_ops: bool, endpoint: i8, include_fenced: bool) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(if include_ops { 1 } else { 0 });
    body.put_i8(endpoint);
    body.put_u8(if include_fenced { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn list_txns_v2(
    states: &[&str],
    pids: &[i64],
    duration: i64,
    pattern: Option<&str>,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, states.len());
    for s in states {
        put_compact_string(&mut body, s);
    }
    put_compact_array_len(&mut body, pids.len());
    for p in pids {
        body.put_i64(*p);
    }
    body.put_i64(duration);
    put_compact_nullable_string(&mut body, pattern);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_dc_list_txn_max_2() {
    let dir = temp_dir("p70", "api");
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
    assert_eq!(found.get(&60), Some(&(0, 2)));
    assert_eq!(found.get(&66), Some(&(0, 2)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_cluster_v2_isfenced_false() {
    let dir = temp_dir("p70", "dc-v2");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(60, 2, 10, Some("a"), &describe_cluster_v2(false, 1, true)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i8(), 1); // EndpointType
    assert_eq!(get_compact_string(&mut src).unwrap(), "volant");
    let _controller = src.get_i32();
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert!(n >= 1);
    for _ in 0..n {
        let _id = src.get_i32();
        let _host = get_compact_string(&mut src).unwrap();
        let _port = src.get_i32();
        let _rack = get_compact_nullable_string(&mut src).unwrap();
        assert_eq!(src.get_u8(), 0); // IsFenced = false
        skip_tag_buffer(&mut src).unwrap();
    }
    let _ops = src.get_i32();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_transactions_v2_pattern_filter() {
    let dir = temp_dir("p70", "list-pat");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (pid_a, epoch_a) = broker.init_producer_id_with_txn("alpha-1");
    let (pid_b, epoch_b) = broker.init_producer_id_with_txn("beta-2");
    assert_eq!(broker.begin_txn(pid_a, epoch_a), 0);
    assert_eq!(broker.begin_txn(pid_b, epoch_b), 0);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Pattern "alpha*" should match only alpha-1.
    let resp = rpc(
        &addr,
        encode_request_flexible(
            66,
            2,
            20,
            Some("a"),
            &list_txns_v2(&[], &[], -1, Some("alpha*")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "alpha-1");
    assert_eq!(src.get_i64(), pid_a as i64);
    assert_eq!(get_compact_string(&mut src).unwrap(), "Ongoing");

    // Null pattern = all open.
    let resp = rpc(
        &addr,
        encode_request_flexible(66, 2, 21, Some("a"), &list_txns_v2(&[], &[], -1, None)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 21);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 2);

    // No match.
    let resp = rpc(
        &addr,
        encode_request_flexible(
            66,
            2,
            22,
            Some("a"),
            &list_txns_v2(&[], &[], -1, Some("gamma*")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 22);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_cluster_v1_still_no_isfenced() {
    let dir = temp_dir("p70", "dc-v1");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_u8(0);
    body.put_i8(1);
    put_empty_tag_buffer(&mut body);
    let resp = rpc(
        &addr,
        encode_request_flexible(60, 1, 5, Some("a"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i8(), 1);
    let _ = get_compact_string(&mut src).unwrap();
    let _ = src.get_i32();
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    for _ in 0..n {
        let _ = src.get_i32();
        let _ = get_compact_string(&mut src).unwrap();
        let _ = src.get_i32();
        let _ = get_compact_nullable_string(&mut src).unwrap();
        // v1: immediately tags (no IsFenced byte)
        skip_tag_buffer(&mut src).unwrap();
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_v3_uses_header_v1() {
    let dir = temp_dir("p70", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (api, ver, corr) in [(60i16, 3i16, 1i32), (66, 3, 2)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("a"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "api {api}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
