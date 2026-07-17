//! Phase 82: AddOffsetsToTxn v4 (wire-identical to v3; KIP-890 TRANSACTION_ABORTABLE never emitted).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// AddOffsetsToTxn flexible body (v3 and v4 share identical wire).
fn add_offsets_flex(txn_id: &str, pid: i64, epoch: i16, group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_compact_string(&mut body, group);
    put_empty_tag_buffer(&mut body);
    body
}

fn init_v2(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_flex(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(22, 2, corr, Some("p"), &init_v2(txn_id)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let pid = src.get_i64();
    let epoch = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    (pid, epoch)
}

fn parse_add_offsets_ok(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap(); // response header v1
    assert_eq!(src.get_i32(), 0); // throttle always 0
    let err = src.get_i16();
    assert_eq!(err, 0);
    assert_ne!(err, 123); // never TRANSACTION_ABORTABLE
    skip_tag_buffer(src).unwrap();
    assert_eq!(src.remaining(), 0);
}

#[tokio::test]
async fn api_versions_add_offsets_to_txn_max_4() {
    let dir = temp_dir("p82", "api");
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
    assert_eq!(found.get(&25), Some(&(0, 4))); // AddOffsetsToTxn
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_offsets_to_txn_v4_success() {
    let dir = temp_dir("p82", "v4");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_flex(&addr, 1, "p82-txn").await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            25,
            4,
            11,
            Some("p"),
            &add_offsets_flex("p82-txn", pid, epoch, "cg-p82"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    parse_add_offsets_ok(&mut src, 11);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_offsets_to_txn_v3_still_works() {
    let dir = temp_dir("p82", "v3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_flex(&addr, 1, "p82-v3").await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            25,
            3,
            21,
            Some("p"),
            &add_offsets_flex("p82-v3", pid, epoch, "cg-v3"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    parse_add_offsets_ok(&mut src, 21);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_offsets_to_txn_v4_invalid_producer() {
    let dir = temp_dir("p82", "bad-pid");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // No InitProducerId — unknown producer should not succeed, and never emit 123.
    let resp = rpc(
        &addr,
        encode_request_flexible(
            25,
            4,
            31,
            Some("p"),
            &add_offsets_flex("ghost", 999_999, 0, "cg-ghost"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 31);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    assert_ne!(err, 0);
    assert_ne!(err, 123); // never TRANSACTION_ABORTABLE
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn add_offsets_to_txn_v5_unsupported() {
    let dir = temp_dir("p82", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(25, 5, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 for flex versions
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
