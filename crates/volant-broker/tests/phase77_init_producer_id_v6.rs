//! Phase 77: InitProducerId v6 (OngoingTxn / 2PC wire honesty).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, put_compact_nullable_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// InitProducerId v6 body: compact nullable txn_id, timeout, resume pid/epoch,
/// enable2pc, keep_prepared_txn, tags.
fn init_v6(txn_id: &str, resume_pid: i64, resume_epoch: i16, enable_2pc: bool, keep_prepared: bool) -> BytesMut {
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

/// InitProducerId v5 body (no 2PC flags) for wire-shape contrast.
fn init_v5(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body.put_i64(-1);
    body.put_i16(-1);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_init_producer_id_max_6() {
    let dir = temp_dir("p77", "api");
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
    assert_eq!(found.get(&22), Some(&(0, 6)), "InitProducerId");
    // Unchanged peers.
    assert_eq!(found.get(&24), Some(&(0, 5)), "AddPartitionsToTxn");
    assert_eq!(found.get(&26), Some(&(0, 5)), "EndTxn");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn init_producer_id_v6_ongoing_txn_minus_one() {
    let dir = temp_dir("p77", "v6");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (enable_2pc, keep) in [(false, false), (true, false), (true, true)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(
                22,
                6,
                42,
                Some("p"),
                &init_v6("app-p77", -1, -1, enable_2pc, keep),
            ),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), 42);
        skip_tag_buffer(&mut src).unwrap(); // response header v1
        assert_eq!(src.get_i32(), 0); // throttle
        assert_eq!(src.get_i16(), 0); // error
        let pid = src.get_i64();
        let epoch = src.get_i16();
        assert!(pid > 0, "allocated pid");
        assert!(epoch >= 0);
        // No prepared txn yet → OngoingTxn* is -1 (Phase 90 surfaces prepared only).
        assert_eq!(src.get_i64(), -1, "OngoingTxnProducerId enable2pc={enable_2pc}");
        assert_eq!(src.get_i16(), -1, "OngoingTxnProducerEpoch");
        skip_tag_buffer(&mut src).unwrap();
        assert!(!src.has_remaining());
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn init_producer_id_v5_has_no_ongoing_txn_fields() {
    let dir = temp_dir("p77", "v5");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(22, 5, 7, Some("p"), &init_v5("app-v5")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    let _pid = src.get_i64();
    let _epoch = src.get_i16();
    // v5: only tags remain (no OngoingTxn int64+int16).
    skip_tag_buffer(&mut src).unwrap();
    assert!(!src.has_remaining(), "v5 response must not include OngoingTxn fields");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn init_producer_id_v7_unsupported_header_v1() {
    let dir = temp_dir("p77", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(22, 7, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn init_v6_then_add_partitions_end_txn_still_works() {
    let dir = temp_dir("p77", "path");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            22,
            6,
            1,
            Some("p"),
            &init_v6("txn-path", -1, -1, false, false),
        ),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    skip_tag_buffer(&mut src).unwrap();
    src.advance(4); // throttle
    assert_eq!(src.get_i16(), 0);
    let pid = src.get_i64();
    let epoch = src.get_i16();
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i16(), -1);

    // AddPartitionsToTxn v3 (flex name path).
    let mut add = BytesMut::new();
    {
        use volant_broker::kafka::codec::{put_compact_array_len, put_compact_string};
        put_compact_string(&mut add, "txn-path");
        add.put_i64(pid);
        add.put_i16(epoch);
        put_compact_array_len(&mut add, 1);
        put_compact_string(&mut add, "events");
        put_compact_array_len(&mut add, 1);
        add.put_i32(0);
        put_empty_tag_buffer(&mut add);
        put_empty_tag_buffer(&mut add);
        put_empty_tag_buffer(&mut add);
    }
    let resp = rpc(
        &addr,
        encode_request_flexible(24, 3, 2, Some("p"), &add),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    // topics array...
    use volant_broker::kafka::codec::get_compact_array_len;
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let _ = volant_broker::kafka::codec::get_compact_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0); // partition ok

    // EndTxn v3 abort.
    let mut end = BytesMut::new();
    {
        use volant_broker::kafka::codec::put_compact_string;
        put_compact_string(&mut end, "txn-path");
        end.put_i64(pid);
        end.put_i16(epoch);
        end.put_u8(0); // abort
        put_empty_tag_buffer(&mut end);
    }
    let resp = rpc(
        &addr,
        encode_request_flexible(26, 3, 3, Some("p"), &end),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
