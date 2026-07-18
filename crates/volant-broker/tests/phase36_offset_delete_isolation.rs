//! Phase 36: Kafka OffsetDelete + Fetch isolation_level honesty.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_bytes, get_string, put_bytes,
    put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

#[tokio::test]
async fn api_versions_includes_offset_delete() {
    let dir = temp_dir("p36", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut keys = Vec::new();
    for _ in 0..n {
        keys.push(src.get_i16());
        let _ = src.get_i16();
        let _ = src.get_i16();
    }
    assert!(keys.contains(&47), "missing OffsetDelete");
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_delete_fetch_round_trip() {
    let dir = temp_dir("p36", "offdel");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // OffsetCommit v0: group, [topic [partition, offset, metadata]]
    let mut cbody = BytesMut::new();
    put_string(&mut cbody, "g1");
    cbody.put_i32(1); // topics
    put_string(&mut cbody, "orders");
    cbody.put_i32(2); // partitions
    cbody.put_i32(0);
    cbody.put_i64(10);
    put_string(&mut cbody, "");
    cbody.put_i32(1);
    cbody.put_i64(20);
    put_string(&mut cbody, "m");
    let cresp = rpc(&addr, encode_request(8, 0, 10, Some("c"), &cbody)).await;
    let mut cs = cresp.freeze();
    cs.advance(4);
    assert_eq!(cs.get_i32(), 1);
    assert_eq!(get_string(&mut cs).unwrap(), "orders");
    assert_eq!(cs.get_i32(), 2);
    assert_eq!(cs.get_i32(), 0);
    assert_eq!(cs.get_i16(), 0);
    assert_eq!(cs.get_i32(), 1);
    assert_eq!(cs.get_i16(), 0);

    // OffsetFetch both partitions
    let mut fbody = BytesMut::new();
    put_string(&mut fbody, "g1");
    fbody.put_i32(1);
    put_string(&mut fbody, "orders");
    fbody.put_i32(2);
    fbody.put_i32(0);
    fbody.put_i32(1);
    let fresp = rpc(&addr, encode_request(9, 0, 11, Some("f"), &fbody)).await;
    let mut fs = fresp.freeze();
    fs.advance(4);
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(get_string(&mut fs).unwrap(), "orders");
    assert_eq!(fs.get_i32(), 2);
    assert_eq!(fs.get_i32(), 0);
    assert_eq!(fs.get_i64(), 10);
    let _ = get_string(&mut fs).unwrap();
    assert_eq!(fs.get_i16(), 0);
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(fs.get_i64(), 20);

    // OffsetDelete partition 0 only
    let mut dbody = BytesMut::new();
    put_string(&mut dbody, "g1");
    dbody.put_i32(1);
    put_string(&mut dbody, "orders");
    dbody.put_i32(1);
    dbody.put_i32(0);
    let dresp = rpc(&addr, encode_request(47, 0, 12, Some("d"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 12);
    assert_eq!(ds.get_i16(), 0); // top-level
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), "orders");
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i16(), 0);

    // OffsetFetch again: p0 unknown (-1), p1 still 20
    let mut f2 = BytesMut::new();
    put_string(&mut f2, "g1");
    f2.put_i32(1);
    put_string(&mut f2, "orders");
    f2.put_i32(2);
    f2.put_i32(0);
    f2.put_i32(1);
    let f2r = rpc(&addr, encode_request(9, 0, 13, Some("f"), &f2)).await;
    let mut f2s = f2r.freeze();
    f2s.advance(4);
    assert_eq!(f2s.get_i32(), 1);
    assert_eq!(get_string(&mut f2s).unwrap(), "orders");
    assert_eq!(f2s.get_i32(), 2);
    assert_eq!(f2s.get_i32(), 0);
    assert_eq!(f2s.get_i64(), -1, "deleted offset should be unknown");
    let _ = get_string(&mut f2s).unwrap();
    assert_eq!(f2s.get_i16(), 0);
    assert_eq!(f2s.get_i32(), 1);
    assert_eq!(f2s.get_i64(), 20);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_delete_acl_denied() {
    let dir = temp_dir("p36", "acl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker
        .configure_acls(true, None, vec!["root".into()], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut dbody = BytesMut::new();
    put_string(&mut dbody, "g1");
    dbody.put_i32(1);
    put_string(&mut dbody, "t");
    dbody.put_i32(1);
    dbody.put_i32(0);
    let dresp = rpc(&addr, encode_request(47, 0, 20, Some("d"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4);
    assert_eq!(ds.get_i16(), 30); // GROUP_AUTHORIZATION_FAILED
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), "t");
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i16(), 30);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_v4_isolation_read_committed_lso_equals_hwm() {
    let dir = temp_dir("p36", "iso");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let topic = TopicName::new("events");
    broker
        .produce_one(
            &topic,
            PartitionId(0),
            volant_core::Message::from_value("hello"),
        )
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (corr, isolation) in [(30i32, 0u8), (31, 1u8)] {
        let mut body = BytesMut::new();
        body.put_i32(-1); // replica
        body.put_i32(500); // max_wait
        body.put_i32(1); // min_bytes
        body.put_i32(1_000_000); // max_bytes v3+
        body.put_u8(isolation); // isolation_level
        body.put_i32(1); // topics
        put_string(&mut body, "events");
        body.put_i32(1); // partitions
        body.put_i32(0);
        body.put_i64(0);
        body.put_i32(1_000_000);
        let resp = rpc(&addr, encode_request(1, 4, corr, Some("f"), &body)).await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        assert_eq!(src.get_i32(), 0); // throttle
        assert_eq!(src.get_i32(), 1);
        assert_eq!(get_string(&mut src).unwrap(), "events");
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i32(), 0); // partition
        assert_eq!(src.get_i16(), 0); // error
        let hwm = src.get_i64();
        let lso = src.get_i64();
        assert_eq!(lso, hwm, "isolation={isolation}: LSO must equal HWM");
        assert!(hwm >= 1);
        assert_eq!(src.get_i32(), 0); // aborted_transactions empty
        let records = get_bytes(&mut src).unwrap();
        assert!(records.is_some());
        assert!(!records.unwrap().is_empty());
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fetch_read_committed_after_txn_abort_empty() {
    let dir = temp_dir("p36", "txn-abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("tx", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // InitProducerId with transactional_id (nullable string)
    let mut ibody = BytesMut::new();
    put_nullable_string(&mut ibody, Some("txn-1"));
    ibody.put_i32(60_000);
    let iresp = rpc(&addr, encode_request(22, 0, 40, Some("p"), &ibody)).await;
    let mut is = iresp.freeze();
    is.advance(4 + 4); // corr + throttle
    assert_eq!(is.get_i16(), 0);
    let pid = is.get_i64();
    let epoch = is.get_i16();

    // AddPartitionsToTxn
    let mut abody = BytesMut::new();
    put_string(&mut abody, "txn-1");
    abody.put_i64(pid);
    abody.put_i16(epoch);
    abody.put_i32(1);
    put_string(&mut abody, "tx");
    abody.put_i32(1);
    abody.put_i32(0);
    let aresp = rpc(&addr, encode_request(24, 0, 41, Some("p"), &abody)).await;
    let mut as_ = aresp.freeze();
    as_.advance(4 + 4);
    assert_eq!(as_.get_i32(), 1);
    assert_eq!(get_string(&mut as_).unwrap(), "tx");
    assert_eq!(as_.get_i32(), 1);
    assert_eq!(as_.get_i32(), 0);
    assert_eq!(as_.get_i16(), 0);

    // Transactional produce (PID allocated with txn id → buffered)
    let batch = encode_record_batch_idempotent(
        &[Record {
            offset: Offset::new(0),
            key: None,
            value: Bytes::from_static(b"secret"),
            timestamp_ms: 1,
            headers: vec![],
        }],
        pid,
        epoch,
        0,
    );
    let mut pbody = BytesMut::new();
    pbody.put_i16(1);
    pbody.put_i32(1000);
    pbody.put_i32(1);
    put_string(&mut pbody, "tx");
    pbody.put_i32(1);
    pbody.put_i32(0);
    put_bytes(&mut pbody, Some(&batch));
    let presp = rpc(&addr, encode_request(0, 0, 42, Some("p"), &pbody)).await;
    let mut ps = presp.freeze();
    ps.advance(4);
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(get_string(&mut ps).unwrap(), "tx");
    assert_eq!(ps.get_i32(), 1);
    assert_eq!(ps.get_i32(), 0);
    assert_eq!(ps.get_i16(), 0, "txn produce should buffer ok");

    // EndTxn abort
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, "txn-1");
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(0); // committed = false
    let eresp = rpc(&addr, encode_request(26, 0, 43, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    assert_eq!(es.get_i16(), 0);

    // Fetch READ_COMMITTED — aborted data filtered; soft abort marker present.
    // Phase 86: write-through leaves records on the log (HWM > 0) but filters them.
    let mut fbody = BytesMut::new();
    fbody.put_i32(-1);
    fbody.put_i32(100);
    fbody.put_i32(1);
    fbody.put_i32(1_000_000);
    fbody.put_u8(1); // READ_COMMITTED
    fbody.put_i32(1);
    put_string(&mut fbody, "tx");
    fbody.put_i32(1);
    fbody.put_i32(0);
    fbody.put_i64(0);
    fbody.put_i32(1_000_000);
    let fresp = rpc(&addr, encode_request(1, 4, 44, Some("f"), &fbody)).await;
    let mut fs = fresp.freeze();
    fs.advance(4 + 4);
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(get_string(&mut fs).unwrap(), "tx");
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(fs.get_i32(), 0);
    assert_eq!(fs.get_i16(), 0);
    let hwm = fs.get_i64();
    let lso = fs.get_i64();
    assert!(hwm >= 1, "write-through abort leaves data on log, hwm={hwm}");
    assert_eq!(lso, hwm, "after abort LSO catches up to HWM");
    let aborted_n = fs.get_i32();
    assert!(aborted_n >= 1, "soft abort marker expected, got {aborted_n}");
    for _ in 0..aborted_n {
        let _aborted_pid = fs.get_i64();
        let _first = fs.get_i64();
    }
    let records = get_bytes(&mut fs).unwrap();
    // Phase 86/89: aborted *data* filtered; Phase 89 may leave ABORT control batch.
    if let Some(bytes) = records.as_ref() {
        if !bytes.is_empty() {
            let attrs = volant_broker::kafka::codec::peek_record_batch_attributes(bytes)
                .map(|(a, _)| a)
                .unwrap_or(0);
            assert_eq!(
                attrs & volant_broker::kafka::codec::RECORD_BATCH_ATTR_CONTROL,
                volant_broker::kafka::codec::RECORD_BATCH_ATTR_CONTROL,
                "READ_COMMITTED must not return aborted application data"
            );
        }
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
