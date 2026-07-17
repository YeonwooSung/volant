//! Phase 31: Kafka transactions on the shim
//! (AddPartitionsToTxn / EndTxn / TxnOffsetCommit / FindCoordinator v1).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_nullable_string, get_string, put_bytes,
    put_nullable_string, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p31-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot_kafka(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_kafka_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

async fn rpc(addr: &str, request: BytesMut) -> BytesMut {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&request).await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        if buf.len() >= 4 {
            let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if buf.len() >= 4 + size {
                let _ = buf.split_to(4);
                return buf.split_to(size);
            }
        }
    }
    panic!("connection closed without full kafka response");
}

fn init_txn_body(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
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

fn add_partitions_body(txn_id: &str, pid: i64, epoch: i16, topic: &str, parts: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(parts.len() as i32);
    for p in parts {
        body.put_i32(*p);
    }
    body
}

fn end_txn_body(txn_id: &str, pid: i64, epoch: i16, committed: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if committed { 1 } else { 0 });
    body
}

fn add_offsets_body(txn_id: &str, pid: i64, epoch: i16, group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_string(&mut body, group);
    body
}

fn txn_offset_commit_body(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    put_string(&mut body, group);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    put_nullable_string(&mut body, Some(""));
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

async fn init_txn(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let resp = rpc(
        addr,
        encode_request(22, 0, corr, Some("p"), &init_txn_body(txn_id)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let pid = src.get_i64();
    let epoch = src.get_i16();
    (pid, epoch)
}

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

#[tokio::test]
async fn api_versions_includes_txn_apis() {
    let dir = temp_dir("api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        found.insert(key, (min, max));
    }
    assert_eq!(found.get(&10), Some(&(0, 4))); // FindCoordinator (Phase 52 flexible)
    assert_eq!(found.get(&24), Some(&(0, 3))); // AddPartitionsToTxn (Phase 62 flex v3)
    assert_eq!(found.get(&25), Some(&(0, 3))); // AddOffsetsToTxn
    assert_eq!(found.get(&26), Some(&(0, 3))); // EndTxn
    assert_eq!(found.get(&28), Some(&(0, 3))); // TxnOffsetCommit

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_v1_transaction_key_type() {
    let dir = temp_dir("fc");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    put_string(&mut body, "txn-app-1");
    body.put_i8(1); // key_type = transaction
    let resp = rpc(&addr, encode_request(10, 1, 2, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let _msg = get_nullable_string(&mut src).unwrap();
    let node = src.get_i32();
    let host = get_string(&mut src).unwrap();
    let port = src.get_i32();
    assert!(node >= 0);
    assert!(!host.is_empty());
    assert!(port > 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn commit_makes_produce_visible() {
    let dir = temp_dir("commit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn(&addr, 1, "app-1").await;

    // Open txn via AddPartitionsToTxn on both partitions.
    let add = rpc(
        &addr,
        encode_request(
            24,
            0,
            2,
            Some("p"),
            &add_partitions_body("app-1", pid, epoch, "events", &[0, 1]),
        ),
    )
    .await;
    let mut asrc = add.freeze();
    assert_eq!(asrc.get_i32(), 2);
    assert_eq!(asrc.get_i32(), 0); // throttle
    assert_eq!(asrc.get_i32(), 1); // topics
    assert_eq!(get_string(&mut asrc).unwrap(), "events");
    assert_eq!(asrc.get_i32(), 2);
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(asrc.get_i16(), 0);

    let batch0 = encode_record_batch_idempotent(&sample_records(b"a"), pid, epoch, 0);
    let (e0, base0) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 3, Some("p"), &produce_body("events", &batch0)),
        )
        .await,
        3,
        "events",
    );
    assert_eq!(e0, 0);
    assert_eq!(base0, 0, "buffered produce reports base 0");

    // Partition 1 needs its own sequence stream (base_seq 0 for that partition).
    let mut body1 = BytesMut::new();
    body1.put_i16(1);
    body1.put_i32(5000);
    body1.put_i32(1);
    put_string(&mut body1, "events");
    body1.put_i32(1);
    body1.put_i32(1); // partition 1
    let batch1 = encode_record_batch_idempotent(&sample_records(b"b"), pid, epoch, 0);
    put_bytes(&mut body1, Some(&batch1));
    let r1 = rpc(&addr, encode_request(0, 0, 4, Some("p"), &body1)).await;
    let mut s1 = r1.freeze();
    assert_eq!(s1.get_i32(), 4);
    assert_eq!(s1.get_i32(), 1);
    assert_eq!(get_string(&mut s1).unwrap(), "events");
    assert_eq!(s1.get_i32(), 1);
    assert_eq!(s1.get_i32(), 1);
    assert_eq!(s1.get_i16(), 0);

    // Not visible yet.
    let pre0 = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    let pre1 = broker
        .fetch(&TopicName::new("events"), PartitionId(1), Offset::new(0), 10)
        .unwrap();
    assert!(pre0.is_empty());
    assert!(pre1.is_empty());

    let end = rpc(
        &addr,
        encode_request(
            26,
            0,
            5,
            Some("p"),
            &end_txn_body("app-1", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 5);
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);

    let post0 = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    let post1 = broker
        .fetch(&TopicName::new("events"), PartitionId(1), Offset::new(0), 10)
        .unwrap();
    assert_eq!(post0.len(), 1);
    assert_eq!(post1.len(), 1);
    assert_eq!(post0[0].value.as_ref(), b"a");
    assert_eq!(post1[0].value.as_ref(), b"b");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn abort_leaves_no_records() {
    let dir = temp_dir("abort");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("gone", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn(&addr, 1, "app-abort").await;
    let _ = rpc(
        &addr,
        encode_request(
            24,
            0,
            2,
            Some("p"),
            &add_partitions_body("app-abort", pid, epoch, "gone", &[0]),
        ),
    )
    .await;

    let batch = encode_record_batch_idempotent(&sample_records(b"drop-me"), pid, epoch, 0);
    let (e, _) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 3, Some("p"), &produce_body("gone", &batch)),
        )
        .await,
        3,
        "gone",
    );
    assert_eq!(e, 0);

    let end = rpc(
        &addr,
        encode_request(
            26,
            0,
            4,
            Some("p"),
            &end_txn_body("app-abort", pid, epoch, false),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 4);
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);

    let post = broker
        .fetch(&TopicName::new("gone"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert!(post.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_without_open_txn_rejected() {
    let dir = temp_dir("noopen");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn(&addr, 1, "lonely").await;
    let batch = encode_record_batch_idempotent(&sample_records(b"x"), pid, epoch, 0);
    let (err, _) = parse_produce_base(
        rpc(
            &addr,
            encode_request(0, 0, 2, Some("p"), &produce_body("t", &batch)),
        )
        .await,
        2,
        "t",
    );
    assert_eq!(err, 48, "INVALID_TXN_STATE without AddPartitionsToTxn");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_applies_on_commit_only() {
    let dir = temp_dir("offsets");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn(&addr, 1, "off-app").await;
    let _ = rpc(
        &addr,
        encode_request(
            24,
            0,
            2,
            Some("p"),
            &add_partitions_body("off-app", pid, epoch, "t", &[0]),
        ),
    )
    .await;

    let add_off = rpc(
        &addr,
        encode_request(
            25,
            0,
            3,
            Some("p"),
            &add_offsets_body("off-app", pid, epoch, "cg1"),
        ),
    )
    .await;
    let mut ao = add_off.freeze();
    assert_eq!(ao.get_i32(), 3);
    assert_eq!(ao.get_i32(), 0);
    assert_eq!(ao.get_i16(), 0);

    let toc = rpc(
        &addr,
        encode_request(
            28,
            0,
            4,
            Some("p"),
            &txn_offset_commit_body("off-app", "cg1", pid, epoch, "t", 0, 42),
        ),
    )
    .await;
    let mut ts = toc.freeze();
    assert_eq!(ts.get_i32(), 4);
    assert_eq!(ts.get_i32(), 0);
    assert_eq!(ts.get_i32(), 1);
    assert_eq!(get_string(&mut ts).unwrap(), "t");
    assert_eq!(ts.get_i32(), 1);
    assert_eq!(ts.get_i32(), 0);
    assert_eq!(ts.get_i16(), 0);

    // Not applied yet.
    let before = broker
        .groups()
        .fetch_offsets("cg1", &[("t".into(), 0)])
        .unwrap();
    assert!(
        before.entries.iter().all(|e| e.offset == u64::MAX),
        "offsets must wait for EndTxn commit, got {:?}",
        before.entries
    );

    let end = rpc(
        &addr,
        encode_request(
            26,
            0,
            5,
            Some("p"),
            &end_txn_body("off-app", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 5);
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);

    let after = broker
        .groups()
        .fetch_offsets("cg1", &[("t".into(), 0)])
        .unwrap();
    assert_eq!(after.entries.len(), 1);
    assert_eq!(after.entries[0].offset, 42);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
