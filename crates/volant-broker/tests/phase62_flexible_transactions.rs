//! Phase 62: Flexible InitProducerId v2 + txn APIs v3
//! (AddPartitionsToTxn / AddOffsetsToTxn / EndTxn / TxnOffsetCommit).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible,
    get_compact_array_len, get_compact_string, put_bytes, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_string,
    skip_tag_buffer,
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
        "volant-p62-{label}-{}-{}",
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

fn init_v2(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    put_empty_tag_buffer(&mut body);
    body
}

fn add_partitions_v3(txn_id: &str, pid: i64, epoch: i16, topic: &str, parts: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, parts.len());
    for p in parts {
        body.put_i32(*p);
    }
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn add_offsets_v3(txn_id: &str, pid: i64, epoch: i16, group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_compact_string(&mut body, group);
    put_empty_tag_buffer(&mut body);
    body
}

fn end_txn_v3(txn_id: &str, pid: i64, epoch: i16, committed: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if committed { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn txn_offset_commit_v3(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    put_compact_string(&mut body, group);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(-1); // generation
    put_compact_string(&mut body, ""); // member_id
    put_compact_nullable_string(&mut body, None); // group_instance_id
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1); // leader_epoch
    put_compact_nullable_string(&mut body, Some(""));
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
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

#[tokio::test]
async fn api_versions_txn_flex_maxes() {
    let dir = temp_dir("api");
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
    assert_eq!(found.get(&22), Some(&(0, 2)));
    assert_eq!(found.get(&24), Some(&(0, 3)));
    assert_eq!(found.get(&25), Some(&(0, 3)));
    assert_eq!(found.get(&26), Some(&(0, 3)));
    assert_eq!(found.get(&28), Some(&(0, 3)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn flex_txn_commit_roundtrip() {
    let dir = temp_dir("commit");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_flex(&addr, 1, "app-flex").await;

    // AddPartitionsToTxn v3
    let add = rpc(
        &addr,
        encode_request_flexible(
            24,
            3,
            2,
            Some("p"),
            &add_partitions_v3("app-flex", pid, epoch, "events", &[0]),
        ),
    )
    .await;
    let mut asrc = add.freeze();
    assert_eq!(asrc.get_i32(), 2);
    skip_tag_buffer(&mut asrc).unwrap();
    assert_eq!(asrc.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut asrc).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);
    skip_tag_buffer(&mut asrc).unwrap(); // partition tags
    skip_tag_buffer(&mut asrc).unwrap(); // topic tags
    skip_tag_buffer(&mut asrc).unwrap(); // response tags

    // Produce transactional batch (classic produce is fine)
    let batch = encode_record_batch_idempotent(&sample_records(b"flex-msg"), pid, epoch, 0);
    let prod = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("events", &batch)),
    )
    .await;
    let mut ps = prod.freeze();
    assert_eq!(ps.get_i32(), 3);
    assert_eq!(ps.get_i32(), 1);
    // not yet visible
    let pre = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert!(pre.is_empty());

    // AddOffsetsToTxn v3
    let ao = rpc(
        &addr,
        encode_request_flexible(
            25,
            3,
            4,
            Some("p"),
            &add_offsets_v3("app-flex", pid, epoch, "cg-flex"),
        ),
    )
    .await;
    let mut aos = ao.freeze();
    assert_eq!(aos.get_i32(), 4);
    skip_tag_buffer(&mut aos).unwrap();
    assert_eq!(aos.get_i32(), 0);
    assert_eq!(aos.get_i16(), 0);
    skip_tag_buffer(&mut aos).unwrap();

    // TxnOffsetCommit v3
    let toc = rpc(
        &addr,
        encode_request_flexible(
            28,
            3,
            5,
            Some("p"),
            &txn_offset_commit_v3("app-flex", "cg-flex", pid, epoch, "events", 0, 1),
        ),
    )
    .await;
    let mut tocs = toc.freeze();
    assert_eq!(tocs.get_i32(), 5);
    skip_tag_buffer(&mut tocs).unwrap();
    assert_eq!(tocs.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut tocs).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut tocs).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut tocs).unwrap(), Some(1));
    assert_eq!(tocs.get_i32(), 0);
    assert_eq!(tocs.get_i16(), 0);
    skip_tag_buffer(&mut tocs).unwrap();
    skip_tag_buffer(&mut tocs).unwrap();
    skip_tag_buffer(&mut tocs).unwrap();

    // EndTxn v3 commit
    let end = rpc(
        &addr,
        encode_request_flexible(
            26,
            3,
            6,
            Some("p"),
            &end_txn_v3("app-flex", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 6);
    skip_tag_buffer(&mut es).unwrap();
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);
    skip_tag_buffer(&mut es).unwrap();

    let post = broker
        .fetch(&TopicName::new("events"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(post.len(), 1);
    assert_eq!(post[0].value.as_ref(), b"flex-msg");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_init_still_works() {
    let dir = temp_dir("classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    volant_broker::kafka::codec::put_nullable_string(&mut body, Some("classic-txn"));
    body.put_i32(60_000);
    let resp = rpc(&addr, encode_request(22, 0, 1, Some("p"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert!(src.get_i64() > 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_txn_versions_use_header_v1() {
    let dir = temp_dir("unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // InitProducerId v3 (beyond max 2) → header v1 + UnsupportedVersion
    let resp = rpc(
        &addr,
        encode_request_flexible(22, 3, 10, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    // AddPartitionsToTxn v4 (broker-batch) → header v1 + UnsupportedVersion
    let resp = rpc(
        &addr,
        encode_request_flexible(24, 4, 11, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
