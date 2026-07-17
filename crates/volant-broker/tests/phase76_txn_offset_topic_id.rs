//! Phase 76: TxnOffsetCommit v6 TopicId (KIP-1319).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string, get_uuid,
    put_compact_array_len, put_compact_nullable_string, put_compact_string, put_empty_tag_buffer,
    put_uuid, skip_tag_buffer, volant_topic_uuid, KAFKA_UUID_ZERO,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p76-{label}-{}-{}",
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
    put_empty_tag_buffer(&mut body);
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

/// TxnOffsetCommit v3 name-based flexible body.
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

/// TxnOffsetCommit v6 TopicId body (same fields as v3 except UUID instead of name).
fn txn_offset_commit_v6(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic_uuid: &[u8; 16],
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
    put_uuid(&mut body, topic_uuid);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1); // leader_epoch
    put_compact_nullable_string(&mut body, Some("by-id"));
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // request tags
    body
}

fn topic_uuid(broker: &Broker, name: &str) -> [u8; 16] {
    let id = broker.metadata(Some(&[TopicName::new(name)])).topics[0]
        .topic_id
        .0;
    volant_topic_uuid(id)
}

async fn init_flex(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let resp = rpc(
        addr,
        encode_request_flexible(22, 2, corr, Some("p"), &init_v2(txn_id)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let pid = src.get_i64();
    let epoch = src.get_i16();
    skip_tag_buffer(&mut src).unwrap();
    (pid, epoch)
}

#[tokio::test]
async fn api_versions_txn_offset_commit_max_6() {
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
    assert_eq!(found.get(&28), Some(&(0, 6))); // TxnOffsetCommit

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_v6_by_topic_id_then_end_txn() {
    let dir = temp_dir("v6-tid");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let uuid = topic_uuid(&broker, "orders");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_flex(&addr, 1, "app-tid").await;

    // AddPartitionsToTxn v3 (open the txn)
    let add = rpc(
        &addr,
        encode_request_flexible(
            24,
            3,
            2,
            Some("p"),
            &add_partitions_v3("app-tid", pid, epoch, "orders", &[0]),
        ),
    )
    .await;
    let mut asrc = add.freeze();
    assert_eq!(asrc.get_i32(), 2);
    skip_tag_buffer(&mut asrc).unwrap();
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut asrc).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut asrc).unwrap(), Some(1));
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);
    skip_tag_buffer(&mut asrc).unwrap();
    skip_tag_buffer(&mut asrc).unwrap();
    skip_tag_buffer(&mut asrc).unwrap();

    // TxnOffsetCommit v6 by TopicId
    let toc = rpc(
        &addr,
        encode_request_flexible(
            28,
            6,
            3,
            Some("p"),
            &txn_offset_commit_v6("app-tid", "cg-tid", pid, epoch, &uuid, 0, 42),
        ),
    )
    .await;
    let mut tocs = toc.freeze();
    assert_eq!(tocs.get_i32(), 3);
    skip_tag_buffer(&mut tocs).unwrap();
    assert_eq!(tocs.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut tocs).unwrap(), Some(1));
    assert_eq!(get_uuid(&mut tocs).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut tocs).unwrap(), Some(1));
    assert_eq!(tocs.get_i32(), 0); // partition
    assert_eq!(tocs.get_i16(), 0); // error
    skip_tag_buffer(&mut tocs).unwrap();
    skip_tag_buffer(&mut tocs).unwrap();
    skip_tag_buffer(&mut tocs).unwrap();

    // Offsets not applied until EndTxn commit
    let before = broker
        .groups()
        .fetch_offsets("cg-tid", &[("orders".into(), 0)])
        .unwrap();
    assert!(before.entries.iter().all(|e| e.offset == u64::MAX));

    // EndTxn commit
    let end = rpc(
        &addr,
        encode_request_flexible(
            26,
            3,
            4,
            Some("p"),
            &end_txn_v3("app-tid", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 4);
    skip_tag_buffer(&mut es).unwrap();
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);
    skip_tag_buffer(&mut es).unwrap();

    let after = broker
        .groups()
        .fetch_offsets("cg-tid", &[("orders".into(), 0)])
        .unwrap();
    assert_eq!(after.entries.len(), 1);
    assert_eq!(after.entries[0].offset, 42);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_v6_unknown_topic_id() {
    let dir = temp_dir("unk");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_flex(&addr, 1, "app-unk").await;

    // Open txn on a real topic so buffer_txn_offsets would succeed if we resolved.
    broker.create_topic("real", 1).unwrap();
    let _ = rpc(
        &addr,
        encode_request_flexible(
            24,
            3,
            2,
            Some("p"),
            &add_partitions_v3("app-unk", pid, epoch, "real", &[0]),
        ),
    )
    .await;

    let mut bad = KAFKA_UUID_ZERO;
    bad[0] = 0xde;
    bad[1] = 0xad;
    let toc = rpc(
        &addr,
        encode_request_flexible(
            28,
            6,
            3,
            Some("p"),
            &txn_offset_commit_v6("app-unk", "cg-unk", pid, epoch, &bad, 0, 1),
        ),
    )
    .await;
    let mut src = toc.freeze();
    assert_eq!(src.get_i32(), 3);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_uuid(&mut src).unwrap(), bad);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 100); // UnknownTopicId
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // EndTxn still succeeds; unknown-id offsets were never buffered.
    let after_end = rpc(
        &addr,
        encode_request_flexible(
            26,
            3,
            4,
            Some("p"),
            &end_txn_v3("app-unk", pid, epoch, true),
        ),
    )
    .await;
    let mut es = after_end.freeze();
    assert_eq!(es.get_i32(), 4);
    skip_tag_buffer(&mut es).unwrap();
    assert_eq!(es.get_i32(), 0); // throttle
    assert_eq!(es.get_i16(), 0); // error

    let offs = broker
        .groups()
        .fetch_offsets("cg-unk", &[("real".into(), 0)])
        .unwrap();
    assert!(offs.entries.iter().all(|e| e.offset == u64::MAX));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_v3_name_path_still_works() {
    let dir = temp_dir("v3-name");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_flex(&addr, 1, "app-v3").await;

    let _ = rpc(
        &addr,
        encode_request_flexible(
            24,
            3,
            2,
            Some("p"),
            &add_partitions_v3("app-v3", pid, epoch, "events", &[0]),
        ),
    )
    .await;

    let toc = rpc(
        &addr,
        encode_request_flexible(
            28,
            3,
            3,
            Some("p"),
            &txn_offset_commit_v3("app-v3", "cg-v3", pid, epoch, "events", 0, 7),
        ),
    )
    .await;
    let mut tocs = toc.freeze();
    assert_eq!(tocs.get_i32(), 3);
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

    let end = rpc(
        &addr,
        encode_request_flexible(
            26,
            3,
            4,
            Some("p"),
            &end_txn_v3("app-v3", pid, epoch, true),
        ),
    )
    .await;
    let mut es = end.freeze();
    assert_eq!(es.get_i32(), 4);
    skip_tag_buffer(&mut es).unwrap();
    assert_eq!(es.get_i32(), 0);
    assert_eq!(es.get_i16(), 0);

    let after = broker
        .groups()
        .fetch_offsets("cg-v3", &[("events".into(), 0)])
        .unwrap();
    assert_eq!(after.entries.len(), 1);
    assert_eq!(after.entries[0].offset, 7);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn txn_offset_commit_v7_unsupported_header_v1() {
    let dir = temp_dir("v7");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(28, 7, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
