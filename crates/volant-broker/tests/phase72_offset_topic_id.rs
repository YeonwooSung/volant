//! Phase 72: OffsetCommit/OffsetFetch v9–10 (TopicId + MemberId fields).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_uuid, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_uuid, skip_tag_buffer, volant_topic_uuid,
    KAFKA_UUID_ZERO,
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
        "volant-p72-{label}-{}-{}",
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

/// OffsetCommit v8/v9 name-based flexible body.
fn commit_v8_name(group: &str, topic: &str, partition: i32, offset: i64, meta: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(0); // generation
    put_compact_string(&mut body, "");
    put_compact_nullable_string(&mut body, None); // group_instance_id
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1); // leader_epoch
    put_compact_nullable_string(&mut body, Some(meta));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

/// OffsetCommit v10 TopicId body.
fn commit_v10_id(
    group: &str,
    topic_uuid: &[u8; 16],
    partition: i32,
    offset: i64,
    meta: &str,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(0);
    put_compact_string(&mut body, "");
    put_compact_nullable_string(&mut body, None);
    put_compact_array_len(&mut body, 1);
    put_uuid(&mut body, topic_uuid);
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

/// OffsetFetch v9 multi-group with MemberId + MemberEpoch.
fn fetch_v9_multi(
    groups: &[(&str, Option<&str>, i32, Option<&[(&str, &[i32])]>)],
    require_stable: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, groups.len());
    for (gid, member_id, member_epoch, topics) in groups {
        put_compact_string(&mut body, gid);
        put_compact_nullable_string(&mut body, *member_id);
        body.put_i32(*member_epoch);
        match topics {
            None => body.put_u8(0), // null = all
            Some(list) => {
                put_compact_array_len(&mut body, list.len());
                for (topic, parts) in *list {
                    put_compact_string(&mut body, topic);
                    put_compact_array_len(&mut body, parts.len());
                    for p in *parts {
                        body.put_i32(*p);
                    }
                    put_empty_tag_buffer(&mut body);
                }
            }
        }
        put_empty_tag_buffer(&mut body);
    }
    body.put_u8(if require_stable { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

/// OffsetFetch v10 multi-group by TopicId (includes v9 member fields).
fn fetch_v10_multi(
    groups: &[(
        &str,
        Option<&str>,
        i32,
        Option<&[(&[u8; 16], &[i32])]>,
    )],
    require_stable: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, groups.len());
    for (gid, member_id, member_epoch, topics) in groups {
        put_compact_string(&mut body, gid);
        put_compact_nullable_string(&mut body, *member_id);
        body.put_i32(*member_epoch);
        match topics {
            None => body.put_u8(0),
            Some(list) => {
                put_compact_array_len(&mut body, list.len());
                for (uuid, parts) in *list {
                    put_uuid(&mut body, uuid);
                    put_compact_array_len(&mut body, parts.len());
                    for p in *parts {
                        body.put_i32(*p);
                    }
                    put_empty_tag_buffer(&mut body);
                }
            }
        }
        put_empty_tag_buffer(&mut body);
    }
    body.put_u8(if require_stable { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn topic_uuid(broker: &Broker, name: &str) -> [u8; 16] {
    let id = broker.metadata(Some(&[TopicName::new(name)])).topics[0]
        .topic_id
        .0;
    volant_topic_uuid(id)
}

#[tokio::test]
async fn api_versions_offset_commit_fetch_max_10() {
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
    assert_eq!(found.get(&8), Some(&(0, 10))); // OffsetCommit
    assert_eq!(found.get(&9), Some(&(0, 10))); // OffsetFetch
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v9_name_based() {
    let dir = temp_dir("commit-v9");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            8,
            9,
            9,
            Some("c"),
            &commit_v8_name("g", "orders", 0, 42, "v9"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "orders");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v10_by_topic_id() {
    let dir = temp_dir("commit-v10");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let uuid = topic_uuid(&broker, "orders");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            8,
            10,
            10,
            Some("c"),
            &commit_v10_id("g-tid", &uuid, 0, 100, "by-id"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // Fetch v10 should see the commit
    let body = fetch_v10_multi(
        &[("g-tid", Some("m1"), 0, Some(&[(&uuid, &[0i32])]))],
        false,
    );
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 10, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "g-tid");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 100);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("by-id")
    );
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v10_unknown_topic_id() {
    let dir = temp_dir("commit-unk");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut bad = KAFKA_UUID_ZERO;
    bad[0] = 0xde;
    bad[1] = 0xad;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            8,
            10,
            12,
            Some("c"),
            &commit_v10_id("g", &bad, 0, 1, ""),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), bad);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 100); // UnknownTopicId
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v9_member_fields_ignored() {
    let dir = temp_dir("fetch-v9");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(
            8,
            8,
            1,
            Some("c"),
            &commit_v8_name("g", "events", 0, 7, "m"),
        ),
    )
    .await;

    let body = fetch_v9_multi(
        &[("g", Some("member-x"), 3, Some(&[("events", &[0i32])]))],
        false,
    );
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 9, 20, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "g");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "events");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 7);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("m")
    );
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v10_unknown_topic_id() {
    let dir = temp_dir("fetch-unk");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut bad = [0u8; 16];
    bad[0] = 0xbe;
    bad[1] = 0xef;
    let body = fetch_v10_multi(
        &[("g", None, 0, Some(&[(&bad, &[0i32])]))],
        false,
    );
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 10, 30, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 30);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "g");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), bad);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), -1);
    assert_eq!(src.get_i32(), -1);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i16(), 100); // UnknownTopicId
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v10_list_all_emits_uuid() {
    let dir = temp_dir("list-all");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("all-t", 1).unwrap();
    let uuid = topic_uuid(&broker, "all-t");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(
            8,
            10,
            1,
            Some("c"),
            &commit_v10_id("g-all", &uuid, 0, 55, "all"),
        ),
    )
    .await;

    let body = fetch_v10_multi(&[("g-all", None, 0, None)], false);
    let resp = rpc(
        &addr,
        encode_request_flexible(9, 10, 40, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 40);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "g-all");
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(get_uuid(&mut src).unwrap(), uuid);
    assert_eq!(get_compact_array_len(&mut src).unwrap().unwrap(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 55);
    assert_eq!(src.get_i32(), -1);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_v11_unsupported_header_v1() {
    let dir = temp_dir("v11");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (api, corr) in [(8i16, 1i32), (9i16, 2i32)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, 11, corr, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35); // UnsupportedVersion
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
