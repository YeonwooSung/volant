//! Phase 57: Flexible OffsetCommit v8 + OffsetFetch v6–7.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_string, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn commit_v8(group: &str, topic: &str, partition: i32, offset: i64, meta: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    body.put_i32(0); // generation (0 = no membership check)
    put_compact_string(&mut body, "");
    put_compact_nullable_string(&mut body, None); // group_instance_id
    put_compact_array_len(&mut body, 1); // topics
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1); // partitions
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(-1); // leader_epoch
    put_compact_nullable_string(&mut body, Some(meta));
    put_empty_tag_buffer(&mut body); // partition tags
    put_empty_tag_buffer(&mut body); // topic tags
    put_empty_tag_buffer(&mut body); // top-level
    body
}

fn fetch_v7(
    group: &str,
    topics: Option<&[(&str, &[i32])]>,
    require_stable: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    match topics {
        None => {
            // null array = all
            body.put_u8(0); // compact array null
        }
        Some(list) => {
            put_compact_array_len(&mut body, list.len());
            for (topic, parts) in list {
                put_compact_string(&mut body, topic);
                put_compact_array_len(&mut body, parts.len());
                for p in *parts {
                    body.put_i32(*p);
                }
                put_empty_tag_buffer(&mut body);
            }
        }
    }
    body.put_u8(if require_stable { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn fetch_v6(group: &str, topics: Option<&[(&str, &[i32])]>) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, group);
    match topics {
        None => body.put_u8(0),
        Some(list) => {
            put_compact_array_len(&mut body, list.len());
            for (topic, parts) in list {
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
    body
}

#[tokio::test]
async fn api_versions_offset_flex_maxes() {
    let dir = temp_dir("p57", "api");
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
    assert_eq!(found.get(&8), Some(&(0, 10))); // Phase 72 TopicId
    assert_eq!(found.get(&9), Some(&(0, 10))); // Phase 72 TopicId
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v8_fetch_v7_roundtrip() {
    let dir = temp_dir("p57", "roundtrip");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Commit via flexible v8
    let cresp = rpc(
        &addr,
        encode_request_flexible(
            8,
            8,
            10,
            Some("c"),
            &commit_v8("cg-flex", "orders", 0, 42, "meta-flex"),
        ),
    )
    .await;
    let mut cs = cresp.freeze();
    assert_eq!(cs.get_i32(), 10);
    skip_tag_buffer(&mut cs).unwrap(); // header v1
    assert_eq!(cs.get_i32(), 0); // throttle
    let n_topics = get_compact_array_len(&mut cs).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(get_compact_string(&mut cs).unwrap(), "orders");
    let n_parts = get_compact_array_len(&mut cs).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    assert_eq!(cs.get_i32(), 0);
    assert_eq!(cs.get_i16(), 0); // error
    skip_tag_buffer(&mut cs).unwrap(); // partition tags
    skip_tag_buffer(&mut cs).unwrap(); // topic tags
    skip_tag_buffer(&mut cs).unwrap(); // top-level
    assert_eq!(cs.remaining(), 0);

    // Fetch via flexible v7 (listed partitions + require_stable)
    let fresp = rpc(
        &addr,
        encode_request_flexible(
            9,
            7,
            11,
            Some("c"),
            &fetch_v7("cg-flex", Some(&[("orders", &[0i32])]), false),
        ),
    )
    .await;
    let mut fs = fresp.freeze();
    assert_eq!(fs.get_i32(), 11);
    skip_tag_buffer(&mut fs).unwrap();
    assert_eq!(fs.get_i32(), 0); // throttle
    let n_topics = get_compact_array_len(&mut fs).unwrap().unwrap();
    assert_eq!(n_topics, 1);
    assert_eq!(get_compact_string(&mut fs).unwrap(), "orders");
    let n_parts = get_compact_array_len(&mut fs).unwrap().unwrap();
    assert_eq!(n_parts, 1);
    assert_eq!(fs.get_i32(), 0); // partition
    assert_eq!(fs.get_i64(), 42); // offset
    assert_eq!(fs.get_i32(), -1); // leader_epoch
    assert_eq!(
        get_compact_nullable_string(&mut fs).unwrap().as_deref(),
        Some("meta-flex")
    );
    assert_eq!(fs.get_i16(), 0); // partition error
    skip_tag_buffer(&mut fs).unwrap(); // partition tags
    skip_tag_buffer(&mut fs).unwrap(); // topic tags
    assert_eq!(fs.get_i16(), 0); // top-level error
    skip_tag_buffer(&mut fs).unwrap(); // top-level tags
    assert_eq!(fs.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_fetch_v6_null_topics_all() {
    let dir = temp_dir("p57", "all");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let _ = rpc(
        &addr,
        encode_request_flexible(8, 8, 1, Some("c"), &commit_v8("g-all", "t", 0, 7, "")),
    )
    .await;

    let resp = rpc(
        &addr,
        encode_request_flexible(9, 6, 2, Some("c"), &fetch_v6("g-all", None)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(n, 1);
    assert_eq!(get_compact_string(&mut src).unwrap(), "t");
    let np = get_compact_array_len(&mut src).unwrap().unwrap();
    assert_eq!(np, 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i64(), 7);
    assert_eq!(src.get_i32(), -1);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 0);
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v7_still_classic() {
    let dir = temp_dir("p57", "classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    put_string(&mut body, "g7");
    body.put_i32(0);
    put_string(&mut body, "");
    body.put_i16(-1); // null instance
    body.put_i32(1);
    put_string(&mut body, "t");
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(11);
    body.put_i32(-1); // epoch
    put_string(&mut body, "c7");

    let resp = rpc(&addr, encode_request(8, 7, 5, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5); // header v0
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i32(), 1); // classic topic count
    assert_eq!(get_string(&mut src).unwrap(), "t");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn offset_commit_v11_unsupported_header_v1() {
    let dir = temp_dir("p57", "v11");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(8, 11, 1, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// OffsetFetch multi-group v8: phase58_flexible_offset_fetch_multi.
