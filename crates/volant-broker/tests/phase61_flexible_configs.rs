//! Phase 61: Flexible DescribeConfigs v4 / AlterConfigs v2 / IncrementalAlterConfigs v1.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_nullable_string, get_string, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_nullable_string,
    put_string, put_unsigned_varint, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn alter_v2(topic: &str, key: &str, value: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(2); // TOPIC
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, key);
    put_compact_nullable_string(&mut body, Some(value));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body.put_u8(0); // validate_only
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_v4(topic: &str, include_docs: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(2);
    put_compact_string(&mut body, topic);
    put_unsigned_varint(&mut body, 0); // null configuration keys = all
    put_empty_tag_buffer(&mut body);
    body.put_u8(1); // include_synonyms
    body.put_u8(if include_docs { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn incremental_v1(topic: &str, key: &str, op: i8, value: Option<&str>) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(2);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, key);
    body.put_i8(op);
    put_compact_nullable_string(&mut body, value);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body.put_u8(0);
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_configs_flex_maxes() {
    let dir = temp_dir("p61", "api");
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
    assert_eq!(found.get(&32), Some(&(0, 4)));
    assert_eq!(found.get(&33), Some(&(0, 2)));
    assert_eq!(found.get(&44), Some(&(0, 1)));
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_describe_flexible_roundtrip() {
    let dir = temp_dir("p61", "roundtrip");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("cfg-t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // AlterConfigs v2
    let resp = rpc(
        &addr,
        encode_request_flexible(
            33,
            2,
            10,
            Some("c"),
            &alter_v2("cfg-t", "retention.ms", "60000"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_compact_string(&mut src).unwrap(), "cfg-t");
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // DescribeConfigs v4 with docs
    let resp = rpc(
        &addr,
        encode_request_flexible(32, 4, 11, Some("c"), &describe_v4("cfg-t", true)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_compact_string(&mut src).unwrap(), "cfg-t");
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    assert!(n >= 1);
    let mut found_ret = false;
    for _ in 0..n {
        let name = get_compact_string(&mut src).unwrap();
        let val = get_compact_nullable_string(&mut src).unwrap();
        assert_eq!(src.get_u8(), 0); // read_only
        let source = src.get_i8();
        assert_eq!(src.get_u8(), 0); // sensitive
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // synonyms
        let ctype = src.get_i8();
        let doc = get_compact_nullable_string(&mut src).unwrap();
        skip_tag_buffer(&mut src).unwrap();
        if name == "retention.ms" {
            found_ret = true;
            assert_eq!(val.as_deref(), Some("60000"));
            assert_eq!(source, 1); // TOPIC_CONFIG
            assert_eq!(ctype, 5); // LONG
            assert!(doc.is_some());
        }
    }
    assert!(found_ret);
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // IncrementalAlterConfigs v1 SET
    let resp = rpc(
        &addr,
        encode_request_flexible(
            44,
            1,
            12,
            Some("c"),
            &incremental_v1("cfg-t", "cleanup.policy", 0, Some("compact")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 12);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_compact_string(&mut src).unwrap(), "cfg-t");
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_configs_still_work() {
    let dir = temp_dir("p61", "classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(2);
    put_string(&mut body, "t");
    body.put_i32(1);
    put_string(&mut body, "retention.ms");
    put_nullable_string(&mut body, Some("120000"));
    body.put_u8(0);
    let resp = rpc(&addr, encode_request(33, 1, 2, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i32(), 0); // no header tags
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_string(&mut src).unwrap(), "t");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("p61", "unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (api, ver, corr) in [(32i16, 5i16, 1i32), (33, 3, 2), (44, 2, 3)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "api={api} ver={ver}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_unknown_topic_v4() {
    let dir = temp_dir("p61", "unknown");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(32, 4, 1, Some("c"), &describe_v4("nope", false)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i16(), 3); // UNKNOWN_TOPIC_OR_PARTITION
    let msg = get_compact_nullable_string(&mut src).unwrap();
    assert!(msg.is_some());
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_compact_string(&mut src).unwrap(), "nope");
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
