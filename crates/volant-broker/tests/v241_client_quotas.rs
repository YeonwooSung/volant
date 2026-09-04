//! v0.241: Kafka Describe/AlterClientQuotas keys 48/49 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, rpc, temp_dir};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn boot_single(label: &str) -> (std::path::PathBuf, Arc<Broker>) {
    let dir = temp_dir("v241", label);
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    (dir, broker)
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

/// Describe filter: one ANY `user` component, not strict.
fn describe_any_filter() -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "user");
    body.put_i8(2); // matchType = ANY
    put_compact_nullable_string(&mut body, None);
    put_empty_tag_buffer(&mut body);
    body.put_u8(0); // strict = false
    put_empty_tag_buffer(&mut body);
    body
}

/// Describe filter: empty components (all entities).
fn describe_empty_filter() -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 0);
    body.put_u8(0);
    put_empty_tag_buffer(&mut body);
    body
}

fn alter_user_entry(name: &str, key: &str, value: f64, validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, "user");
    put_compact_nullable_string(&mut body, Some(name));
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, key);
    body.put_f64(value);
    body.put_u8(0); // remove = false
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body.put_u8(u8::from(validate_only));
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_lists_client_quotas_48_49() {
    let (dir, broker) = boot_single("api");
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
    assert!(found.len() >= 46);
    assert_eq!(found.get(&48), Some(&(0, 0)));
    assert_eq!(found.get(&49), Some(&(0, 0)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_any_filter_is_empty() {
    let (dir, broker) = boot_single("desc");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (corr, body) in [
        (10, describe_any_filter()),
        (11, describe_empty_filter()),
    ] {
        let resp = rpc(
            &addr,
            encode_request_flexible(48, 0, corr, Some("admin"), &body),
        )
        .await;
        let mut src = resp.freeze();
        skip_flex_header(&mut src, corr);
        assert_eq!(src.get_i32(), 0); // throttle
        assert_eq!(src.get_i16(), 0); // error
        assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
        skip_tag_buffer(&mut src).unwrap();
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_any_entry_is_42_and_does_not_persist() {
    let (dir, broker) = boot_single("alter");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (corr, validate_only) in [(20, false), (21, true)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(
                49,
                0,
                corr,
                Some("admin"),
                &alter_user_entry("alice", "producer_byte_rate", 1024.0, validate_only),
            ),
        )
        .await;
        let mut src = resp.freeze();
        skip_flex_header(&mut src, corr);
        assert_eq!(src.get_i32(), 0); // throttle
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
        assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
        let msg = get_compact_nullable_string(&mut src).unwrap();
        assert_eq!(msg.as_deref(), Some("quotas not supported"));
        assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
        assert_eq!(get_compact_string(&mut src).unwrap(), "user");
        assert_eq!(
            get_compact_nullable_string(&mut src).unwrap().as_deref(),
            Some("alice")
        );
        skip_tag_buffer(&mut src).unwrap();
        skip_tag_buffer(&mut src).unwrap();
        skip_tag_buffer(&mut src).unwrap();
    }

    let resp = rpc(
        &addr,
        encode_request_flexible(48, 0, 22, Some("admin"), &describe_any_filter()),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 22);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(
        get_compact_array_len(&mut src).unwrap(),
        Some(0),
        "alter must not persist quotas"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn client_quotas_v1_is_35() {
    let (dir, broker) = boot_single("v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for key in [48i16, 49] {
        let resp = rpc(
            &addr,
            encode_request_flexible(key, 1, 99, Some("c"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), 99);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35);
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
