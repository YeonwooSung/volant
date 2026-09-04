//! v0.258: Kafka CreateDelegationToken key 38 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_bytes, get_compact_string,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, skip_tag_buffer,
};

fn create_token_v0(renewers: &[(&str, &str)], max_life_ms: i64) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, renewers.len());
    for (ty, name) in renewers {
        put_compact_string(&mut body, ty);
        put_compact_string(&mut body, name);
        put_empty_tag_buffer(&mut body);
    }
    body.put_i64(max_life_ms);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn dir_has_delegation_token(root: &std::path::Path) -> bool {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name();
            let lower = name.to_string_lossy().to_ascii_lowercase();
            if lower.contains("delegation") {
                return true;
            }
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                stack.push(e.path());
            }
        }
    }
    false
}

#[tokio::test]
async fn api_versions_lists_create_delegation_token_38() {
    let (_dir, broker) = broker_temp("v258", "api");
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
    assert!(found.len() >= 57);
    assert_eq!(found.get(&38), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn create_delegation_token_v0_is_42_nothing_persisted() {
    let (dir, broker) = broker_temp("v258", "create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(
            38,
            0,
            10,
            Some("admin"),
            &create_token_v0(&[("User", "alice")], -1),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // principalType
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // principalName
    assert_eq!(src.get_i64(), -1); // issueTimestampMs
    assert_eq!(src.get_i64(), -1); // expiryTimestampMs
    assert_eq!(src.get_i64(), -1); // maxTimestampMs
    assert_eq!(get_compact_string(&mut src).unwrap(), ""); // tokenId
    assert_eq!(
        get_compact_bytes(&mut src).unwrap().unwrap_or_default().as_ref(),
        b""
    ); // hmac
    assert_eq!(src.get_i32(), 0); // throttleTimeMs
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert!(
        !dir_has_delegation_token(&dir),
        "CreateDelegationToken must not persist a token"
    );

    server.abort();
}

#[tokio::test]
async fn create_delegation_token_v1_unsupported() {
    let (_dir, broker) = broker_temp("v258", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(38, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
