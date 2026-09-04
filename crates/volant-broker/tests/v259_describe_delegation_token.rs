//! v0.259: Kafka DescribeDelegationToken key 41 v0.

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_compact_array_len,
    put_empty_tag_buffer, put_unsigned_varint, skip_tag_buffer,
};

fn empty_owners() -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body
}

fn null_owners() -> BytesMut {
    let mut body = BytesMut::new();
    put_unsigned_varint(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body
}

fn snapshot_files(root: &Path) -> BTreeMap<PathBuf, u64> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(meta) = e.metadata() {
                let rel = p.strip_prefix(root).unwrap_or(&p).to_path_buf();
                out.insert(rel, meta.len());
            }
        }
    }
    out
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

/// Official DescribeDelegationTokenResponse.json: error, tokens, throttle.
fn assert_empty_tokens(src: &mut impl Buf, corr: i32) {
    skip_flex_header(src, corr);
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_array_len(src).unwrap(), Some(0));
    assert_eq!(src.get_i32(), 0); // throttle
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_describe_delegation_token_41() {
    let (_dir, broker) = broker_temp("v259", "api");
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
    assert_eq!(found.get(&41), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn describe_v0_empty_or_null_owners_is_empty_and_does_not_persist() {
    let (dir, broker) = broker_temp("v259", "empty");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = snapshot_files(&dir);

    for (corr, body) in [(10, empty_owners()), (11, null_owners())] {
        let resp = rpc(
            &addr,
            encode_request_flexible(41, 0, corr, Some("admin"), &body),
        )
        .await;
        let mut src = resp.freeze();
        assert_empty_tokens(&mut src, corr);
    }

    assert_eq!(
        snapshot_files(&dir),
        before,
        "describe must not create files under data_dir"
    );

    server.abort();
}

#[tokio::test]
async fn describe_delegation_token_v1_is_35() {
    let (_dir, broker) = broker_temp("v259", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request_flexible(41, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
