//! v0.252: Kafka ListClientMetricsResources key 74 v0.

#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, put_empty_tag_buffer,
    skip_tag_buffer,
};

fn empty_request_body() -> BytesMut {
    let mut body = BytesMut::new();
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

#[tokio::test]
async fn api_versions_lists_list_client_metrics_resources_74() {
    let (_dir, broker) = broker_temp("v252", "api");
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
    assert!(found.len() >= 53);
    assert_eq!(found.get(&74), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn list_v0_is_empty_and_does_not_persist() {
    let (dir, broker) = broker_temp("v252", "empty");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let before = snapshot_files(&dir);

    let resp = rpc(
        &addr,
        encode_request_flexible(74, 0, 10, Some("admin"), &empty_request_body()),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    skip_tag_buffer(&mut src).unwrap();

    assert_eq!(
        snapshot_files(&dir),
        before,
        "list must not create files under data_dir"
    );

    server.abort();
}

#[tokio::test]
async fn list_client_metrics_resources_v1_is_35() {
    let (_dir, broker) = broker_temp("v252", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(74, 1, 99, Some("c"), &[]),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i16(), 35);

    server.abort();
}
