//! v0.244: Kafka UpdateFeatures key 57 v0–1.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    skip_tag_buffer,
};
use volant_broker::Broker;

fn update_v0(feature: &str, max_version_level: i16) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(5_000); // TimeoutMs ignored
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, feature);
    body.put_i16(max_version_level);
    body.put_u8(0); // allowDowngrade
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

async fn api_versions_v3_features_empty(addr: &str) {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, "volant");
    put_compact_string(&mut body, "0.2.0");
    put_empty_tag_buffer(&mut body);
    let resp = rpc(addr, encode_request_flexible(18, 3, 7, Some("t"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 7);
    assert_eq!(src.get_i16(), 0); // header v0 + error
    let n = get_compact_array_len(&mut src).unwrap().unwrap_or(0);
    for _ in 0..n {
        let _key = src.get_i16();
        let _min = src.get_i16();
        let _max = src.get_i16();
        skip_tag_buffer(&mut src).unwrap();
    }
    assert_eq!(src.get_i32(), 0); // throttle
                                  // Empty tags: no SupportedFeatures / FinalizedFeatures.
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);
}

#[tokio::test]
async fn api_versions_lists_update_features_57() {
    let (_dir, broker) = broker_temp("v244", "api");
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
    assert_eq!(found.get(&57), Some(&(0, 1)));

    server.abort();
}

fn dir_has_feature_store(root: &std::path::Path) -> bool {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name();
            let lower = name.to_string_lossy().to_ascii_lowercase();
            if lower.contains("feature") {
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
async fn update_any_feature_is_92_nothing_persisted() {
    let (dir, broker) = broker_temp("v244", "upd");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(57, 0, 10, Some("admin"), &update_v0("metadata.version", 1)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "metadata.version");
    let feat_err = src.get_i16();
    assert!(
        feat_err == 92 || feat_err == 42,
        "per-feature error 92 FEATURE_UPDATE_FAILED or 42 INVALID_REQUEST, got {feat_err}"
    );
    let _ = get_compact_nullable_string(&mut src).unwrap();

    api_versions_v3_features_empty(&addr).await;
    assert!(
        !dir_has_feature_store(&dir),
        "UpdateFeatures must not persist a feature store"
    );

    server.abort();
}

#[tokio::test]
async fn update_features_not_controller_is_41() {
    let base = unique_dir("v244", "nc");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19461, 19462]);
    let broker = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(!broker.is_controller());
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(57, 0, 13, Some("admin"), &update_v0("metadata.version", 1)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 13);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 41); // NOT_CONTROLLER

    server.abort();
}

#[tokio::test]
async fn update_features_v2_unsupported() {
    let (_dir, broker) = broker_temp("v244", "v2");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(57, 2, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
