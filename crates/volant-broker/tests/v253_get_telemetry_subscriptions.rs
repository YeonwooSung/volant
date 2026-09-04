//! v0.253: Kafka GetTelemetrySubscriptions key 71 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_uuid, put_empty_tag_buffer,
    put_uuid, skip_tag_buffer,
};

fn get_telemetry_v0(client_instance_id: &[u8; 16], subscription_id: i32) -> BytesMut {
    let mut body = BytesMut::new();
    put_uuid(&mut body, client_instance_id);
    body.put_i32(subscription_id);
    put_empty_tag_buffer(&mut body);
    body
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

fn dir_has_telemetry(root: &std::path::Path) -> bool {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name();
            let lower = name.to_string_lossy().to_ascii_lowercase();
            if lower.contains("telemetry") {
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
async fn api_versions_lists_get_telemetry_subscriptions_71() {
    let (_dir, broker) = broker_temp("v253", "api");
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
    assert_eq!(found.get(&71), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn get_telemetry_v0_empty_nothing_persisted() {
    let (dir, broker) = broker_temp("v253", "empty");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = (0xA0 + i) as u8;
    }

    let resp = rpc(
        &addr,
        encode_request_flexible(71, 0, 10, Some("c"), &get_telemetry_v0(&id, 99)),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_uuid(&mut src).unwrap(), id);
    assert_eq!(src.get_i32(), 0); // subscriptionId
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // compression
    assert_eq!(src.get_i32(), -1); // pushIntervalMs
    assert_eq!(src.get_i32(), 0); // telemetryMaxBytes
    assert_eq!(src.get_u8(), 0); // deltaTemporality
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // metrics
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert!(
        !dir_has_telemetry(&dir),
        "GetTelemetrySubscriptions must not persist telemetry"
    );

    server.abort();
}

#[tokio::test]
async fn get_telemetry_v1_unsupported() {
    let (_dir, broker) = broker_temp("v253", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(71, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
