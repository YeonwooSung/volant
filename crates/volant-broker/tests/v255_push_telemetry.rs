//! v0.255: Kafka PushTelemetry key 72 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, put_compact_bytes, put_empty_tag_buffer, put_uuid,
    skip_tag_buffer,
};

fn push_telemetry_v0(client_instance_id: &[u8; 16], metrics: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_uuid(&mut body, client_instance_id);
    body.put_i32(1); // subscriptionId
    body.put_u8(0); // terminating
    body.put_i8(0); // compressionType
    put_compact_bytes(&mut body, Some(metrics));
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
async fn api_versions_lists_push_telemetry_72() {
    let (_dir, broker) = broker_temp("v255", "api");
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
    assert_eq!(found.get(&71), Some(&(0, 0)));
    assert_eq!(found.get(&72), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn push_telemetry_v0_rejects_nothing_persisted() {
    let (dir, broker) = broker_temp("v255", "reject");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = (0xA0 + i) as u8;
    }

    let resp = rpc(
        &addr,
        encode_request_flexible(
            72,
            0,
            10,
            Some("c"),
            &push_telemetry_v0(&id, b"otlp-metrics"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert!(
        !dir_has_telemetry(&dir),
        "PushTelemetry must not persist telemetry"
    );

    server.abort();
}

#[tokio::test]
async fn push_telemetry_v1_unsupported() {
    let (_dir, broker) = broker_temp("v255", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(72, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
