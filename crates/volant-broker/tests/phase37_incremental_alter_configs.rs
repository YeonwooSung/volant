//! Phase 37: Kafka IncrementalAlterConfigs on the shim.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// Build IncrementalAlterConfigs v0 body for one TOPIC resource.
fn incremental_body(
    topic: &str,
    configs: &[(&str, i8, Option<&str>)],
    validate_only: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1); // resources
    body.put_i8(2); // TOPIC
    put_string(&mut body, topic);
    body.put_i32(configs.len() as i32);
    for (name, op, value) in configs {
        put_string(&mut body, name);
        body.put_i8(*op);
        put_nullable_string(&mut body, *value);
    }
    body.put_u8(if validate_only { 1 } else { 0 });
    body
}

fn describe_retention_ms(src: &mut impl Buf) -> Option<String> {
    // DescribeConfigs v0 (Phase 46 framing): after corr → throttle, then
    // [error, error_message, resource_type, resource_name, configs[…]]
    let _throttle = src.get_i32();
    let n = src.get_i32();
    assert_eq!(n, 1);
    assert_eq!(src.get_i16(), 0); // error
    let _ = get_nullable_string(src).unwrap(); // error_message
    assert_eq!(src.get_i8(), 2); // TOPIC
    let _ = get_string(src).unwrap(); // name
    let cn = src.get_i32();
    for _ in 0..cn {
        let k = get_string(src).unwrap();
        let v = get_nullable_string(src).unwrap();
        let _ro = src.get_u8();
        let _def = src.get_u8();
        let _sens = src.get_u8();
        if k == "retention.ms" {
            return v;
        }
    }
    None
}

#[tokio::test]
async fn api_versions_includes_incremental_alter_configs() {
    let dir = temp_dir("p37", "api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut found = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        if key == 44 {
            found = Some((min_v, max_v));
        }
    }
    assert_eq!(found, Some((0, 1))); // Phase 61 flex v1
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn set_and_delete_topic_config() {
    let dir = temp_dir("p37", "setdel");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // SET retention.ms = 3600000
    let body = incremental_body(
        "orders",
        &[("retention.ms", 0, Some("3600000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(44, 0, 10, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let _ = get_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_string(&mut src).unwrap(), "orders");

    // DescribeConfigs — retention.ms present
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    dbody.put_i8(2);
    put_string(&mut dbody, "orders");
    dbody.put_i32(-1); // all keys
    let dresp = rpc(&addr, encode_request(32, 0, 11, Some("admin"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4); // corr
    assert_eq!(describe_retention_ms(&mut ds).as_deref(), Some("3600000"));

    // DELETE retention.ms
    let body2 = incremental_body("orders", &[("retention.ms", 1, None)], false);
    let resp2 = rpc(&addr, encode_request(44, 0, 12, Some("admin"), &body2)).await;
    let mut s2 = resp2.freeze();
    s2.advance(4 + 4);
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(s2.get_i16(), 0);

    let dresp2 = rpc(&addr, encode_request(32, 0, 13, Some("admin"), &dbody)).await;
    let mut ds2 = dresp2.freeze();
    ds2.advance(4);
    // Cleared → empty / default
    let v = describe_retention_ms(&mut ds2);
    assert!(v.as_deref().unwrap_or("").is_empty(), "expected cleared, got {v:?}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn validate_only_does_not_persist() {
    let dir = temp_dir("p37", "validate");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = incremental_body("t", &[("retention.ms", 0, Some("999"))], true);
    let resp = rpc(&addr, encode_request(44, 0, 20, Some("a"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);

    let cfg = broker.describe_configs("t").unwrap().2;
    assert!(cfg.retention_ms.is_none());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn append_rejected() {
    let dir = temp_dir("p37", "append");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = incremental_body("t", &[("retention.ms", 2, Some("1"))], false);
    let resp = rpc(&addr, encode_request(44, 0, 30, Some("a"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 40); // INVALID_CONFIG
    let msg = get_nullable_string(&mut src).unwrap();
    assert!(
        msg.as_deref().unwrap_or("").contains("APPEND"),
        "msg={msg:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn non_topic_and_acl_denied() {
    let dir = temp_dir("p37", "deny");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    broker
        .configure_acls(true, None, vec!["root".into()], "token".into())
        .unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Unsupported resource type (BROKER_LOGGER); BROKER is Phase 99.
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(8); // BROKER_LOGGER
    put_string(&mut body, "1");
    body.put_i32(1);
    put_string(&mut body, "log.retention.ms");
    body.put_i8(0);
    put_nullable_string(&mut body, Some("1"));
    body.put_u8(0);
    let resp = rpc(&addr, encode_request(44, 0, 40, Some("a"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST

    // TOPIC but ACL deny
    let body2 = incremental_body("t", &[("retention.ms", 0, Some("1"))], false);
    let resp2 = rpc(&addr, encode_request(44, 0, 41, Some("a"), &body2)).await;
    let mut s2 = resp2.freeze();
    s2.advance(4 + 4);
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(s2.get_i16(), 29); // TOPIC_AUTHORIZATION_FAILED

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn set_cleanup_policy_compact() {
    let dir = temp_dir("p37", "compact");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("c", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = incremental_body(
        "c",
        &[("cleanup.policy", 0, Some("compact"))],
        false,
    );
    let resp = rpc(&addr, encode_request(44, 0, 50, Some("a"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);

    let cfg = broker.describe_configs("c").unwrap().2;
    assert!(cfg.compact);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
