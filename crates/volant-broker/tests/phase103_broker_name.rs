//! Phase 103: BROKER config resource name must match this broker's node_id (or empty).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::broker_config::{KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

const ERR_NONE: i16 = 0;
const ERR_INVALID_REQUEST: i16 = 42;
const RES_BROKER: i8 = 4;
const RES_TOPIC: i8 = 2;

fn describe_resource_body(rtype: i8, name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(rtype);
    put_string(&mut body, name);
    body.put_i32(-1); // all keys
    body
}

fn alter_resource_body(
    rtype: i8,
    name: &str,
    configs: &[(&str, Option<&str>)],
    validate_only: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(rtype);
    put_string(&mut body, name);
    body.put_i32(configs.len() as i32);
    for (k, v) in configs {
        put_string(&mut body, k);
        put_nullable_string(&mut body, *v);
    }
    body.put_u8(if validate_only { 1 } else { 0 });
    body
}

fn incremental_broker_body(
    name: &str,
    configs: &[(&str, i8, Option<&str>)],
    validate_only: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(RES_BROKER);
    put_string(&mut body, name);
    body.put_i32(configs.len() as i32);
    for (k, op, v) in configs {
        put_string(&mut body, k);
        body.put_i8(*op);
        put_nullable_string(&mut body, *v);
    }
    body.put_u8(if validate_only { 1 } else { 0 });
    body
}

/// Parse first resource result of DescribeConfigs v0 → (error_code, config count).
fn parse_describe_error(src: &mut impl Buf) -> (i16, i32) {
    let _throttle = src.get_i32();
    let n = src.get_i32();
    assert_eq!(n, 1);
    let code = src.get_i16();
    let _ = get_nullable_string(src).unwrap();
    let _rtype = src.get_i8();
    let _ = get_string(src).unwrap();
    let cn = src.get_i32();
    (code, cn)
}

/// Parse first resource result of AlterConfigs / IncrementalAlterConfigs v0.
fn parse_alter_error(src: &mut impl Buf) -> i16 {
    let _throttle = src.get_i32();
    let n = src.get_i32();
    assert_eq!(n, 1);
    let code = src.get_i16();
    let _ = get_nullable_string(src).unwrap();
    let _rtype = src.get_i8();
    let _ = get_string(src).unwrap();
    code
}

async fn setup(label: &str) -> (
    std::path::PathBuf,
    Arc<Broker>,
    String,
    tokio::task::JoinHandle<()>,
) {
    // Distinct labels + common::temp_dir seq keep parallel cases isolated.
    let dir = temp_dir("p103", label);
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    assert_eq!(broker.node_id(), 0, "single-node default node_id is 0");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    (dir, broker, addr, server)
}

#[tokio::test]
async fn describe_name_matching_node_id_succeeds() {
    let (dir, broker, addr, server) = setup("desc-match").await;
    let node = broker.node_id().to_string();
    let resp = rpc(
        &addr,
        encode_request(
            32,
            0,
            1,
            Some("c"),
            &describe_resource_body(RES_BROKER, &node),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1); // corr
    let (code, cn) = parse_describe_error(&mut src);
    assert_eq!(code, ERR_NONE);
    assert_eq!(cn, 6);
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_empty_name_succeeds() {
    let (dir, _broker, addr, server) = setup("desc-empty").await;
    let resp = rpc(
        &addr,
        encode_request(
            32,
            0,
            2,
            Some("c"),
            &describe_resource_body(RES_BROKER, ""),
        ),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    let (code, cn) = parse_describe_error(&mut src);
    assert_eq!(code, ERR_NONE);
    assert_eq!(cn, 6);
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_wrong_name_invalid_request() {
    let (dir, _broker, addr, server) = setup("desc-wrong").await;
    for bad in ["1", "999", "00", "broker-0", " 0"] {
        let resp = rpc(
            &addr,
            encode_request(
                32,
                0,
                3,
                Some("c"),
                &describe_resource_body(RES_BROKER, bad),
            ),
        )
        .await;
        let mut src = resp.freeze();
        src.advance(4);
        let (code, cn) = parse_describe_error(&mut src);
        assert_eq!(code, ERR_INVALID_REQUEST, "name={bad:?}");
        assert_eq!(cn, 0, "name={bad:?}");
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_name_matching_and_empty_succeed() {
    let (dir, broker, addr, server) = setup("alter-match").await;
    let node = broker.node_id().to_string();

    // Alter with matching node_id.
    let body = alter_resource_body(
        RES_BROKER,
        &node,
        &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, Some("111000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 10, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_NONE);
    assert_eq!(broker.transaction_max_timeout_ms(), 111_000);

    // Alter with empty name.
    let body = alter_resource_body(
        RES_BROKER,
        "",
        &[(KEY_SWEEP_INTERVAL_MS, Some("250"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 11, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_NONE);
    assert_eq!(broker.sweep_interval_ms(), 250);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_and_incremental_wrong_name_invalid_request() {
    let (dir, broker, addr, server) = setup("alter-wrong").await;
    let before = broker.transaction_max_timeout_ms();

    let body = alter_resource_body(
        RES_BROKER,
        "1",
        &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, Some("222000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 20, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_INVALID_REQUEST);
    assert_eq!(
        broker.transaction_max_timeout_ms(),
        before,
        "wrong name must not mutate"
    );

    let body = incremental_broker_body(
        "999",
        &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, 0, Some("333000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(44, 0, 21, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_INVALID_REQUEST);
    assert_eq!(broker.transaction_max_timeout_ms(), before);

    // Incremental with matching name still works.
    let body = incremental_broker_body(
        "0",
        &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, 0, Some("444000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(44, 0, 22, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_NONE);
    assert_eq!(broker.transaction_max_timeout_ms(), 444_000);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn topic_resources_unchanged() {
    let (dir, broker, addr, server) = setup("topic-ok").await;
    broker.create_topic("p103-topic", 1).unwrap();

    // Describe TOPIC still works.
    let resp = rpc(
        &addr,
        encode_request(
            32,
            0,
            30,
            Some("c"),
            &describe_resource_body(RES_TOPIC, "p103-topic"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    let _throttle = src.get_i32();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), ERR_NONE);
    let _ = get_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i8(), RES_TOPIC);
    assert_eq!(get_string(&mut src).unwrap(), "p103-topic");

    // Alter TOPIC retention still works.
    let body = alter_resource_body(
        RES_TOPIC,
        "p103-topic",
        &[("retention.ms", Some("3600000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 31, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_NONE);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn regression_alter_known_knob() {
    let (dir, broker, addr, server) = setup("alter-knob").await;
    let body = alter_resource_body(
        RES_BROKER,
        "0",
        &[(KEY_SWEEP_INTERVAL_MS, Some("77"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 40, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(parse_alter_error(&mut src), ERR_NONE);
    assert_eq!(broker.sweep_interval_ms(), 77);

    // Describe reflects the change.
    let resp = rpc(
        &addr,
        encode_request(
            32,
            0,
            41,
            Some("c"),
            &describe_resource_body(RES_BROKER, "0"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    let (code, cn) = parse_describe_error(&mut src);
    assert_eq!(code, ERR_NONE);
    assert_eq!(cn, 6);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
