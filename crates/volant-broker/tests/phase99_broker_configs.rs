//! Phase 99: Describe/AlterConfigs for BROKER txn/session/sweep knobs.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::broker_config::{
    DEFAULT_OPEN_TXN_TIMEOUT_MS, DEFAULT_PREPARED_TXN_TIMEOUT_MS, DEFAULT_SWEEP_INTERVAL_MS,
    DEFAULT_TRANSACTION_MAX_TIMEOUT_MS, KEY_FETCH_SESSION_IDLE_MS, KEY_FETCH_SESSION_MAX,
    KEY_OPEN_TXN_TIMEOUT_MS, KEY_PREPARED_TXN_TIMEOUT_MS, KEY_SWEEP_INTERVAL_MS,
    KEY_TRANSACTION_MAX_TIMEOUT_MS,
};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::kafka::fetch_session::{DEFAULT_IDLE_TIMEOUT_MS, DEFAULT_MAX_SESSIONS};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// DescribeConfigs v0 body for one BROKER resource (all keys).
fn describe_broker_body(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(4); // BROKER
    put_string(&mut body, name);
    body.put_i32(-1); // all keys
    body
}

/// Parse DescribeConfigs v0 response → map of config name → value.
fn parse_describe_v0(src: &mut impl Buf) -> HashMap<String, String> {
    let _throttle = src.get_i32();
    let n = src.get_i32();
    assert_eq!(n, 1);
    assert_eq!(src.get_i16(), 0); // error
    let _ = get_nullable_string(src).unwrap();
    assert_eq!(src.get_i8(), 4); // BROKER
    let _ = get_string(src).unwrap();
    let cn = src.get_i32();
    let mut map = HashMap::new();
    for _ in 0..cn {
        let k = get_string(src).unwrap();
        let v = get_nullable_string(src).unwrap().unwrap_or_default();
        let _ro = src.get_u8();
        let _def = src.get_u8();
        let _sens = src.get_u8();
        map.insert(k, v);
    }
    map
}

fn incremental_broker_body(
    name: &str,
    configs: &[(&str, i8, Option<&str>)],
    validate_only: bool,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(4); // BROKER
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

fn alter_broker_body(name: &str, configs: &[(&str, Option<&str>)], validate_only: bool) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(4); // BROKER
    put_string(&mut body, name);
    body.put_i32(configs.len() as i32);
    for (k, v) in configs {
        put_string(&mut body, k);
        put_nullable_string(&mut body, *v);
    }
    body.put_u8(if validate_only { 1 } else { 0 });
    body
}

#[tokio::test]
async fn describe_broker_defaults() {
    let dir = temp_dir("p99", "desc");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(32, 0, 1, Some("c"), &describe_broker_body("0")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1); // corr
    let map = parse_describe_v0(&mut src);

    // Live getters (env may override product defaults at construction).
    assert_eq!(
        map.get(KEY_TRANSACTION_MAX_TIMEOUT_MS),
        Some(&broker.transaction_max_timeout_ms().to_string())
    );
    assert_eq!(
        map.get(KEY_OPEN_TXN_TIMEOUT_MS),
        Some(&broker.open_txn_timeout_ms().to_string())
    );
    assert_eq!(
        map.get(KEY_PREPARED_TXN_TIMEOUT_MS),
        Some(&broker.prepared_txn_timeout_ms().to_string())
    );
    assert_eq!(
        map.get(KEY_FETCH_SESSION_IDLE_MS),
        Some(&broker.fetch_session_idle_ms().to_string())
    );
    assert_eq!(
        map.get(KEY_FETCH_SESSION_MAX),
        Some(&broker.fetch_session_max().to_string())
    );
    assert_eq!(
        map.get(KEY_SWEEP_INTERVAL_MS),
        Some(&broker.sweep_interval_ms().to_string())
    );
    assert_eq!(map.len(), 7); // six Phase 99 knobs + Phase 128 registry TTL

    // When no env overrides, product defaults match getters.
    if std::env::var("VOLANT_TRANSACTION_MAX_TIMEOUT_MS").is_err()
        && std::env::var("VOLANT_OPEN_TXN_TIMEOUT_MS").is_err()
        && std::env::var("VOLANT_PREPARED_TXN_TIMEOUT_MS").is_err()
        && std::env::var("VOLANT_FETCH_SESSION_IDLE_MS").is_err()
        && std::env::var("VOLANT_FETCH_SESSION_MAX").is_err()
        && std::env::var("VOLANT_SWEEP_INTERVAL_MS").is_err()
    {
        assert_eq!(
            map[KEY_TRANSACTION_MAX_TIMEOUT_MS],
            DEFAULT_TRANSACTION_MAX_TIMEOUT_MS.to_string()
        );
        assert_eq!(
            map[KEY_OPEN_TXN_TIMEOUT_MS],
            DEFAULT_OPEN_TXN_TIMEOUT_MS.to_string()
        );
        assert_eq!(
            map[KEY_PREPARED_TXN_TIMEOUT_MS],
            DEFAULT_PREPARED_TXN_TIMEOUT_MS.to_string()
        );
        assert_eq!(
            map[KEY_FETCH_SESSION_IDLE_MS],
            DEFAULT_IDLE_TIMEOUT_MS.to_string()
        );
        assert_eq!(
            map[KEY_FETCH_SESSION_MAX],
            DEFAULT_MAX_SESSIONS.to_string()
        );
        assert_eq!(
            map[KEY_SWEEP_INTERVAL_MS],
            DEFAULT_SWEEP_INTERVAL_MS.to_string()
        );
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn setter_then_describe_reflects() {
    let dir = temp_dir("p99", "setter");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_transaction_max_timeout_ms(123_456);
    broker.set_open_txn_timeout_ms(7_000);
    broker.set_prepared_txn_timeout_ms(8_000);
    broker.set_fetch_session_idle_ms(9_000);
    broker.set_fetch_session_max(42);
    broker.set_sweep_interval_ms(250);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request(32, 0, 2, Some("c"), &describe_broker_body("0")),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4);
    let map = parse_describe_v0(&mut src);
    assert_eq!(map[KEY_TRANSACTION_MAX_TIMEOUT_MS], "123456");
    assert_eq!(map[KEY_OPEN_TXN_TIMEOUT_MS], "7000");
    assert_eq!(map[KEY_PREPARED_TXN_TIMEOUT_MS], "8000");
    assert_eq!(map[KEY_FETCH_SESSION_IDLE_MS], "9000");
    assert_eq!(map[KEY_FETCH_SESSION_MAX], "42");
    assert_eq!(map[KEY_SWEEP_INTERVAL_MS], "250");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn incremental_set_and_delete() {
    let dir = temp_dir("p99", "inc");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // SET transaction.max.timeout.ms
    let body = incremental_broker_body(
        "0",
        &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, 0, Some("111000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(44, 0, 10, Some("admin"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let _ = get_nullable_string(&mut src).unwrap();
    assert_eq!(src.get_i8(), 4);
    assert_eq!(get_string(&mut src).unwrap(), "0");
    assert_eq!(broker.transaction_max_timeout_ms(), 111_000);

    let dresp = rpc(
        &addr,
        encode_request(32, 0, 11, Some("admin"), &describe_broker_body("0")),
    )
    .await;
    let mut ds = dresp.freeze();
    ds.advance(4);
    let map = parse_describe_v0(&mut ds);
    assert_eq!(map[KEY_TRANSACTION_MAX_TIMEOUT_MS], "111000");

    // DELETE → product default
    let body2 = incremental_broker_body("0", &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, 1, None)], false);
    let resp2 = rpc(&addr, encode_request(44, 0, 12, Some("admin"), &body2)).await;
    let mut s2 = resp2.freeze();
    s2.advance(4 + 4);
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(s2.get_i16(), 0);
    assert_eq!(
        broker.transaction_max_timeout_ms(),
        DEFAULT_TRANSACTION_MAX_TIMEOUT_MS
    );

    // SET sweep + open via incremental
    let body3 = incremental_broker_body(
        "0",
        &[
            (KEY_SWEEP_INTERVAL_MS, 0, Some("50")),
            (KEY_OPEN_TXN_TIMEOUT_MS, 0, Some("15000")),
        ],
        false,
    );
    let resp3 = rpc(&addr, encode_request(44, 0, 13, Some("admin"), &body3)).await;
    let mut s3 = resp3.freeze();
    s3.advance(4 + 4);
    assert_eq!(s3.get_i16(), 0);
    assert_eq!(broker.sweep_interval_ms(), 50);
    assert_eq!(broker.open_txn_timeout_ms(), 15_000);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_configs_set_and_unknown_key() {
    let dir = temp_dir("p99", "alter");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = alter_broker_body(
        "0",
        &[(KEY_PREPARED_TXN_TIMEOUT_MS, Some("45000"))],
        false,
    );
    let resp = rpc(&addr, encode_request(33, 0, 20, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(broker.prepared_txn_timeout_ms(), 45_000);

    // Unknown key
    let body2 = alter_broker_body("0", &[("log.retention.ms", Some("1"))], false);
    let resp2 = rpc(&addr, encode_request(33, 0, 21, Some("c"), &body2)).await;
    let mut s2 = resp2.freeze();
    s2.advance(4 + 4);
    assert_eq!(s2.get_i32(), 1);
    assert_eq!(s2.get_i16(), 40); // INVALID_CONFIG

    // Unsupported resource type
    let mut body3 = BytesMut::new();
    body3.put_i32(1);
    body3.put_i8(8); // BROKER_LOGGER
    put_string(&mut body3, "0");
    body3.put_i32(1);
    put_string(&mut body3, KEY_SWEEP_INTERVAL_MS);
    put_nullable_string(&mut body3, Some("1"));
    body3.put_u8(0);
    let resp3 = rpc(&addr, encode_request(33, 0, 22, Some("c"), &body3)).await;
    let mut s3 = resp3.freeze();
    s3.advance(4 + 4);
    assert_eq!(s3.get_i32(), 1);
    assert_eq!(s3.get_i16(), 42); // INVALID_REQUEST

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn topic_configs_still_work() {
    let dir = temp_dir("p99", "topic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Alter topic retention
    let mut abody = BytesMut::new();
    abody.put_i32(1);
    abody.put_i8(2); // TOPIC
    put_string(&mut abody, "orders");
    abody.put_i32(1);
    put_string(&mut abody, "retention.ms");
    put_nullable_string(&mut abody, Some("60000"));
    abody.put_u8(0);
    let aresp = rpc(&addr, encode_request(33, 0, 30, Some("c"), &abody)).await;
    let mut asrc = aresp.freeze();
    asrc.advance(4 + 4);
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(asrc.get_i16(), 0);

    // Describe topic
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    dbody.put_i8(2);
    put_string(&mut dbody, "orders");
    dbody.put_i32(1);
    put_string(&mut dbody, "retention.ms");
    let dresp = rpc(&addr, encode_request(32, 0, 31, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 31);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    let _ = get_nullable_string(&mut ds).unwrap();
    assert_eq!(ds.get_i8(), 2);
    assert_eq!(get_string(&mut ds).unwrap(), "orders");
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), "retention.ms");
    assert_eq!(
        get_nullable_string(&mut ds).unwrap().as_deref(),
        Some("60000")
    );

    // Broker describe still works alongside
    let bresp = rpc(
        &addr,
        encode_request(32, 0, 32, Some("c"), &describe_broker_body("0")),
    )
    .await;
    let mut bs = bresp.freeze();
    bs.advance(4);
    let map = parse_describe_v0(&mut bs);
    assert!(map.contains_key(KEY_TRANSACTION_MAX_TIMEOUT_MS));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn validate_only_does_not_persist_broker() {
    let dir = temp_dir("p99", "val");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let before = broker.sweep_interval_ms();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let body = incremental_broker_body(
        "0",
        &[(KEY_SWEEP_INTERVAL_MS, 0, Some("999"))],
        true, // validate_only
    );
    let resp = rpc(&addr, encode_request(44, 0, 40, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(broker.sweep_interval_ms(), before);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
