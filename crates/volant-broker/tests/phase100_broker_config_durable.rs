//! Phase 100: durable dynamic BROKER config survives restart.

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
    KEY_TRANSACTION_MAX_TIMEOUT_MS, BROKER_CONFIG_DIR,
};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::kafka::fetch_session::{DEFAULT_IDLE_TIMEOUT_MS, DEFAULT_MAX_SESSIONS};
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn storage(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        ..StorageConfig::default()
    }
}

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

async fn describe_map(addr: &str, corr: i32) -> HashMap<String, String> {
    let resp = rpc(
        addr,
        encode_request(32, 0, corr, Some("c"), &describe_broker_body("0")),
    )
    .await;
    let mut src = resp.freeze();
    src.advance(4); // corr
    parse_describe_v0(&mut src)
}

/// AlterConfigs SET survives drop + reopen of same data_dir.
#[tokio::test]
async fn alter_survives_restart() {
    let dir = temp_dir("p100", "alter-restart");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let body = alter_broker_body(
            "0",
            &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, Some("222000"))],
            false,
        );
        let resp = rpc(&addr, encode_request(33, 0, 1, Some("admin"), &body)).await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i32(), 0); // throttle
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i16(), 0);
        assert_eq!(broker.transaction_max_timeout_ms(), 222_000);

        // Durable file written
        let path = dir.join(BROKER_CONFIG_DIR).join("state.json");
        assert!(path.exists(), "expected durable state.json");

        server.abort();
        // Drop broker explicitly by ending scope
    }

    // Reopen same data_dir
    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.transaction_max_timeout_ms(), 222_000);

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 2).await;
    assert_eq!(map[KEY_TRANSACTION_MAX_TIMEOUT_MS], "222000");

    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// IncrementalAlter SET of several knobs restores after restart.
#[tokio::test]
async fn incremental_multi_survives_restart() {
    let dir = temp_dir("p100", "inc-restart");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let body = incremental_broker_body(
            "0",
            &[
                (KEY_OPEN_TXN_TIMEOUT_MS, 0, Some("15000")),
                (KEY_PREPARED_TXN_TIMEOUT_MS, 0, Some("25000")),
                (KEY_FETCH_SESSION_IDLE_MS, 0, Some("9000")),
                (KEY_FETCH_SESSION_MAX, 0, Some("42")),
                (KEY_SWEEP_INTERVAL_MS, 0, Some("50")),
            ],
            false,
        );
        let resp = rpc(&addr, encode_request(44, 0, 10, Some("admin"), &body)).await;
        let mut src = resp.freeze();
        src.advance(4 + 4);
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i16(), 0);

        assert_eq!(broker.open_txn_timeout_ms(), 15_000);
        assert_eq!(broker.prepared_txn_timeout_ms(), 25_000);
        assert_eq!(broker.fetch_session_idle_ms(), 9_000);
        assert_eq!(broker.fetch_session_max(), 42);
        assert_eq!(broker.sweep_interval_ms(), 50);

        server.abort();
    }

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.open_txn_timeout_ms(), 15_000);
    assert_eq!(broker2.prepared_txn_timeout_ms(), 25_000);
    assert_eq!(broker2.fetch_session_idle_ms(), 9_000);
    assert_eq!(broker2.fetch_session_max(), 42);
    assert_eq!(broker2.sweep_interval_ms(), 50);

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 11).await;
    assert_eq!(map[KEY_OPEN_TXN_TIMEOUT_MS], "15000");
    assert_eq!(map[KEY_PREPARED_TXN_TIMEOUT_MS], "25000");
    assert_eq!(map[KEY_FETCH_SESSION_IDLE_MS], "9000");
    assert_eq!(map[KEY_FETCH_SESSION_MAX], "42");
    assert_eq!(map[KEY_SWEEP_INTERVAL_MS], "50");

    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// DELETE restores product default live and after restart (not prior alter).
#[tokio::test]
async fn delete_then_restart_product_default() {
    let dir = temp_dir("p100", "delete-restart");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        // SET then DELETE
        let body = alter_broker_body(
            "0",
            &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, Some("333000"))],
            false,
        );
        let resp = rpc(&addr, encode_request(33, 0, 20, Some("admin"), &body)).await;
        let mut src = resp.freeze();
        src.advance(4 + 4);
        assert_eq!(src.get_i16(), 0);
        assert_eq!(broker.transaction_max_timeout_ms(), 333_000);

        // Incremental DELETE
        let body2 =
            incremental_broker_body("0", &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, 1, None)], false);
        let resp2 = rpc(&addr, encode_request(44, 0, 21, Some("admin"), &body2)).await;
        let mut s2 = resp2.freeze();
        s2.advance(4 + 4);
        assert_eq!(s2.get_i32(), 1);
        assert_eq!(s2.get_i16(), 0);
        assert_eq!(
            broker.transaction_max_timeout_ms(),
            DEFAULT_TRANSACTION_MAX_TIMEOUT_MS
        );

        server.abort();
    }

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(
        broker2.transaction_max_timeout_ms(),
        DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
        "restart must not re-apply prior altered value after DELETE"
    );

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 22).await;
    assert_eq!(
        map[KEY_TRANSACTION_MAX_TIMEOUT_MS],
        DEFAULT_TRANSACTION_MAX_TIMEOUT_MS.to_string()
    );

    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// validate_only must not create or change the durable file.
#[tokio::test]
async fn validate_only_does_not_write_file() {
    let dir = temp_dir("p100", "val-only");
    let broker = Arc::new(Broker::new(storage(&dir)));
    let path = dir.join(BROKER_CONFIG_DIR).join("state.json");
    assert!(!path.exists());

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let body = incremental_broker_body(
        "0",
        &[(KEY_SWEEP_INTERVAL_MS, 0, Some("999"))],
        true, // validate_only
    );
    let resp = rpc(&addr, encode_request(44, 0, 30, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_ne!(broker.sweep_interval_ms(), 999);
    assert!(!path.exists(), "validate_only must not create state.json");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Empty AlterConfigs value = DELETE and durable product default after restart.
#[tokio::test]
async fn empty_alter_delete_survives_restart() {
    let dir = temp_dir("p100", "empty-alter");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let set = alter_broker_body("0", &[(KEY_SWEEP_INTERVAL_MS, Some("77"))], false);
        let r = rpc(&addr, encode_request(33, 0, 40, Some("c"), &set)).await;
        let mut s = r.freeze();
        s.advance(4 + 4);
        assert_eq!(s.get_i16(), 0);
        assert_eq!(broker.sweep_interval_ms(), 77);

        // Empty value = DELETE
        let del = alter_broker_body("0", &[(KEY_SWEEP_INTERVAL_MS, Some(""))], false);
        let r2 = rpc(&addr, encode_request(33, 0, 41, Some("c"), &del)).await;
        let mut s2 = r2.freeze();
        s2.advance(4 + 4);
        assert_eq!(s2.get_i16(), 0);
        assert_eq!(broker.sweep_interval_ms(), DEFAULT_SWEEP_INTERVAL_MS);

        server.abort();
    }

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.sweep_interval_ms(), DEFAULT_SWEEP_INTERVAL_MS);

    let _ = std::fs::remove_dir_all(&dir);
}

/// TOPIC configs still work; broker durable path does not break them.
#[tokio::test]
async fn topic_configs_still_work() {
    let dir = temp_dir("p100", "topic");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        broker.create_topic("orders", 1).unwrap();
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        // Alter broker + topic in same process
        let body = alter_broker_body(
            "0",
            &[(KEY_PREPARED_TXN_TIMEOUT_MS, Some("45000"))],
            false,
        );
        let resp = rpc(&addr, encode_request(33, 0, 50, Some("c"), &body)).await;
        let mut src = resp.freeze();
        src.advance(4 + 4);
        assert_eq!(src.get_i16(), 0);

        let mut abody = BytesMut::new();
        abody.put_i32(1);
        abody.put_i8(2); // TOPIC
        put_string(&mut abody, "orders");
        abody.put_i32(1);
        put_string(&mut abody, "retention.ms");
        put_nullable_string(&mut abody, Some("60000"));
        abody.put_u8(0);
        let aresp = rpc(&addr, encode_request(33, 0, 51, Some("c"), &abody)).await;
        let mut asrc = aresp.freeze();
        asrc.advance(4 + 4);
        assert_eq!(asrc.get_i32(), 1);
        assert_eq!(asrc.get_i16(), 0);

        server.abort();
    }

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.prepared_txn_timeout_ms(), 45_000);
    // Topic config durable separately
    let (_id, _pc, cfg) = broker2.describe_configs("orders").unwrap();
    assert_eq!(cfg.retention_ms, Some(60_000));

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 52).await;
    assert_eq!(map[KEY_PREPARED_TXN_TIMEOUT_MS], "45000");
    // Product defaults for untouched keys still present
    if std::env::var("VOLANT_OPEN_TXN_TIMEOUT_MS").is_err() {
        assert_eq!(
            map[KEY_OPEN_TXN_TIMEOUT_MS],
            DEFAULT_OPEN_TXN_TIMEOUT_MS.to_string()
        );
    }
    if std::env::var("VOLANT_FETCH_SESSION_IDLE_MS").is_err() {
        assert_eq!(
            map[KEY_FETCH_SESSION_IDLE_MS],
            DEFAULT_IDLE_TIMEOUT_MS.to_string()
        );
    }
    if std::env::var("VOLANT_FETCH_SESSION_MAX").is_err() {
        assert_eq!(
            map[KEY_FETCH_SESSION_MAX],
            DEFAULT_MAX_SESSIONS.to_string()
        );
    }

    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Direct setters do not auto-persist (Alter path only).
#[tokio::test]
async fn setters_do_not_auto_persist() {
    let dir = temp_dir("p100", "setters");
    {
        let broker = Broker::new(storage(&dir));
        broker.set_sweep_interval_ms(123);
        assert_eq!(broker.sweep_interval_ms(), 123);
        // No alter → no durable file required
        let path = dir.join(BROKER_CONFIG_DIR).join("state.json");
        assert!(!path.exists());
    }
    let broker2 = Broker::new(storage(&dir));
    // Env or product default — not 123
    assert_ne!(broker2.sweep_interval_ms(), 123);
    let _ = std::fs::remove_dir_all(&dir);
}

// Silence unused import warnings when env overrides product defaults.
#[allow(dead_code)]
fn _product_defaults_touch() {
    let _ = (
        DEFAULT_OPEN_TXN_TIMEOUT_MS,
        DEFAULT_PREPARED_TXN_TIMEOUT_MS,
        DEFAULT_SWEEP_INTERVAL_MS,
        DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
        DEFAULT_IDLE_TIMEOUT_MS,
        DEFAULT_MAX_SESSIONS,
    );
}
