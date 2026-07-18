//! Phase 102: sparse durable BROKER config (only altered keys; env re-applies after DELETE).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::broker_config::{
    BrokerConfigStore, DEFAULT_SWEEP_INTERVAL_MS, DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
    KEY_FETCH_SESSION_IDLE_MS, KEY_FETCH_SESSION_MAX, KEY_OPEN_TXN_TIMEOUT_MS,
    KEY_PREPARED_TXN_TIMEOUT_MS, KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS,
    BROKER_CONFIG_DIR,
};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// Serialize tests that mutate process-global env vars.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn storage(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        ..StorageConfig::default()
    }
}

fn describe_broker_body(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(4); // BROKER
    put_string(&mut body, name);
    body.put_i32(-1); // all keys
    body
}

fn parse_describe_v0(src: &mut impl Buf) -> HashMap<String, String> {
    let _throttle = src.get_i32();
    let n = src.get_i32();
    assert_eq!(n, 1);
    assert_eq!(src.get_i16(), 0);
    let _ = get_nullable_string(src).unwrap();
    assert_eq!(src.get_i8(), 4);
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
    body.put_i8(4);
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
    body.put_i8(4);
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
    src.advance(4);
    parse_describe_v0(&mut src)
}

fn load_file_keys(dir: &std::path::Path) -> Option<HashMap<String, u64>> {
    let store = BrokerConfigStore::open(dir).unwrap();
    store.load().unwrap().map(|f| f.configs)
}

/// Alter one key while env overrides another → restart keeps alter from file
/// and env key from env (not frozen by full snapshot).
#[tokio::test]
async fn alter_one_key_leaves_env_key_unfrozen() {
    let _guard = env_lock().lock().unwrap();
    let env_key = "VOLANT_OPEN_TXN_TIMEOUT_MS";
    let prev = std::env::var(env_key).ok();
    std::env::set_var(env_key, "77777");

    let dir = temp_dir("p102", "sparse-env");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        assert_eq!(broker.open_txn_timeout_ms(), 77_777);
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let body = alter_broker_body(
            "0",
            &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, Some("222000"))],
            false,
        );
        let resp = rpc(&addr, encode_request(33, 0, 1, Some("admin"), &body)).await;
        let mut src = resp.freeze();
        src.advance(4 + 4);
        assert_eq!(src.get_i16(), 0);
        assert_eq!(broker.transaction_max_timeout_ms(), 222_000);
        assert_eq!(broker.open_txn_timeout_ms(), 77_777);

        // Sparse: only the altered key is on disk
        let keys = load_file_keys(&dir).expect("state.json after SET");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.get(KEY_TRANSACTION_MAX_TIMEOUT_MS), Some(&222_000));
        assert!(!keys.contains_key(KEY_OPEN_TXN_TIMEOUT_MS));

        server.abort();
    }

    // Restart with same env still set
    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(
        broker2.transaction_max_timeout_ms(),
        222_000,
        "altered key restored from sparse file"
    );
    assert_eq!(
        broker2.open_txn_timeout_ms(),
        77_777,
        "untouched env key must not be frozen by alter of another key"
    );

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 2).await;
    assert_eq!(map[KEY_TRANSACTION_MAX_TIMEOUT_MS], "222000");
    assert_eq!(map[KEY_OPEN_TXN_TIMEOUT_MS], "77777");
    server2.abort();

    restore_env(env_key, prev);
    let _ = std::fs::remove_dir_all(&dir);
}

/// DELETE altered key drops it from file; restart with env set applies env.
#[tokio::test]
async fn delete_drops_key_env_reapplies_on_restart() {
    let _guard = env_lock().lock().unwrap();
    let env_key = "VOLANT_SWEEP_INTERVAL_MS";
    let prev = std::env::var(env_key).ok();
    // Start without env so product default is baseline, then set after DELETE.
    std::env::remove_var(env_key);

    let dir = temp_dir("p102", "delete-env");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let set = alter_broker_body("0", &[(KEY_SWEEP_INTERVAL_MS, Some("77"))], false);
        let r = rpc(&addr, encode_request(33, 0, 10, Some("c"), &set)).await;
        let mut s = r.freeze();
        s.advance(4 + 4);
        assert_eq!(s.get_i16(), 0);
        assert_eq!(broker.sweep_interval_ms(), 77);

        let keys = load_file_keys(&dir).unwrap();
        assert_eq!(keys.get(KEY_SWEEP_INTERVAL_MS), Some(&77));

        // Incremental DELETE
        let del = incremental_broker_body("0", &[(KEY_SWEEP_INTERVAL_MS, 1, None)], false);
        let r2 = rpc(&addr, encode_request(44, 0, 11, Some("c"), &del)).await;
        let mut s2 = r2.freeze();
        s2.advance(4 + 4);
        assert_eq!(s2.get_i32(), 1);
        assert_eq!(s2.get_i16(), 0);
        assert_eq!(broker.sweep_interval_ms(), DEFAULT_SWEEP_INTERVAL_MS);

        // File drops key (empty overlay → file removed)
        assert!(
            load_file_keys(&dir).is_none(),
            "DELETE must remove key; empty overlay clears file"
        );
        let path = dir.join(BROKER_CONFIG_DIR).join("state.json");
        assert!(!path.exists());

        server.abort();
    }

    // Env set only for restart
    std::env::set_var(env_key, "333");
    let broker2 = Broker::new(storage(&dir));
    assert_eq!(
        broker2.sweep_interval_ms(),
        333,
        "after DELETE, env must re-apply on restart"
    );

    restore_env(env_key, prev);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Multi-key SET still restores those keys after restart (sparse multi).
#[tokio::test]
async fn multi_key_set_survives_restart() {
    let dir = temp_dir("p102", "multi");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let body = incremental_broker_body(
            "0",
            &[
                (KEY_OPEN_TXN_TIMEOUT_MS, 0, Some("15000")),
                (KEY_PREPARED_TXN_TIMEOUT_MS, 0, Some("25000")),
                (KEY_SWEEP_INTERVAL_MS, 0, Some("50")),
            ],
            false,
        );
        let resp = rpc(&addr, encode_request(44, 0, 20, Some("admin"), &body)).await;
        let mut src = resp.freeze();
        src.advance(4 + 4);
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i16(), 0);

        let keys = load_file_keys(&dir).unwrap();
        assert_eq!(keys.len(), 3, "sparse file has only the three SET keys");
        assert!(!keys.contains_key(KEY_TRANSACTION_MAX_TIMEOUT_MS));
        assert!(!keys.contains_key(KEY_FETCH_SESSION_IDLE_MS));
        assert!(!keys.contains_key(KEY_FETCH_SESSION_MAX));

        server.abort();
    }

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.open_txn_timeout_ms(), 15_000);
    assert_eq!(broker2.prepared_txn_timeout_ms(), 25_000);
    assert_eq!(broker2.sweep_interval_ms(), 50);
    // Untouched keys stay product/env (not written as product defaults)
    if std::env::var("VOLANT_TRANSACTION_MAX_TIMEOUT_MS").is_err() {
        assert_eq!(
            broker2.transaction_max_timeout_ms(),
            DEFAULT_TRANSACTION_MAX_TIMEOUT_MS
        );
    }

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 21).await;
    assert_eq!(map[KEY_OPEN_TXN_TIMEOUT_MS], "15000");
    assert_eq!(map[KEY_PREPARED_TXN_TIMEOUT_MS], "25000");
    assert_eq!(map[KEY_SWEEP_INTERVAL_MS], "50");
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// validate_only must not create or change the durable file.
#[tokio::test]
async fn validate_only_does_not_write_file() {
    let dir = temp_dir("p102", "val-only");
    let broker = Arc::new(Broker::new(storage(&dir)));
    let path = dir.join(BROKER_CONFIG_DIR).join("state.json");
    assert!(!path.exists());

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let body = incremental_broker_body(
        "0",
        &[(KEY_SWEEP_INTERVAL_MS, 0, Some("999"))],
        true,
    );
    let resp = rpc(&addr, encode_request(44, 0, 30, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_ne!(broker.sweep_interval_ms(), 999);
    assert!(!path.exists());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Phase 100 regression: single-key alter survives restart.
#[tokio::test]
async fn single_key_alter_survives_restart() {
    let dir = temp_dir("p102", "alter-restart");
    {
        let broker = Arc::new(Broker::new(storage(&dir)));
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

        let body = alter_broker_body(
            "0",
            &[(KEY_TRANSACTION_MAX_TIMEOUT_MS, Some("222000"))],
            false,
        );
        let resp = rpc(&addr, encode_request(33, 0, 40, Some("admin"), &body)).await;
        let mut src = resp.freeze();
        src.advance(4 + 4);
        assert_eq!(src.get_i16(), 0);
        assert_eq!(broker.transaction_max_timeout_ms(), 222_000);
        assert!(dir.join(BROKER_CONFIG_DIR).join("state.json").exists());

        server.abort();
    }

    let broker2 = Arc::new(Broker::new(storage(&dir)));
    assert_eq!(broker2.transaction_max_timeout_ms(), 222_000);

    let (addr2, server2) = boot_kafka(Arc::clone(&broker2)).await;
    let map = describe_map(&addr2, 41).await;
    assert_eq!(map[KEY_TRANSACTION_MAX_TIMEOUT_MS], "222000");
    server2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Direct setters still do not auto-persist.
#[tokio::test]
async fn setters_do_not_auto_persist() {
    let dir = temp_dir("p102", "setters");
    {
        let broker = Broker::new(storage(&dir));
        broker.set_sweep_interval_ms(123);
        assert_eq!(broker.sweep_interval_ms(), 123);
        assert!(!dir.join(BROKER_CONFIG_DIR).join("state.json").exists());
    }
    let broker2 = Broker::new(storage(&dir));
    assert_ne!(broker2.sweep_interval_ms(), 123);
    let _ = std::fs::remove_dir_all(&dir);
}

fn restore_env(key: &str, prev: Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}
