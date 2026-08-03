//! Phase 128: BROKER Describe/Alter for txn coordinator registry TTL.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::{
    Broker, DEFAULT_TXN_COORDINATOR_TTL_MS, KEY_TXN_COORDINATOR_TTL_MS,
};
use volant_storage::StorageConfig;

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

#[tokio::test]
async fn describe_includes_registry_ttl_default() {
    let dir = temp_dir("p128", "desc");
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
    assert_eq!(src.get_i32(), 1);
    let map = parse_describe_v0(&mut src);
    assert!(map.contains_key(KEY_TXN_COORDINATOR_TTL_MS));
    assert_eq!(
        map[KEY_TXN_COORDINATOR_TTL_MS],
        DEFAULT_TXN_COORDINATOR_TTL_MS.to_string()
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_ttl_live_and_sparse_durable() {
    let dir = temp_dir("p128", "alter");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request(
            33,
            0,
            2,
            Some("c"),
            &alter_broker_body(
                "0",
                &[(KEY_TXN_COORDINATOR_TTL_MS, Some("120000"))],
                false,
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    let _throttle = src.get_i32();
    assert_eq!(src.get_i32(), 1); // resources
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(broker.txn_coordinator_ttl_ms(), 120_000);

    // Restart: sparse durable restores.
    drop(broker);
    server.abort();
    let broker2 = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    assert_eq!(broker2.txn_coordinator_ttl_ms(), 120_000);

    // DELETE → product default live.
    broker2
        .alter_broker_configs(&[(KEY_TXN_COORDINATOR_TTL_MS.into(), "".into())])
        .unwrap();
    assert_eq!(
        broker2.txn_coordinator_ttl_ms(),
        DEFAULT_TXN_COORDINATOR_TTL_MS
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn alter_disables_gc_with_zero() {
    let dir = temp_dir("p128", "zero");
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    broker
        .alter_broker_configs(&[(KEY_TXN_COORDINATOR_TTL_MS.into(), "0".into())])
        .unwrap();
    assert_eq!(broker.txn_coordinator_ttl_ms(), 0);
    broker.note_txn_coordinator("x", 1, 1);
    broker
        .txn_coordinator_registry()
        .test_set_id_last_ms("x", 1);
    broker
        .txn_coordinator_registry()
        .test_set_pid_last_ms(1, 1);
    assert_eq!(broker.expire_txn_coordinator_registry(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}
