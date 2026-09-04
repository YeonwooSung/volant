//! v0.268: Kafka ControllerRegistration key 70 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_nullable_string, put_compact_array_len,
    put_compact_string, put_empty_tag_buffer, put_uuid, skip_tag_buffer,
};
use volant_broker::Broker;

fn register_v0(
    controller_id: i32,
    incarnation: &[u8; 16],
    zk_migration_ready: bool,
    listeners: &[(&str, &str, u16, i16)],
    features: &[(&str, i16, i16)],
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(controller_id);
    put_uuid(&mut body, incarnation);
    body.put_u8(u8::from(zk_migration_ready));
    put_compact_array_len(&mut body, listeners.len());
    for (name, host, port, security) in listeners {
        put_compact_string(&mut body, name);
        put_compact_string(&mut body, host);
        body.put_u16(*port);
        body.put_i16(*security);
        put_empty_tag_buffer(&mut body);
    }
    put_compact_array_len(&mut body, features.len());
    for (name, min_v, max_v) in features {
        put_compact_string(&mut body, name);
        body.put_i16(*min_v);
        body.put_i16(*max_v);
        put_empty_tag_buffer(&mut body);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn overlay_ids(b: &Broker) -> Vec<u32> {
    b.list_membership().brokers.iter().map(|x| x.id).collect()
}

fn membership_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("cluster").join("membership.json")
}

#[tokio::test]
async fn api_versions_lists_controller_registration_70() {
    let (_dir, broker) = broker_temp("v268", "api");
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
    assert!(found.len() >= 65);
    assert_eq!(found.get(&70), Some(&(0, 0)));
    assert_eq!(found.get(&69), Some(&(0, 0)));
    assert_eq!(found.get(&71), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn controller_registration_v0_is_42_membership_unchanged() {
    let (dir, broker) = broker_temp("v268", "reg");
    let before = overlay_ids(&broker);
    assert!(
        !membership_file(&dir).exists(),
        "single-node must not start with membership.json"
    );

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut incarnation = [0u8; 16];
    incarnation[15] = 0x68;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            70,
            0,
            10,
            Some("admin"),
            &register_v0(
                1,
                &incarnation,
                false,
                &[("CONTROLLER", "127.0.0.1", 19094, 0)],
                &[("metadata.version", 1, 20)],
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KRaft controller registration")
    );
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(overlay_ids(&broker), before);
    assert!(
        !membership_file(&dir).exists(),
        "ControllerRegistration must not create membership.json"
    );
    server.abort();

    // Existing overlay brokers stay put; a new id is not added.
    let base = unique_dir("v268", "exist");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19681, 19682]);
    let clustered =
        Arc::new(Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap());
    clustered
        .add_broker(3, "127.0.0.1".into(), 19683, None)
        .unwrap();
    let before_cluster = overlay_ids(&clustered);
    assert!(before_cluster.contains(&3));

    let (addr, server) = boot_kafka(Arc::clone(&clustered)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            70,
            0,
            11,
            Some("admin"),
            &register_v0(
                4,
                &incarnation,
                true,
                &[("CONTROLLER", "127.0.0.1", 19684, 0)],
                &[],
            ),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 42);
    assert_eq!(
        get_compact_nullable_string(&mut src).unwrap().as_deref(),
        Some("not KRaft controller registration")
    );

    let after = overlay_ids(&clustered);
    assert_eq!(after, before_cluster);
    assert!(!after.contains(&4));
    server.abort();
}

#[tokio::test]
async fn controller_registration_v1_unsupported() {
    let (_dir, broker) = broker_temp("v268", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(70, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
