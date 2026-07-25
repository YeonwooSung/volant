//! Phase 113 PR3: BROKER config fan-out (controller → peers).

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::broker_config::{
    DEFAULT_SWEEP_INTERVAL_MS, KEY_SWEEP_INTERVAL_MS, KEY_TRANSACTION_MAX_TIMEOUT_MS,
};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::{
    fanout_cluster_broker_config, serve_listener, start_background_tasks, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_storage::StorageConfig;

const ERR_NONE: i16 = 0;
const ERR_NOT_CONTROLLER: i16 = 41;
const RES_BROKER: i8 = 4;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p113cfg-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn cluster_config(ports: [u16; 3]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 5000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=3)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: ports[(id - 1) as usize],
                rack: None,
            })
            .collect(),
    }
}

fn alter_broker_body(name: &str, key: &str, value: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(RES_BROKER);
    put_string(&mut body, name);
    body.put_i32(1);
    put_string(&mut body, key);
    put_nullable_string(&mut body, Some(value));
    body.put_u8(0); // validate_only = false
    body
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_alter_fans_out_to_peers() {
    let base = unique_dir("fanout");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    assert!(b1.is_controller());

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];
    for (l, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(l, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Touch membership so all stay live for fan-out peer selection.
    for b in [&b1, &b2, &b3] {
        let _ = b.live_brokers();
    }

    let entries = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "77".to_string())];
    let gen = b1
        .alter_broker_configs(&entries)
        .expect("controller alter")
        .expect("cluster generation");
    assert_eq!(gen, 1);
    assert_eq!(b1.sweep_interval_ms(), 77);
    assert_eq!(b1.config_generation(), 1);
    assert_eq!(b1.applied_config_generation(), 1);

    fanout_cluster_broker_config(&b1, gen, &entries).await;

    // Peers apply live knobs.
    for _ in 0..40 {
        if b2.sweep_interval_ms() == 77 && b3.sweep_interval_ms() == 77 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(b2.sweep_interval_ms(), 77, "peer2 should receive config");
    assert_eq!(b3.sweep_interval_ms(), 77, "peer3 should receive config");
    assert_eq!(b2.applied_config_generation(), 1);
    assert_eq!(b3.applied_config_generation(), 1);

    // Stale generation ignored.
    let (code, applied) =
        b2.handle_cluster_broker_config(1, &[(KEY_SWEEP_INTERVAL_MS.into(), "1".into())]);
    assert_eq!(code, 0);
    assert_eq!(applied, 1);
    assert_eq!(b2.sweep_interval_ms(), 77, "stale push must not overwrite");

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_controller_alter_rejected() {
    let base = unique_dir("not-ctrl");
    let _g = Guard(base.clone());
    let cfg = cluster_config([19001, 19002, 19003]);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", 19000 + id as u16);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    assert!(b1.is_controller());
    assert!(!b2.is_controller());

    let err = b2
        .alter_broker_configs(&[(KEY_SWEEP_INTERVAL_MS.into(), "50".into())])
        .expect_err("non-controller must fail");
    assert!(
        err.to_string().contains("not controller"),
        "got: {err}"
    );
    assert_eq!(b2.sweep_interval_ms(), DEFAULT_SWEEP_INTERVAL_MS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_not_controller_and_controller_fanout() {
    let base = unique_dir("kafka");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];
    for (l, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(l, b).await;
        });
    }
    // Kafka listeners on each broker.
    let (k1, _h1) = boot_kafka(Arc::clone(&b1)).await;
    let (k2, _h2) = boot_kafka(Arc::clone(&b2)).await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Non-controller Kafka Alter → 41.
    let body = alter_broker_body("", KEY_SWEEP_INTERVAL_MS, "88");
    // ApiKey AlterConfigs = 33
    let req = encode_request(33, 0, 9, Some("c"), &body);
    let resp = rpc(&k2, req).await;
    let mut src = resp.freeze();
    src.advance(4); // correlation id
    let code = parse_alter_error(&mut src);
    assert_eq!(code, ERR_NOT_CONTROLLER, "expected NOT_CONTROLLER on peer");

    // Controller Alter → success + fan-out.
    let body = alter_broker_body("1", KEY_SWEEP_INTERVAL_MS, "88");
    let req = encode_request(33, 0, 10, Some("c"), &body);
    let resp = rpc(&k1, req).await;
    let mut src = resp.freeze();
    src.advance(4);
    let code = parse_alter_error(&mut src);
    assert_eq!(code, ERR_NONE);
    assert_eq!(b1.sweep_interval_ms(), 88);

    for _ in 0..60 {
        if b2.sweep_interval_ms() == 88 && b3.sweep_interval_ms() == 88 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(b2.sweep_interval_ms(), 88);
    assert_eq!(b3.sweep_interval_ms(), 88);

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_restart_keeps_sparse_config() {
    let base = unique_dir("restart");
    let _g = Guard(base.clone());
    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let cfg = ClusterConfig {
        default_replication_factor: 2,
        min_insync_replicas: 1,
        session_timeout_ms: 5000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: p1,
                rack: None,
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: p2,
                rack: None,
            },
        ],
    };

    let storage1 = StorageConfig {
        data_dir: base.join("node-1"),
        ..StorageConfig::default()
    };
    let storage2 = StorageConfig {
        data_dir: base.join("node-2"),
        ..StorageConfig::default()
    };
    let b1 = {
        let b = Broker::with_cluster(storage1, 1, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };
    let b2 = {
        let b = Broker::with_cluster(storage2, 2, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", p2);
        Arc::new(b)
    };

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
    ];
    for (l, b) in [(l1, &b1), (l2, &b2)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(l, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let entries = vec![(
        KEY_TRANSACTION_MAX_TIMEOUT_MS.to_string(),
        "111000".to_string(),
    )];
    let gen = b1.alter_broker_configs(&entries).unwrap().unwrap();
    fanout_cluster_broker_config(&b1, gen, &entries).await;
    assert_eq!(b2.transaction_max_timeout_ms(), 111000);

    // Drop b2 and reload from disk.
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
    drop(b2);

    let b2b = {
        let storage = StorageConfig {
            data_dir: base.join("node-2"),
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, 2, cfg).unwrap();
        Arc::new(b)
    };
    assert_eq!(
        b2b.transaction_max_timeout_ms(),
        111000,
        "sparse durable file must restore on peer restart"
    );
}

#[test]
fn single_node_alter_unchanged() {
    let dir = temp_dir("p113", "single-cfg");
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    });
    let r = broker
        .alter_broker_configs(&[(KEY_SWEEP_INTERVAL_MS.into(), "42".into())])
        .unwrap();
    assert!(r.is_none(), "single-node has no fan-out generation");
    assert_eq!(broker.sweep_interval_ms(), 42);
    assert!(broker.cluster_broker_config_fanout_peers().is_empty());
}

#[test]
fn delete_restores_product_default_and_fans_out_locally() {
    let base = unique_dir("delete");
    let _g = Guard(base.clone());
    let cfg = cluster_config([19101, 19102, 19103]);
    let mk = |id: u32| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    b1.alter_broker_configs(&[(KEY_SWEEP_INTERVAL_MS.into(), "55".into())])
        .unwrap();
    // Direct apply on peer (simulate push).
    b2.handle_cluster_broker_config(1, &[(KEY_SWEEP_INTERVAL_MS.into(), "55".into())]);
    assert_eq!(b2.sweep_interval_ms(), 55);

    let gen = b1
        .alter_broker_configs(&[(KEY_SWEEP_INTERVAL_MS.into(), "".into())])
        .unwrap()
        .unwrap();
    assert_eq!(b1.sweep_interval_ms(), DEFAULT_SWEEP_INTERVAL_MS);
    b2.handle_cluster_broker_config(gen, &[(KEY_SWEEP_INTERVAL_MS.into(), "".into())]);
    assert_eq!(b2.sweep_interval_ms(), DEFAULT_SWEEP_INTERVAL_MS);
}
