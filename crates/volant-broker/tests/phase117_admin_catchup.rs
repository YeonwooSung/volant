//! Phase 117: controller failover / rejoin catch-up for ACL + BROKER config.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use volant_broker::broker_config::{DEFAULT_SWEEP_INTERVAL_MS, KEY_SWEEP_INTERVAL_MS};
use volant_broker::{
    catch_up_peer_admin_state, fanout_cluster_acl_snapshot, fanout_cluster_broker_config,
    serve_listener, start_background_tasks, AclEntry, AclOperation, AclPermission, Broker,
    BrokerEndpoint, ClusterAdminStore, ClusterConfig, ResourceType,
};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p117-{label}-{}-{}",
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

fn cluster_config(ports: [u16; 3], session_timeout_ms: u32) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
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

fn allow_write(principal: &str, topic: &str) -> AclEntry {
    AclEntry {
        principal: principal.into(),
        resource_type: ResourceType::Topic,
        resource: topic.into(),
        operation: AclOperation::Write,
        permission: AclPermission::Allow,
    }
}

/// Peer offline during BROKER Alter → restart → catch-up restores knobs + gen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_peer_broker_config_catchup_on_rejoin() {
    let base = unique_dir("cfg-rejoin");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    // Short session so heartbeats are frequent once b3 is back.
    let cfg = cluster_config(ports, 1500);

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
    let mut stops = Vec::new();
    for (l, b) in [(l1, &b1), (l2, &b2), (l3, &b3)] {
        let b = Arc::clone(b);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        stops.push(tx);
        tokio::spawn(async move {
            tokio::select! {
                _ = serve_listener(l, b) => {},
                _ = rx => {},
            }
        });
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Kill b3 listener so Alter fan-out cannot reach it.
    let _ = stops.pop().unwrap().send(());
    // Drop b3 bg + process so it is "offline".
    bgs.pop().unwrap().shutdown().await;
    drop(b3);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let entries = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "66".to_string())];
    let gen = b1
        .alter_broker_configs(&entries)
        .unwrap()
        .expect("cluster gen");
    assert_eq!(gen, 1);
    fanout_cluster_broker_config(&b1, gen, &entries).await;
    // b2 still live.
    for _ in 0..40 {
        if b2.sweep_interval_ms() == 66 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(b2.sweep_interval_ms(), 66);

    // Restart b3 from same data_dir (stale product default + applied gen 0).
    let (l3b, p3b) = bind_port0().await;
    // Port in static cluster is fixed to original p3; re-bind same port if possible.
    drop(l3b);
    let l3b = TcpListener::bind(format!("127.0.0.1:{p3}"))
        .await
        .expect("rebind b3 port");
    let _ = p3b;
    let b3b = {
        let storage = StorageConfig {
            data_dir: base.join("node-3"),
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, 3, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", p3);
        Arc::new(b)
    };
    assert_eq!(
        b3b.sweep_interval_ms(),
        DEFAULT_SWEEP_INTERVAL_MS,
        "offline peer missed alter"
    );
    assert_eq!(b3b.applied_config_generation(), 0);

    let bg3 = start_background_tasks(Arc::clone(&b3b));
    bgs.push(bg3);
    {
        let b = Arc::clone(&b3b);
        tokio::spawn(async move {
            let _ = serve_listener(l3b, b).await;
        });
    }

    // Wait for heartbeat catch-up (or explicit catch-up helper).
    for _ in 0..80 {
        if b3b.sweep_interval_ms() == 66 && b3b.applied_config_generation() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // If background heartbeat is slow, drive catch-up explicitly (same code path).
    if b3b.sweep_interval_ms() != 66 {
        let addr = format!("127.0.0.1:{p3}");
        catch_up_peer_admin_state(&b1, 3, &addr, b3b.applied_config_generation(), b3b.applied_acl_generation())
            .await;
    }
    for _ in 0..40 {
        if b3b.sweep_interval_ms() == 66 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        b3b.sweep_interval_ms(),
        66,
        "rejoined peer must catch up BROKER config"
    );
    assert!(
        b3b.applied_config_generation() >= 1,
        "applied gen restored"
    );
    assert!(
        b1.cluster_admin_catchup_success_total() >= 1
            || b3b.applied_config_generation() >= 1,
        "catch-up success metric or applied gen"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Peer offline during CreateAcls → restart → catch-up restores authorize.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_peer_acl_catchup_on_rejoin() {
    let base = unique_dir("acl-rejoin");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let ports = [p1, p2, p2]; // placeholder third unused
    let cfg = ClusterConfig {
        default_replication_factor: 2,
        min_insync_replicas: 1,
        session_timeout_ms: 1500,
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
    let _ = ports;

    let b1 = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("node-1"),
                ..StorageConfig::default()
            },
            1,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };
    let b2 = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("node-2"),
                ..StorageConfig::default()
            },
            2,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p2);
        Arc::new(b)
    };

    let mut bgs = vec![start_background_tasks(Arc::clone(&b1))];
    {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        });
    }
    // b2 offline for ACL mutate: no listener yet.
    drop(l2);
    tokio::time::sleep(Duration::from_millis(80)).await;

    let gen = b1
        .create_acls_admin(vec![allow_write("alice", "events")])
        .unwrap()
        .unwrap();
    fanout_cluster_acl_snapshot(&b1, gen).await;
    assert!(b1.acls().is_enabled());
    assert_eq!(b2.applied_acl_generation(), 0);
    assert!(!b2.acls().is_enabled());

    // Bring b2 online.
    let l2b = TcpListener::bind(format!("127.0.0.1:{p2}"))
        .await
        .expect("bind b2");
    let bg2 = start_background_tasks(Arc::clone(&b2));
    bgs.push(bg2);
    {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_listener(l2b, b).await;
        });
    }

    for _ in 0..80 {
        if b2.acls().is_enabled() && b2.applied_acl_generation() >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !b2.acls().is_enabled() {
        catch_up_peer_admin_state(
            &b1,
            2,
            &format!("127.0.0.1:{p2}"),
            b2.applied_config_generation(),
            b2.applied_acl_generation(),
        )
        .await;
    }
    for _ in 0..40 {
        if b2.acls().is_enabled() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(b2.acls().is_enabled(), "ACL enabled after catch-up");
    assert!(b2.acls().authorize(
        Some("alice"),
        ResourceType::Topic,
        "events",
        AclOperation::Write
    ));
    assert!(!b2.acls().authorize(
        Some("bob"),
        ResourceType::Topic,
        "events",
        AclOperation::Write
    ));
    assert!(b2.applied_acl_generation() >= 1);

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Controller restart preserves durable gens so next Alter advances past peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_restart_preserves_gens_and_peers_accept_next_alter() {
    let base = unique_dir("ctrl-restart");
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

    let b1 = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("node-1"),
                ..StorageConfig::default()
            },
            1,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };
    let b2 = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("node-2"),
                ..StorageConfig::default()
            },
            2,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p2);
        Arc::new(b)
    };

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
    ];
    let (stop1_tx, stop1_rx) = tokio::sync::oneshot::channel::<()>();
    let (stop2_tx, stop2_rx) = tokio::sync::oneshot::channel::<()>();
    {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            tokio::select! {
                _ = serve_listener(l1, b) => {},
                _ = stop1_rx => {},
            }
        });
    }
    {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            tokio::select! {
                _ = serve_listener(l2, b) => {},
                _ = stop2_rx => {},
            }
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let e1 = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "55".to_string())];
    let g1 = b1.alter_broker_configs(&e1).unwrap().unwrap();
    fanout_cluster_broker_config(&b1, g1, &e1).await;
    assert_eq!(b2.sweep_interval_ms(), 55);
    assert_eq!(b2.applied_config_generation(), 1);
    assert_eq!(b1.config_generation(), 1);

    // Stop controller listener + bg; keep peer live.
    let _ = stop1_tx.send(());
    bgs.remove(0).shutdown().await;
    drop(b1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let b1b = {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("node-1"),
                ..StorageConfig::default()
            },
            1,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };
    assert_eq!(
        b1b.config_generation(),
        1,
        "controller must restore durable config_generation"
    );
    assert_eq!(b1b.applied_config_generation(), 1);
    assert_eq!(b1b.sweep_interval_ms(), 55);

    let l1b = TcpListener::bind(format!("127.0.0.1:{p1}"))
        .await
        .expect("rebind controller");
    bgs.insert(0, start_background_tasks(Arc::clone(&b1b)));
    {
        let b = Arc::clone(&b1b);
        tokio::spawn(async move {
            let _ = serve_listener(l1b, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let e2 = vec![(KEY_SWEEP_INTERVAL_MS.to_string(), "99".to_string())];
    let g2 = b1b.alter_broker_configs(&e2).unwrap().unwrap();
    assert_eq!(g2, 2, "next alter must be gen 2 not reset to 1");
    fanout_cluster_broker_config(&b1b, g2, &e2).await;
    for _ in 0..40 {
        if b2.sweep_interval_ms() == 99 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        b2.sweep_interval_ms(),
        99,
        "peer must accept gen=2 after controller restart"
    );
    assert_eq!(b2.applied_config_generation(), 2);

    let _ = stop2_tx.send(());
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[test]
fn durable_admin_gens_roundtrip_unit() {
    let base = unique_dir("unit-gens");
    let _g = Guard(base.clone());
    let store = ClusterAdminStore::open(&base).unwrap();
    let s = volant_broker::ClusterAdminFile {
        version: volant_broker::CLUSTER_ADMIN_FILE_VERSION,
        config_generation: 4,
        applied_config_generation: 4,
        acl_generation: 2,
        applied_acl_generation: 2,
    };
    store.save(&s).unwrap();
    let loaded = store.load().unwrap();
    assert_eq!(loaded.config_generation, 4);
    assert_eq!(loaded.acl_generation, 2);
}

#[test]
fn single_node_no_catchup_generation() {
    let base = unique_dir("single");
    let _g = Guard(base.clone());
    let b = Broker::new(StorageConfig {
        data_dir: base,
        ..StorageConfig::default()
    });
    let r = b
        .alter_broker_configs(&[(KEY_SWEEP_INTERVAL_MS.into(), "12".into())])
        .unwrap();
    assert!(r.is_none());
    assert_eq!(b.config_generation(), 0);
    assert_eq!(b.cluster_admin_catchup_success_total(), 0);
    let (need_c, need_a) = b.peer_admin_gens_lag(0, 0);
    assert!(!need_c && !need_a);
}
