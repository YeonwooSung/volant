//! Phase 113 PR4: ACL snapshot fan-out (controller → peers).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use volant_broker::{
    fanout_cluster_acl_snapshot, serve_listener, start_background_tasks, AclEntry, AclOperation,
    AclPermission, Broker, BrokerEndpoint, ClusterConfig, ResourceType, CLUSTER_RESOURCE,
};
use volant_protocol::ErrorCode;
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p113acl-{label}-{}-{}",
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

fn allow_cluster_alter(principal: &str) -> AclEntry {
    AclEntry {
        principal: principal.into(),
        resource_type: ResourceType::Cluster,
        resource: CLUSTER_RESOURCE.into(),
        operation: AclOperation::Alter,
        permission: AclPermission::Allow,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_create_acls_fans_out_and_denies_on_peer() {
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
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Enable ACLs with no allows → default deny once enabled via create.
    // Create ACL that allows only "alice" Write on "events".
    let gen = b1
        .create_acls_admin(vec![allow_write("alice", "events")])
        .unwrap()
        .expect("cluster generation");
    assert_eq!(gen, 1);
    assert!(b1.acls().is_enabled());
    assert!(
        b1.acls()
            .authorize(Some("alice"), ResourceType::Topic, "events", AclOperation::Write)
    );
    assert!(
        !b1.acls()
            .authorize(Some("bob"), ResourceType::Topic, "events", AclOperation::Write)
    );

    fanout_cluster_acl_snapshot(&b1, gen).await;

    for _ in 0..40 {
        if b2.acls().is_enabled() && b3.acls().is_enabled() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(b2.acls().is_enabled(), "peer2 ACL enabled after fan-out");
    assert!(b3.acls().is_enabled(), "peer3 ACL enabled after fan-out");
    assert!(
        b2.acls()
            .authorize(Some("alice"), ResourceType::Topic, "events", AclOperation::Write)
    );
    assert!(
        !b2.acls()
            .authorize(Some("bob"), ResourceType::Topic, "events", AclOperation::Write),
        "peer2 must deny bob after ACL fan-out"
    );
    assert_eq!(b2.applied_acl_generation(), 1);
    assert_eq!(b3.applied_acl_generation(), 1);

    // Stale generation ignored.
    let empty = b1.acl_snapshot_wire_bytes().unwrap();
    let (code, applied) = b2.handle_cluster_acl_snapshot(1, &empty);
    assert_eq!(code, 0);
    assert_eq!(applied, 1);
    assert!(
        b2.acls()
            .authorize(Some("alice"), ResourceType::Topic, "events", AclOperation::Write),
        "stale push must not wipe ACLs"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[test]
fn non_controller_create_rejected() {
    let base = unique_dir("not-ctrl");
    let _g = Guard(base.clone());
    let cfg = cluster_config([19201, 19202, 19203]);
    let mk = |id: u32| {
        Arc::new(
            Broker::with_cluster(
                StorageConfig {
                    data_dir: base.join(format!("n{id}")),
                    ..StorageConfig::default()
                },
                id,
                cfg.clone(),
            )
            .unwrap(),
        )
    };
    let b1 = mk(1);
    let b2 = mk(2);
    assert!(b1.is_controller());
    let err = b2
        .create_acls_admin(vec![allow_write("alice", "t")])
        .expect_err("non-controller");
    assert!(err.to_string().contains("not controller"));
    assert!(!b2.acls().is_enabled());
}

#[test]
fn single_node_create_unchanged() {
    let dir = unique_dir("single");
    let _g = Guard(dir.clone());
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    });
    let gen = broker
        .create_acls_admin(vec![allow_write("alice", "t")])
        .unwrap();
    assert!(gen.is_none());
    assert!(broker.acls().is_enabled());
    assert!(broker.cluster_acl_fanout_peers().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_restart_reloads_durable_acls() {
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
    for (l, b) in [(l1, &b1), (l2, &b2)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(l, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let gen = b1
        .create_acls_admin(vec![
            allow_write("alice", "events"),
            allow_cluster_alter("alice"),
        ])
        .unwrap()
        .unwrap();
    fanout_cluster_acl_snapshot(&b1, gen).await;
    assert!(b2.acls().is_enabled());

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
    drop(b2);

    let b2b = Broker::with_cluster(
        StorageConfig {
            data_dir: base.join("node-2"),
            ..StorageConfig::default()
        },
        2,
        cfg,
    )
    .unwrap();
    assert!(
        b2b.acls().is_enabled(),
        "peer must reload durable ACLs on restart"
    );
    assert!(b2b.acls().authorize(
        Some("alice"),
        ResourceType::Topic,
        "events",
        AclOperation::Write
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_acls_fans_out() {
    let base = unique_dir("delete");
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
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("node-{id}")),
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
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
    tokio::time::sleep(Duration::from_millis(80)).await;

    let e = allow_write("alice", "events");
    let gen = b1.create_acls_admin(vec![e.clone()]).unwrap().unwrap();
    fanout_cluster_acl_snapshot(&b1, gen).await;
    assert!(b2.acls().authorize(
        Some("alice"),
        ResourceType::Topic,
        "events",
        AclOperation::Write
    ));

    let (n, gen2) = b1.delete_acls_admin(&[e]).unwrap();
    assert_eq!(n, 1);
    let gen2 = gen2.expect("delete should bump gen");
    fanout_cluster_acl_snapshot(&b1, gen2).await;

    for _ in 0..40 {
        if !b2.acls().authorize(
            Some("alice"),
            ResourceType::Topic,
            "events",
            AclOperation::Write,
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // After delete, no allow rules for alice write — default deny when enabled.
    assert!(
        !b2.acls().authorize(
            Some("alice"),
            ResourceType::Topic,
            "events",
            AclOperation::Write
        ),
        "peer should drop deleted ACL after fan-out"
    );

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

#[test]
fn handle_cluster_acl_snapshot_invalid_bytes() {
    let dir = unique_dir("bad");
    let _g = Guard(dir.clone());
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    });
    let (code, _) = broker.handle_cluster_acl_snapshot(1, b"not-json");
    assert_eq!(code, ErrorCode::InvalidArg as u16);
}
