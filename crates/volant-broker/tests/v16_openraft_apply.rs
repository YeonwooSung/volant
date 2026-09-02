//! v0.16 — opt-in openraft `SetAssignment` log apply.
//!
//! Flag off keeps lowest-id CreateTopic. Flag on: CreateTopic / DeleteTopic
//! on the openraft leader replicate via `client_write`; followers install
//! the topic in live assignment. Leader abort keeps applied state on survivors.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, default_storage, unique_dir, Guard};
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, ClientConfig};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::remove_var("VOLANT_OPENRAFT_METADATA");
    }
}

fn live_has_topic(b: &Broker, name: &str) -> bool {
    b.clone_live_assignment()
        .map(|a| a.topics.contains_key(name))
        .unwrap_or(false)
}

struct Triple {
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    ports: [u16; 3],
    h1: tokio::task::JoinHandle<()>,
    h2: tokio::task::JoinHandle<()>,
    h3: tokio::task::JoinHandle<()>,
}

impl Triple {
    async fn boot(label: &str) -> (Self, Guard) {
        let base = unique_dir("v16", label);
        let guard = Guard(base.clone());
        let (l1, p1) = bind_port0().await;
        let (l2, p2) = bind_port0().await;
        let (l3, p3) = bind_port0().await;
        let ports = [p1, p2, p3];
        let cfg = cluster_config(ports);
        let mk = |id: u32| {
            let b = Broker::with_cluster(
                default_storage(base.join(format!("n{id}"))),
                id,
                cfg.clone(),
            )
            .unwrap();
            b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
            Arc::new(b)
        };
        let b1 = mk(1);
        let b2 = mk(2);
        let b3 = mk(3);
        let h1 = {
            let b = Arc::clone(&b1);
            tokio::spawn(async move {
                let _ = serve_listener(l1, b).await;
            })
        };
        let h2 = {
            let b = Arc::clone(&b2);
            tokio::spawn(async move {
                let _ = serve_listener(l2, b).await;
            })
        };
        let h3 = {
            let b = Arc::clone(&b3);
            tokio::spawn(async move {
                let _ = serve_listener(l3, b).await;
            })
        };
        tokio::time::sleep(Duration::from_millis(120)).await;
        (
            Self {
                b1,
                b2,
                b3,
                ports,
                h1,
                h2,
                h3,
            },
            guard,
        )
    }

    fn broker(&self, id: u32) -> Arc<Broker> {
        match id {
            1 => Arc::clone(&self.b1),
            2 => Arc::clone(&self.b2),
            3 => Arc::clone(&self.b3),
            _ => panic!("bad id {id}"),
        }
    }

    fn live(&self, ids: &[u32]) -> Vec<Arc<Broker>> {
        ids.iter().copied().map(|id| self.broker(id)).collect()
    }

    fn port_of(&self, id: u32) -> u16 {
        self.ports[(id - 1) as usize]
    }

    fn abort_all(&self) {
        self.h1.abort();
        self.h2.abort();
        self.h3.abort();
    }

    fn abort_id(&self, id: u32) {
        match id {
            1 => self.h1.abort(),
            2 => self.h2.abort(),
            3 => self.h3.abort(),
            _ => panic!("bad id {id}"),
        }
    }
}

async fn wait_agreed_leader(nodes: &[Arc<Broker>], timeout: Duration) -> u32 {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = Vec::new();
    while tokio::time::Instant::now() < deadline {
        last = nodes.iter().map(|n| n.controller_id()).collect();
        if last.iter().all(|id| *id != 0) && last.iter().all(|id| *id == last[0]) {
            let leader = last[0];
            if nodes.iter().any(|n| n.node_id() == leader) {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no agreed openraft leader within {timeout:?}; last={last:?}");
}

async fn wait_all_have_topic(nodes: &[Arc<Broker>], topic: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if nodes.iter().all(|n| live_has_topic(n, topic)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let got: Vec<(u32, bool)> = nodes
        .iter()
        .map(|n| (n.node_id(), live_has_topic(n, topic)))
        .collect();
    panic!("topic {topic} not on all nodes within {timeout:?}: {got:?}");
}

async fn wait_missing_topic(nodes: &[Arc<Broker>], topic: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if nodes.iter().all(|n| !live_has_topic(n, topic)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let got: Vec<(u32, bool)> = nodes
        .iter()
        .map(|n| (n.node_id(), live_has_topic(n, topic)))
        .collect();
    panic!("topic {topic} still present within {timeout:?}: {got:?}");
}

async fn connect_leader(port: u16) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{port}")],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap()
}

/// Flag default off: CreateTopic does not require openraft; lowest-id controller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_off_create_topic_lowest_id() {
    set_openraft_env(false);
    let (t, _g) = Triple::boot("off").await;
    assert!(
        !t.b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA must default off"
    );
    assert_eq!(t.b1.controller_id(), 1);
    assert_eq!(t.b2.controller_id(), 1);
    assert_eq!(t.b3.controller_id(), 1);
    assert!(t.b1.is_controller());
    assert!(!t.b2.is_controller());

    let admin = connect_leader(t.ports[0]).await;
    admin.create_topic("plain", 1).await.unwrap();
    assert!(
        live_has_topic(&t.b1, "plain"),
        "lowest-id controller must write live assignment without openraft"
    );

    t.abort_all();
}

/// Flag on, 3-node: CreateTopic via the openraft leader → all three have the topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_create_topic_replicates_to_all() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("create").await;
    assert!(t.b1.openraft_metadata_enabled());
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;

    let admin = connect_leader(t.port_of(leader)).await;
    admin.create_topic("events", 1).await.unwrap();
    wait_all_have_topic(&nodes, "events", Duration::from_secs(8)).await;
    for n in &nodes {
        assert!(
            live_has_topic(n, "events"),
            "broker {} missing topic after openraft apply",
            n.node_id()
        );
    }

    t.abort_all();
}

/// Flag on: DeleteTopic on the leader removes the topic from a follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_delete_topic_removes_from_follower() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("delete").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;

    let admin = connect_leader(t.port_of(leader)).await;
    admin.create_topic("gone", 1).await.unwrap();
    wait_all_have_topic(&nodes, "gone", Duration::from_secs(8)).await;

    admin.delete_topic("gone").await.unwrap();
    let followers: Vec<Arc<Broker>> = nodes
        .iter()
        .filter(|n| n.node_id() != leader)
        .cloned()
        .collect();
    wait_missing_topic(&followers, "gone", Duration::from_secs(8)).await;
    assert!(
        followers.iter().any(|n| !live_has_topic(n, "gone")),
        "at least one follower must drop the topic from live assignment"
    );

    t.abort_all();
}

/// Leader abort after a committed CreateTopic: new leader still has the topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_abort_keeps_applied_topic() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("abort").await;
    let all = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&all, Duration::from_secs(8)).await;

    let admin = connect_leader(t.port_of(leader)).await;
    admin.create_topic("keep", 1).await.unwrap();
    wait_all_have_topic(&all, "keep", Duration::from_secs(8)).await;

    t.broker(leader).test_set_inter_broker_blocked(true);
    t.abort_id(leader);

    let survivors: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let survivor_nodes = t.live(&survivors);
    let new_leader = wait_agreed_leader(&survivor_nodes, Duration::from_secs(10)).await;
    assert_ne!(new_leader, leader, "must elect a different leader");
    assert!(
        live_has_topic(&t.broker(new_leader), "keep"),
        "new leader {new_leader} must still have applied topic after abort"
    );
    for n in &survivor_nodes {
        assert!(
            live_has_topic(n, "keep"),
            "survivor {} lost applied topic after leader abort",
            n.node_id()
        );
    }

    t.abort_all();
}
