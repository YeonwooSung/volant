//! v0.11 — opt-in openraft metadata leader election.
//!
//! Default off keeps lowest-id controller. Flag on elects exactly one leader
//! and `Broker::controller_id()` reports that id. Leader abort → new election.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, default_storage, propagate_async, unique_dir, Guard,
};
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
    }
}

fn batch_value(s: impl Into<String>) -> MessageBatch {
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value(s.into()));
    batch
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
        let base = unique_dir("v11", label);
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

    fn abort_all(&self) {
        self.h1.abort();
        self.h2.abort();
        self.h3.abort();
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

/// Phase 155: unset env defaults the flag on.
#[test]
fn unset_env_defaults_on() {
    std::env::remove_var("VOLANT_OPENRAFT_METADATA");
    assert!(
        volant_broker::default_openraft_metadata_enabled(),
        "unset VOLANT_OPENRAFT_METADATA must default on"
    );
    std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
    assert!(!volant_broker::default_openraft_metadata_enabled());
}

/// Flag explicit off: controller is still lowest live id (regression).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_off_lowest_id_controller() {
    set_openraft_env(false);
    let (t, _g) = Triple::boot("off").await;
    assert!(
        !t.b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA=0 keeps lowest-id"
    );
    assert_eq!(t.b1.controller_id(), 1);
    assert_eq!(t.b2.controller_id(), 1);
    assert_eq!(t.b3.controller_id(), 1);
    assert!(t.b1.is_controller());
    assert!(!t.b2.is_controller());
    t.abort_all();
}

/// Flag on: 3-node cluster elects one leader; controller_id matches on all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_elects_one_leader() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("on").await;
    assert!(t.b1.openraft_metadata_enabled());
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    assert!(
        (1..=3).contains(&leader),
        "leader {leader} must be a cluster member"
    );
    for n in &nodes {
        assert_eq!(n.controller_id(), leader);
        assert_eq!(n.is_controller(), n.node_id() == leader);
    }
    let terms: Vec<u64> = nodes.iter().map(|n| n.openraft_term()).collect();
    assert!(terms.iter().all(|t| *t > 0), "term must advance: {terms:?}");
    t.abort_all();
}

/// Kill the openraft leader; survivors elect a new one; term does not go
/// backwards; produce acks=1 still works on an existing topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_abort_elects_new_leader_produce_acks1() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("abort").await;
    let all = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&all, Duration::from_secs(8)).await;
    let term_before = t.broker(leader).openraft_term();
    assert!(term_before > 0);

    let admin = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", t.ports[0])],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 1).await.unwrap();
    propagate_async(&[&t.b1, &t.b2, &t.b3], "events").await;

    t.broker(leader).test_set_inter_broker_blocked(true);
    match leader {
        1 => t.h1.abort(),
        2 => t.h2.abort(),
        3 => t.h3.abort(),
        _ => panic!("bad leader {leader}"),
    }

    let survivors: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let survivor_nodes = t.live(&survivors);
    let new_leader = wait_agreed_leader(&survivor_nodes, Duration::from_secs(10)).await;
    assert_ne!(new_leader, leader, "must elect a different leader");
    assert!(
        survivors.contains(&new_leader),
        "new leader {new_leader} must be a survivor"
    );
    let term_after = t.broker(new_leader).openraft_term();
    assert!(
        term_after >= term_before,
        "term must not go backwards ({term_before} → {term_after})"
    );

    let topic = TopicName::new("events");
    let writer = t.broker(new_leader);
    writer
        .produce(&topic, PartitionId(0), batch_value("post-election"))
        .expect("acks=1 produce on existing topic after leader abort");

    t.abort_all();
}
