//! v0.39 — restore assignment if add-broker joint overlay rolls back.
//!
//! `VOLANT_REASSIGN_ON_ADD` expands under-replicated topics inside
//! `add_broker` before openraft joint. When v0.34 overlay rollback runs on
//! the dispatch path, assignment.json + live replica sets are restored too
//! (unless `VOLANT_REASSIGN_ON_ADD_ROLLBACK=0`). In-process `add_broker`
//! stays v0.18 (no assignment rewind).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, default_storage, rpc_seq, unique_dir, Guard};
use volant_broker::cluster::load_assignment;
use volant_broker::{
    load_membership_overlay, reassign_on_add_enabled, reassign_on_add_rollback_enabled,
    serve_listener, Broker, BrokerEndpoint, ClusterConfig, ENV_REASSIGN_ON_ADD,
    ENV_REASSIGN_ON_ADD_ROLLBACK,
};
use volant_core::TopicName;
use volant_protocol::{ErrorCode, Request, Response};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::remove_var("VOLANT_OPENRAFT_METADATA");
    }
}

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self { key, prev }
    }

    fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
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
        let base = unique_dir("v39", label);
        let guard = Guard(base.clone());
        let (l1, p1) = bind_port0().await;
        let (l2, p2) = bind_port0().await;
        let (l3, p3) = bind_port0().await;
        let ports = [p1, p2, p3];
        let mut cfg = cluster_config(ports);
        // RF=4 with N=3 → create is capped at 3; add id=4 is under-replicated.
        cfg.default_replication_factor = 4;
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

fn overlay_has_id(b: &Broker, id: u32) -> bool {
    load_membership_overlay(&b.cluster_state().unwrap().data_dir)
        .ok()
        .flatten()
        .map(|o| o.brokers.iter().any(|ep| ep.id == id))
        .unwrap_or(false)
}

fn part_replicas(b: &Broker, topic: &str, pid: u32) -> Vec<u32> {
    let asg = b.clone_live_assignment().expect("cluster");
    asg.topics
        .get(topic)
        .and_then(|t| t.partitions.get(&pid))
        .map(|p| {
            let mut r = p.replicas.clone();
            r.sort_unstable();
            r
        })
        .unwrap_or_default()
}

fn file_replicas(b: &Broker, topic: &str, pid: u32) -> Vec<u32> {
    let dir = &b.cluster_state().expect("cluster").data_dir;
    load_assignment(dir)
        .ok()
        .and_then(|asg| {
            asg.topics
                .get(topic)
                .and_then(|t| t.partitions.get(&pid))
                .map(|p| {
                    let mut r = p.replicas.clone();
                    r.sort_unstable();
                    r
                })
        })
        .unwrap_or_default()
}

/// Flag on + leader: joint fail rolls back overlay **and** assignment replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_joint_fail_rolls_back_overlay_and_assignment() {
    set_openraft_env(true);
    let _reassign = EnvGuard::set(ENV_REASSIGN_ON_ADD, "1");
    let _rollback = EnvGuard::unset(ENV_REASSIGN_ON_ADD_ROLLBACK);
    assert!(reassign_on_add_enabled());
    assert!(
        reassign_on_add_rollback_enabled(),
        "assignment rollback must default on"
    );

    let (t, _g) = Triple::boot("rollback").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    assert!(leader.openraft_joint_rollback_armed());

    leader
        .create_topic(TopicName::new("events"), 1)
        .expect("create under-replicated topic on leader");
    assert_eq!(part_replicas(&leader, "events", 0), vec![1, 2, 3]);
    let prev_gen = leader.membership_generation();
    let prev_n = leader.configured_broker_count();
    let asg_gen_before = leader.generation();

    leader.fail_next_change_membership();
    let addr = format!("127.0.0.1:{}", t.ports[(leader_id - 1) as usize]);
    let resps = rpc_seq(
        &addr,
        &[Request::AddBroker {
            id: 4,
            host: "127.0.0.1".into(),
            port: t.ports[0].saturating_add(70),
            rack: None,
        }],
    )
    .await;
    let error_code = match &resps[0] {
        Response::AddBroker { error_code, .. } => *error_code,
        Response::Error { code, .. } => *code,
        other => panic!("unexpected add-broker response: {other:?}"),
    };
    assert_eq!(
        error_code,
        ErrorCode::NotEnoughReplicas as u16,
        "documented joint-fail code is NotEnoughReplicas (15)"
    );
    assert_eq!(leader.configured_broker_count(), prev_n);
    assert_eq!(leader.membership_generation(), prev_gen);
    assert!(
        !overlay_has_id(&leader, 4),
        "overlay must not keep id=4 after rollback"
    );
    let live = part_replicas(&leader, "events", 0);
    assert_eq!(
        live,
        vec![1, 2, 3],
        "live replicas must not mention rolled-back id=4: {live:?}"
    );
    assert!(
        !live.contains(&4),
        "live assignment must not include dropped id"
    );
    let on_disk = file_replicas(&leader, "events", 0);
    assert_eq!(
        on_disk,
        vec![1, 2, 3],
        "assignment.json must not mention rolled-back id=4: {on_disk:?}"
    );
    assert_eq!(
        leader.generation(),
        asg_gen_before,
        "assignment generation must rewind with the snapshot"
    );
    t.abort_all();
}

/// Happy path: in-process add still expands under-replicated topics (v0.18).
#[test]
fn happy_path_add_still_expands_replicas() {
    set_openraft_env(false);
    let _reassign = EnvGuard::set(ENV_REASSIGN_ON_ADD, "1");
    assert!(reassign_on_add_enabled());

    let base = unique_dir("v39", "happy");
    let _g = Guard(base.clone());
    // default RF=3 but only 2 brokers → create is RF-capped at 2.
    let cfg = ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 1,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=2)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: 19350 + id as u16,
                rack: None,
            })
            .collect(),
    };
    let b1 = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
    b1.set_advertised("127.0.0.1", 19351);
    b1.create_topic(TopicName::new("events"), 1).unwrap();
    assert_eq!(part_replicas(&b1, "events", 0), vec![1, 2]);

    b1.add_broker(3, "127.0.0.1".into(), 19353, None).unwrap();
    let replicas = part_replicas(&b1, "events", 0);
    assert_eq!(
        replicas,
        vec![1, 2, 3],
        "flag-on add should append new id when unique < min(rf, N)"
    );
    let on_disk = file_replicas(&b1, "events", 0);
    assert_eq!(on_disk, vec![1, 2, 3]);
}
