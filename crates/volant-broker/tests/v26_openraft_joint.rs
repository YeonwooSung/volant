//! v0.26 — openraft joint membership on add/remove broker.
//!
//! Flag off keeps the v0.10 overlay path (no raft membership change).
//! Flag on: the openraft leader `change_membership`s to the configured
//! voter set after overlay add/remove.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, cluster_config_n2, default_storage, unique_dir, Guard,
};
use volant_broker::{load_membership_overlay, serve_listener, Broker};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::remove_var("VOLANT_OPENRAFT_METADATA");
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
        let base = unique_dir("v26", label);
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

fn overlay_has_id(b: &Broker, id: u32) -> bool {
    load_membership_overlay(&b.cluster_state().unwrap().data_dir)
        .ok()
        .flatten()
        .map(|o| o.brokers.iter().any(|ep| ep.id == id))
        .unwrap_or(false)
}

/// Flag off: AddBroker still writes overlay; no raft membership change.
#[test]
fn flag_off_add_broker_writes_overlay_no_raft_target() {
    set_openraft_env(false);
    let base = unique_dir("v26", "off");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19601, 19602]);
    let b1 = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
    assert!(
        !b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA must default off"
    );
    assert!(b1.test_last_openraft_membership_target().is_none());
    assert!(b1.openraft_voter_ids().is_empty());

    let gen = b1.add_broker(3, "127.0.0.1".into(), 19603, None).unwrap();
    assert!(gen >= 1);
    assert_eq!(b1.configured_broker_count(), 3);
    assert!(overlay_has_id(&b1, 3));
    // Flag off: overlay only; hook stays empty; no raft voters.
    assert!(
        b1.test_last_openraft_membership_target().is_none(),
        "flag off must not record a change_membership target"
    );
    assert!(b1.openraft_voter_ids().is_empty());
}

/// Flag on, 3-node: AddBroker id=4 (endpoint only). Leader voter set or hook
/// includes 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_add_broker_includes_new_voter() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("add4").await;
    assert!(t.b1.openraft_metadata_enabled());
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);

    let gen = leader
        .add_broker(4, "127.0.0.1".into(), t.ports[0].saturating_add(50), None)
        .unwrap();
    assert!(gen >= 1);
    assert_eq!(leader.configured_broker_count(), 4);
    assert!(overlay_has_id(&leader, 4));

    let _ = leader.change_openraft_membership().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut last_voters = Vec::new();
    let mut last_target = None;
    while tokio::time::Instant::now() < deadline {
        last_voters = leader.openraft_voter_ids();
        last_target = leader.test_last_openraft_membership_target();
        if last_voters.contains(&4)
            || last_target
                .as_ref()
                .map(|t| t.contains(&4))
                .unwrap_or(false)
        {
            t.abort_all();
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    t.abort_all();
    panic!(
        "id=4 never appeared in voters or change_membership target; voters={last_voters:?} target={last_target:?}"
    );
}

/// Flag on: add 4 then remove 4 shrinks the voter set (or hook).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_remove_broker_shrinks_voter_set() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("rm4").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);

    leader
        .add_broker(4, "127.0.0.1".into(), t.ports[0].saturating_add(60), None)
        .unwrap();
    let _ = leader.change_openraft_membership().await;

    let deadline_add = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline_add {
        let voters = leader.openraft_voter_ids();
        let target = leader.test_last_openraft_membership_target();
        if voters.contains(&4) || target.as_ref().map(|t| t.contains(&4)).unwrap_or(false) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    leader.remove_broker(4).unwrap();
    assert_eq!(leader.configured_broker_count(), 3);
    assert!(!overlay_has_id(&leader, 4));
    let _ = leader.change_openraft_membership().await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut last_voters = Vec::new();
    let mut last_target = None;
    while tokio::time::Instant::now() < deadline {
        last_voters = leader.openraft_voter_ids();
        last_target = leader.test_last_openraft_membership_target();
        let voters_shrunk = !last_voters.is_empty() && !last_voters.contains(&4);
        let hook_shrunk = last_target
            .as_ref()
            .map(|t| !t.contains(&4) && t.len() == 3)
            .unwrap_or(false);
        if voters_shrunk || hook_shrunk {
            t.abort_all();
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    t.abort_all();
    panic!(
        "id=4 still in voters/target after remove; voters={last_voters:?} target={last_target:?}"
    );
}
