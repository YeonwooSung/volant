//! v0.34 / v0.212 — joint fail never writes overlay.
//!
//! Flag off keeps the v0.10 overlay path. Flag on + leader + rollback
//! (default): dispatch validates, joints the **pending** target, and
//! persists overlay only after commit (v0.212). Fail → native **15**,
//! disk unchanged (v0.34 restore is a no-op). In-process `add_broker`
//! / `remove_broker` invert the same way when raft is started (v0.217).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, cluster_config_n2, default_storage, rpc_seq, unique_dir, Guard,
};
use volant_broker::{load_membership_overlay, serve_listener, Broker};
use volant_protocol::{ErrorCode, Request, Response};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
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
        let base = unique_dir("v34", label);
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

/// Flag off: AddBroker still writes overlay (v0.10).
#[test]
fn flag_off_add_broker_writes_overlay() {
    set_openraft_env(false);
    let base = unique_dir("v34", "off");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19701, 19702]);
    let b1 = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
    assert!(
        !b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA=0 keeps lowest-id"
    );

    let gen = b1.add_broker(3, "127.0.0.1".into(), 19703, None).unwrap();
    assert!(gen >= 1);
    assert_eq!(b1.configured_broker_count(), 3);
    assert!(overlay_has_id(&b1, 3));
    assert!(b1.test_last_openraft_membership_target().is_none());
}

/// Flag on, happy path: in-process add persists overlay after joint (v0.217).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_add_broker_still_works() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("add4").await;
    assert!(t.b1.openraft_metadata_enabled());
    assert!(
        t.b1.openraft_joint_rollback_enabled(),
        "joint rollback must default on when env is unset"
    );
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
    let target = leader.test_last_openraft_membership_target();
    assert!(
        target.as_ref().map(|ids| ids.contains(&4)).unwrap_or(false)
            || leader.openraft_voter_ids().contains(&4)
            || overlay_has_id(&leader, 4),
        "happy path must keep overlay id=4 after in-process joint-then-persist"
    );
    t.abort_all();
}

/// Flag on + in-process add + `fail_next_change_membership`: overlay not written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_in_process_add_fail_next_does_not_write_overlay() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("inproc-fail").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    assert!(leader.openraft_metadata_enabled());
    assert!(leader.openraft_started());

    let prev_gen = leader.membership_generation();
    let prev_n = leader.configured_broker_count();
    leader.fail_next_change_membership();

    let err = leader
        .add_broker(4, "127.0.0.1".into(), t.ports[0].saturating_add(70), None)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("15") || msg.contains("not enough replicas"),
        "joint fail is native 15 / Error, got {msg}"
    );
    assert_eq!(leader.configured_broker_count(), prev_n);
    assert_eq!(leader.membership_generation(), prev_gen);
    assert!(
        !overlay_has_id(&leader, 4),
        "in-process fail_next must not write overlay id=4"
    );
    if prev_gen == 0 {
        assert!(
            !leader.membership_overlay_path().exists(),
            "failed in-process joint must not create membership.json"
        );
    }
    t.abort_all();
}

/// Flag on + leader: forced `change_membership` fail never writes overlay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_change_membership_fail_rolls_back_overlay() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("rollback").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    assert!(leader.openraft_joint_rollback_armed());

    let prev_gen = leader.membership_generation();
    let prev_n = leader.configured_broker_count();
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
    assert_ne!(error_code, 0, "client must see a non-zero error");
    assert_eq!(
        error_code,
        ErrorCode::NotEnoughReplicas as u16,
        "documented joint-fail code is NotEnoughReplicas (15)"
    );
    assert_eq!(leader.configured_broker_count(), prev_n);
    assert_eq!(leader.membership_generation(), prev_gen);
    assert!(
        !overlay_has_id(&leader, 4),
        "overlay must not keep id=4 after joint fail"
    );
    if prev_gen == 0 {
        assert!(
            !leader.membership_overlay_path().exists(),
            "failed joint must not create membership.json"
        );
    }
    t.abort_all();
}
