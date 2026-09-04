//! v0.38 — follower AddBroker / RemoveBroker forwards to the openraft leader.
//!
//! Flag off keeps the v0.10 any-node overlay write. Flag on: a non-leader
//! does not persist `membership.json`; it RPCs the same body to
//! `controller_id()` and returns the leader response. No leader / RPC fail
//! → native NotController (14) and local generation is unchanged.
//!
//! v0.216: `EntryPayload::Membership` apply (and snapshot install) also
//! writes the overlay, so a follower ends consistent without relying only
//! on `MembershipPut`.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, cluster_config_n2, default_storage, rpc_seq, unique_dir, Guard,
};
use volant_broker::cluster::AssignmentSnapshot;
use volant_broker::{load_membership_overlay, serve_listener, Broker};
use volant_protocol::{ErrorCode, Request, Response};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
    }
    std::env::remove_var("VOLANT_OPENRAFT_FORWARD_MEMBERSHIP");
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
        let base = unique_dir("v38", label);
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

fn overlay_on(b: &Broker) -> Option<volant_broker::MembershipOverlay> {
    load_membership_overlay(&b.cluster_state().unwrap().data_dir)
        .ok()
        .flatten()
}

fn overlay_has_id(b: &Broker, id: u32) -> bool {
    overlay_on(b)
        .map(|o| o.brokers.iter().any(|ep| ep.id == id))
        .unwrap_or(false)
}

fn add_error_code(resp: &Response) -> u16 {
    match resp {
        Response::AddBroker { error_code, .. } => *error_code,
        Response::Error { code, .. } => *code,
        other => panic!("unexpected add-broker response: {other:?}"),
    }
}

/// Flag off: any node still writes local overlay (v0.10).
#[test]
fn flag_off_any_node_add_writes_local_overlay() {
    set_openraft_env(false);
    let base = unique_dir("v38", "off");
    let _g = Guard(base.clone());
    let cfg = cluster_config_n2([19801, 19802]);
    let b2 = Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap();
    assert!(
        !b2.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA=0 keeps lowest-id"
    );
    assert!(!b2.should_forward_membership(), "flag off must not forward");

    let gen = b2.add_broker(3, "127.0.0.1".into(), 19803, None).unwrap();
    assert!(gen >= 1);
    assert_eq!(b2.configured_broker_count(), 3);
    assert!(overlay_has_id(&b2, 3));
    assert_eq!(b2.membership_generation(), gen);
}

/// Flag on, 3-node: AddBroker to a non-leader bumps overlay generation on
/// all nodes via the leader path + MembershipPut. Contacted follower
/// matches; no follower-only split generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_follower_add_forwards_and_converges() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("fwd").await;
    assert!(t.b1.openraft_metadata_enabled());
    assert!(
        t.b1.openraft_forward_membership_enabled(),
        "membership forward must default on"
    );
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    let follower_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("follower");
    let follower = t.broker(follower_id);
    assert!(
        follower.should_forward_membership(),
        "non-leader must forward when flag on"
    );
    assert!(!leader.should_forward_membership());

    // Offline id=4 cannot join voters; skip 5s joint wait / v0.34 rollback
    // so this slice asserts forward + MembershipPut, not joint success.
    for n in &nodes {
        n.set_openraft_joint_rollback(false);
    }
    leader.fail_next_change_membership();

    let prev_follower_gen = follower.membership_generation();
    let prev_leader_gen = leader.membership_generation();
    let addr = format!("127.0.0.1:{}", t.ports[(follower_id - 1) as usize]);
    let resps = rpc_seq(
        &addr,
        &[Request::AddBroker {
            id: 4,
            host: "127.0.0.1".into(),
            port: t.ports[0].saturating_add(80),
            rack: None,
        }],
    )
    .await;
    let error_code = add_error_code(&resps[0]);
    assert_eq!(error_code, 0, "forwarded add must succeed: {:?}", resps[0]);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut last = Vec::new();
    while tokio::time::Instant::now() < deadline {
        last = nodes
            .iter()
            .map(|n| {
                (
                    n.node_id(),
                    n.membership_generation(),
                    overlay_has_id(n, 4),
                    n.configured_broker_count(),
                )
            })
            .collect();
        let gens: Vec<u64> = last.iter().map(|r| r.1).collect();
        if gens.iter().all(|g| *g == gens[0])
            && gens[0] > prev_leader_gen
            && gens[0] > prev_follower_gen
            && last.iter().all(|r| r.2)
            && last.iter().all(|r| r.3 == 4)
        {
            let follower_overlay = overlay_on(&follower).expect("follower overlay");
            let leader_overlay = overlay_on(&leader).expect("leader overlay");
            assert_eq!(follower_overlay.generation, leader_overlay.generation);
            assert_eq!(follower_overlay.brokers.len(), leader_overlay.brokers.len());
            t.abort_all();
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    t.abort_all();
    panic!("overlays did not converge after follower AddBroker; last={last:?}");
}

/// Flag on, no leader: AddBroker returns error ≠ 0 and does not write a
/// higher local generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_no_leader_add_does_not_write_overlay() {
    set_openraft_env(true);
    let base = unique_dir("v38", "noleader");
    let _g = Guard(base.clone());
    let (l1, p1) = bind_port0().await;
    let (_l2, p2) = bind_port0().await;
    let (_l3, p3) = bind_port0().await;
    let cfg = cluster_config([p1, p2, p3]);
    let b1 = {
        let b = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };
    assert!(b1.openraft_metadata_enabled());
    assert!(b1.openraft_forward_membership_enabled());
    let h1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        b1.controller_id(),
        0,
        "solo voter of a 3-node group must not elect"
    );
    assert!(
        b1.should_forward_membership(),
        "no leader ⇒ not controller ⇒ forward path"
    );

    let prev_gen = b1.membership_generation();
    let prev_n = b1.configured_broker_count();
    let addr = format!("127.0.0.1:{p1}");
    let resps = rpc_seq(
        &addr,
        &[Request::AddBroker {
            id: 4,
            host: "127.0.0.1".into(),
            port: p1.saturating_add(40),
            rack: None,
        }],
    )
    .await;
    let error_code = add_error_code(&resps[0]);
    assert_ne!(error_code, 0, "no-leader add must fail: {:?}", resps[0]);
    assert_eq!(
        error_code,
        ErrorCode::NotController as u16,
        "documented no-leader / RPC-fail code is NotController (14)"
    );
    assert_eq!(b1.membership_generation(), prev_gen);
    assert_eq!(b1.configured_broker_count(), prev_n);
    assert!(
        !overlay_has_id(&b1, 4),
        "follower must not persist overlay when there is no leader"
    );
    h1.abort();
}

/// After joint/initialize, a follower overlay write from Membership apply
/// (extra voter) does not rely on MembershipPut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_follower_overlay_from_membership_apply() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("apply").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let follower_id = [1u32, 2, 3]
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("follower");
    let follower = t.broker(follower_id);
    assert!(
        overlay_on(&follower).is_none() || !overlay_has_id(&follower, 4),
        "id=4 must not be present before apply"
    );

    let addrs = [
        format!("127.0.0.1:{}", t.ports[0]),
        format!("127.0.0.1:{}", t.ports[1]),
        format!("127.0.0.1:{}", t.ports[2]),
        "127.0.0.1:19999".to_string(),
    ];
    follower
        .test_openraft_sm_apply_membership(
            8,
            &[
                (1, addrs[0].as_str()),
                (2, addrs[1].as_str()),
                (3, addrs[2].as_str()),
                (4, addrs[3].as_str()),
            ],
        )
        .await;

    assert!(
        overlay_has_id(&follower, 4),
        "Membership apply must write follower overlay without Put"
    );
    assert_eq!(follower.configured_broker_count(), 4);
    t.abort_all();
}

/// SM apply of Membership writes overlay (including a new voter) without Put.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sm_apply_membership_writes_overlay_without_put() {
    set_openraft_env(true);
    let base = unique_dir("v38", "sm-apply");
    let _g = Guard(base.clone());
    let cfg = cluster_config([19701, 19702, 19703]);
    let b2 = Arc::new(Broker::with_cluster(default_storage(base.join("n2")), 2, cfg).unwrap());
    assert!(overlay_on(&b2).is_none(), "no overlay before apply");
    assert_eq!(b2.configured_broker_count(), 3);

    b2.test_openraft_sm_apply_membership(
        4,
        &[
            (1, "127.0.0.1:19701"),
            (2, "127.0.0.1:19702"),
            (3, "127.0.0.1:19703"),
            (4, "127.0.0.1:19704"),
        ],
    )
    .await;

    let overlay = overlay_on(&b2).expect("apply must write overlay");
    assert!(overlay.generation >= 4);
    let mut ids: Vec<u32> = overlay.brokers.iter().map(|b| b.id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    assert_eq!(b2.configured_broker_count(), 4);
    assert!(overlay.brokers.iter().all(|b| b.rack.is_none()));
}

/// Snapshot install with last_membership writes overlay (no Put).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sm_install_snapshot_membership_writes_overlay() {
    set_openraft_env(true);
    let base = unique_dir("v38", "sm-install");
    let _g = Guard(base.clone());
    let cfg = cluster_config([19711, 19712, 19713]);
    let b3 = Arc::new(Broker::with_cluster(default_storage(base.join("n3")), 3, cfg).unwrap());
    assert!(overlay_on(&b3).is_none());

    let bytes = Broker::test_openraft_snapshot_bytes(&AssignmentSnapshot::default());
    b3.test_openraft_sm_install_snapshot_with_membership(
        bytes,
        7,
        &[
            (1, "127.0.0.1:19711"),
            (2, "127.0.0.1:19712"),
            (3, "127.0.0.1:19713"),
            (5, "127.0.0.1:19715"),
        ],
    )
    .await;

    let overlay = overlay_on(&b3).expect("install_snapshot must write overlay");
    assert!(overlay.generation >= 7);
    assert!(overlay.brokers.iter().any(|b| b.id == 5));
    assert_eq!(b3.configured_broker_count(), 4);
}
