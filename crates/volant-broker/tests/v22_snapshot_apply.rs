//! v0.22 — InstallSnapshot applies snapshot assignment to live state.
//!
//! A non-empty snapshot `assignment` is written via `apply_cluster_state`.
//! Empty assignment is a no-op (does not wipe existing topics). Flag default
//! remains off. Homemade 154 is not exercised.

#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, default_storage, unique_dir, Guard};
use volant_broker::cluster::{
    load_assignment, AssignmentSnapshot, PartitionAssignment, TopicAssignment,
};
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, ClientConfig};

fn set_openraft_env(on: bool, snapshot_logs: Option<&str>) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::remove_var("VOLANT_OPENRAFT_METADATA");
    }
    match snapshot_logs {
        Some(v) => std::env::set_var("VOLANT_OPENRAFT_SNAPSHOT_LOGS", v),
        None => std::env::remove_var("VOLANT_OPENRAFT_SNAPSHOT_LOGS"),
    }
}

fn live_has_topic(b: &Broker, name: &str) -> bool {
    b.clone_live_assignment()
        .map(|a| a.topics.contains_key(name))
        .unwrap_or(false)
}

fn disk_has_topic(data_dir: &Path, name: &str) -> bool {
    load_assignment(data_dir)
        .map(|a| a.topics.contains_key(name))
        .unwrap_or(false)
}

fn sample_assignment(name: &str, generation: u32) -> AssignmentSnapshot {
    let mut partitions = HashMap::new();
    partitions.insert(
        0,
        PartitionAssignment {
            replicas: vec![1, 2, 3],
            leader: 1,
            isr: vec![1, 2, 3],
            leader_epoch: 0,
        },
    );
    let mut topics = HashMap::new();
    topics.insert(
        name.to_string(),
        TopicAssignment {
            topic_id: 1,
            name: name.to_string(),
            partitions,
        },
    );
    AssignmentSnapshot { generation, topics }
}

struct Triple {
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    ports: [u16; 3],
    dirs: [std::path::PathBuf; 3],
    h1: tokio::task::JoinHandle<()>,
    h2: tokio::task::JoinHandle<()>,
    h3: Option<tokio::task::JoinHandle<()>>,
}

impl Triple {
    async fn boot(
        label: &str,
        start_third: bool,
    ) -> (Self, Guard, Option<tokio::net::TcpListener>) {
        let base = unique_dir("v22", label);
        let guard = Guard(base.clone());
        let (l1, p1) = bind_port0().await;
        let (l2, p2) = bind_port0().await;
        let (l3, p3) = bind_port0().await;
        let ports = [p1, p2, p3];
        let dirs = [base.join("n1"), base.join("n2"), base.join("n3")];
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
        let (h3, held_l3) = if start_third {
            let b = Arc::clone(&b3);
            (
                Some(tokio::spawn(async move {
                    let _ = serve_listener(l3, b).await;
                })),
                None,
            )
        } else {
            (None, Some(l3))
        };
        tokio::time::sleep(Duration::from_millis(120)).await;
        (
            Self {
                b1,
                b2,
                b3,
                ports,
                dirs,
                h1,
                h2,
                h3,
            },
            guard,
            held_l3,
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

    fn start_third(&mut self, l3: tokio::net::TcpListener) {
        let b = Arc::clone(&self.b3);
        self.h3 = Some(tokio::spawn(async move {
            let _ = serve_listener(l3, b).await;
        }));
    }

    fn abort_all(&self) {
        self.h1.abort();
        self.h2.abort();
        if let Some(h) = &self.h3 {
            h.abort();
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

async fn wait_snapshot(
    nodes: &[Arc<Broker>],
    timeout: Duration,
) -> (u32, String, Option<u64>, Vec<u8>) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = String::from("none");
    while tokio::time::Instant::now() < deadline {
        for n in nodes {
            if let Some((id, idx, bytes)) = n.test_openraft_current_snapshot().await {
                if !bytes.is_empty() {
                    return (n.node_id(), id, idx, bytes);
                }
                last = format!("node {} empty snapshot id={id}", n.node_id());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no snapshot within {timeout:?}; last={last}");
}

async fn wait_purged(nodes: &[Arc<Broker>], timeout: Duration) -> (u32, u64) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for n in nodes {
            if let Some(idx) = n.test_openraft_last_purged_index() {
                return (n.node_id(), idx);
            }
        }
        for n in nodes {
            let _ = n.test_openraft_trigger_purge().await;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let seen: Vec<_> = nodes
        .iter()
        .map(|n| (n.node_id(), n.test_openraft_last_purged_index()))
        .collect();
    panic!("last_purged did not advance within {timeout:?}; seen={seen:?}");
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

fn clustered_broker(label: &str, id: u32) -> (Arc<Broker>, std::path::PathBuf, Guard) {
    let base = unique_dir("v22", label);
    let guard = Guard(base.clone());
    let cfg = cluster_config([19091, 19092, 19093]);
    let dir = base.join(format!("n{id}"));
    let b = Broker::with_cluster(default_storage(dir.clone()), id, cfg).unwrap();
    b.set_advertised("127.0.0.1", 19090 + id as u16);
    (Arc::new(b), dir, guard)
}

/// Snapshot payload with a topic; install on a fresh empty broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn install_snapshot_applies_assignment_to_empty_broker() {
    set_openraft_env(false, None);
    let (dest, dest_dir, _g) = clustered_broker("empty", 1);
    assert!(
        !live_has_topic(&dest, "snap-topic"),
        "fresh broker must start with empty assignment"
    );
    assert!(!disk_has_topic(&dest_dir, "snap-topic"));

    let bytes = Broker::test_openraft_snapshot_bytes(&sample_assignment("snap-topic", 7));
    dest.test_openraft_sm_install_snapshot(bytes).await;

    assert!(
        live_has_topic(&dest, "snap-topic"),
        "install_snapshot must apply snapshot assignment to live topics"
    );
    assert!(
        disk_has_topic(&dest_dir, "snap-topic"),
        "install_snapshot must write assignment.json"
    );
    let asg = dest.clone_live_assignment().expect("clustered");
    assert_eq!(asg.generation, 7);
}

/// Empty snapshot assignment must not wipe an existing live assignment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_snapshot_assignment_does_not_wipe_live() {
    set_openraft_env(false, None);
    let (dest, dest_dir, _g) = clustered_broker("keep", 1);
    dest.apply_cluster_state(3, 1, &sample_assignment("keep-me", 3).to_wire_topics())
        .unwrap();
    assert!(live_has_topic(&dest, "keep-me"));
    assert!(disk_has_topic(&dest_dir, "keep-me"));

    let empty = Broker::test_openraft_snapshot_bytes(&AssignmentSnapshot::default());
    dest.test_openraft_sm_install_snapshot(empty).await;

    assert!(
        live_has_topic(&dest, "keep-me"),
        "empty snapshot assignment must not wipe live topics"
    );
    assert!(
        disk_has_topic(&dest_dir, "keep-me"),
        "empty snapshot assignment must not wipe assignment.json"
    );
    let asg = dest.clone_live_assignment().expect("clustered");
    assert_eq!(asg.generation, 3);
}

/// 2-of-3 majority CreateTopic + snapshot; late third node gets the topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_node_installs_snapshot_assignment() {
    set_openraft_env(true, Some("1"));
    let (mut t, _g, held_l3) = Triple::boot("lag", false).await;
    let l3 = held_l3.expect("third listener held");
    let majority = t.live(&[1, 2]);
    let leader_id = wait_agreed_leader(&majority, Duration::from_secs(8)).await;
    let admin = connect_leader(t.port_of(leader_id)).await;
    admin.create_topic("late-snap", 1).await.unwrap();
    wait_all_have_topic(&majority, "late-snap", Duration::from_secs(8)).await;

    let leader = t.broker(leader_id);
    let _ = leader.test_openraft_trigger_snapshot().await;
    let (_nid, _sid, _idx, bytes) = wait_snapshot(&majority, Duration::from_secs(8)).await;
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("late-snap"),
        "snapshot payload should include the topic: {body}"
    );
    let _ = leader.test_openraft_trigger_purge().await;
    let _ = wait_purged(&majority, Duration::from_secs(6)).await;

    t.start_third(l3);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let all = t.live(&[1, 2, 3]);
    let _ = wait_agreed_leader(&all, Duration::from_secs(12)).await;
    wait_all_have_topic(&[Arc::clone(&t.b3)], "late-snap", Duration::from_secs(12)).await;
    assert!(
        live_has_topic(&t.b3, "late-snap"),
        "late node must have topic after InstallSnapshot or catch-up; rx={}",
        t.b3.test_openraft_install_snapshot_rx()
    );
    assert!(
        disk_has_topic(&t.dirs[2], "late-snap"),
        "late node assignment.json must include the topic"
    );
    t.abort_all();
}
