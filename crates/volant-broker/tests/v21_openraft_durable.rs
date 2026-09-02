//! v0.21 — durable openraft log + vote/hard state under `__openraft/`.
//!
//! Flag off must not create the dir. Flag on persists after CreateTopic.
//! Process restart on the same data_dirs re-elects and keeps the topic.

#[path = "common/mod.rs"]
mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, default_storage, unique_dir, Guard};
use volant_broker::{
    serve_listener, Broker, OPENRAFT_DIR, OPENRAFT_HARD_STATE_FILE, OPENRAFT_LOG_FILE,
    OPENRAFT_SNAPSHOT_FILE,
};
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

fn openraft_dir(base: &Path, id: u32) -> PathBuf {
    base.join(format!("n{id}")).join(OPENRAFT_DIR)
}

fn openraft_has_store_files(dir: &Path) -> bool {
    dir.join(OPENRAFT_LOG_FILE).is_file()
        || dir.join(OPENRAFT_HARD_STATE_FILE).is_file()
        || dir.join(OPENRAFT_SNAPSHOT_FILE).is_file()
}

struct Triple {
    base: PathBuf,
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    ports: [u16; 3],
    h1: tokio::task::JoinHandle<()>,
    h2: tokio::task::JoinHandle<()>,
    h3: tokio::task::JoinHandle<()>,
}

impl Triple {
    async fn boot_at(base: PathBuf) -> Self {
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
        Self {
            base,
            b1,
            b2,
            b3,
            ports,
            h1,
            h2,
            h3,
        }
    }

    async fn boot(label: &str) -> (Self, Guard) {
        let base = unique_dir("v21", label);
        let guard = Guard(base.clone());
        (Self::boot_at(base).await, guard)
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

    /// Drop this cluster and start new processes on the same data_dirs.
    async fn restart(self) -> Self {
        let base = self.base.clone();
        self.abort_all();
        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(self);
        Self::boot_at(base).await
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

async fn connect_leader(port: u16) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{port}")],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap()
}

/// Flag off: CreateTopic must not create `{data_dir}/__openraft/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_off_does_not_create_openraft_dir() {
    set_openraft_env(false);
    let (t, _g) = Triple::boot("off").await;
    assert!(
        !t.b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA must default off"
    );
    assert_eq!(t.b1.controller_id(), 1);

    let admin = connect_leader(t.ports[0]).await;
    admin.create_topic("plain", 1).await.unwrap();
    assert!(live_has_topic(&t.b1, "plain"));

    for id in 1u32..=3 {
        let dir = openraft_dir(&t.base, id);
        assert!(
            !dir.exists(),
            "flag off must not create {}: exists={}",
            dir.display(),
            dir.exists()
        );
    }

    t.abort_all();
}

/// Flag on: after CreateTopic, `__openraft/` has log or snapshot files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_persists_openraft_files() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("persist").await;
    assert!(t.b1.openraft_metadata_enabled());
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;

    let admin = connect_leader(t.port_of(leader)).await;
    admin.create_topic("events", 1).await.unwrap();
    wait_all_have_topic(&nodes, "events", Duration::from_secs(8)).await;

    let leader_dir = openraft_dir(&t.base, leader);
    assert!(
        leader_dir.is_dir(),
        "leader {leader} missing {}: exists={}",
        leader_dir.display(),
        leader_dir.exists()
    );
    assert!(
        openraft_has_store_files(&leader_dir),
        "leader {leader} {} has no log/hard_state/snapshot",
        leader_dir.display()
    );

    let any_files = (1u32..=3).any(|id| openraft_has_store_files(&openraft_dir(&t.base, id)));
    assert!(any_files, "expected persist files on at least one node");

    t.abort_all();
}

/// 3-node restart: same data_dirs, re-elect, topic still present, live leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_reelects_and_keeps_topic() {
    set_openraft_env(true);
    let (t, _g) = Triple::boot("restart").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;

    let admin = connect_leader(t.port_of(leader)).await;
    admin.create_topic("keep", 1).await.unwrap();
    wait_all_have_topic(&nodes, "keep", Duration::from_secs(8)).await;

    assert!(
        (1u32..=3).any(|id| openraft_has_store_files(&openraft_dir(&t.base, id))),
        "pre-restart persist missing under __openraft/"
    );

    let t = t.restart().await;
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(12)).await;
    assert!(
        (1..=3).contains(&leader),
        "restarted leader {leader} must be a live member"
    );
    for n in &nodes {
        assert_eq!(n.controller_id(), leader);
        assert!(
            live_has_topic(n, "keep"),
            "broker {} lost topic after restart (not only assignment.json leftover)",
            n.node_id()
        );
        assert!(
            n.openraft_metadata_enabled(),
            "openraft flag must stay on after restart"
        );
        assert!(
            n.openraft_term() > 0,
            "broker {} term must advance after restart",
            n.node_id()
        );
    }
    assert!(
        t.broker(leader).is_controller(),
        "controller_id {leader} must be the live openraft leader"
    );

    t.abort_all();
}
