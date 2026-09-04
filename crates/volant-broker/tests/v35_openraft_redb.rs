//! v0.35 — openraft log store on redb (`{data_dir}/__openraft/raft.redb`).
//!
//! Flag off must not create `__openraft/`. Flag on persists `raft.redb` after
//! CreateTopic. Restart on the same data_dirs re-elects and keeps the topic.
//! Many appends + snapshot purge still advance `last_purged`.

#[path = "common/mod.rs"]
mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::cluster::{bind_port0, cluster_config, default_storage, unique_dir, Guard};
use volant_broker::{serve_listener, Broker, OPENRAFT_DIR, OPENRAFT_LOG_FILE, OPENRAFT_REDB_FILE};

fn set_openraft_env(on: bool, snapshot_logs: Option<&str>) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
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

fn openraft_dir(base: &Path, id: u32) -> PathBuf {
    base.join(format!("n{id}")).join(OPENRAFT_DIR)
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
        let base = unique_dir("v35", label);
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
        // Release raft.redb exclusive locks before the next process opens them.
        self.b1.drop_openraft_metadata();
        self.b2.drop_openraft_metadata();
        self.b3.drop_openraft_metadata();
        self.abort_all();
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(self);
        tokio::time::sleep(Duration::from_millis(100)).await;
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

async fn connect_leader(port: u16) -> volant_client::Client {
    volant_client::Client::connect(volant_client::ClientConfig {
        brokers: vec![format!("127.0.0.1:{port}")],
        acks: 1,
        ..volant_client::ClientConfig::default()
    })
    .await
    .unwrap()
}

async fn write_noops(leader: &Broker, n: usize) {
    for i in 0..n {
        let mut last_err = None;
        for _ in 0..20 {
            match leader.test_openraft_client_write_noop().await {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        if let Some(e) = last_err {
            panic!("noop {i} failed: {e}");
        }
    }
}

async fn wait_snapshot(nodes: &[Arc<Broker>], timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for n in nodes {
            if let Some((_, _, bytes)) = n.test_openraft_current_snapshot().await {
                if !bytes.is_empty() {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no snapshot within {timeout:?}");
}

async fn wait_purged(nodes: &[Arc<Broker>], timeout: Duration) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for n in nodes {
            if let Some(idx) = n.test_openraft_last_purged_index() {
                return idx;
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

/// Flag off: CreateTopic must not create `{data_dir}/__openraft/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_off_does_not_create_openraft_dir() {
    set_openraft_env(false, None);
    let (t, _g) = Triple::boot("off").await;
    assert!(
        !t.b1.openraft_metadata_enabled(),
        "VOLANT_OPENRAFT_METADATA=0 keeps lowest-id"
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
        assert!(
            !dir.join(OPENRAFT_REDB_FILE).exists(),
            "flag off must not create raft.redb"
        );
    }

    t.abort_all();
}

/// Flag on: after CreateTopic, `{data_dir}/__openraft/raft.redb` exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flag_on_create_topic_writes_raft_redb() {
    set_openraft_env(true, None);
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
    let redb = leader_dir.join(OPENRAFT_REDB_FILE);
    assert!(
        redb.is_file(),
        "leader {leader} missing raft.redb at {}",
        redb.display()
    );
    // Incremental redb writes; do not rewrite the v0.21 full-file log.
    assert!(
        !leader_dir.join(OPENRAFT_LOG_FILE).exists(),
        "v0.35 must not rewrite log.json"
    );

    let any_redb =
        (1u32..=3).any(|id| openraft_dir(&t.base, id).join(OPENRAFT_REDB_FILE).is_file());
    assert!(any_redb, "expected raft.redb on at least one node");

    t.abort_all();
}

/// 3-node restart: same data_dirs, re-elect, topic still present, live leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_reelects_and_keeps_topic() {
    set_openraft_env(true, None);
    let (t, _g) = Triple::boot("restart").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;

    let admin = connect_leader(t.port_of(leader)).await;
    admin.create_topic("keep", 1).await.unwrap();
    wait_all_have_topic(&nodes, "keep", Duration::from_secs(8)).await;

    assert!(
        (1u32..=3).any(|id| openraft_dir(&t.base, id).join(OPENRAFT_REDB_FILE).is_file()),
        "pre-restart raft.redb missing under __openraft/"
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
    assert!(
        openraft_dir(&t.base, leader)
            .join(OPENRAFT_REDB_FILE)
            .is_file(),
        "raft.redb must still exist after restart"
    );

    t.abort_all();
}

/// Many appends + snapshot purge still work against the redb log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_appends_snapshot_purge_still_works() {
    set_openraft_env(true, Some("1"));
    let (t, _g) = Triple::boot("purge").await;
    let nodes = t.live(&[1, 2, 3]);
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = t.broker(leader_id);
    write_noops(&leader, 12).await;
    let _ = leader.test_openraft_trigger_snapshot().await;
    wait_snapshot(&nodes, Duration::from_secs(8)).await;
    let purged = wait_purged(&nodes, Duration::from_secs(10)).await;
    assert!(purged > 0, "last_purged should advance, got {purged}");
    write_noops(&leader, 4).await;
    assert!(
        openraft_dir(&t.base, leader_id)
            .join(OPENRAFT_REDB_FILE)
            .is_file(),
        "raft.redb must survive snapshot purge"
    );
    t.abort_all();
}
