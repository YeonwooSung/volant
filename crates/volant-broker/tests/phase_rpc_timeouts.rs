//! Inter-broker RPC timeout (5s default) and DeleteRecords fan-out budget (20s).

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use volant_broker::{
    delete_records_fanout_budget, fanout_delete_records, inter_broker_rpc,
    inter_broker_rpc_timeout, serve_listener, Broker, BrokerEndpoint, ClusterConfig,
    DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS, DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS,
    MAX_INTER_BROKER_TIMEOUT_MS, MIN_INTER_BROKER_TIMEOUT_MS,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_protocol::Request;
use volant_storage::StorageConfig;

/// Serialize tests that mutate process-global env vars.
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Restore a previous env value on drop (including panic).
struct EnvRestore {
    key: &'static str,
    prev: Option<String>,
}

impl EnvRestore {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn unique_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-rpc-to-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(std::path::PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn default_timeout_constants() {
    assert_eq!(DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS, 5_000);
    assert_eq!(DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS, 20_000);
    assert_eq!(MIN_INTER_BROKER_TIMEOUT_MS, 1);
    assert_eq!(MAX_INTER_BROKER_TIMEOUT_MS, 600_000);
}

/// Env `0` clamps to 1ms; huge values clamp to MAX (10 min). Not “disable”.
#[test]
fn env_timeout_clamps_zero_and_huge() {
    let _lock = env_lock().lock().unwrap();
    let _rpc = EnvRestore::set("VOLANT_INTER_BROKER_RPC_TIMEOUT_MS", "0");
    let _bud = EnvRestore::set("VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS", "0");
    assert_eq!(
        inter_broker_rpc_timeout(),
        Duration::from_millis(MIN_INTER_BROKER_TIMEOUT_MS)
    );
    assert_eq!(
        delete_records_fanout_budget(),
        Duration::from_millis(MIN_INTER_BROKER_TIMEOUT_MS)
    );

    // Drop prior guards and set huge values (re-capture for restore).
    drop(_rpc);
    drop(_bud);
    let _rpc = EnvRestore::set("VOLANT_INTER_BROKER_RPC_TIMEOUT_MS", "999999999");
    let _bud = EnvRestore::set("VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS", "999999999");
    assert_eq!(
        inter_broker_rpc_timeout(),
        Duration::from_millis(MAX_INTER_BROKER_TIMEOUT_MS)
    );
    assert_eq!(
        delete_records_fanout_budget(),
        Duration::from_millis(MAX_INTER_BROKER_TIMEOUT_MS)
    );
}

/// Connecting to a black-hole address must fail within ~timeout, not hang.
#[tokio::test]
async fn inter_broker_rpc_times_out_on_blackhole() {
    let _lock = env_lock().lock().unwrap();
    let base = unique_dir("rpc");
    let _g = Guard(base.clone());
    // Short timeout for a fast test.
    let _env = EnvRestore::set("VOLANT_INTER_BROKER_RPC_TIMEOUT_MS", "200");

    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: base.join("n1"),
        ..StorageConfig::default()
    }));
    // TEST-NET-1 documentation range — typically drops / no service.
    // Use 127.0.0.1 on an unbound high port: connect fails fast OR
    // use a listening-but-never-reading peer for true hang → timeout.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    // Accept but never read — client blocks until timeout.
    let accept = tokio::spawn(async move {
        let (_sock, _) = listener.accept().await.unwrap();
        // Hold connection open without reading.
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let t0 = Instant::now();
    let res = inter_broker_rpc(
        &broker,
        &addr,
        &Request::Metadata {
            topics: vec![],
        },
    )
    .await;
    let elapsed = t0.elapsed();
    assert!(res.is_err(), "expected timeout error, got {res:?}");
    let msg = format!("{}", res.err().unwrap());
    assert!(
        msg.contains("timed out"),
        "error should mention timeout: {msg}"
    );
    // Should complete near 200ms, not hang for tens of seconds.
    assert!(
        elapsed < Duration::from_secs(2),
        "timeout took too long: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(150),
        "returned too fast to be a real timeout: {elapsed:?}"
    );

    accept.abort();
}

/// Fan-out with a black-hole peer finishes near the overall budget (not hang forever).
#[tokio::test]
async fn delete_records_fanout_respects_overall_budget() {
    let _lock = env_lock().lock().unwrap();
    let base = unique_dir("budget");
    let _g = Guard(base.clone());
    // Tight budgets for a fast test.
    let _rpc = EnvRestore::set("VOLANT_INTER_BROKER_RPC_TIMEOUT_MS", "300");
    let _bud = EnvRestore::set("VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS", "800");

    // Bind two ports first so cluster config can advertise them.
    let (l1, p1) = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        (l, p)
    };
    let (l2, p2) = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        (l, p)
    };

    let cfg = ClusterConfig {
        default_replication_factor: 2,
        min_insync_replicas: 1,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
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
    let b1 = Arc::new(
        Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("n1"),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            1,
            cfg.clone(),
        )
        .unwrap(),
    );
    b1.set_advertised("127.0.0.1", p1);
    let b2 = Arc::new(
        Broker::with_cluster(
            StorageConfig {
                data_dir: base.join("n2"),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            2,
            cfg,
        )
        .unwrap(),
    );
    b2.set_advertised("127.0.0.1", p2);
    b1.note_peer_live(2);
    b2.note_peer_live(1);

    b1.create_topic("t", 1).unwrap();
    for _ in 0..20 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("t").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let topic = TopicName::new("t");
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let (leader, follower_id, leader_listener, blackhole_listener) = if leader_id == 1 {
        (Arc::clone(&b1), 2u32, l1, l2)
    } else {
        (Arc::clone(&b2), 1u32, l2, l1)
    };

    // Leader serves normally; follower port is a black-hole (accept, never read).
    let s_leader = {
        let b = Arc::clone(&leader);
        tokio::spawn(async move {
            serve_listener(leader_listener, b).await.ok();
        })
    };
    let blackhole = tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = blackhole_listener.accept().await else {
                break;
            };
            let _ = sock;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    for i in 0..3 {
        let mut batch = MessageBatch::default();
        batch
            .messages
            .push(Message::from_value(format!("m{i}")));
        let (_, err) = leader
            .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
            .unwrap();
        assert_eq!(err, 0);
    }
    let (_low, err) = leader.delete_records("t", 0, 1).unwrap();
    assert_eq!(err, 0);

    let peers = leader.delete_records_fanout_peers("t", 0);
    assert!(
        peers.iter().any(|(id, _, _)| *id == follower_id),
        "expected follower {follower_id} in fan-out list, got {peers:?}"
    );

    let t0 = Instant::now();
    fanout_delete_records(leader.as_ref(), "t", 0, 1).await;
    let elapsed = t0.elapsed();

    // Overall budget 800ms — must not hang for many seconds.
    assert!(
        elapsed < Duration::from_secs(3),
        "fan-out exceeded reasonable bound: {elapsed:?}"
    );
    // Black-hole RPC should approach the 300ms per-RPC timeout.
    assert!(
        elapsed >= Duration::from_millis(200),
        "returned suspiciously fast (no black-hole wait?): {elapsed:?}"
    );

    s_leader.abort();
    blackhole.abort();
}
