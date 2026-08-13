//! Phase 145: rack-aware partition assignment MVP on topic create.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use volant_broker::cluster::{
    assign_replicas, assign_replicas_round_robin, rack_aware_assignment_enabled,
};
use volant_broker::{Broker, BrokerEndpoint, ClusterConfig};
use volant_storage::StorageConfig;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct EnvRestore {
    key: &'static str,
    prev: Option<String>,
}

impl EnvRestore {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: tests serialize env mutations via env_lock.
        std::env::set_var(key, value);
        Self { key, prev }
    }

    fn remove(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
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

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p145-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn multi_rack_brokers() -> Vec<(u32, Option<&'static str>)> {
    // racks a, a, b — RF=2 can always span both racks.
    vec![(1, Some("a")), (2, Some("a")), (3, Some("b"))]
}

/// 1. Multi-rack 3 brokers racks a,a,b with RF=2 → each partition spans both racks.
#[test]
fn multi_rack_rf2_spans_both_racks() {
    let _lock = env_lock().lock().unwrap();
    let _env = EnvRestore::remove("VOLANT_RACK_AWARE_ASSIGNMENT");
    assert!(rack_aware_assignment_enabled());

    let brokers = multi_rack_brokers();
    let (parts, rack_aware) = assign_replicas("events", 8, brokers.iter().copied(), 2);
    assert!(rack_aware);
    let rack_of = |id: u32| match id {
        1 | 2 => "a",
        3 => "b",
        _ => panic!("bad id {id}"),
    };
    for (i, p) in parts.iter().enumerate() {
        assert_eq!(p.len(), 2, "partition {i}");
        assert_ne!(
            rack_of(p[0]),
            rack_of(p[1]),
            "partition {i} must span racks: {p:?}"
        );
        assert_ne!(p[0], p[1]);
    }
}

/// 2. No racks → same as legacy round-robin.
#[test]
fn no_racks_matches_legacy_round_robin() {
    let _lock = env_lock().lock().unwrap();
    let _env = EnvRestore::remove("VOLANT_RACK_AWARE_ASSIGNMENT");

    let brokers: Vec<(u32, Option<&str>)> = vec![(1, None), (2, None), (3, None)];
    let (parts, rack_aware) = assign_replicas("events", 5, brokers.iter().copied(), 3);
    assert!(!rack_aware);
    let legacy = assign_replicas_round_robin("events", 5, &[1, 2, 3], 3);
    assert_eq!(parts, legacy);
}

/// 3. Single rack → no panic; RF filled; legacy path.
#[test]
fn single_rack_fills_rf() {
    let _lock = env_lock().lock().unwrap();
    let _env = EnvRestore::remove("VOLANT_RACK_AWARE_ASSIGNMENT");

    let brokers: Vec<(u32, Option<&str>)> =
        vec![(1, Some("r")), (2, Some("r")), (3, Some("r"))];
    let (parts, rack_aware) = assign_replicas("single", 4, brokers.iter().copied(), 3);
    assert!(!rack_aware);
    for p in &parts {
        assert_eq!(p.len(), 3);
        let mut s = p.clone();
        s.sort();
        assert_eq!(s, vec![1, 2, 3]);
    }
    let legacy = assign_replicas_round_robin("single", 4, &[1, 2, 3], 3);
    assert_eq!(parts, legacy);
}

/// 4. Env off → legacy placement even with multi-rack.
#[test]
fn env_off_uses_legacy_even_with_multi_rack() {
    let _lock = env_lock().lock().unwrap();
    let _env = EnvRestore::set("VOLANT_RACK_AWARE_ASSIGNMENT", "0");
    assert!(!rack_aware_assignment_enabled());

    let brokers = multi_rack_brokers();
    let (parts, rack_aware) = assign_replicas("events", 5, brokers.iter().copied(), 2);
    assert!(!rack_aware);
    let legacy = assign_replicas_round_robin("events", 5, &[1, 2, 3], 2);
    assert_eq!(parts, legacy);
}

/// 5. Integration: create_topic on 3-node multi-rack cluster; p0 replicas rack-diverse.
#[test]
fn create_topic_multi_rack_cluster_metadata() {
    let _lock = env_lock().lock().unwrap();
    let _env = EnvRestore::remove("VOLANT_RACK_AWARE_ASSIGNMENT");

    let base = unique_dir("create");
    let _g = Guard(base.clone());

    let cfg = ClusterConfig {
        default_replication_factor: 2,
        min_insync_replicas: 1,
        session_timeout_ms: 30_000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: vec![
            BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 19451,
                rack: Some("a".into()),
            },
            BrokerEndpoint {
                id: 2,
                host: "127.0.0.1".into(),
                port: 19452,
                rack: Some("a".into()),
            },
            BrokerEndpoint {
                id: 3,
                host: "127.0.0.1".into(),
                port: 19453,
                rack: Some("b".into()),
            },
        ],
    };

    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("node-{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", 19450 + id as u16);
        std::sync::Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    let before = b1.rack_aware_assignment_total();
    b1.create_topic("racked", 3).unwrap();
    assert!(
        b1.rack_aware_assignment_total() > before,
        "create should bump rack-aware metric"
    );

    // Propagate assignment so all nodes see topic metadata.
    for _ in 0..40 {
        let (_, gen, cid, topics) = b1.cluster_state_snapshot();
        let _ = b2.apply_cluster_state(gen, cid, &topics);
        let _ = b3.apply_cluster_state(gen, cid, &topics);
        if b2.partition_count_opt("racked").is_some()
            && b3.partition_count_opt("racked").is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let meta = b1.metadata(None);
    let topic = meta
        .topics
        .iter()
        .find(|t| t.name.as_str() == "racked")
        .expect("topic");
    assert_eq!(topic.partitions.len(), 3);

    let rack_of = |id: u32| match id {
        1 | 2 => "a",
        3 => "b",
        _ => panic!("bad id"),
    };
    let p0 = &topic.partitions[0];
    assert_eq!(p0.replicas.len(), 2);
    assert_ne!(
        rack_of(p0.replicas[0]),
        rack_of(p0.replicas[1]),
        "p0 replicas must span racks: {:?}",
        p0.replicas
    );
    // Leader is first replica.
    assert_eq!(p0.leader, p0.replicas[0]);

    // Every partition diverse when RF=2 on this topology.
    for p in &topic.partitions {
        let racks: HashSet<_> = p.replicas.iter().map(|id| rack_of(*id)).collect();
        assert_eq!(racks.len(), 2, "partition {:?}: {:?}", p.partition_id, p.replicas);
    }
}
