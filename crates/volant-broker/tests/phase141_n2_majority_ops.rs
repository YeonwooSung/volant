//! Phase 141: N=2 majority ops tooling — health gauges + Broker helpers.
//!
//! In-process membership only (no TCP). Journal majority uses **configured N**
//! (`floor(N/2)+1`); when `live < majority`, journal wait can never succeed.

use std::path::PathBuf;
use std::sync::Arc;

use volant_broker::{render_metrics, Broker, BrokerEndpoint, ClusterConfig};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p141-{label}-{}-{}",
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

fn storage(dir: PathBuf) -> StorageConfig {
    StorageConfig {
        data_dir: dir,
        flush_every_n: 1,
        ..StorageConfig::default()
    }
}

fn cluster_config_n(ids_ports: &[(u32, u16)]) -> ClusterConfig {
    let n = ids_ports.len() as u32;
    ClusterConfig {
        default_replication_factor: n.min(3),
        min_insync_replicas: ((n / 2) + 1).min(n),
        session_timeout_ms: 30_000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: ids_ports
            .iter()
            .map(|(id, port)| BrokerEndpoint {
                id: *id,
                host: "127.0.0.1".into(),
                port: *port,
                rack: None,
            })
            .collect(),
    }
}

fn boot_n(base: &std::path::Path, ids_ports: &[(u32, u16)]) -> Vec<Arc<Broker>> {
    let cfg = cluster_config_n(ids_ports);
    ids_ports
        .iter()
        .map(|(id, port)| {
            let b = Broker::with_cluster(storage(base.join(format!("n{id}"))), *id, cfg.clone())
                .unwrap();
            b.set_advertised("127.0.0.1", *port);
            Arc::new(b)
        })
        .collect()
}

fn assert_ops(b: &Broker, configured: u64, live: u64, quorum: u64, impossible: bool) {
    assert_eq!(b.configured_broker_count(), configured, "configured");
    assert_eq!(b.live_broker_count(), live, "live");
    assert_eq!(b.majority_quorum_size(), quorum, "quorum");
    assert_eq!(b.majority_impossible(), impossible, "impossible");
}

#[test]
fn single_node_majority_reachable() {
    let dir = unique_dir("single");
    let _g = Guard(dir.clone());
    let b = Broker::new(storage(dir));
    assert_ops(&b, 1, 1, 1, false);
}

#[test]
fn n3_all_live_majority_reachable() {
    let base = unique_dir("n3-live");
    let _g = Guard(base.clone());
    let nodes = boot_n(&base, &[(1, 14101), (2, 14102), (3, 14103)]);
    for b in &nodes {
        assert_ops(b, 3, 3, 2, false);
    }
}

#[test]
fn n2_both_live_majority_reachable() {
    let base = unique_dir("n2-live");
    let _g = Guard(base.clone());
    let nodes = boot_n(&base, &[(1, 14111), (2, 14112)]);
    for b in &nodes {
        assert_ops(b, 2, 2, 2, false);
    }
}

#[test]
fn n2_one_dead_majority_impossible() {
    let base = unique_dir("n2-dead");
    let _g = Guard(base.clone());
    let nodes = boot_n(&base, &[(1, 14121), (2, 14122)]);
    let b1 = &nodes[0];
    assert_ops(b1, 2, 2, 2, false);

    b1.on_broker_death(2).unwrap();
    assert_ops(b1, 2, 1, 2, true);
    // Self cannot mark itself dead via death path; peer still sees full live until
    // it observes the death. Configured N unchanged.
    assert_eq!(b1.configured_broker_count(), 2);
    assert_eq!(b1.majority_quorum_size(), 2);
    assert!(b1.majority_impossible());
}

#[test]
fn n3_one_dead_majority_still_reachable() {
    let base = unique_dir("n3-dead");
    let _g = Guard(base.clone());
    let nodes = boot_n(&base, &[(1, 14131), (2, 14132), (3, 14133)]);
    let b1 = &nodes[0];
    assert_ops(b1, 3, 3, 2, false);

    b1.on_broker_death(3).unwrap();
    // live=2, quorum=2 → still reachable
    assert_ops(b1, 3, 2, 2, false);
}

#[test]
fn metrics_text_includes_cluster_majority_gauges() {
    let base = unique_dir("metrics");
    let _g = Guard(base.clone());
    let nodes = boot_n(&base, &[(1, 14141), (2, 14142)]);
    let b1 = &nodes[0];
    b1.on_broker_death(2).unwrap();
    assert!(b1.majority_impossible());

    let text = render_metrics(b1);
    for name in [
        "volant_cluster_configured_brokers",
        "volant_cluster_live_brokers",
        "volant_cluster_majority_quorum",
        "volant_cluster_majority_impossible",
    ] {
        assert!(
            text.contains(&format!("# TYPE {name} gauge")),
            "missing TYPE for {name}"
        );
        assert!(text.contains(name), "missing series {name}");
    }
    assert!(
        text.contains("volant_cluster_configured_brokers 2\n"),
        "configured value"
    );
    assert!(
        text.contains("volant_cluster_live_brokers 1\n"),
        "live value"
    );
    assert!(
        text.contains("volant_cluster_majority_quorum 2\n"),
        "quorum value"
    );
    assert!(
        text.contains("volant_cluster_majority_impossible 1\n"),
        "impossible value"
    );
}

#[test]
fn metrics_text_single_node_zeros_impossible() {
    let dir = unique_dir("metrics-single");
    let _g = Guard(dir.clone());
    let b = Broker::new(storage(dir));
    let text = render_metrics(&b);
    assert!(text.contains("volant_cluster_configured_brokers 1\n"));
    assert!(text.contains("volant_cluster_live_brokers 1\n"));
    assert!(text.contains("volant_cluster_majority_quorum 1\n"));
    assert!(text.contains("volant_cluster_majority_impossible 0\n"));
}
