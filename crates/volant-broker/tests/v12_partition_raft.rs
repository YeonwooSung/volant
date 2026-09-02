//! v0.12 — `__cluster_metadata` topic + per-partition Raft log MVP.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, default_storage, propagate_async, unique_dir, Guard,
};
use volant_broker::{
    assignment_path, Broker, BrokerEndpoint, ClusterConfig, PartitionRaftGroup,
    CLUSTER_METADATA_TOPIC,
};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
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

fn cluster_config_n1(port: u16) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 1,
        min_insync_replicas: 1,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: vec![BrokerEndpoint {
            id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }],
    }
}

fn boot_n1(dir: &std::path::Path) -> Broker {
    let b = Broker::with_cluster(
        default_storage(dir.to_path_buf()),
        1,
        cluster_config_n1(19092),
    )
    .unwrap();
    b.set_advertised("127.0.0.1", 19092);
    b
}

fn batch_value(s: impl Into<String>) -> MessageBatch {
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value(s.into()));
    batch
}

/// Flag off: no `__cluster_metadata` auto-create; produce/fetch unchanged.
#[test]
fn flag_off_no_cluster_metadata_topic_produce_unchanged() {
    let dir = unique_dir("v12", "flag-off");
    let _g = Guard(dir.clone());
    let _env = EnvGuard::set("VOLANT_CLUSTER_METADATA_TOPIC", "0");
    let broker = boot_n1(&dir);
    assert!(!broker.cluster_metadata_topic_enabled());

    let topic = TopicName::new("events");
    broker.create_topic(topic.clone(), 1).unwrap();
    broker
        .produce(&topic, PartitionId(0), batch_value("hello"))
        .unwrap();
    let got = broker
        .fetch(&topic, PartitionId(0), volant_core::Offset::ZERO, 10)
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].value.as_ref(), b"hello");

    assert!(
        broker.partition_count_opt(CLUSTER_METADATA_TOPIC).is_none(),
        "__cluster_metadata must not be auto-created when the flag is off"
    );
    assert!(
        !dir.join(CLUSTER_METADATA_TOPIC).exists(),
        "no on-disk __cluster_metadata dir when flag is off"
    );
    assert!(broker.last_cluster_metadata_snapshot().is_none());
}

/// Flag on: CreateTopic appends a snapshot whose value contains the topic name.
#[test]
fn flag_on_create_topic_appends_cluster_metadata_record() {
    let dir = unique_dir("v12", "flag-on");
    let _g = Guard(dir.clone());
    let _env = EnvGuard::set("VOLANT_CLUSTER_METADATA_TOPIC", "1");
    let broker = boot_n1(&dir);
    assert!(broker.cluster_metadata_topic_enabled());
    assert!(
        broker.partition_count_opt(CLUSTER_METADATA_TOPIC).is_some(),
        "controller must ensure __cluster_metadata on start"
    );

    let topic = TopicName::new("orders");
    broker.create_topic(topic.clone(), 1).unwrap();
    let gen = broker.generation();
    assert!(gen >= 1, "generation={gen}");

    let (rec_gen, snap) = broker
        .last_cluster_metadata_snapshot()
        .expect("__cluster_metadata must have a record");
    assert_eq!(rec_gen, gen);
    assert_eq!(snap.generation, gen);
    let raw = serde_json::to_string(&snap).unwrap();
    assert!(
        raw.contains("orders"),
        "snapshot value must contain the created topic: {raw}"
    );
    assert!(snap.topics.contains_key("orders"));
}

/// Wipe assignment.json and reopen: topic restored from `__cluster_metadata`.
#[test]
fn wipe_assignment_rebuilds_from_cluster_metadata() {
    let dir = unique_dir("v12", "rebuild");
    let _g = Guard(dir.clone());
    let _env = EnvGuard::set("VOLANT_CLUSTER_METADATA_TOPIC", "1");

    let gen;
    {
        let broker = boot_n1(&dir);
        broker.create_topic(TopicName::new("restored"), 2).unwrap();
        gen = broker.generation();
        assert!(broker.partition_count_opt("restored").is_some());
        assert!(broker.last_cluster_metadata_snapshot().is_some());
    }

    let asg_path = assignment_path(&dir);
    assert!(asg_path.exists(), "assignment.json written");
    std::fs::remove_file(&asg_path).unwrap();
    assert!(!asg_path.exists());

    let broker = boot_n1(&dir);
    assert_eq!(
        broker.partition_count_opt("restored"),
        Some(2),
        "topic must be rebuilt from __cluster_metadata"
    );
    assert_eq!(broker.generation(), gen);
    let (_g2, snap) = broker
        .last_cluster_metadata_snapshot()
        .expect("metadata log survives reopen");
    assert!(snap.topics.contains_key("restored"));
}

/// In-process 3-replica partition Raft: majority commits; minority does not.
#[test]
fn partition_raft_majority_commit_and_minority_does_not() {
    let dir = unique_dir("v12", "praft-group");
    let _g = Guard(dir.clone());

    let group = PartitionRaftGroup::open(&dir, "events", 0, &[1, 2, 3], 1);

    // Majority: leader + one follower (2 of 3).
    let (idx, committed) = group.append_replicated(10, 0x1111, &[1, 2]);
    assert_eq!(idx, 1);
    assert!(committed, "majority of 3 must commit");
    assert_eq!(group.replica(1).unwrap().commit_index(), 1);
    assert_eq!(group.replica(2).unwrap().commit_index(), 1);
    assert_eq!(
        group.replica(3).unwrap().commit_index(),
        0,
        "non-acking follower must not see commit"
    );
    let applied_l = group.apply(1);
    let applied_f = group.apply(2);
    assert_eq!(applied_l.len(), 1);
    assert_eq!(applied_f.len(), 1);
    assert_eq!(applied_l[0].payload.offset, 10);
    assert_eq!(applied_f[0].payload.crc, 0x1111);
    assert!(group.apply(3).is_empty());

    // Minority: only the leader acks (1 of 3).
    let (idx2, committed2) = group.append_replicated(11, 0x2222, &[1]);
    assert_eq!(idx2, 2);
    assert!(!committed2, "1 of 3 must not commit");
    assert_eq!(group.replica(1).unwrap().commit_index(), 1);
    assert!(group.apply(1).is_empty());
    assert_eq!(group.replica(1).unwrap().last_index(), 2);
}

/// Optional: 3-node cluster `acks=all` still works with both flags on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_acks_all_with_v12_flags_on() {
    let base = unique_dir("v12", "acks-all");
    let _g = Guard(base.clone());
    let _cmeta = EnvGuard::set("VOLANT_CLUSTER_METADATA_TOPIC", "1");
    let _praft = EnvGuard::set("VOLANT_PARTITION_RAFT", "1");

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config([p1, p2, p3]);

    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    let b1 = mk(1, p1);
    let b2 = mk(2, p2);
    let b3 = mk(3, p3);

    let _h1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = volant_broker::serve_listener(l1, b).await;
        })
    };
    let _h2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = volant_broker::serve_listener(l2, b).await;
        })
    };
    let _h3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            let _ = volant_broker::serve_listener(l3, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Propagate the controller-created __cluster_metadata topic.
    propagate_async(&[&b1, &b2, &b3], CLUSTER_METADATA_TOPIC).await;

    let admin = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 1).await.unwrap();
    propagate_async(&[&b1, &b2, &b3], "events").await;

    let meta = admin.metadata().await.unwrap();
    let events = meta
        .topics
        .iter()
        .find(|t| t.name == "events")
        .expect("events topic");
    let leader_id = events.partitions[0].leader;
    let port_of = |id: u32| match id {
        1 => p1,
        2 => p2,
        3 => p3,
        _ => panic!("bad id"),
    };

    let producer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    for i in 0..3u32 {
        let r = producer
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(format!("v12-{i}"))],
                255,
            )
            .await
            .expect("acks=all produce with v0.12 flags on");
        assert_eq!(r.count, 1);
        assert_eq!(r.base_offset, i as u64);
    }

    let leader = match leader_id {
        1 => &b1,
        2 => &b2,
        _ => &b3,
    };
    // Flag-on create enables the log on replicas that apply the assignment.
    assert!(
        leader.partition_raft_enabled_for("events", 0)
            || b1.partition_raft_enabled_for("events", 0),
        "VOLANT_PARTITION_RAFT=1 must enable raft on new topics"
    );
    if leader.partition_raft_enabled_for("events", 0) {
        assert!(
            leader.partition_raft_last_index("events", 0) >= 1,
            "acks=all produce must dual-write a raft entry on the leader"
        );
    }

    let (rec_gen, snap) = b1
        .last_cluster_metadata_snapshot()
        .expect("controller __cluster_metadata record");
    assert_eq!(rec_gen, b1.generation());
    assert!(snap.topics.contains_key("events"));
}
