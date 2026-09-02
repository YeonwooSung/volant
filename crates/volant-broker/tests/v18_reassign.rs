//! v0.18 partition reassignment after add-broker.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use volant_broker::{
    reassign_on_add_enabled, Broker, BrokerEndpoint, ClusterConfig, ENV_REASSIGN_ON_ADD,
};
use volant_core::{Error, Message, PartitionId, TopicName};

fn boot_n2(base: &std::path::Path, ports: [u16; 2]) -> (Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config_n2(ports);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    (mk(1, ports[0]), mk(2, ports[1]))
}

fn part_replicas(b: &Broker, topic: &str, pid: u32) -> Vec<u32> {
    let asg = b.clone_live_assignment().expect("cluster");
    asg.topics
        .get(topic)
        .and_then(|t| t.partitions.get(&pid))
        .map(|p| {
            let mut r = p.replicas.clone();
            r.sort_unstable();
            r
        })
        .unwrap_or_default()
}

fn assignment_gen(b: &Broker) -> u32 {
    b.clone_live_assignment().map(|a| a.generation).unwrap_or(0)
}

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, val);
        Self { key, prev }
    }

    fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::remove_var(key);
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

#[test]
fn add_broker_without_auto_keeps_replicas_explicit_reassign_adds() {
    let _env = EnvGuard::unset(ENV_REASSIGN_ON_ADD);
    assert!(!reassign_on_add_enabled());

    let base = unique_dir("v18", "explicit");
    let _g = Guard(base.clone());
    let (b1, b2) = boot_n2(&base, [19201, 19202]);
    let topic = TopicName::new("events");
    b1.create_topic(topic.clone(), 1).unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    let _ = b2.apply_cluster_state(gen, cid, &topics);

    assert_eq!(part_replicas(&b1, "events", 0), vec![1, 2]);
    let gen_before = assignment_gen(&b1);

    b1.add_broker(3, "127.0.0.1".into(), 19203, None).unwrap();
    assert_eq!(b1.configured_broker_count(), 3);
    assert_eq!(
        part_replicas(&b1, "events", 0),
        vec![1, 2],
        "flag-off add must not rewrite replica sets"
    );
    assert_eq!(assignment_gen(&b1), gen_before);

    let new_gen = b1
        .reassign_partitions("events", u32::MAX, &[1, 2, 3])
        .unwrap();
    assert!(
        new_gen > gen_before,
        "generation={new_gen} before={gen_before}"
    );
    let replicas = part_replicas(&b1, "events", 0);
    assert!(replicas.contains(&3), "replicas={replicas:?}");
    assert_eq!(replicas, vec![1, 2, 3]);
}

#[test]
fn new_replica_opens_local_partition_and_produce_acks1_still_works() {
    let _env = EnvGuard::unset(ENV_REASSIGN_ON_ADD);
    let base = unique_dir("v18", "local");
    let _g = Guard(base.clone());
    let (b1, b2) = boot_n2(&base, [19211, 19212]);
    let topic = TopicName::new("events");
    b1.create_topic(topic.clone(), 1).unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    let _ = b2.apply_cluster_state(gen, cid, &topics);

    let leader = b1
        .metadata(Some(&[topic.clone()]))
        .topics
        .iter()
        .find(|t| t.name.as_str() == "events")
        .and_then(|t| t.partitions.first())
        .map(|p| p.leader)
        .unwrap();
    let producer: &Broker = if leader == b1.node_id() { &b1 } else { &b2 };

    let rec = producer
        .produce_one(&topic, PartitionId(0), Message::from_value("before"))
        .unwrap();
    assert_eq!(rec.offset.raw(), 0);

    b1.add_broker(3, "127.0.0.1".into(), 19213, None).unwrap();
    b1.reassign_partitions("events", u32::MAX, &[1, 2, 3])
        .unwrap();

    let mut cfg3 = cluster_config_n2([19211, 19212]);
    cfg3.brokers.push(BrokerEndpoint {
        id: 3,
        host: "127.0.0.1".into(),
        port: 19213,
        rack: None,
    });
    let b3 = Broker::with_cluster(default_storage(base.join("n3")), 3, cfg3).unwrap();
    b3.set_advertised("127.0.0.1", 19213);
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    b3.apply_cluster_state(gen, cid, &topics).unwrap();

    let listed = part_replicas(&b1, "events", 0);
    assert!(listed.contains(&3), "assignment lists 3: {listed:?}");
    assert!(
        b3.follower_targets()
            .iter()
            .any(|(t, p, _, _)| t == "events" && *p == 0),
        "broker 3 should open a local follower partition"
    );

    let rec2 = producer
        .produce_one(&topic, PartitionId(0), Message::from_value("after"))
        .unwrap();
    assert_eq!(rec2.offset.raw(), 1);
}

#[test]
fn unknown_topic_and_unknown_replica_id_rejected() {
    let _env = EnvGuard::unset(ENV_REASSIGN_ON_ADD);
    let base = unique_dir("v18", "reject");
    let _g = Guard(base.clone());
    let (b1, _b2) = boot_n2(&base, [19221, 19222]);
    b1.create_topic(TopicName::new("events"), 1).unwrap();

    match b1.reassign_partitions("no-such-topic", u32::MAX, &[1, 2]) {
        Err(Error::NotFound(m)) => assert!(m.contains("no-such-topic"), "{m}"),
        other => panic!("expected NotFound, got {other:?}"),
    }

    match b1.reassign_partitions("events", u32::MAX, &[1, 2, 99]) {
        Err(Error::InvalidArgument(m)) => {
            assert!(m.contains("not in membership"), "{m}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

#[test]
fn flag_off_add_broker_does_not_rewrite_replica_sets() {
    let _env = EnvGuard::unset(ENV_REASSIGN_ON_ADD);
    assert!(!reassign_on_add_enabled());

    let base = unique_dir("v18", "flagoff");
    let _g = Guard(base.clone());
    let (b1, b2) = boot_n2(&base, [19231, 19232]);
    let topic = TopicName::new("events");
    b1.create_topic(topic.clone(), 1).unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    let _ = b2.apply_cluster_state(gen, cid, &topics);

    let before = part_replicas(&b1, "events", 0);
    assert_eq!(before, vec![1, 2]);
    let gen_before = assignment_gen(&b1);

    b1.add_broker(3, "127.0.0.1".into(), 19233, None).unwrap();
    assert_eq!(part_replicas(&b1, "events", 0), before);
    assert_eq!(assignment_gen(&b1), gen_before);

    let rec = b1
        .produce_one(&topic, PartitionId(0), Message::from_value("still"))
        .or_else(|_| b2.produce_one(&topic, PartitionId(0), Message::from_value("still")))
        .unwrap();
    assert_eq!(rec.offset.raw(), 0);
}

#[test]
fn auto_reassign_empty_list_uses_assign_replicas() {
    let _env = EnvGuard::unset(ENV_REASSIGN_ON_ADD);
    let base = unique_dir("v18", "auto");
    let _g = Guard(base.clone());
    let (b1, _b2) = boot_n2(&base, [19241, 19242]);
    b1.create_topic(TopicName::new("events"), 1).unwrap();
    b1.add_broker(3, "127.0.0.1".into(), 19243, None).unwrap();
    let gen_before = assignment_gen(&b1);

    let new_gen = b1.reassign_partitions("events", u32::MAX, &[]).unwrap();
    assert!(new_gen > gen_before);
    let replicas = part_replicas(&b1, "events", 0);
    assert!(!replicas.is_empty());
    // Auto uses rf = min(default_rf=2, N=3) = 2 over brokers {1,2,3}.
    assert_eq!(replicas.len(), 2);
    for id in &replicas {
        assert!(matches!(*id, 1 | 2 | 3), "unexpected replica {id}");
    }
}

#[test]
fn auto_on_add_expands_underreplicated_when_flag_on() {
    let _env = EnvGuard::set(ENV_REASSIGN_ON_ADD, "1");
    assert!(reassign_on_add_enabled());

    let base = unique_dir("v18", "autoadd");
    let _g = Guard(base.clone());
    // default RF=3 but only 2 brokers → create is RF-capped at 2.
    let cfg = ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 1,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=2)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: 19250 + id as u16,
                rack: None,
            })
            .collect(),
    };
    let b1 = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
    b1.set_advertised("127.0.0.1", 19251);
    b1.create_topic(TopicName::new("events"), 1).unwrap();
    assert_eq!(part_replicas(&b1, "events", 0), vec![1, 2]);

    b1.add_broker(3, "127.0.0.1".into(), 19253, None).unwrap();
    let replicas = part_replicas(&b1, "events", 0);
    assert_eq!(
        replicas,
        vec![1, 2, 3],
        "flag-on add should append new id when unique < min(rf, N)"
    );
}

#[tokio::test]
async fn native_admin_reassign_via_tcp() {
    use common::cluster::{bind_port0, rpc_seq};
    use volant_broker::serve_listener;
    use volant_protocol::{Request, Response};

    let _env = EnvGuard::unset(ENV_REASSIGN_ON_ADD);
    let base = unique_dir("v18", "tcp");
    let _g = Guard(base.clone());
    let (l1, p1) = bind_port0().await;
    let (_l2, p2) = bind_port0().await;
    let cfg = cluster_config_n2([p1, p2]);
    let b1 = Arc::new({
        let b = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b
    });
    b1.create_topic(TopicName::new("events"), 1).unwrap();
    b1.add_broker(3, "127.0.0.1".into(), 19263, None).unwrap();

    let server = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let addr = format!("127.0.0.1:{p1}");

    let resps = rpc_seq(
        &addr,
        &[
            Request::ReassignPartitions {
                topic: "events".into(),
                partition: u32::MAX,
                replicas: vec![1, 2, 3],
            },
            Request::ReassignPartitions {
                topic: "missing".into(),
                partition: u32::MAX,
                replicas: vec![1],
            },
            Request::ReassignPartitions {
                topic: "events".into(),
                partition: u32::MAX,
                replicas: vec![1, 99],
            },
        ],
    )
    .await;
    match &resps[0] {
        Response::ReassignPartitions {
            error_code,
            generation,
        } => {
            assert_eq!(*error_code, 0);
            assert!(*generation >= 1);
        }
        other => panic!("reassign ok: {other:?}"),
    }
    match &resps[1] {
        Response::ReassignPartitions { error_code, .. } => {
            assert_eq!(*error_code, volant_protocol::ErrorCode::NotFound as u16);
        }
        other => panic!("unknown topic: {other:?}"),
    }
    match &resps[2] {
        Response::ReassignPartitions { error_code, .. } => {
            assert_eq!(*error_code, volant_protocol::ErrorCode::InvalidArg as u16);
        }
        other => panic!("bad replica: {other:?}"),
    }
    assert_eq!(part_replicas(&b1, "events", 0), vec![1, 2, 3]);
    server.abort();
}
