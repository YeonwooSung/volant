//! v0.7: Preferred-replica redirect throttle + optional TCP connect probe.
//!
//! Default-off: no env → Phase 126/133/140 selector + Fetch `throttle_time_ms=0`.
//! Probe is an extra filter on top of rack / LEO / ISR gates (not a replacement).

#[path = "common/mod.rs"]
mod common;

use std::net::TcpListener as StdTcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{encode_request, get_bytes, get_string, put_string};
use volant_broker::{
    serve_kafka_listener, start_background_tasks, BackgroundTasks, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

use common::cluster::{bind_port0, unique_dir, Guard};
use common::rpc;

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
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn cluster_config_racks(ports: [u16; 3], racks: [Option<&str>; 3]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 30_000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=3)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: ports[(id - 1) as usize],
                rack: racks[(id - 1) as usize].map(|s| s.to_string()),
            })
            .collect(),
    }
}

fn boot_triple_racks(
    base: &Path,
    ports: [u16; 3],
    racks: [Option<&str>; 3],
) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config_racks(ports, racks);
    let mk = |id: u32| {
        let storage = StorageConfig {
            data_dir: base.join(format!("n{id}")),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    (mk(1), mk(2), mk(3))
}

fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes.iter().all(|n| n.partition_count_opt(topic).is_some()) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("assignment did not propagate for topic {topic}");
}

fn catch_up_isr(leader: &Broker, topic: &str) {
    let t = TopicName::new(topic);
    let leo = leader.log_end_offset(&t, PartitionId(0)).unwrap();
    let isr = leader.local_partition_isr(&t, PartitionId(0)).unwrap();
    let lid = leader.node_id();
    for fid in isr {
        if fid != lid {
            leader
                .test_set_follower_leo(&t, PartitionId(0), fid, leo)
                .unwrap();
        }
    }
}

fn leader_of(b1: &Arc<Broker>, b2: &Arc<Broker>, b3: &Arc<Broker>, topic: &str) -> Arc<Broker> {
    let tname = TopicName::new(topic);
    let leader_id = b1
        .metadata(None)
        .topics
        .iter()
        .find(|t| t.name == tname)
        .map(|t| t.partitions[0].leader)
        .expect("topic metadata");
    match leader_id {
        1 => Arc::clone(b1),
        2 => Arc::clone(b2),
        3 => Arc::clone(b3),
        _ => panic!("bad leader {leader_id}"),
    }
}

/// 3-node same-rack cluster, produce + ISR LEO catch-up (in-process).
fn setup_caught_up(label: &str, ports: [u16; 3]) -> (Guard, Arc<Broker>, TopicName, Vec<u32>) {
    let base = unique_dir("v07", label);
    let guard = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_racks(
        &base,
        ports,
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );
    b1.create_topic(label, 1).unwrap();
    propagate(&[&b1, &b2, &b3], label);
    let topic = TopicName::new(label);
    let leader = leader_of(&b1, &b2, &b3, label);
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("v07"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, label);
    let lid = leader.node_id();
    let followers: Vec<u32> = leader
        .local_partition_isr(&topic, PartitionId(0))
        .unwrap()
        .into_iter()
        .filter(|id| *id != lid)
        .collect();
    assert_eq!(followers.len(), 2, "expected 2 followers; leader={lid}");
    let _keep = (b1, b2, b3);
    (guard, leader, topic, followers)
}

fn fetch_body_v11(topic: &str, rack: Option<&str>) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // consumer
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // READ_UNCOMMITTED
    body.put_i32(0); // session_id
    body.put_i32(-1); // FINAL
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(0); // partition
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(0); // fetch_offset
    body.put_i64(-1); // log_start
    body.put_i32(1_000_000); // partition_max_bytes
    body.put_i32(0); // forgotten
    put_string(&mut body, rack.unwrap_or(""));
    body
}

/// (throttle_time_ms, hwm, preferred_read_replica, records_len)
fn parse_fetch_v11(mut src: bytes::Bytes) -> (i32, i64, i32, usize) {
    let throttle = src.get_i32();
    let top_err = src.get_i16();
    let _session = src.get_i32();
    assert_eq!(src.get_i32(), 1, "expected 1 topic, top_err={top_err}");
    let _name = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let part_err = src.get_i16();
    let hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(src.get_i32(), 0); // aborted
    let preferred = src.get_i32();
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    assert_eq!(top_err, 0);
    assert_eq!(part_err, 0, "partition error {part_err}");
    (throttle, hwm, preferred, records.len())
}

fn reserve_ports() -> [u16; 3] {
    let ls: [StdTcpListener; 3] = [
        StdTcpListener::bind("127.0.0.1:0").unwrap(),
        StdTcpListener::bind("127.0.0.1:0").unwrap(),
        StdTcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    [
        ls[0].local_addr().unwrap().port(),
        ls[1].local_addr().unwrap().port(),
        ls[2].local_addr().unwrap().port(),
    ]
}

struct KafkaCluster {
    _guard: Guard,
    leader: Arc<Broker>,
    leader_addr: String,
    topic: String,
    servers: [tokio::task::JoinHandle<()>; 3],
    _bg: [BackgroundTasks; 3],
    _nodes: [Arc<Broker>; 3],
}

async fn setup_kafka_cluster(label: &str) -> KafkaCluster {
    let base = unique_dir("v07", label);
    let guard = Guard(base.clone());
    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let (b1, b2, b3) = boot_triple_racks(
        &base,
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );
    let bg = [
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_kafka_listener(l1, b).await.ok();
        })
    };
    let s2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            serve_kafka_listener(l2, b).await.ok();
        })
    };
    let s3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            serve_kafka_listener(l3, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    b1.create_topic(label, 1).unwrap();
    {
        let nodes: [&Broker; 3] = [&b1, &b2, &b3];
        let src = nodes[0];
        let mut ok = false;
        for _ in 0..50 {
            let (_, gen, cid, topics) = src.cluster_state_snapshot();
            for n in nodes.iter().skip(1) {
                let _ = n.apply_cluster_state(gen, cid, &topics);
            }
            if nodes.iter().all(|n| n.partition_count_opt(label).is_some()) {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ok, "assignment did not propagate for {label}");
    }

    let topic = TopicName::new(label);
    let leader = leader_of(&b1, &b2, &b3, label);
    let leader_id = leader.node_id();
    let leader_addr = format!("127.0.0.1:{}", [p1, p2, p3][(leader_id - 1) as usize]);

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("v07-wire"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, label);

    KafkaCluster {
        _guard: guard,
        leader,
        leader_addr,
        topic: label.to_string(),
        servers: [s1, s2, s3],
        _bg: bg,
        _nodes: [b1, b2, b3],
    }
}

impl Drop for KafkaCluster {
    fn drop(&mut self) {
        for s in &self.servers {
            s.abort();
        }
    }
}

/// Default: no env → same eligible peer as today; Fetch v11 throttle_time_ms == 0.
#[tokio::test]
async fn default_no_env_selects_and_zero_throttle() {
    let h = setup_kafka_cluster("default-off").await;
    assert_eq!(h.leader.preferred_replica_throttle_ms(), 0);
    assert!(!h.leader.preferred_replica_tcp_probe());

    let topic = TopicName::new(&h.topic);
    let expected = h
        .leader
        .select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(
        expected.is_some(),
        "same-rack caught-up follower must be eligible"
    );

    let before = h.leader.preferred_replica_throttled_total();
    let resp = rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11(&h.topic, Some("rack-a")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    let (throttle, hwm, preferred, rec_len) = parse_fetch_v11(src);
    assert!(hwm > 0);
    assert_eq!(preferred, expected.unwrap() as i32);
    assert_eq!(rec_len, 0, "redirect responses carry empty records");
    assert_eq!(throttle, 0, "default throttle stays 0");
    assert_eq!(
        h.leader.preferred_replica_throttled_total(),
        before,
        "default must not increment throttle metric"
    );
}

/// Throttle env set: redirect Fetch gets configured throttle; no-redirect stays 0.
#[tokio::test]
async fn throttle_env_sets_fetch_throttle() {
    const THROTTLE: u32 = 42;
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvRestore::set("VOLANT_PREFERRED_REPLICA_THROTTLE_MS", "42");

    let h = setup_kafka_cluster("throttle-on").await;
    assert_eq!(h.leader.preferred_replica_throttle_ms(), THROTTLE);

    let topic = TopicName::new(&h.topic);
    let expected = h
        .leader
        .select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(expected.is_some());

    let before = h.leader.preferred_replica_throttled_total();
    let resp = rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11(&h.topic, Some("rack-a")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    let (throttle, _hwm, preferred, rec_len) = parse_fetch_v11(src);
    assert_eq!(preferred, expected.unwrap() as i32);
    assert_eq!(rec_len, 0);
    assert_eq!(throttle, THROTTLE as i32);
    assert!(
        h.leader.preferred_replica_throttled_total() > before,
        "redirect throttle must increment metric; before={before} after={}",
        h.leader.preferred_replica_throttled_total()
    );

    // No rack → no redirect → throttle stays 0; metric unchanged.
    let after_redir = h.leader.preferred_replica_throttled_total();
    let resp_nr = rpc(
        &h.leader_addr,
        encode_request(1, 11, 6, Some("c"), &fetch_body_v11(&h.topic, None)),
    )
    .await;
    let mut src_nr = resp_nr.freeze();
    assert_eq!(src_nr.get_i32(), 6);
    let (throttle_nr, _hwm, preferred_nr, rec_len_nr) = parse_fetch_v11(src_nr);
    assert_eq!(preferred_nr, -1);
    assert!(rec_len_nr > 0, "leader serves records when not redirecting");
    assert_eq!(
        throttle_nr, 0,
        "non-redirect Fetch must not add this throttle"
    );
    assert_eq!(h.leader.preferred_replica_throttled_total(), after_redir);
}

/// Single-node (no cluster preferred candidate) stays throttle 0 even when env set.
#[tokio::test]
async fn throttle_not_applied_without_redirect_single_node() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _env = EnvRestore::set("VOLANT_PREFERRED_REPLICA_THROTTLE_MS", "99");

    let dir = unique_dir("v07", "single-throttle");
    let _g = Guard(dir.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.join("n1"),
        flush_every_n: 1,
        ..StorageConfig::default()
    }));
    assert_eq!(broker.preferred_replica_throttle_ms(), 99);
    broker.create_topic("t", 1).unwrap();
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("solo"));
    let (_, err) = broker
        .produce_with_acks(&TopicName::new("t"), PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let b = Arc::clone(&broker);
    let server = tokio::spawn(async move {
        serve_kafka_listener(listener, b).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(15)).await;

    let before = broker.preferred_replica_throttled_total();
    let resp = rpc(
        &addr,
        encode_request(1, 11, 5, Some("c"), &fetch_body_v11("t", Some("rack-a"))),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    let (throttle, hwm, preferred, rec_len) = parse_fetch_v11(src);
    assert!(hwm > 0);
    assert_eq!(preferred, -1);
    assert!(rec_len > 0);
    assert_eq!(throttle, 0);
    assert_eq!(broker.preferred_replica_throttled_total(), before);
    server.abort();
}

/// Probe off (default): configured peer remains selectable without a live TCP accept.
#[test]
fn probe_off_selects_without_listen() {
    let ports = reserve_ports();
    let (_g, leader, topic, followers) = setup_caught_up("probe-off", ports);
    assert!(!leader.preferred_replica_tcp_probe());
    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(pref.is_some());
    assert!(followers.contains(&pref.unwrap()));
    assert_eq!(leader.preferred_replica_probe_fail_total(), 0);
}

/// Probe on: listening advertised port is selected; closed port is skipped.
#[test]
fn probe_on_listen_selected_closed_skipped() {
    // Bind a live listener for follower A; leave follower B on a closed port.
    let listen = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let open_port = listen.local_addr().unwrap().port();
    let closed = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let closed_port = closed.local_addr().unwrap().port();
    drop(closed);
    let leader_port = StdTcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();

    // node1=leader-ish ports assigned by id, not by role. We don't know who
    // becomes leader until after create_topic. Give *all three* distinct
    // advertised ports; after we know the leader, enable probe and listen only
    // on one follower.
    let ports = [leader_port, open_port, closed_port];
    let base = unique_dir("v07", "probe-on");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_racks(
        &base,
        ports,
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );
    b1.create_topic("probe-on", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "probe-on");
    let topic = TopicName::new("probe-on");
    let leader = leader_of(&b1, &b2, &b3, "probe-on");
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("probe"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "probe-on");

    // Keep `listen` alive so open_port accepts TCP connect.
    let _listen = listen;
    leader.set_preferred_replica_tcp_probe(true);

    let lid = leader.node_id();
    let open_id = match open_port {
        p if p == ports[0] => 1,
        p if p == ports[1] => 2,
        p if p == ports[2] => 3,
        _ => unreachable!(),
    };
    let closed_id = match closed_port {
        p if p == ports[0] => 1,
        p if p == ports[1] => 2,
        p if p == ports[2] => 3,
        _ => unreachable!(),
    };

    let before_fail = leader.preferred_replica_probe_fail_total();
    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));

    if open_id == lid {
        // Leader advertised the only listening port; both followers are closed
        // (leader_port was released after bind, closed_port dropped).
        assert_eq!(pref, None, "no follower has a live advertised port");
        assert!(
            leader.preferred_replica_probe_fail_total() > before_fail,
            "closed followers must increment probe-fail"
        );
    } else {
        assert_eq!(
            pref,
            Some(open_id),
            "listening follower must win; leader={lid} closed={closed_id}"
        );
        if closed_id != lid {
            assert!(
                leader.preferred_replica_probe_fail_total() > before_fail,
                "closed follower must increment probe-fail"
            );
        }
    }
}

/// Probe on + 127.0.0.1:1 advertised for every follower → skip all, preferred None.
#[test]
fn probe_on_closed_port_skips_peer() {
    let ports = [1u16, 1, 1];
    let (_g, leader, topic, _followers) = setup_caught_up("probe-closed", ports);
    // Probe off still selects (usable_addr: host non-empty, port != 0).
    assert!(!leader.preferred_replica_tcp_probe());
    let off = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(off.is_some(), "probe-off must still pick a peer on :1");

    leader.set_preferred_replica_tcp_probe(true);
    let before = leader.preferred_replica_probe_fail_total();
    let on = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_eq!(on, None, "probe-on must skip closed 127.0.0.1:1");
    assert!(
        leader.preferred_replica_probe_fail_total() > before,
        "each failed probe increments the metric"
    );
}

/// Probe does not bypass rack or LEO≥HWM gates.
#[test]
fn probe_does_not_bypass_rack_or_leo_gates() {
    let listen_b = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let listen_a = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let p1 = StdTcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let p2 = listen_b.local_addr().unwrap().port();
    let p3 = listen_a.local_addr().unwrap().port();
    let _keep = (listen_b, listen_a);

    // node1 rack-a, node2 rack-b (listening), node3 rack-a (listening).
    let base = unique_dir("v07", "probe-gates");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_racks(
        &base,
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-b"), Some("rack-a")],
    );
    b1.create_topic("gates", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "gates");
    let topic = TopicName::new("gates");
    let leader = leader_of(&b1, &b2, &b3, "gates");
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("gates"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "gates");

    leader.set_preferred_replica_tcp_probe(true);
    let lid = leader.node_id();
    let hwm = leader.committed_hwm(&topic, PartitionId(0)).unwrap();

    // Other rack never selected even with a live TCP listener + higher LEO.
    for fid in leader.local_partition_isr(&topic, PartitionId(0)).unwrap() {
        if fid != lid {
            let leo = if fid == 2 { hwm + 100 } else { hwm };
            leader
                .test_set_follower_leo(&topic, PartitionId(0), fid, leo)
                .unwrap();
        }
    }
    let pref_a = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_ne!(pref_a, Some(2), "rack-b must never win for client rack-a");
    if let Some(id) = pref_a {
        assert_eq!(leader.broker_rack(id).as_deref(), Some("rack-a"));
    }

    // LEO < HWM still excludes, even with a live listener.
    if lid != 3 {
        leader
            .test_set_follower_leo(&topic, PartitionId(0), 3, hwm.saturating_sub(1))
            .unwrap();
        // If 3 was the only rack-a follower, preferred becomes None.
        if lid == 2 {
            assert_eq!(
                leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a")),
                None,
                "LEO < HWM must exclude even when TCP probe would pass"
            );
        } else {
            let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
            assert_ne!(pref, Some(3), "under-HWM peer must not be preferred");
        }
    }
}

/// Env parse: unset/0/invalid → 0 / probe off; 1/true/yes/on enable probe.
#[test]
fn env_parse_defaults_and_truthy() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    {
        let _t = EnvRestore::set("VOLANT_PREFERRED_REPLICA_THROTTLE_MS", "not-a-number");
        let _p = EnvRestore::set("VOLANT_PREFERRED_REPLICA_TCP_PROBE", "nope");
        let dir = unique_dir("v07", "env-bad");
        let _g = Guard(dir.clone());
        let b = Broker::new(StorageConfig {
            data_dir: dir,
            ..StorageConfig::default()
        });
        assert_eq!(b.preferred_replica_throttle_ms(), 0);
        assert!(!b.preferred_replica_tcp_probe());
    }
    {
        let _t = EnvRestore::set("VOLANT_PREFERRED_REPLICA_THROTTLE_MS", "0");
        let dir = unique_dir("v07", "env-zero");
        let _g = Guard(dir.clone());
        let b = Broker::new(StorageConfig {
            data_dir: dir,
            ..StorageConfig::default()
        });
        assert_eq!(b.preferred_replica_throttle_ms(), 0);
    }
    {
        let _p = EnvRestore::set("VOLANT_PREFERRED_REPLICA_TCP_PROBE", "yes");
        let dir = unique_dir("v07", "env-yes");
        let _g = Guard(dir.clone());
        let b = Broker::new(StorageConfig {
            data_dir: dir,
            ..StorageConfig::default()
        });
        assert!(b.preferred_replica_tcp_probe());
    }
    {
        let _p = EnvRestore::set("VOLANT_PREFERRED_REPLICA_TCP_PROBE", "on");
        let dir = unique_dir("v07", "env-on");
        let _g = Guard(dir.clone());
        let b = Broker::new(StorageConfig {
            data_dir: dir,
            ..StorageConfig::default()
        });
        assert!(b.preferred_replica_tcp_probe());
    }
}
