//! Phase 140: Preferred-replica selector depth — max LEO lag + suppress metric.
//!
//! In-process selector tests (like phase133) plus one wire-level READ_COMMITTED
//! suppress counter check (phase126 isolation style).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{encode_request, get_bytes, get_string, put_string};
use volant_broker::{
    serve_kafka_listener, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p140-{label}-{}-{}",
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
    base: &std::path::Path,
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

fn boot_triple(base: &std::path::Path, ports: [u16; 3]) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    boot_triple_racks(
        base,
        ports,
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    )
}

fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes
            .iter()
            .all(|n| n.partition_count_opt(topic).is_some())
        {
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

/// Setup: 3-node same-rack cluster, one partition, produce + catch-up ISR LEOs.
fn setup_caught_up(label: &str) -> (Guard, Arc<Broker>, TopicName, Vec<u32>) {
    let base = unique_dir(label);
    let guard = Guard(base.clone());
    let ports = [29391, 29392, 29393];
    let (b1, b2, b3) = boot_triple(&base, ports);

    b1.create_topic(label, 1).unwrap();
    propagate(&[&b1, &b2, &b3], label);

    let topic = TopicName::new(label);
    let leader = leader_of(&b1, &b2, &b3, label);

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("p140"));
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
    assert_eq!(
        followers.len(),
        2,
        "expected 2 followers in ISR; leader={lid} isr={:?}",
        leader.local_partition_isr(&topic, PartitionId(0)).unwrap()
    );

    (guard, leader, topic, followers)
}

/// Other rack never selected even with higher LEO.
#[test]
fn multi_rack_other_rack_never_selected() {
    let base = unique_dir("multi-rack");
    let _g = Guard(base.clone());
    // node1 rack-a, node2 rack-b, node3 rack-a
    let ports = [29401, 29402, 29403];
    let (b1, b2, b3) = boot_triple_racks(
        &base,
        ports,
        [Some("rack-a"), Some("rack-b"), Some("rack-a")],
    );

    b1.create_topic("mr", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "mr");
    let topic = TopicName::new("mr");
    let leader = leader_of(&b1, &b2, &b3, "mr");

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("mr"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "mr");

    let hwm = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    let lid = leader.node_id();
    // Give rack-b peer a strictly higher LEO so ranking alone would prefer it.
    for fid in leader.local_partition_isr(&topic, PartitionId(0)).unwrap() {
        if fid != lid {
            let leo = if fid == 2 { hwm + 100 } else { hwm };
            leader
                .test_set_follower_leo(&topic, PartitionId(0), fid, leo)
                .unwrap();
        }
    }

    let pref_a = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_ne!(
        pref_a,
        Some(2),
        "rack-b peer must never be preferred for client rack-a; got {pref_a:?}"
    );
    // When leader is rack-a, the other rack-a peer is eligible; when leader is 2,
    // rack-a client may still pick a rack-a follower.
    if let Some(id) = pref_a {
        assert_ne!(id, 2);
        assert_eq!(
            leader.broker_rack(id).as_deref(),
            Some("rack-a"),
            "preferred must be same rack as client"
        );
    }

    let pref_b = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-b"));
    // Only broker 2 is rack-b; if leader is 2, no other rack-b follower → None.
    // If leader is not 2, preferred may be 2 only when 2 is a follower.
    if lid == 2 {
        assert_eq!(pref_b, None, "leader is sole rack-b → no preferred follower");
    } else {
        assert_eq!(pref_b, Some(2), "only rack-b follower is broker 2");
    }
    // Never return a rack-a id for rack-b client.
    if let Some(id) = pref_b {
        assert_eq!(leader.broker_rack(id).as_deref(), Some("rack-b"));
    }
}

/// Max LEO lag excludes over-lag peer when a fresher (within-lag) peer exists.
#[test]
fn max_leo_lag_excludes_over_lag_when_fresher_exists() {
    let (_g, leader, topic, mut followers) = setup_caught_up("lag-exclude");
    followers.sort_unstable();
    let stale_id = followers[0];
    let fresh_id = followers[1];

    // Advance leader LEO without catching followers up fully.
    for i in 0..20 {
        let mut batch = MessageBatch::default();
        batch
            .messages
            .push(Message::from_value(format!("more-{i}")));
        let (_, err) = leader
            .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
            .unwrap();
        assert_eq!(err, 0);
    }
    let leader_leo = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    let hwm = leader.committed_hwm(&topic, PartitionId(0)).unwrap();
    assert!(leader_leo > hwm || leader_leo > 1);

    // Stale: LEO just at HWM (large lag vs leader). Fresh: lag 2.
    let fresh_leo = leader_leo.saturating_sub(2).max(hwm);
    leader
        .test_set_follower_leo(&topic, PartitionId(0), stale_id, hwm)
        .unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), fresh_id, fresh_leo)
        .unwrap();

    let stale_lag = leader_leo.saturating_sub(hwm);
    let fresh_lag = leader_leo.saturating_sub(fresh_leo);
    assert!(
        stale_lag > fresh_lag,
        "stale must lag more: stale_lag={stale_lag} fresh_lag={fresh_lag}"
    );
    // Cap between fresh and stale so only fresh remains eligible.
    let max_lag = fresh_lag.max(1);
    assert!(
        stale_lag > max_lag,
        "stale must exceed max_lag={max_lag} (stale_lag={stale_lag})"
    );
    leader.set_preferred_replica_max_leo_lag(max_lag);

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_eq!(
        pref,
        Some(fresh_id),
        "within-lag fresher peer must win; stale={stale_id} fresh={fresh_id} \
         leader_leo={leader_leo} hwm={hwm} max_lag={max_lag}"
    );
    assert_ne!(pref, Some(stale_id), "over-lag peer must not be preferred");

    // Tighten so even the fresh peer is over lag → None.
    if fresh_lag > 0 {
        leader.set_preferred_replica_max_leo_lag(fresh_lag.saturating_sub(1));
        assert_eq!(
            leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a")),
            None,
            "all peers over max lag → no preferred"
        );
    }
}

/// Default unlimited lag: higher LEO still wins (phase 133 regression).
#[test]
fn default_unlimited_higher_leo_wins() {
    let (_g, leader, topic, mut followers) = setup_caught_up("unlimited-leo");
    followers.sort_unstable();
    let low_id = followers[0];
    let high_id = followers[1];
    assert!(low_id < high_id);

    // Unlimited by default (env unset → u64::MAX).
    assert_eq!(leader.preferred_replica_max_leo_lag(), u64::MAX);

    let hwm_base = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), low_id, hwm_base)
        .unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), high_id, hwm_base + 10)
        .unwrap();

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert_eq!(
        pref,
        Some(high_id),
        "highest LEO must win under unlimited lag; low_id={low_id} high_id={high_id}"
    );
}

/// Dead peer skipped via controller alive-set.
#[test]
fn dead_peer_skipped() {
    let (_g, leader, topic, mut followers) = setup_caught_up("dead-peer");
    followers.sort_unstable();
    let low_id = followers[0];
    let high_id = followers[1];

    let hwm_base = leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), low_id, hwm_base)
        .unwrap();
    leader
        .test_set_follower_leo(&topic, PartitionId(0), high_id, hwm_base + 10)
        .unwrap();

    assert_eq!(
        leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a")),
        Some(high_id)
    );

    let lid = leader.node_id();
    let alive: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != high_id)
        .collect();
    leader.apply_controller_alive_set(&alive).unwrap();
    assert!(!leader.live_brokers().contains(&high_id));

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    match pref {
        Some(id) => {
            assert_eq!(id, low_id);
            assert_ne!(id, high_id);
        }
        None => {
            let isr = leader.local_partition_isr(&topic, PartitionId(0)).unwrap();
            assert!(
                !isr.contains(&low_id) || !leader.live_brokers().contains(&low_id),
                "None only when no other eligible peer; isr={isr:?} live={:?}",
                leader.live_brokers()
            );
        }
    }

    leader.apply_controller_alive_set(&[lid]).unwrap();
    assert_eq!(
        leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a")),
        None
    );
}

fn fetch_body_v11(topic: &str, isolation: u8, rack: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // consumer
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(isolation);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i64(0);
    body.put_i64(-1);
    body.put_i32(1_000_000);
    body.put_i32(0);
    put_string(&mut body, rack);
    body
}

fn parse_fetch_v11_preferred(mut src: bytes::Bytes) -> (i64, i32, usize) {
    let _throttle = src.get_i32();
    let top_err = src.get_i16();
    let _session = src.get_i32();
    assert_eq!(src.get_i32(), 1);
    let _name = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let part_err = src.get_i16();
    let hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(src.get_i32(), 0);
    let preferred = src.get_i32();
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    assert_eq!(top_err, 0);
    assert_eq!(part_err, 0);
    (hwm, preferred, records.len())
}

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

async fn kafka_rpc(addr: &str, request: BytesMut) -> BytesMut {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&request).await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        if buf.len() >= 4 {
            let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if buf.len() >= 4 + size {
                let _ = buf.split_to(4);
                return buf.split_to(size);
            }
        }
    }
    panic!("no kafka response");
}

/// READ_COMMITTED increments suppressed counter when a preferred candidate exists.
#[tokio::test]
async fn read_committed_increments_suppressed() {
    let base = unique_dir("rc-suppress-metric");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config_racks(
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );

    let mk = |id: u32| {
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
        b.set_advertised("127.0.0.1", [p1, p2, p3][(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let _bg2 = start_background_tasks(Arc::clone(&b2));
    let _bg3 = start_background_tasks(Arc::clone(&b3));

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
    tokio::time::sleep(Duration::from_millis(30)).await;

    b1.create_topic("rcm", 1).unwrap();
    // Async propagate (same as phase126 isolation).
    {
        let nodes: [&Broker; 3] = [&b1, &b2, &b3];
        let src = nodes[0];
        let mut ok = false;
        for _ in 0..50 {
            let (_, gen, cid, topics) = src.cluster_state_snapshot();
            for n in nodes.iter().skip(1) {
                let _ = n.apply_cluster_state(gen, cid, &topics);
            }
            if nodes.iter().all(|n| n.partition_count_opt("rcm").is_some()) {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ok, "assignment did not propagate for rcm");
    }
    let topic = TopicName::new("rcm");
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!("bad leader"),
    };
    let leader_addr = format!("127.0.0.1:{}", [p1, p2, p3][(leader_id - 1) as usize]);

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("hello-rcm"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "rcm");

    let expected = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(expected.is_some(), "expected preferred candidate");

    let before = leader.preferred_replica_suppressed_total();
    let before_redir = leader.preferred_replica_redirect_total();

    let resp_rc = kafka_rpc(
        &leader_addr,
        encode_request(1, 11, 5, Some("c"), &fetch_body_v11("rcm", 1, "rack-a")),
    )
    .await;
    let mut src_rc = resp_rc.freeze();
    assert_eq!(src_rc.get_i32(), 5);
    let (hwm_rc, preferred_rc, rec_len_rc) = parse_fetch_v11_preferred(src_rc);
    assert!(hwm_rc > 0);
    assert_eq!(preferred_rc, -1, "READ_COMMITTED must suppress preferred");
    assert!(rec_len_rc > 0, "leader serves records when preferred suppressed");

    assert!(
        leader.preferred_replica_suppressed_total() > before,
        "READ_COMMITTED must increment suppressed counter; before={before} after={}",
        leader.preferred_replica_suppressed_total()
    );
    assert_eq!(
        leader.preferred_replica_redirect_total(),
        before_redir,
        "suppress path must not increment redirect counter"
    );

    s1.abort();
    s2.abort();
    s3.abort();
}
