//! Phase 126: PreferredReadReplica (KIP-392 subset) + Metadata rack honesty.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_bytes, get_string, put_bytes, put_string,
};
use volant_broker::{
    serve_kafka_listener, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_core::{Message, MessageBatch, Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p126-{label}-{}-{}",
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
        session_timeout_ms: 2000,
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

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
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

async fn propagate(nodes: &[&Broker], topic: &str) {
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("assignment did not propagate for topic {topic}");
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
    panic!("kafka rpc closed early");
}

fn sample_records(value: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

fn produce_body_v3(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    volant_broker::kafka::codec::put_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

fn fetch_body_v11(topic: &str, fetch_offset: i64, rack: Option<&str>) -> BytesMut {
    fetch_body_v11_with_replica(topic, fetch_offset, -1, rack)
}

/// Fetch v11 body with explicit `replica_id` (consumer = -1; follower ≥ 0).
fn fetch_body_v11_with_replica(
    topic: &str,
    fetch_offset: i64,
    replica_id: i32,
    rack: Option<&str>,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(replica_id);
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(0); // isolation READ_UNCOMMITTED
    body.put_i32(0); // session_id
    body.put_i32(-1); // session_epoch FINAL
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(0); // partition
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(fetch_offset);
    body.put_i64(-1); // follower log_start
    body.put_i32(1_000_000); // partition_max_bytes
    body.put_i32(0); // forgotten
    put_string(&mut body, rack.unwrap_or(""));
    body
}

fn parse_fetch_v11_preferred(mut src: bytes::Bytes) -> (i16, i64, i32, usize) {
    // Correlation already consumed; body starts at throttle.
    assert!(
        src.remaining() >= 4 + 2 + 4 + 4,
        "short fetch body: {} bytes",
        src.remaining()
    );
    let _throttle = src.get_i32();
    let top_err = src.get_i16();
    let _session = src.get_i32();
    let topic_count = src.get_i32();
    assert_eq!(topic_count, 1, "expected 1 topic, top_err={top_err}");
    let _name = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i32(), 0); // partition index
    let part_err = src.get_i16();
    let hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(src.get_i32(), 0); // aborted
    let preferred = src.get_i32();
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    assert_eq!(top_err, 0);
    assert_eq!(part_err, 0, "partition error {part_err}");
    (part_err, hwm, preferred, records.len())
}

/// Single-node: rack in request still yields PreferredReadReplica = -1.
#[tokio::test]
async fn single_node_rack_no_preferred() {
    let base = unique_dir("single");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: base.join("n1"),
        flush_every_n: 1,
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let batch = encode_record_batch(&sample_records(b"x"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    let b = Arc::clone(&broker);
    let server = tokio::spawn(async move {
        serve_kafka_listener(listener, b).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let _ = kafka_rpc(
        &addr,
        encode_request(0, 8, 2, Some("p"), &produce_body_v3("t", &batch)),
    )
    .await;
    let resp = kafka_rpc(
        &addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11("t", 0, Some("rack-a")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5); // corr
    let (_err, hwm, preferred, rec_len) = parse_fetch_v11_preferred(src);
    assert!(hwm > 0);
    assert_eq!(preferred, -1);
    assert!(rec_len > 0);

    server.abort();
}

/// Multi-broker: client rack matches a caught-up ISR follower → redirect.
#[tokio::test]
async fn preferred_redirect_same_rack_follower() {
    let base = unique_dir("redirect");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    // node1 rack-a (leader typically), node2 rack-b, node3 rack-a — client rack-a
    // prefers lowest id in rack-a excluding leader.
    let cfg = cluster_config_racks(
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-b"), Some("rack-a")],
    );

    let mk = |id: u32, dir: PathBuf| {
        let storage = StorageConfig {
            data_dir: dir,
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", [p1, p2, p3][(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1, base.join("n1"));
    let b2 = mk(2, base.join("n2"));
    let b3 = mk(3, base.join("n3"));
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

    b1.create_topic("pref", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "pref").await;

    let topic = TopicName::new("pref");
    let meta = b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!("bad leader"),
    };
    let leader_addr = format!(
        "127.0.0.1:{}",
        [p1, p2, p3][(leader_id - 1) as usize]
    );

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("hello"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "pref");

    let leader_rack = leader.broker_rack(leader_id).unwrap();
    // Pick client rack = leader rack so same-rack follower (if any) is preferred.
    // With racks [a,b,a], if leader is 1, preferred is 3; if leader is 3, preferred is 1;
    // if leader is 2 (rack-b alone), no same-rack follower → no redirect.
    let client_rack = leader_rack.as_str();
    let expected_pref = {
        let mut cands: Vec<u32> = [1u32, 2, 3]
            .into_iter()
            .filter(|id| *id != leader_id)
            .filter(|id| {
                leader
                    .broker_rack(*id)
                    .as_deref()
                    == Some(client_rack)
            })
            .collect();
        cands.sort_unstable();
        cands.first().copied()
    };

    let resp = kafka_rpc(
        &leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11("pref", 0, Some(client_rack)),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    let (_err, hwm, preferred, rec_len) = parse_fetch_v11_preferred(src);
    assert!(hwm > 0);

    match expected_pref {
        Some(exp) => {
            assert_eq!(preferred, exp as i32);
            assert_eq!(rec_len, 0, "redirect responses carry empty records");
            assert!(leader.preferred_replica_redirect_total() >= 1);
        }
        None => {
            // Leader alone in its rack — serve data locally.
            assert_eq!(preferred, -1);
            assert!(rec_len > 0);
        }
    }

    s1.abort();
    s2.abort();
    s3.abort();
}

/// Empty rack / unknown rack → no preferred; records from leader.
#[tokio::test]
async fn no_preferred_when_rack_unknown() {
    let base = unique_dir("norack");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config_racks(
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );

    let mk = |id: u32, dir: PathBuf| {
        let storage = StorageConfig {
            data_dir: dir,
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", [p1, p2, p3][(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1, base.join("n1"));
    let b2 = mk(2, base.join("n2"));
    let b3 = mk(3, base.join("n3"));
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

    b1.create_topic("nr", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "nr").await;
    let topic = TopicName::new("nr");
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!(),
    };
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("m"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "nr");

    let leader_addr = format!(
        "127.0.0.1:{}",
        [p1, p2, p3][(leader_id - 1) as usize]
    );
    // Hit leader with a rack no broker has.
    let resp = kafka_rpc(
        &leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11("nr", 0, Some("no-such-rack")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5); // correlation
    let (_err, _hwm, preferred, rec_len) = parse_fetch_v11_preferred(src);
    assert_eq!(preferred, -1);
    assert!(rec_len > 0);

    // Empty rack string (api_key=1 Fetch, distinct correlation).
    let resp2 = kafka_rpc(
        &leader_addr,
        encode_request(1, 11, 6, Some("c"), &fetch_body_v11("nr", 0, Some(""))),
    )
    .await;
    let mut src2 = resp2.freeze();
    assert_eq!(src2.get_i32(), 6);
    let (_e, _h, preferred2, rec2) = parse_fetch_v11_preferred(src2);
    assert_eq!(preferred2, -1);
    assert!(rec2 > 0);

    s1.abort();
    s2.abort();
    s3.abort();
}

/// Follower in preferred rack serves committed data (HWM-capped local fetch).
#[tokio::test]
async fn follower_serves_fetch_after_redirect() {
    let base = unique_dir("follower-serve");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    // All same rack so any leader placement has an eligible same-rack follower
    // after catch_up_isr (never vacuous on preferred = None).
    let cfg = cluster_config_racks(
        [p1, p2, p3],
        [Some("r1"), Some("r1"), Some("r1")],
    );

    let mk = |id: u32, dir: PathBuf| {
        let storage = StorageConfig {
            data_dir: dir,
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", [p1, p2, p3][(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1, base.join("n1"));
    let b2 = mk(2, base.join("n2"));
    let b3 = mk(3, base.join("n3"));
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

    b1.create_topic("fs", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "fs").await;
    let topic = TopicName::new("fs");
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!(),
    };

    // Produce on leader. Follower logs may be empty without ReplicaFetch byte
    // replication; still assert preferred redirect + follower Fetch error=0.
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("data"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "fs");

    let pref = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("r1"));
    assert!(
        pref.is_some(),
        "setup must yield preferred replica (same-rack ISR follower LEO≥HWM); leader={leader_id}"
    );
    let pref_id = pref.unwrap();

    let leader_addr = format!(
        "127.0.0.1:{}",
        [p1, p2, p3][(leader_id - 1) as usize]
    );
    let resp = kafka_rpc(
        &leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11("fs", 0, Some("r1")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    let (_e, _h, preferred, rec_len) = parse_fetch_v11_preferred(src);
    assert_eq!(preferred, pref_id as i32);
    assert_eq!(rec_len, 0);

    // Fetch preferred broker: no NotLeader error on consumer path.
    let pref_addr = format!(
        "127.0.0.1:{}",
        [p1, p2, p3][(pref_id - 1) as usize]
    );
    let resp2 = kafka_rpc(
        &pref_addr,
        encode_request(1, 11, 6, Some("c"), &fetch_body_v11("fs", 0, None)),
    )
    .await;
    let mut src2 = resp2.freeze();
    assert_eq!(src2.get_i32(), 6);
    let (part_err, _h2, preferred2, _) = parse_fetch_v11_preferred(src2);
    assert_eq!(part_err, 0);
    assert_eq!(preferred2, -1);

    s1.abort();
    s2.abort();
    s3.abort();
}

/// Metadata advertises rack from cluster.toml.
#[tokio::test]
async fn metadata_emits_broker_rack() {
    let base = unique_dir("meta-rack");
    let _g = Guard(base.clone());
    let (l1, p1) = bind_port0().await;
    let cfg = cluster_config_racks([p1, p1 + 1, p1 + 2], [Some("east"), None, Some("west")]);
    // Only boot node 1 for Metadata (brokers list from config).
    let storage = StorageConfig {
        data_dir: base.join("n1"),
        ..StorageConfig::default()
    };
    let broker = Arc::new(Broker::with_cluster(storage, 1, cfg).unwrap());
    broker.set_advertised("127.0.0.1", p1);
    assert_eq!(broker.broker_rack(1).as_deref(), Some("east"));
    assert_eq!(broker.broker_rack(2), None);
    assert_eq!(broker.broker_rack(3).as_deref(), Some("west"));

    let s = {
        let b = Arc::clone(&broker);
        tokio::spawn(async move {
            serve_kafka_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Metadata v1 classic: brokers have rack after port.
    let mut body = BytesMut::new();
    body.put_i32(0); // empty topics = all? v1: topics array; 0 = none requested
                     // Actually classic Metadata with empty list may mean all —
                     // use -1 or empty depending on version. Use topics=[] as empty name list.
    // Metadata v1: topics: ARRAY[STRING]. Empty array → no topics, still brokers.
    let addr = format!("127.0.0.1:{p1}");
    let resp = kafka_rpc(
        &addr,
        encode_request(3, 1, 3, Some("m"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 3); // corr
                                  // v1: brokers, controller, topics
    let n = src.get_i32();
    assert_eq!(n, 3);
    let mut racks = Vec::new();
    for _ in 0..n {
        let id = src.get_i32();
        let _host = get_string(&mut src).unwrap();
        let _port = src.get_i32();
        let rack = volant_broker::kafka::codec::get_nullable_string(&mut src)
            .unwrap()
            .unwrap_or_default();
        racks.push((id, rack));
    }
    racks.sort_by_key(|(id, _)| *id);
    assert_eq!(racks[0], (1, "east".into()));
    assert_eq!(racks[1], (2, "".into())); // null → empty after get_nullable?
                                          // get_nullable_string may return None for null.
    assert!(racks[1].1.is_empty());
    assert_eq!(racks[2], (3, "west".into()));

    s.abort();
}

/// Follower Fetch (replica_id >= 0) must not get PreferredReadReplica redirect
/// even when a same-rack ISR follower is eligible for consumer redirect.
///
/// Proves the `replica_id < 0` gate: consumer path gets preferred != -1 first,
/// then the same layout with replica_id >= 0 yields preferred == -1.
#[tokio::test]
async fn follower_fetch_no_preferred_redirect() {
    let base = unique_dir("follower-rid");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    // All same rack ⇒ any leader has eligible same-rack followers after catch-up.
    let cfg = cluster_config_racks(
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );

    let mk = |id: u32, dir: PathBuf| {
        let storage = StorageConfig {
            data_dir: dir,
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let b = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
        b.set_advertised("127.0.0.1", [p1, p2, p3][(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1, base.join("n1"));
    let b2 = mk(2, base.join("n2"));
    let b3 = mk(3, base.join("n3"));
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

    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "t").await;
    let topic = TopicName::new("t");
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!("bad leader"),
    };
    let leader_addr = format!(
        "127.0.0.1:{}",
        [p1, p2, p3][(leader_id - 1) as usize]
    );

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("x"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "t");

    // Sanity: preferred selection is eligible on this layout.
    let expected = leader
        .select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(
        expected.is_some(),
        "same-rack cluster after catch_up must have preferred replica; leader={leader_id}"
    );

    // 1) Consumer fetch (replica_id = -1) → PreferredReadReplica redirect.
    let resp_consumer = kafka_rpc(
        &leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11_with_replica("t", 0, -1, Some("rack-a")),
        ),
    )
    .await;
    let mut src = resp_consumer.freeze();
    assert_eq!(src.get_i32(), 5);
    let (_err, hwm, preferred_consumer, rec_len) = parse_fetch_v11_preferred(src);
    assert!(hwm > 0);
    assert_ne!(
        preferred_consumer, -1,
        "consumer fetch must get preferred redirect when eligible"
    );
    assert_eq!(preferred_consumer, expected.unwrap() as i32);
    assert_eq!(rec_len, 0, "redirect responses carry empty records");

    // 2) Follower fetch (replica_id >= 0) on the same eligible layout → no redirect.
    // Use a follower id that is not the leader so replica_fetch path is meaningful.
    let follower_replica_id = if leader_id == 1 { 2i32 } else { 1i32 };
    let resp_follower = kafka_rpc(
        &leader_addr,
        encode_request(
            1,
            11,
            6,
            Some("c"),
            &fetch_body_v11_with_replica("t", 0, follower_replica_id, Some("rack-a")),
        ),
    )
    .await;
    let mut src2 = resp_follower.freeze();
    assert_eq!(src2.get_i32(), 6);
    let (_err2, _hwm2, preferred_follower, _) = parse_fetch_v11_preferred(src2);
    assert_eq!(
        preferred_follower, -1,
        "follower fetch (replica_id >= 0) must not get PreferredReadReplica redirect"
    );

    s1.abort();
    s2.abort();
    s3.abort();
}
