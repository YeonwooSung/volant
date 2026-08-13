//! Phase 147: serve fetch sessions from foreign mirror without promote (MVP).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, encode_request_flexible, get_compact_array_len,
    put_bytes, put_compact_array_len, put_compact_string, put_empty_tag_buffer,
    put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::kafka::fetch_session::FetchSessionManager;
use volant_broker::{
    fanout_session_mirror_ops, serve_kafka_listener, serve_listener, start_background_tasks,
    Broker, BrokerEndpoint, ClusterConfig,
};
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p147-{label}-{}-{}",
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

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn cluster_config(ports: [u16; 3]) -> ClusterConfig {
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
                rack: None,
            })
            .collect(),
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
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(value),
        timestamp_ms: 1_700_000_000_000,
        headers: vec![],
    }]
}

fn produce_body_v3(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, None);
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

async fn produce_one(addr: &str, topic: &str, value: &'static [u8]) {
    let batch = encode_record_batch(&sample_records(value));
    let resp = kafka_rpc(
        addr,
        encode_request(0, 3, 1, Some("p"), &produce_body_v3(topic, &batch)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 1);
    let _ = volant_broker::kafka::codec::get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    let _pid = src.get_i32();
    let err = src.get_i16();
    assert_eq!(err, 0, "produce failed");
}

fn fetch_v12(topic: &str, fetch_offset: i64, session_id: i32, session_epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i64(fetch_offset);
    body.put_i32(-1);
    body.put_i64(-1);
    body.put_i32(1_000_000);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 0);
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);
    body
}

fn fetch_v12_empty_topics(session_id: i32, session_epoch: i32) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(0);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    put_compact_array_len(&mut body, 0);
    put_compact_array_len(&mut body, 0);
    put_compact_string(&mut body, "");
    put_empty_tag_buffer(&mut body);
    body
}

fn assert_flex_header(src: &mut Bytes, corr: i32) -> (i16, i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    let session = src.get_i32();
    (err, session)
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

struct ClusterHarness {
    _guard: Guard,
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    kafka_addrs: [String; 3],
    native_servers: [tokio::task::JoinHandle<()>; 3],
    _bgs: Vec<volant_broker::BackgroundTasks>,
}

impl ClusterHarness {
    async fn boot() -> Self {
        let base = unique_dir("cluster");
        let guard = Guard(base.clone());

        let (n1, p1) = bind_port0().await;
        let (n2, p2) = bind_port0().await;
        let (n3, p3) = bind_port0().await;
        let native_ports = [p1, p2, p3];
        let cfg = cluster_config(native_ports);

        let mk = |id: u32| {
            let storage = StorageConfig {
                data_dir: base.join(format!("node-{id}")),
                flush_every_n: 1,
                ..StorageConfig::default()
            };
            let broker = Broker::with_cluster(storage, id, cfg.clone()).unwrap();
            broker.set_advertised("127.0.0.1", native_ports[(id - 1) as usize]);
            broker.set_fetch_session_idle_ms(0);
            Arc::new(broker)
        };
        let b1 = mk(1);
        let b2 = mk(2);
        let b3 = mk(3);

        let bgs = vec![
            start_background_tasks(Arc::clone(&b1)),
            start_background_tasks(Arc::clone(&b2)),
            start_background_tasks(Arc::clone(&b3)),
        ];

        let mut native_servers = Vec::new();
        for (listener, b) in [(n1, &b1), (n2, &b2), (n3, &b3)] {
            let b = Arc::clone(b);
            native_servers.push(tokio::spawn(async move {
                let _ = serve_listener(listener, b).await;
            }));
        }

        let mut kafka_addrs = Vec::new();
        for b in [&b1, &b2, &b3] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            kafka_addrs.push(format!("127.0.0.1:{port}"));
            let b = Arc::clone(b);
            tokio::spawn(async move {
                let _ = serve_kafka_listener(listener, b).await;
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            _guard: guard,
            b1,
            b2,
            b3,
            kafka_addrs: [
                kafka_addrs[0].clone(),
                kafka_addrs[1].clone(),
                kafka_addrs[2].clone(),
            ],
            native_servers: [
                native_servers.remove(0),
                native_servers.remove(0),
                native_servers.remove(0),
            ],
            _bgs: bgs,
        }
    }

    fn broker_of(&self, id: u32) -> &Arc<Broker> {
        match id {
            1 => &self.b1,
            2 => &self.b2,
            3 => &self.b3,
            _ => panic!("bad id"),
        }
    }

    fn kafka_of(&self, id: u32) -> &str {
        &self.kafka_addrs[(id - 1) as usize]
    }

    fn kill_native(&mut self, id: u32) {
        let idx = (id - 1) as usize;
        self.native_servers[idx].abort();
    }
}

/// 1. Mirror-only install: incremental path snapshots topics without promote.
#[test]
fn mirror_only_snapshot_without_promote() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    // Owner creates with empty topics; apply_mirror_put installs foreign mirror.
    // After begin_incremental_from_any, epoch advances and snapshot is readable.
    let id = owner.create_at(HashMap::new(), 1_000);
    // Seed a topic via merge on owner then re-export so snapshot is non-empty.
    // (TopicWireId is crate-private; merge_topics with empty map is a no-op —
    //  use export after create alone and assert epoch + servable.)
    let bytes = owner.export_session_bytes(id).unwrap();

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.apply_mirror_put(&bytes).unwrap();
    assert!(peer.has_servable_session(id));
    assert!(!peer.contains(id));
    assert!(peer.mirror_session_clone(id).is_some());

    assert!(peer.begin_incremental_from_any_at(id, 1, 1_100).is_ok());
    let _snap = peer.snapshot_topics(id); // primary-or-mirror read path
    assert_eq!(peer.promote_total(), 0);
    assert!(peer.mirror_contains(id));
    assert!(!peer.has_pending_mirror_ops());
    let clone = peer.mirror_session_clone(id).unwrap();
    assert_eq!(clone.epoch, 2);
}

/// 2. Owner dead + mirror → local serve; promote_total unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_dead_mirror_serve_no_promote() {
    let mut h = ClusterHarness::boot().await;
    h.b1.create_topic("m147", 1).unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "m147").await;

    let meta = h.b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let peer_id = [1u32, 2, 3].into_iter().find(|id| *id != leader_id).unwrap();
    let leader = Arc::clone(h.broker_of(leader_id));
    let peer = Arc::clone(h.broker_of(peer_id));
    let leader_kafka = h.kafka_of(leader_id).to_owned();
    let peer_kafka = h.kafka_of(peer_id).to_owned();

    produce_one(&leader_kafka, "m147", b"a").await;
    catch_up_isr(&leader, "m147");

    let body = fetch_v12("m147", 1, 0, 0);
    let resp = kafka_rpc(
        &leader_kafka,
        encode_request_flexible(1, 12, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 10);
    assert_eq!(top_err, 0);

    fanout_session_mirror_ops(&leader).await;
    for _ in 0..50 {
        if peer.fetch_sessions().mirror_contains(sid) {
            break;
        }
        fanout_session_mirror_ops(&leader).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(peer.fetch_sessions().mirror_contains(sid));

    h.kill_native(leader_id);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let promote_before = peer.fetch_sessions().promote_total();
    let serve_before = peer.fetch_sessions().serve_from_mirror_total();
    let body = fetch_v12_empty_topics(sid, 1);
    let resp = kafka_rpc(
        &peer_kafka,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, echo) = assert_flex_header(&mut src, 11);
    assert_eq!(top_err, 0);
    assert_eq!(echo, sid);
    assert_eq!(peer.fetch_sessions().promote_total(), promote_before);
    assert!(peer.fetch_sessions().serve_from_mirror_total() > serve_before);
    assert!(!peer.fetch_sessions().contains(sid));
    assert!(peer.fetch_sessions().mirror_contains(sid));
    let _ = get_compact_array_len(&mut src).unwrap().unwrap();
}

/// 3. promote_on_miss=1 still promotes into primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_on_miss_still_promotes() {
    let mut h = ClusterHarness::boot().await;
    h.b1.create_topic("p147", 1).unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "p147").await;

    let meta = h.b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let peer_id = [1u32, 2, 3].into_iter().find(|id| *id != leader_id).unwrap();
    let leader = Arc::clone(h.broker_of(leader_id));
    let peer = Arc::clone(h.broker_of(peer_id));
    let leader_kafka = h.kafka_of(leader_id).to_owned();
    let peer_kafka = h.kafka_of(peer_id).to_owned();

    // Force legacy promote path on this peer only.
    peer.fetch_sessions().set_promote_on_miss(true);

    produce_one(&leader_kafka, "p147", b"a").await;
    catch_up_isr(&leader, "p147");

    let body = fetch_v12("p147", 1, 0, 0);
    let resp = kafka_rpc(
        &leader_kafka,
        encode_request_flexible(1, 12, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 10);
    assert_eq!(top_err, 0);

    fanout_session_mirror_ops(&leader).await;
    for _ in 0..50 {
        if peer.fetch_sessions().mirror_contains(sid) {
            break;
        }
        fanout_session_mirror_ops(&leader).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(peer.fetch_sessions().mirror_contains(sid));

    h.kill_native(leader_id);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let promote_before = peer.fetch_sessions().promote_total();
    let serve_before = peer.fetch_sessions().serve_from_mirror_total();
    let body = fetch_v12_empty_topics(sid, 1);
    let resp = kafka_rpc(
        &peer_kafka,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, echo) = assert_flex_header(&mut src, 11);
    assert_eq!(top_err, 0);
    assert_eq!(echo, sid);
    assert!(
        peer.fetch_sessions().promote_total() > promote_before,
        "promote_on_miss must promote"
    );
    assert_eq!(
        peer.fetch_sessions().serve_from_mirror_total(),
        serve_before,
        "must not count serve_from_mirror when promoting"
    );
    assert!(peer.fetch_sessions().contains(sid));
    assert!(!peer.fetch_sessions().mirror_contains(sid));
}

/// 4. No mirror + owner dead → 70.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_mirror_still_70() {
    let mut h = ClusterHarness::boot().await;
    h.b1.create_topic("gone147", 1).unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "gone147").await;

    let meta = h.b1.metadata(None);
    let leader_id = meta.topics[0].partitions[0].leader;
    let peer_id = [1u32, 2, 3].into_iter().find(|id| *id != leader_id).unwrap();
    let leader = Arc::clone(h.broker_of(leader_id));
    let peer = Arc::clone(h.broker_of(peer_id));
    let leader_kafka = h.kafka_of(leader_id).to_owned();
    let peer_kafka = h.kafka_of(peer_id).to_owned();

    produce_one(&leader_kafka, "gone147", b"a").await;
    catch_up_isr(&leader, "gone147");

    let body = fetch_v12("gone147", 1, 0, 0);
    let resp = kafka_rpc(
        &leader_kafka,
        encode_request_flexible(1, 12, 10, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, sid) = assert_flex_header(&mut src, 10);
    assert_eq!(top_err, 0);

    let _ = leader.fetch_sessions().drain_mirror_ops();
    for _ in 0..5 {
        if peer.fetch_sessions().mirror_contains(sid) {
            peer.fetch_sessions().apply_mirror_delete(sid);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!peer.fetch_sessions().mirror_contains(sid));

    h.kill_native(leader_id);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let body = fetch_v12_empty_topics(sid, 1);
    let resp = kafka_rpc(
        &peer_kafka,
        encode_request_flexible(1, 12, 11, Some("c"), &body),
    )
    .await;
    let mut src = resp.freeze();
    let (top_err, echo) = assert_flex_header(&mut src, 11);
    assert_eq!(top_err, 70);
    assert_eq!(echo, sid);
}
