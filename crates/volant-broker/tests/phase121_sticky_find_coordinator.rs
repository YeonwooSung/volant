//! Phase 121: sticky FindCoordinator assignment (multi-broker).

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, get_nullable_string, get_string, put_compact_array_len,
    put_compact_nullable_string, put_compact_string, put_empty_tag_buffer, put_string,
    skip_tag_buffer,
};
use volant_broker::{
    sticky_coordinator_id, serve_kafka_listener, serve_listener, start_background_tasks, Broker,
    BrokerEndpoint, ClusterConfig,
};
use volant_client::{Client, ClientConfig};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p121-{label}-{}-{}",
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

fn find_coord_v4_body(key_type: i8, keys: &[&str]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i8(key_type);
    put_compact_array_len(&mut body, keys.len());
    for k in keys {
        put_compact_string(&mut body, k);
    }
    put_empty_tag_buffer(&mut body);
    body
}

fn find_coord_v1_body(key: &str, key_type: i8) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, key);
    body.put_i8(key_type);
    body
}

/// Parse FindCoordinator v4+ batch response → list of (key, node_id).
fn parse_v4_nodes(mut src: bytes::Bytes, corr: i32) -> Vec<(String, i32)> {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let key = get_compact_string(&mut src).unwrap();
        let node = src.get_i32();
        let host = get_compact_string(&mut src).unwrap();
        let port = src.get_i32();
        let err = src.get_i16();
        assert_eq!(err, 0, "FindCoordinator error for {key}");
        assert_ne!(err, 123);
        let _ = get_compact_nullable_string(&mut src).unwrap();
        skip_tag_buffer(&mut src).unwrap();
        assert!(!host.is_empty() && port > 0);
        out.push((key, node));
    }
    skip_tag_buffer(&mut src).unwrap();
    out
}

struct ClusterHarness {
    _guard: Guard,
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    native_ports: [u16; 3],
    kafka_addrs: [String; 3],
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

        for (listener, b) in [(n1, &b1), (n2, &b2), (n3, &b3)] {
            let b = Arc::clone(b);
            tokio::spawn(async move {
                let _ = serve_listener(listener, b).await;
            });
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
        tokio::time::sleep(Duration::from_millis(150)).await;

        Self {
            _guard: guard,
            b1,
            b2,
            b3,
            native_ports,
            kafka_addrs: [
                kafka_addrs[0].clone(),
                kafka_addrs[1].clone(),
                kafka_addrs[2].clone(),
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
}

async fn find_v4(addr: &str, corr: i32, key_type: i8, keys: &[&str]) -> Vec<(String, i32)> {
    let resp = kafka_rpc(
        addr,
        encode_request_flexible(10, 4, corr, Some("fc"), &find_coord_v4_body(key_type, keys)),
    )
    .await;
    parse_v4_nodes(resp.freeze(), corr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_stable_across_brokers() {
    let h = ClusterHarness::boot().await;
    let key = "txn-stable-alpha";
    let mut nodes = Vec::new();
    for id in 1u32..=3 {
        let entries = find_v4(h.kafka_of(id), id as i32, 1, &[key]).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, key);
        nodes.push(entries[0].1);
    }
    assert_eq!(nodes[0], nodes[1]);
    assert_eq!(nodes[1], nodes[2]);
    assert!((1..=3).contains(&nodes[0]));

    // Repeatability on same broker.
    let again = find_v4(h.kafka_of(1), 99, 1, &[key]).await;
    assert_eq!(again[0].1, nodes[0]);

    // Matches pure sticky helper over full live ring.
    let expected = sticky_coordinator_id(key.as_bytes(), &[1, 2, 3], &[1, 2, 3]).unwrap() as i32;
    assert_eq!(nodes[0], expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_keys_can_spread() {
    let h = ClusterHarness::boot().await;
    let keys: Vec<String> = (0..64).map(|i| format!("group-spread-{i}")).collect();
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let entries = find_v4(h.kafka_of(2), 7, 0, &key_refs).await;
    assert_eq!(entries.len(), 64);
    let distinct: HashSet<i32> = entries.iter().map(|(_, n)| *n).collect();
    assert!(
        distinct.len() >= 2,
        "expected spread across brokers, got {distinct:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_preferred_fails_over_to_next_live() {
    let h = ClusterHarness::boot().await;
    let key = "failover-me";
    let preferred = sticky_coordinator_id(key.as_bytes(), &[1, 2, 3], &[1, 2, 3]).unwrap();
    let live_without: Vec<u32> = [1u32, 2, 3]
        .into_iter()
        .filter(|id| *id != preferred)
        .collect();
    let expected_failover =
        sticky_coordinator_id(key.as_bytes(), &[1, 2, 3], &live_without).unwrap();

    // Mark preferred dead on every observer so membership agrees.
    for id in 1u32..=3 {
        if id != preferred {
            h.broker_of(id).on_broker_death(preferred).unwrap();
        }
    }
    // Preferred itself cannot mark self dead; resolve from a peer.
    let ask = if preferred == 1 { 2 } else { 1 };
    let entries = find_v4(h.kafka_of(ask), 3, 0, &[key]).await;
    assert_eq!(entries[0].1 as u32, expected_failover);

    // Revive preferred → sticky returns preferred again.
    for id in 1u32..=3 {
        h.broker_of(id).note_peer_live(preferred);
    }
    let back = find_v4(h.kafka_of(ask), 4, 0, &[key]).await;
    assert_eq!(back[0].1 as u32, preferred);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_override_returns_init_owner() {
    let h = ClusterHarness::boot().await;
    let key = "txn-init-elsewhere";
    let sticky = sticky_coordinator_id(key.as_bytes(), &[1, 2, 3], &[1, 2, 3]).unwrap();
    let owner = if sticky == 1 { 2 } else { 1 };

    // Init on non-sticky owner via Kafka InitProducerId v0 (classic).
    let mut body = BytesMut::new();
    put_string(&mut body, key);
    body.put_i32(60_000);
    let resp = kafka_rpc(
        h.kafka_of(owner),
        encode_request(22, 0, 1, Some("p"), &body),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    let _pid = src.get_i64();
    let _epoch = src.get_i16();

    // Allow Init registration fan-out.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // FindCoordinator from any broker should return Init owner, not sticky hash.
    for id in 1u32..=3 {
        let entries = find_v4(h.kafka_of(id), 10 + id as i32, 1, &[key]).await;
        assert_eq!(
            entries[0].1 as u32, owner,
            "broker {id} should return Init owner {owner}, sticky was {sticky}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endtxn_forward_still_works_with_sticky_find() {
    // Phase 120 interaction: Init on sticky coordinator; EndTxn via other broker.
    let h = ClusterHarness::boot().await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", h.native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    // Propagate assignment.
    for _ in 0..50 {
        let (_, gen, cid, topics) = h.b1.cluster_state_snapshot();
        let _ = h.b2.apply_cluster_state(gen, cid, &topics);
        let _ = h.b3.apply_cluster_state(gen, cid, &topics);
        if h.b2.partition_count_opt("events").is_some()
            && h.b3.partition_count_opt("events").is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let key = "txn-sticky-end";
    let sticky = sticky_coordinator_id(key.as_bytes(), &[1, 2, 3], &[1, 2, 3]).unwrap();
    let other = if sticky == 1 { 2 } else { 1 };

    // FindCoordinator before Init → sticky.
    let pre = find_v4(h.kafka_of(other), 1, 1, &[key]).await;
    assert_eq!(pre[0].1 as u32, sticky);

    // Init v6 classic (Enable2Pc=false) on sticky coordinator.
    let mut ibody = BytesMut::new();
    put_compact_nullable_string(&mut ibody, Some(key));
    ibody.put_i32(60_000);
    ibody.put_i64(-1);
    ibody.put_i16(-1);
    ibody.put_u8(0); // enable_2pc false — classic one-shot
    ibody.put_u8(0);
    put_empty_tag_buffer(&mut ibody);
    let iresp = kafka_rpc(
        h.kafka_of(sticky),
        encode_request_flexible(22, 6, 1, Some("p"), &ibody),
    )
    .await;
    let mut isrc = iresp.freeze();
    assert_eq!(isrc.get_i32(), 1);
    skip_tag_buffer(&mut isrc).unwrap();
    assert_eq!(isrc.get_i32(), 0);
    assert_eq!(isrc.get_i16(), 0);
    let pid = isrc.get_i64();
    let epoch = isrc.get_i16();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Open txn via AddPartitions on sticky (required before EndTxn).
    let mut abody = BytesMut::new();
    put_string(&mut abody, key);
    abody.put_i64(pid);
    abody.put_i16(epoch);
    abody.put_i32(1);
    put_string(&mut abody, "events");
    abody.put_i32(1);
    abody.put_i32(0);
    let aresp = kafka_rpc(
        h.kafka_of(sticky),
        encode_request(24, 0, 2, Some("p"), &abody),
    )
    .await;
    let mut asrc = aresp.freeze();
    asrc.advance(4 + 4);
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(get_string(&mut asrc).unwrap(), "events");
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(asrc.get_i32(), 0);
    assert_eq!(asrc.get_i16(), 0);
    tokio::time::sleep(Duration::from_millis(80)).await;

    // EndTxn on non-coordinator should forward (Phase 120).
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, key);
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(1); // commit
    let eresp = kafka_rpc(
        h.kafka_of(other),
        encode_request(26, 0, 3, Some("p"), &ebody),
    )
    .await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    let err = es.get_i16();
    assert_eq!(err, 0, "EndTxn via non-coordinator should succeed via forward");
    assert!(
        h.broker_of(other).txn_forward_total() > 0,
        "forward metric should increment on non-coordinator"
    );
}

#[tokio::test]
async fn single_node_find_coordinator_self() {
    let dir = unique_dir("single");
    let _g = Guard(dir.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_advertised("127.0.0.1", 19092);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{port}");
    let b = Arc::clone(&broker);
    let server = tokio::spawn(async move {
        let _ = serve_kafka_listener(listener, b).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = kafka_rpc(
        &addr,
        encode_request(10, 1, 5, Some("c"), &find_coord_v1_body("g1", 0)),
    )
    .await;
    // Classic v1 response header is correlation only (no tags).
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);
    let node = src.get_i32();
    let host = get_string(&mut src).unwrap();
    let p = src.get_i32();
    assert_eq!(node, broker.node_id() as i32);
    assert_eq!(host, "127.0.0.1");
    assert_eq!(p, 19092);

    server.abort();
}

#[test]
fn sticky_helper_unit() {
    let ring = [1u32, 2, 3];
    let live = [1u32, 2, 3];
    assert_eq!(
        sticky_coordinator_id(b"a", &ring, &live),
        sticky_coordinator_id(b"a", &ring, &live)
    );
    let mut seen = HashSet::new();
    for i in 0..100u32 {
        let k = format!("k{i}");
        seen.insert(sticky_coordinator_id(k.as_bytes(), &ring, &live).unwrap());
    }
    assert!(seen.len() >= 2);
}
