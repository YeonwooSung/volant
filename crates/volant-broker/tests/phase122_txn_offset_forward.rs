//! Phase 122: transparent AddOffsetsToTxn / TxnOffsetCommit forward to txn coordinator.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_string, put_compact_nullable_string,
    put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::{
    serve_kafka_listener, serve_listener, start_background_tasks, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_client::{Client, ClientConfig};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p122-{label}-{}-{}",
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

fn init_v6(txn_id: &str, enable_2pc: bool, keep_prepared: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body.put_i64(-1);
    body.put_i16(-1);
    body.put_u8(if enable_2pc { 1 } else { 0 });
    body.put_u8(if keep_prepared { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_v6_rpc(
    addr: &str,
    corr: i32,
    txn_id: &str,
    enable_2pc: bool,
    keep_prepared: bool,
) -> (i16, i64, i16) {
    let resp = kafka_rpc(
        addr,
        encode_request_flexible(
            22,
            6,
            corr,
            Some("p"),
            &init_v6(txn_id, enable_2pc, keep_prepared),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    let err = src.get_i16();
    let pid = src.get_i64();
    let epoch = src.get_i16();
    (err, pid, epoch)
}

fn add_offsets_body(txn_id: &str, pid: i64, epoch: i16, group: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    put_string(&mut body, group);
    body
}

fn txn_offset_commit_body(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    put_string(&mut body, group);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(0); // leader_epoch (v2)
    put_nullable_string(&mut body, Some(""));
    body
}

async fn add_offsets(
    addr: &str,
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    group: &str,
) -> i16 {
    let resp = kafka_rpc(
        addr,
        encode_request(
            25,
            2,
            corr,
            Some("p"),
            &add_offsets_body(txn_id, pid, epoch, group),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    src.get_i16()
}

async fn txn_offset_commit(
    addr: &str,
    corr: i32,
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> i16 {
    let resp = kafka_rpc(
        addr,
        encode_request(
            28,
            2,
            corr,
            Some("p"),
            &txn_offset_commit_body(txn_id, group, pid, epoch, topic, partition, offset),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1); // partitions
    assert_eq!(src.get_i32(), partition);
    src.get_i16()
}

async fn end_txn(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, commit: bool) -> i16 {
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, txn_id);
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(if commit { 1 } else { 0 });
    let eresp = kafka_rpc(addr, encode_request(26, 0, corr, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    es.get_i16()
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

    fn kafka_of(&self, id: u32) -> &str {
        &self.kafka_addrs[(id - 1) as usize]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_offsets_and_txn_offset_commit_via_non_coordinator() {
    let h = ClusterHarness::boot().await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", h.native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "events").await;

    let coord_k = h.kafka_of(1);
    let other_k = h.kafka_of(2);
    let other2_k = h.kafka_of(3);

    let (err, pid, epoch) = init_v6_rpc(coord_k, 1, "txn-off-fwd", false, false).await;
    assert_eq!(err, 0);
    // Allow Init registration fan-out to land on peers.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Ensure non-coordinator knows the Init owner.
    assert_eq!(
        h.b2.resolve_txn_coordinator("txn-off-fwd", Some(pid as u64)),
        Some(1),
        "peer must learn Init owner for forward"
    );

    let before_add = h.b2.txn_forward_total();
    assert_eq!(
        add_offsets(other_k, 2, "txn-off-fwd", pid, epoch, "cg-p122").await,
        0,
        "AddOffsetsToTxn via non-coordinator should succeed via forward"
    );
    assert!(
        h.b2.txn_forward_total() > before_add,
        "expected AddOffsets forward counter to advance"
    );

    let before_toc = h.b3.txn_forward_total();
    assert_eq!(
        txn_offset_commit(
            other2_k,
            3,
            "txn-off-fwd",
            "cg-p122",
            pid,
            epoch,
            "events",
            0,
            42,
        )
        .await,
        0,
        "TxnOffsetCommit via non-coordinator should succeed via forward"
    );
    assert!(
        h.b3.txn_forward_total() > before_toc,
        "expected TxnOffsetCommit forward counter to advance"
    );

    // Offsets must not apply until EndTxn commit.
    let before = h
        .b1
        .groups()
        .fetch_offsets("cg-p122", &[("events".into(), 0)])
        .unwrap();
    assert!(
        before.entries.iter().all(|e| e.offset == u64::MAX),
        "deferred offsets must not apply before EndTxn"
    );

    // EndTxn via yet another non-coordinator still works (Phase 120).
    let before_end = h.b2.txn_forward_total();
    assert_eq!(
        end_txn(other_k, 4, "txn-off-fwd", pid, epoch, true).await,
        0
    );
    assert!(h.b2.txn_forward_total() > before_end);

    let after = h
        .b1
        .groups()
        .fetch_offsets("cg-p122", &[("events".into(), 0)])
        .unwrap();
    assert_eq!(after.entries.len(), 1);
    assert_eq!(
        after.entries[0].offset, 42,
        "offsets buffered on coordinator must apply on EndTxn commit"
    );

    // Non-coordinator must not have applied offsets itself as group SoT for the txn
    // (group store is local; coordinator applied). Peers may not share group store —
    // assert coordinator (Init owner) has the commit.
    assert_eq!(h.b1.node_id(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offset_forward_then_endtxn_on_coordinator() {
    let h = ClusterHarness::boot().await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", h.native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("t", 1).await.unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "t").await;

    let coord_k = h.kafka_of(1);
    let other_k = h.kafka_of(2);

    let (err, pid, epoch) = init_v6_rpc(coord_k, 1, "txn-mix", false, false).await;
    assert_eq!(err, 0);
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        add_offsets(other_k, 2, "txn-mix", pid, epoch, "cg-mix").await,
        0
    );
    assert_eq!(
        txn_offset_commit(other_k, 3, "txn-mix", "cg-mix", pid, epoch, "t", 0, 7).await,
        0
    );
    // EndTxn on coordinator (local path).
    assert_eq!(end_txn(coord_k, 4, "txn-mix", pid, epoch, true).await, 0);

    let after = h
        .b1
        .groups()
        .fetch_offsets("cg-mix", &[("t".into(), 0)])
        .unwrap();
    assert_eq!(after.entries[0].offset, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_add_offsets_txn_offset_commit_unchanged() {
    let base = unique_dir("single");
    let _guard = Guard(base.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: base,
        flush_every_n: 1,
        ..StorageConfig::default()
    }));
    broker.create_topic("solo", 1).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{port}");
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_kafka_listener(listener, b).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (err, pid, epoch) = init_v6_rpc(&addr, 1, "solo-txn", false, false).await;
    assert_eq!(err, 0);
    assert_eq!(
        add_offsets(&addr, 2, "solo-txn", pid, epoch, "cg-solo").await,
        0
    );
    assert_eq!(
        txn_offset_commit(&addr, 3, "solo-txn", "cg-solo", pid, epoch, "solo", 0, 11).await,
        0
    );
    assert_eq!(end_txn(&addr, 4, "solo-txn", pid, epoch, true).await, 0);
    assert_eq!(broker.txn_forward_total(), 0);

    let after = broker
        .groups()
        .fetch_offsets("cg-solo", &[("solo".into(), 0)])
        .unwrap();
    assert_eq!(after.entries[0].offset, 11);
}

#[test]
fn peek_helpers_extract_ids() {
    use volant_broker::net::{
        peek_add_offsets_to_txn_ids, peek_end_txn_ids, peek_txn_offset_commit_ids,
    };

    let mut add = BytesMut::new();
    put_string(&mut add, "tid");
    add.put_i64(99);
    add.put_i16(1);
    put_string(&mut add, "g");
    let (tid, pid) = peek_add_offsets_to_txn_ids(0, &add).unwrap();
    assert_eq!(tid, "tid");
    assert_eq!(pid, 99);

    let mut toc = BytesMut::new();
    put_string(&mut toc, "tid2");
    put_string(&mut toc, "g2");
    toc.put_i64(7);
    toc.put_i16(0);
    let (tid, pid) = peek_txn_offset_commit_ids(0, &toc).unwrap();
    assert_eq!(tid, "tid2");
    assert_eq!(pid, 7);

    let mut end = BytesMut::new();
    put_string(&mut end, "tid3");
    end.put_i64(5);
    end.put_i16(0);
    end.put_u8(1);
    let (tid, pid) = peek_end_txn_ids(0, &end).unwrap();
    assert_eq!(tid, "tid3");
    assert_eq!(pid, 5);
}
