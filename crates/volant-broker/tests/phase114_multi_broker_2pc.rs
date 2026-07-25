//! Phase 114: multi-broker Enable2Pc prepare/commit across partition leaders.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, encode_request_flexible, get_bytes, get_string,
    put_bytes, put_compact_nullable_string, put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::{
    run_txn_2pc_fanout, serve_kafka_listener, serve_listener, start_background_tasks, Broker,
    BrokerEndpoint, ClusterConfig, Txn2pcFanout,
};
use volant_client::{Client, ClientConfig};
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p114-{label}-{}-{}",
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

async fn add_partitions(
    addr: &str,
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partitions: &[i32],
) {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(partitions.len() as i32);
    for &p in partitions {
        body.put_i32(p);
    }
    let resp = kafka_rpc(addr, encode_request(24, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4); // corr + throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), partitions.len() as i32);
    for &p in partitions {
        assert_eq!(src.get_i32(), p);
        assert_eq!(src.get_i16(), 0, "add partitions error on p={p}");
    }
}

fn sample(val: &'static [u8]) -> Vec<volant_core::Record> {
    use volant_core::{Offset, Record};
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(val),
        timestamp_ms: 1,
        headers: vec![],
    }]
}

async fn produce_txn(
    addr: &str,
    corr: i32,
    topic: &str,
    partition: i32,
    pid: i64,
    epoch: i16,
    seq: i32,
    val: &'static [u8],
) {
    let batch = encode_record_batch_idempotent(&sample(val), pid, epoch, seq);
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    put_bytes(&mut body, Some(&batch));
    let resp = kafka_rpc(addr, encode_request(0, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), partition);
    let err = src.get_i16();
    assert_eq!(err, 0, "produce error on partition {partition}");
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

async fn fetch_v4_lso(
    addr: &str,
    corr: i32,
    topic: &str,
    partition: i32,
    isolation: u8,
) -> (i64, i64, Bytes) {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(100);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(isolation);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(0);
    body.put_i32(1_048_576);
    let resp = kafka_rpc(addr, encode_request(1, 4, corr, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), partition);
    assert_eq!(src.get_i16(), 0);
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let aborted_n = src.get_i32();
    for _ in 0..aborted_n {
        let _ = src.get_i64();
        let _ = src.get_i64();
    }
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    (hwm, lso, records)
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

        // Kafka shim per node (client produce/endtxn).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_broker_enable_2pc_prepare_then_commit() {
    let h = ClusterHarness::boot().await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", h.native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 2).await.unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "events").await;

    let meta = controller.metadata().await.unwrap();
    let leader0 = meta.topics[0].partitions[0].leader;
    let leader1 = meta.topics[0].partitions[1].leader;
    assert_ne!(
        leader0, leader1,
        "test requires partitions on different leaders; got both on {leader0}"
    );

    // Init Enable2Pc + AddPartitions on controller (txn coordinator).
    let ctrl_k = h.kafka_of(1);
    let (err, pid, epoch) = init_v6_rpc(ctrl_k, 1, "txn-mb", true, false).await;
    assert_eq!(err, 0);
    add_partitions(ctrl_k, 2, "txn-mb", pid, epoch, "events", &[0, 1]).await;

    // Produce to each partition leader's kafka port.
    produce_txn(
        h.kafka_of(leader0),
        3,
        "events",
        0,
        pid,
        epoch,
        0,
        b"p0",
    )
    .await;
    produce_txn(
        h.kafka_of(leader1),
        4,
        "events",
        1,
        pid,
        epoch,
        0,
        b"p1",
    )
    .await;

    // Wait for ISR replication so HWM advances past write-through offsets.
    for (leader, part) in [(leader0, 0i32), (leader1, 1)] {
        let b = h.broker_of(leader);
        let mut ok = false;
        for _ in 0..80 {
            if b.high_watermark(&TopicName::new("events"), volant_core::PartitionId(part as u32))
                .unwrap_or(0)
                > 0
            {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(ok, "HWM did not advance on leader {leader} p{part}");
    }

    // First EndTxn → prepare cluster-wide.
    assert_eq!(end_txn(ctrl_k, 5, "txn-mb", pid, epoch, true).await, 0);
    assert_eq!(h.b1.describe_transaction("txn-mb").unwrap().0, "PrepareCommit");
    // Participant leaders should also show prepared (or at least hold LSO).
    for (leader, part) in [(leader0, 0i32), (leader1, 1)] {
        let b = h.broker_of(leader);
        let state = b
            .describe_transaction("txn-mb")
            .map(|d| d.0)
            .unwrap_or_default();
        assert!(
            state == "PrepareCommit" || state == "PrepareAbort" || state == "Empty",
            "unexpected state {state:?} on leader {leader}"
        );
        // If this leader held write ranges, prepare keeps LSO < HWM.
        let (hwm, lso, _) = fetch_v4_lso(h.kafka_of(leader), 10 + part, "events", part, 1).await;
        if state == "PrepareCommit" {
            assert!(
                hwm > lso,
                "leader {leader} p{part}: prepared should hold LSO (hwm={hwm} lso={lso})"
            );
        } else {
            // Empty on a peer that led a partition is a failure of prepare fan-out.
            panic!("leader {leader} missing PrepareCommit after multi-broker prepare (state={state})");
        }
    }

    // Second EndTxn → complete commit cluster-wide.
    assert_eq!(end_txn(ctrl_k, 6, "txn-mb", pid, epoch, true).await, 0);
    assert_eq!(h.b1.describe_transaction("txn-mb").unwrap().0, "Empty");
    for (leader, part) in [(leader0, 0i32), (leader1, 1)] {
        let (hwm, lso, records) =
            fetch_v4_lso(h.kafka_of(leader), 20 + part, "events", part, 1).await;
        assert_eq!(hwm, lso, "LSO catches HWM after complete on p{part}");
        assert!(!records.is_empty(), "committed data visible on p{part}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_broker_prepare_then_fence_aborts_cluster_wide() {
    let h = ClusterHarness::boot().await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", h.native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 2).await.unwrap();
    propagate(&[&h.b1, &h.b2, &h.b3], "events").await;

    let meta = controller.metadata().await.unwrap();
    let leader0 = meta.topics[0].partitions[0].leader;
    let leader1 = meta.topics[0].partitions[1].leader;
    assert_ne!(leader0, leader1);

    let ctrl_k = h.kafka_of(1);
    let (err, pid, epoch) = init_v6_rpc(ctrl_k, 1, "txn-fence", true, false).await;
    assert_eq!(err, 0);
    add_partitions(ctrl_k, 2, "txn-fence", pid, epoch, "events", &[0, 1]).await;
    produce_txn(h.kafka_of(leader0), 3, "events", 0, pid, epoch, 0, b"x").await;
    produce_txn(h.kafka_of(leader1), 4, "events", 1, pid, epoch, 0, b"y").await;
    assert_eq!(end_txn(ctrl_k, 5, "txn-fence", pid, epoch, true).await, 0);

    // Wait for HWM so prepare has real ranges on both leaders.
    for (leader, part) in [(leader0, 0u32), (leader1, 1)] {
        let b = h.broker_of(leader);
        for _ in 0..80 {
            if b.high_watermark(&TopicName::new("events"), volant_core::PartitionId(part))
                .unwrap_or(0)
                > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    assert_eq!(
        h.broker_of(leader0)
            .describe_transaction("txn-fence")
            .map(|d| d.0)
            .as_deref(),
        Some("PrepareCommit")
    );
    assert_eq!(
        h.broker_of(leader1)
            .describe_transaction("txn-fence")
            .map(|d| d.0)
            .as_deref(),
        Some("PrepareCommit")
    );

    // Fence: KeepPreparedTxn=false aborts prepared on the Init target.
    let (err2, pid2, epoch2) = init_v6_rpc(ctrl_k, 6, "txn-fence", true, false).await;
    assert_eq!(err2, 0);
    assert_eq!(pid2, pid);
    assert!(epoch2 > epoch);
    assert_eq!(h.b1.describe_transaction("txn-fence").unwrap().0, "Empty");

    // Phase 114: commit=false complete force-aborts peer PrepareCommit (fence).
    let fanout = Txn2pcFanout::Complete {
        transactional_id: "txn-fence".into(),
        producer_id: pid as u64,
        producer_epoch: epoch as u16,
        commit: false,
    };
    assert!(
        run_txn_2pc_fanout(h.b1.as_ref(), &fanout).await,
        "fence fan-out should succeed for live peers"
    );

    for id in [leader0, leader1] {
        let b = h.broker_of(id);
        if let Some((state, ..)) = b.describe_transaction("txn-fence") {
            assert_ne!(
                state, "PrepareCommit",
                "peer {id} still prepared after fence"
            );
        }
    }

    // Old epoch cannot complete.
    assert_ne!(
        end_txn(ctrl_k, 7, "txn-fence", pid, epoch, true).await,
        0
    );
}

#[tokio::test]
async fn single_node_enable_2pc_unchanged() {
    // Phase 90 regression: no cluster ⇒ local prepare/complete only.
    let dir = unique_dir("single");
    let _g = Guard(dir.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 1).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_kafka_listener(listener, b).await;
    });
    let addr = format!("127.0.0.1:{port}");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (err, pid, epoch) = init_v6_rpc(&addr, 1, "txn-sn", true, false).await;
    assert_eq!(err, 0);
    add_partitions(&addr, 2, "txn-sn", pid, epoch, "events", &[0]).await;
    produce_txn(&addr, 3, "events", 0, pid, epoch, 0, b"hi").await;
    assert_eq!(end_txn(&addr, 4, "txn-sn", pid, epoch, true).await, 0);
    assert_eq!(broker.describe_transaction("txn-sn").unwrap().0, "PrepareCommit");
    assert_eq!(end_txn(&addr, 5, "txn-sn", pid, epoch, true).await, 0);
    assert_eq!(broker.describe_transaction("txn-sn").unwrap().0, "Empty");
    assert_eq!(broker.txn_2pc_fanout_errors_total(), 0);
}
