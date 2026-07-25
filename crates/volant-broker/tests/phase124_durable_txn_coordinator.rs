//! Phase 124: durable txn coordinator registry survives broker restart.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, put_compact_nullable_string, put_empty_tag_buffer,
    put_string, skip_tag_buffer,
};
use volant_broker::{
    serve_kafka_listener, serve_kafka_listener_until, serve_listener, serve_listener_until,
    start_background_tasks, Broker, BrokerEndpoint, ClusterConfig, TXN_COORDINATOR_DIR,
    TXN_COORDINATOR_FILE,
};
use volant_client::{Client, ClientConfig};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p124-{label}-{}-{}",
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
    assert_eq!(
        volant_broker::kafka::codec::get_string(&mut src).unwrap(),
        topic
    );
    assert_eq!(src.get_i32(), partitions.len() as i32);
    for &p in partitions {
        assert_eq!(src.get_i32(), p);
        assert_eq!(src.get_i16(), 0, "add partitions error on p={p}");
    }
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

/// note → drop broker → reopen same data_dir → resolve still works.
#[test]
fn registry_survives_single_node_restart() {
    let base = unique_dir("single");
    let _g = Guard(base.clone());
    let dir = base.join("node");
    std::fs::create_dir_all(&dir).unwrap();

    {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.note_txn_coordinator("txn-single", 42, 1);
        assert_eq!(
            broker.resolve_txn_coordinator("txn-single", Some(42)),
            Some(1)
        );
        assert!(
            dir.join(TXN_COORDINATOR_DIR)
                .join(TXN_COORDINATOR_FILE)
                .is_file(),
            "durable snapshot should exist after note"
        );
    }

    let broker2 = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    assert!(
        broker2.txn_coordinator_registry_restored() >= 1,
        "expected restored entries, got {}",
        broker2.txn_coordinator_registry_restored()
    );
    assert_eq!(
        broker2.resolve_txn_coordinator("txn-single", None),
        Some(1)
    );
    assert_eq!(broker2.resolve_txn_coordinator("", Some(42)), Some(1));

    // Idempotent reload.
    let broker3 = Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    });
    assert_eq!(
        broker3.resolve_txn_coordinator("txn-single", Some(42)),
        Some(1)
    );
    assert_eq!(
        broker3.txn_coordinator_registry().id_count(),
        broker2.txn_coordinator_registry().id_count()
    );
}

/// Multi-node: Init fan-out persists on peer; peer process restart reloads map
/// (resolve + FindCoordinator override) without re-Init.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_registry_reloads_after_restart() {
    let base = unique_dir("peer-reload");
    let _g = Guard(base.clone());

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

    let mut bgs = vec![
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
    tokio::time::sleep(Duration::from_millis(120)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(&[&b1, &b2, &b3], "events").await;

    let (err, pid, _epoch) = init_v6_rpc(&kafka_addrs[0], 1, "txn-reload", false, false).await;
    assert_eq!(err, 0);
    tokio::time::sleep(Duration::from_millis(120)).await;

    assert_eq!(
        b2.resolve_txn_coordinator("txn-reload", Some(pid as u64)),
        Some(1),
        "peer should have learned Init owner before restart"
    );
    assert!(
        base.join("node-2")
            .join(TXN_COORDINATOR_DIR)
            .join(TXN_COORDINATOR_FILE)
            .is_file(),
        "peer durable snapshot should exist after fan-out"
    );

    // Drop peer broker and reopen from same data_dir (process restart simulation).
    bgs.remove(1).shutdown().await;
    drop(b2);

    let b2b = {
        let storage = StorageConfig {
            data_dir: base.join("node-2"),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, 2, cfg.clone()).unwrap();
        broker.set_advertised("127.0.0.1", native_ports[1]);
        Arc::new(broker)
    };

    assert!(
        b2b.txn_coordinator_registry_restored() >= 1,
        "restarted peer should restore registry, got {}",
        b2b.txn_coordinator_registry_restored()
    );
    assert_eq!(
        b2b.resolve_txn_coordinator("txn-reload", Some(pid as u64)),
        Some(1),
        "registry must survive peer restart"
    );

    // Sticky FindCoordinator override uses restored map.
    let (fc_id, _, _) = b2b.resolve_find_coordinator("txn-reload", 1);
    assert_eq!(fc_id, 1, "FindCoordinator should prefer restored Init owner");

    // Cleanup remaining bg tasks.
    for bg in bgs {
        bg.shutdown().await;
    }
}

/// Peer-only restart (same ports): after reload, EndTxn via peer still transparent-forwards
/// while the coordinator still holds the open txn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_peer_forwards_endtxn() {
    let base = unique_dir("peer-fwd");
    let _g = Guard(base.clone());

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

    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];

    // Coordinator + node 3 stay up for the whole test.
    for (listener, b) in [(n1, &b1), (n3, &b3)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }

    // Peer 2 uses until-channels so we can release ports and rebind after restart.
    let (n2_stop_tx, n2_stop_rx) = oneshot::channel::<()>();
    {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_listener_until(n2, b, async move {
                let _ = n2_stop_rx.await;
            })
            .await;
        });
    }

    let mut kafka_addrs = Vec::new();
    // kafka for 1 and 3
    for b in [&b1, &b3] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        kafka_addrs.push(format!("127.0.0.1:{port}"));
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_kafka_listener(listener, b).await;
        });
    }
    // kafka for peer 2 with stop
    let (k2_stop_tx, k2_stop_rx) = oneshot::channel::<()>();
    let k2_addr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}");
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_kafka_listener_until(listener, b, async move {
                let _ = k2_stop_rx.await;
            })
            .await;
        });
        addr
    };
    // kafka_addrs: [b1, b3, b2] — keep indexed carefully
    let k1 = kafka_addrs[0].clone();
    let _ = k2_addr; // initial peer kafka stopped before EndTxn; rebound below

    tokio::time::sleep(Duration::from_millis(120)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", native_ports[0])],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(&[&b1, &b2, &b3], "events").await;

    let (err, pid, epoch) = init_v6_rpc(&k1, 1, "txn-fwd-dur", false, false).await;
    assert_eq!(err, 0);
    tokio::time::sleep(Duration::from_millis(80)).await;
    add_partitions(&k1, 2, "txn-fwd-dur", pid, epoch, "events", &[0]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        b2.resolve_txn_coordinator("txn-fwd-dur", Some(pid as u64)),
        Some(1)
    );
    assert!(
        base.join("node-2")
            .join(TXN_COORDINATOR_DIR)
            .join(TXN_COORDINATOR_FILE)
            .is_file()
    );

    // Stop peer 2 listeners and bg; reopen from same data_dir; rebind same native port.
    let _ = n2_stop_tx.send(());
    let _ = k2_stop_tx.send(());
    bgs.remove(1).shutdown().await;
    drop(b2);
    tokio::time::sleep(Duration::from_millis(80)).await;

    let b2b = {
        let storage = StorageConfig {
            data_dir: base.join("node-2"),
            flush_every_n: 1,
            ..StorageConfig::default()
        };
        let broker = Broker::with_cluster(storage, 2, cfg.clone()).unwrap();
        broker.set_advertised("127.0.0.1", native_ports[1]);
        Arc::new(broker)
    };
    let bg2 = start_background_tasks(Arc::clone(&b2b));

    // Rebind native p2 (released by serve_listener_until).
    let n2b = TcpListener::bind(format!("127.0.0.1:{}", native_ports[1]))
        .await
        .expect("rebind native port for restarted peer");
    {
        let b = Arc::clone(&b2b);
        tokio::spawn(async move {
            let _ = serve_listener(n2b, b).await;
        });
    }
    // Fresh kafka listen for peer 2.
    let k2 = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}");
        let b = Arc::clone(&b2b);
        tokio::spawn(async move {
            let _ = serve_kafka_listener(listener, b).await;
        });
        addr
    };
    // Re-apply assignment from controller (local open from disk may already have topics).
    propagate(&[&b1, &b2b, &b3], "events").await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    assert!(
        b2b.txn_coordinator_registry_restored() >= 1,
        "restored={}",
        b2b.txn_coordinator_registry_restored()
    );
    assert_eq!(
        b2b.resolve_txn_coordinator("txn-fwd-dur", Some(pid as u64)),
        Some(1)
    );

    // Coordinator still has open txn (never restarted). EndTxn via restarted peer → forward.
    let before = b2b.txn_forward_total();
    let err = end_txn(&k2, 9, "txn-fwd-dur", pid, epoch, true).await;
    assert_eq!(
        err, 0,
        "EndTxn via restarted peer should forward to live coordinator"
    );
    assert!(
        b2b.txn_forward_total() > before,
        "forward counter should advance on restarted peer"
    );

    bg2.shutdown().await;
    for bg in bgs {
        bg.shutdown().await;
    }
}
