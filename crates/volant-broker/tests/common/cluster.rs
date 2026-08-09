//! Shared native-protocol multi-broker test helpers (RF=3 static membership).
//!
//! Used by journal fence / auth / DeleteRecords residual ITs so each phase
//! file does not re-copy `unique_dir` / `Guard` / `cluster_config` / `propagate`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::{BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::{
    serve_listener, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig, BackgroundTasks,
};
use volant_protocol::{
    codec::{decode_frame, encode_frame},
    decode_response, pack_request, Request, Response,
};
use volant_storage::StorageConfig;

use super::temp_dir;

/// Remove a temp data dir on drop.
pub struct Guard(pub PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Unique temp dir under a phase prefix (`"p132"`, …).
pub fn unique_dir(prefix: &str, label: &str) -> PathBuf {
    temp_dir(prefix, label)
}

/// Static 3-broker cluster config on localhost ports.
pub fn cluster_config(ports: [u16; 3]) -> ClusterConfig {
    cluster_config_with_session(ports, 2000)
}

/// Like [`cluster_config`] with an explicit session timeout.
pub fn cluster_config_with_session(ports: [u16; 3], session_timeout_ms: u32) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
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

/// Bind `127.0.0.1:0` and return listener + port.
pub async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// Storage with multi-message segments (for mid-segment DeleteRecords clamp).
pub fn multi_msg_storage(data_dir: PathBuf) -> StorageConfig {
    StorageConfig {
        data_dir,
        flush_every_n: 1,
        segment_size: 1024,
        ..StorageConfig::default()
    }
}

/// Default single-node-ish storage under `data_dir`.
pub fn default_storage(data_dir: PathBuf) -> StorageConfig {
    StorageConfig {
        data_dir,
        flush_every_n: 1,
        ..StorageConfig::default()
    }
}

/// Three in-process cluster brokers (no TCP). Ports are advertised only.
pub fn boot_triple_inprocess(
    base: &Path,
    ports: [u16; 3],
) -> (Arc<Broker>, Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config(ports);
    let mk = |id: u32| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    (mk(1), mk(2), mk(3))
}

/// Propagate controller assignment snapshot until all nodes know `topic`.
pub fn propagate(nodes: &[&Broker], topic: &str) {
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
    panic!("propagate failed for {topic}");
}

/// Async variant of [`propagate`].
pub async fn propagate_async(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes.iter().all(|n| n.partition_count_opt(topic).is_some()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("propagate failed for {topic}");
}

/// Single-node broker in a unique temp dir.
pub fn new_single_broker(prefix: &str, label: &str) -> (Arc<Broker>, Guard) {
    let dir = unique_dir(prefix, label);
    let guard = Guard(dir.clone());
    let broker = Arc::new(Broker::new(default_storage(dir)));
    (broker, guard)
}

/// Serve native protocol on an ephemeral port.
pub async fn boot_listener(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        let _ = serve_listener(listener, broker).await;
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), h)
}

/// Framed multi-request RPC on one TCP connection (Auth + journal opcodes, etc.).
pub async fn rpc_seq(addr: &str, reqs: &[Request]) -> Vec<Response> {
    let mut stream = TcpStream::connect(addr).await.expect("tcp connect");
    let mut out_all = BytesMut::new();
    for (i, req) in reqs.iter().enumerate() {
        let frame = pack_request(i as u32, req).expect("pack_request");
        encode_frame(&frame, &mut out_all).expect("encode_frame");
    }
    stream.write_all(&out_all).await.expect("write");

    let mut buf = BytesMut::with_capacity(8 * 1024);
    let mut resps = Vec::with_capacity(reqs.len());
    while resps.len() < reqs.len() {
        if let Some(frame) = decode_frame(&mut buf).expect("decode_frame") {
            let resp =
                decode_response(frame.header.opcode, &frame.payload).expect("decode_response");
            resps.push(resp);
            continue;
        }
        let n = stream.read_buf(&mut buf).await.expect("read");
        if n == 0 {
            panic!(
                "connection closed after {} of {} responses",
                resps.len(),
                reqs.len()
            );
        }
    }
    resps
}

/// Two live native listeners + background tasks (ports from bind).
pub struct DualServed {
    pub ports: [u16; 2],
    pub b1: Arc<Broker>,
    pub b2: Arc<Broker>,
    pub bgs: Vec<BackgroundTasks>,
    pub servers: Vec<tokio::task::JoinHandle<()>>,
}

impl DualServed {
    pub async fn boot(prefix: &str, label: &str) -> (Self, Guard) {
        let base = unique_dir(prefix, label);
        let guard = Guard(base.clone());
        let (l1, p1) = bind_port0().await;
        let (l2, p2) = bind_port0().await;
        // Third port unused (static membership size 3 for majority math if needed).
        let p3 = p2.saturating_add(50).max(33_000);
        let cfg = cluster_config([p1, p2, p3]);
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
        let b1 = mk(1, p1);
        let b2 = mk(2, p2);
        let bgs = vec![
            start_background_tasks(Arc::clone(&b1)),
            start_background_tasks(Arc::clone(&b2)),
        ];
        let mut servers = Vec::new();
        for (listener, b) in [(l1, &b1), (l2, &b2)] {
            let b = Arc::clone(b);
            servers.push(tokio::spawn(async move {
                let _ = serve_listener(listener, b).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
        (
            Self {
                ports: [p1, p2],
                b1,
                b2,
                bgs,
                servers,
            },
            guard,
        )
    }

    pub fn addr2(&self) -> String {
        format!("127.0.0.1:{}", self.ports[1])
    }

    pub async fn shutdown(mut self) {
        for s in self.servers.drain(..) {
            s.abort();
        }
        for bg in self.bgs.drain(..) {
            bg.shutdown().await;
        }
    }
}
