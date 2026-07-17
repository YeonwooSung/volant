//! Phase 65: SaslAuthenticate v2 flexible + DescribeCluster v0 + ListTransactions v0.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_bytes,
    get_compact_nullable_string, get_compact_string, get_nullable_string, get_bytes,
    put_bytes, put_compact_array_len, put_compact_bytes, put_compact_string, put_empty_tag_buffer,
    put_string, skip_tag_buffer,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p65-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot_kafka(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_kafka_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

async fn rpc(addr: &str, request: BytesMut) -> BytesMut {
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
    panic!("connection closed without full kafka response");
}

struct KafkaClient {
    stream: TcpStream,
}

impl KafkaClient {
    async fn connect(addr: &str) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
        }
    }

    async fn rpc(&mut self, request: BytesMut) -> BytesMut {
        self.stream.write_all(&request).await.unwrap();
        let mut buf = BytesMut::with_capacity(64 * 1024);
        loop {
            let n = self.stream.read_buf(&mut buf).await.unwrap();
            if n == 0 {
                panic!("connection closed");
            }
            if buf.len() >= 4 {
                let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if buf.len() >= 4 + size {
                    let _ = buf.split_to(4);
                    return buf.split_to(size);
                }
            }
        }
    }
}

fn sasl_auth_v2(auth: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_bytes(&mut body, Some(auth));
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_cluster_v0(include_ops: bool) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u8(if include_ops { 1 } else { 0 });
    put_empty_tag_buffer(&mut body);
    body
}

fn list_txns_v0(state_filters: &[&str], pid_filters: &[i64]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, state_filters.len());
    for s in state_filters {
        put_compact_string(&mut body, s);
    }
    put_compact_array_len(&mut body, pid_filters.len());
    for p in pid_filters {
        body.put_i64(*p);
    }
    put_empty_tag_buffer(&mut body);
    body
}

#[tokio::test]
async fn api_versions_p65_maxes() {
    let dir = temp_dir("api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        found.insert(key, (min_v, max_v));
    }
    assert_eq!(found.get(&36), Some(&(0, 2))); // SaslAuthenticate
    assert_eq!(found.get(&60), Some(&(0, 2))); // DescribeCluster (Phase 70)
    assert_eq!(found.get(&66), Some(&(0, 2))); // ListTransactions (Phase 70)
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sasl_authenticate_v2_plain() {
    let dir = temp_dir("sasl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let mut hs = BytesMut::new();
    put_string(&mut hs, "PLAIN");
    let hs_resp = c.rpc(encode_request(17, 0, 1, Some("p"), &hs)).await;
    let mut hsrc = hs_resp.freeze();
    assert_eq!(hsrc.get_i32(), 1);
    assert_eq!(hsrc.get_i16(), 0);

    let ar = c
        .rpc(encode_request_flexible(
            36,
            2,
            2,
            Some("p"),
            &sasl_auth_v2(b"\0alice\0s3cret"),
        ))
        .await;
    let mut asrc = ar.freeze();
    assert_eq!(asrc.get_i32(), 2);
    skip_tag_buffer(&mut asrc).unwrap(); // response header v1
    assert_eq!(asrc.get_i16(), 0);
    assert_eq!(get_compact_nullable_string(&mut asrc).unwrap(), None);
    let _ = get_compact_bytes(&mut asrc).unwrap();
    assert_eq!(asrc.get_i64(), 0); // session_lifetime
    skip_tag_buffer(&mut asrc).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn classic_sasl_authenticate_v0_still_works() {
    let dir = temp_dir("classic");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("bob", "pw").unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let mut hs = BytesMut::new();
    put_string(&mut hs, "PLAIN");
    let _ = c.rpc(encode_request(17, 0, 1, Some("p"), &hs)).await;

    let mut ab = BytesMut::new();
    put_bytes(&mut ab, Some(b"\0bob\0pw"));
    let ar = c.rpc(encode_request(36, 0, 2, Some("p"), &ab)).await;
    let mut asrc = ar.freeze();
    assert_eq!(asrc.get_i32(), 2);
    // classic header v0 — no tag buffer
    assert_eq!(asrc.get_i16(), 0);
    let _ = get_nullable_string(&mut asrc).unwrap();
    let _ = get_bytes(&mut asrc).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_cluster_returns_brokers_and_cluster_id() {
    let dir = temp_dir("dc");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(
        &addr,
        encode_request_flexible(60, 0, 10, Some("a"), &describe_cluster_v0(true)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // error
    assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
    assert_eq!(get_compact_string(&mut src).unwrap(), "volant");
    let controller = src.get_i32();
    assert!(controller >= 0);
    let n_brokers = get_compact_array_len(&mut src).unwrap().unwrap();
    assert!(n_brokers >= 1);
    for _ in 0..n_brokers {
        let _id = src.get_i32();
        let host = get_compact_string(&mut src).unwrap();
        assert!(!host.is_empty());
        let _port = src.get_i32();
        assert_eq!(get_compact_nullable_string(&mut src).unwrap(), None);
        skip_tag_buffer(&mut src).unwrap();
    }
    let ops = src.get_i32();
    assert_ne!(ops, i32::MIN); // included
    skip_tag_buffer(&mut src).unwrap();

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_transactions_empty_and_ongoing() {
    let dir = temp_dir("ltxn");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Empty when no open txns
    let resp = rpc(
        &addr,
        encode_request_flexible(66, 0, 20, Some("a"), &list_txns_v0(&[], &[])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 20);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // unknown
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0)); // states
    skip_tag_buffer(&mut src).unwrap();

    // Open a txn via broker API
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-a");
    assert_eq!(broker.begin_txn(pid, epoch), 0);

    let resp = rpc(
        &addr,
        encode_request_flexible(66, 0, 21, Some("a"), &list_txns_v0(&[], &[])),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 21);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "txn-a");
    assert_eq!(src.get_i64(), pid as i64);
    assert_eq!(get_compact_string(&mut src).unwrap(), "Ongoing");
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();

    // Unknown state filter
    let resp = rpc(
        &addr,
        encode_request_flexible(
            66,
            0,
            22,
            Some("a"),
            &list_txns_v0(&["NotARealState"], &[]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 22);
    skip_tag_buffer(&mut src).unwrap();
    src.advance(4 + 2); // throttle + error
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "NotARealState");
    // No matching Ongoing against unknown-only filter
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(0));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_versions_use_header_v1() {
    let dir = temp_dir("unsup");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // SaslAuthenticate v3; DescribeCluster/ListTransactions v3 (v2 closed by Phase 70).
    for (api, ver, corr) in [(36i16, 3i16, 40i32), (60, 3, 41), (66, 3, 42)] {
        let resp = rpc(
            &addr,
            encode_request_flexible(api, ver, corr, Some("a"), &[]),
        )
        .await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), corr, "api {api}");
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35, "UnsupportedVersion api {api}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
