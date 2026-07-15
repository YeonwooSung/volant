//! Phase 30: Kafka SASL PLAIN + SCRAM-SHA-256 on the shim.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_bytes, get_nullable_string, get_string, put_bytes,
    put_string,
};
use volant_broker::scram::client_proof_and_server_sig;
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p30-{label}-{}-{}",
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

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

#[tokio::test]
async fn api_versions_includes_sasl_keys() {
    let dir = temp_dir("api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let resp = c.rpc(encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut hs = None;
    let mut auth = None;
    for _ in 0..n {
        let key = src.get_i16();
        let min = src.get_i16();
        let max = src.get_i16();
        if key == 17 {
            hs = Some((min, max));
        }
        if key == 36 {
            auth = Some((min, max));
        }
    }
    assert_eq!(hs, Some((0, 1)));
    assert_eq!(auth, Some((0, 1)));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn plain_auth_then_produce() {
    let dir = temp_dir("plain");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    // Produce without SASL must fail (users registered).
    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"nope"),
        timestamp_ms: 1,
        headers: vec![],
    }]);
    let denied = c
        .rpc(encode_request(0, 0, 1, Some("p"), &produce_body("t", &batch)))
        .await;
    let mut dsrc = denied.freeze();
    assert_eq!(dsrc.get_i32(), 1);
    assert_eq!(dsrc.get_i16(), 58, "SASL_AUTHENTICATION_FAILED");

    // Handshake PLAIN
    let mut hs = BytesMut::new();
    put_string(&mut hs, "PLAIN");
    let hs_resp = c.rpc(encode_request(17, 0, 2, Some("p"), &hs)).await;
    let mut hsrc = hs_resp.freeze();
    assert_eq!(hsrc.get_i32(), 2);
    assert_eq!(hsrc.get_i16(), 0);
    let n_mech = hsrc.get_i32();
    assert!(n_mech >= 2);

    // Authenticate
    let mut ab = BytesMut::new();
    put_bytes(&mut ab, Some(b"\0alice\0s3cret"));
    let ar = c.rpc(encode_request(36, 0, 3, Some("p"), &ab)).await;
    let mut asrc = ar.freeze();
    assert_eq!(asrc.get_i32(), 3);
    assert_eq!(asrc.get_i16(), 0);
    let _ = get_nullable_string(&mut asrc).unwrap();
    let _ = get_bytes(&mut asrc).unwrap();

    // Produce succeeds
    let ok = c
        .rpc(encode_request(0, 0, 4, Some("p"), &produce_body("t", &batch)))
        .await;
    let mut osrc = ok.freeze();
    assert_eq!(osrc.get_i32(), 4);
    assert_eq!(osrc.get_i32(), 1);
    assert_eq!(get_string(&mut osrc).unwrap(), "t");
    assert_eq!(osrc.get_i32(), 1);
    assert_eq!(osrc.get_i32(), 0);
    assert_eq!(osrc.get_i16(), 0);
    assert_eq!(osrc.get_i64(), 0);

    // Bad password on a new connection
    let mut c2 = KafkaClient::connect(&addr).await;
    let mut hs2 = BytesMut::new();
    put_string(&mut hs2, "PLAIN");
    let _ = c2.rpc(encode_request(17, 0, 1, Some("p"), &hs2)).await;
    let mut ab2 = BytesMut::new();
    put_bytes(&mut ab2, Some(b"\0alice\0wrong"));
    let bad = c2.rpc(encode_request(36, 0, 2, Some("p"), &ab2)).await;
    let mut bsrc = bad.freeze();
    assert_eq!(bsrc.get_i32(), 2);
    assert_eq!(bsrc.get_i16(), 58);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn scram_sha256_auth_roundtrip() {
    let dir = temp_dir("scram");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("bob", "hunter2").unwrap();
    broker.create_topic("s", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let mut hs = BytesMut::new();
    put_string(&mut hs, "SCRAM-SHA-256");
    let hs_resp = c.rpc(encode_request(17, 0, 1, Some("p"), &hs)).await;
    let mut hsrc = hs_resp.freeze();
    assert_eq!(hsrc.get_i32(), 1);
    assert_eq!(hsrc.get_i16(), 0);

    let client_nonce = "cliNonceABC123xyz";
    let first = format!("n,,n=bob,r={client_nonce}");
    let mut ab1 = BytesMut::new();
    put_bytes(&mut ab1, Some(first.as_bytes()));
    let r1 = c.rpc(encode_request(36, 0, 2, Some("p"), &ab1)).await;
    let mut s1 = r1.freeze();
    assert_eq!(s1.get_i32(), 2);
    assert_eq!(s1.get_i16(), 0);
    let _ = get_nullable_string(&mut s1).unwrap();
    let server_first = get_bytes(&mut s1).unwrap().unwrap_or_default();
    let server_first = std::str::from_utf8(&server_first).unwrap();

    let mut combined = None;
    let mut salt_b64 = None;
    let mut iterations = None;
    for part in server_first.split(',') {
        if let Some(r) = part.strip_prefix("r=") {
            combined = Some(r.to_owned());
        } else if let Some(s) = part.strip_prefix("s=") {
            salt_b64 = Some(s.to_owned());
        } else if let Some(i) = part.strip_prefix("i=") {
            iterations = Some(i.parse::<u32>().unwrap());
        }
    }
    let combined = combined.expect("combined nonce");
    let salt = B64.decode(salt_b64.unwrap()).unwrap();
    let iterations = iterations.unwrap();
    let (proof, expected_sig) = client_proof_and_server_sig(
        "bob",
        "hunter2",
        client_nonce,
        &combined,
        &salt,
        iterations,
    )
    .unwrap();
    let final_msg = format!("c=biws,r={combined},p={}", B64.encode(&proof));
    let mut ab2 = BytesMut::new();
    put_bytes(&mut ab2, Some(final_msg.as_bytes()));
    let r2 = c.rpc(encode_request(36, 0, 3, Some("p"), &ab2)).await;
    let mut s2 = r2.freeze();
    assert_eq!(s2.get_i32(), 3);
    assert_eq!(s2.get_i16(), 0, "SCRAM final should succeed");
    let _ = get_nullable_string(&mut s2).unwrap();
    let server_final = get_bytes(&mut s2).unwrap().unwrap_or_default();
    let server_final = std::str::from_utf8(&server_final).unwrap();
    assert!(server_final.starts_with("v="));
    let sig_b64 = &server_final[2..];
    let sig = B64.decode(sig_b64).unwrap();
    assert_eq!(sig, expected_sig);

    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k")),
        value: Bytes::from_static(b"scram-ok"),
        timestamp_ms: 99,
        headers: vec![],
    }]);
    let ok = c
        .rpc(encode_request(0, 0, 4, Some("p"), &produce_body("s", &batch)))
        .await;
    let mut osrc = ok.freeze();
    assert_eq!(osrc.get_i32(), 4);
    assert_eq!(osrc.get_i32(), 1);
    assert_eq!(get_string(&mut osrc).unwrap(), "s");
    assert_eq!(osrc.get_i32(), 1);
    assert_eq!(osrc.get_i32(), 0);
    assert_eq!(osrc.get_i16(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unsupported_mechanism() {
    let dir = temp_dir("mech");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let mut hs = BytesMut::new();
    put_string(&mut hs, "GSSAPI");
    let resp = c.rpc(encode_request(17, 0, 1, Some("p"), &hs)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 33, "UNSUPPORTED_SASL_MECHANISM");
    let n = src.get_i32();
    assert!(n >= 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
