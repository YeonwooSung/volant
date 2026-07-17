//! Phase 34: Kafka SASL SCRAM-SHA-512.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, temp_dir};

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use volant_broker::kafka::codec::{
    encode_record_batch, encode_request, get_bytes, get_nullable_string, get_string, put_bytes,
    put_string,
};
use volant_broker::scram::{client_proof_and_server_sig_for, ScramHash};
use volant_broker::Broker;
use volant_core::{Offset, Record};
use volant_storage::StorageConfig;

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
async fn handshake_lists_scram_sha512() {
    let dir = temp_dir("p34", "hs");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let mut hs = BytesMut::new();
    put_string(&mut hs, "GSSAPI"); // unsupported → list enabled
    let resp = c.rpc(encode_request(17, 0, 1, Some("t"), &hs)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 33); // UNSUPPORTED_SASL_MECHANISM
    let n = src.get_i32();
    let mut mechs = Vec::new();
    for _ in 0..n {
        mechs.push(get_string(&mut src).unwrap());
    }
    assert!(mechs.iter().any(|m| m == "SCRAM-SHA-512"));
    assert!(mechs.iter().any(|m| m == "SCRAM-SHA-256"));
    assert!(mechs.iter().any(|m| m == "PLAIN"));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn scram_sha512_auth_then_produce() {
    let dir = temp_dir("p34", "auth");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("dave", "s3cure!").unwrap();
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let mut hs = BytesMut::new();
    put_string(&mut hs, "SCRAM-SHA-512");
    let hs_resp = c.rpc(encode_request(17, 0, 1, Some("p"), &hs)).await;
    let mut hsrc = hs_resp.freeze();
    assert_eq!(hsrc.get_i32(), 1);
    assert_eq!(hsrc.get_i16(), 0);

    let client_nonce = "cli512NonceXYZ";
    let first = format!("n,,n=dave,r={client_nonce}");
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
    let combined = combined.expect("combined");
    let salt = B64.decode(salt_b64.unwrap()).unwrap();
    let iterations = iterations.unwrap();
    let (proof, expected_sig) = client_proof_and_server_sig_for(
        ScramHash::Sha512,
        "dave",
        "s3cure!",
        client_nonce,
        &combined,
        &salt,
        iterations,
    )
    .unwrap();
    assert_eq!(proof.len(), 64);

    let final_msg = format!("c=biws,r={combined},p={}", B64.encode(&proof));
    let mut ab2 = BytesMut::new();
    put_bytes(&mut ab2, Some(final_msg.as_bytes()));
    let r2 = c.rpc(encode_request(36, 0, 3, Some("p"), &ab2)).await;
    let mut s2 = r2.freeze();
    assert_eq!(s2.get_i32(), 3);
    assert_eq!(s2.get_i16(), 0, "SCRAM-SHA-512 final should succeed");
    let _ = get_nullable_string(&mut s2).unwrap();
    let server_final = get_bytes(&mut s2).unwrap().unwrap_or_default();
    let server_final = std::str::from_utf8(&server_final).unwrap();
    assert!(server_final.starts_with("v="));
    let sig = B64.decode(&server_final[2..]).unwrap();
    assert_eq!(sig, expected_sig);

    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"sha512-ok"),
        timestamp_ms: 1,
        headers: vec![],
    }]);
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

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn same_user_both_scram_mechanisms() {
    let dir = temp_dir("p34", "both");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("erin", "dual-pass").unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for (mech, hash) in [
        ("SCRAM-SHA-256", ScramHash::Sha256),
        ("SCRAM-SHA-512", ScramHash::Sha512),
    ] {
        let mut c = KafkaClient::connect(&addr).await;
        let mut hs = BytesMut::new();
        put_string(&mut hs, mech);
        let r = c.rpc(encode_request(17, 0, 1, Some("p"), &hs)).await;
        let mut src = r.freeze();
        assert_eq!(src.get_i32(), 1);
        assert_eq!(src.get_i16(), 0);

        let client_nonce = "sharedUserNonce1";
        let first = format!("n,,n=erin,r={client_nonce}");
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
        let combined = combined.unwrap();
        let salt = B64.decode(salt_b64.unwrap()).unwrap();
        let iterations = iterations.unwrap();
        let (proof, _) = client_proof_and_server_sig_for(
            hash,
            "erin",
            "dual-pass",
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
        assert_eq!(s2.get_i16(), 0, "auth must succeed for {mech}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
