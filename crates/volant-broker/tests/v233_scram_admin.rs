//! v0.233: Kafka Describe/AlterUserScramCredentials keys 50/51 v0.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{default_storage, unique_dir, Guard};
use common::{boot_kafka, rpc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    get_compact_string, put_bytes, put_compact_array_len, put_compact_bytes, put_compact_string,
    put_empty_tag_buffer, put_string, skip_tag_buffer,
};
use volant_broker::scram::{salted_password_for, ScramHash};
use volant_broker::Broker;

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

async fn sasl_plain(c: &mut KafkaClient, user: &str, pass: &str) {
    let mut hs = BytesMut::new();
    put_string(&mut hs, "PLAIN");
    let hs_resp = c.rpc(encode_request(17, 0, 2, Some("t"), &hs)).await;
    let mut hsrc = hs_resp.freeze();
    assert_eq!(hsrc.get_i32(), 2);
    assert_eq!(hsrc.get_i16(), 0);

    let mut auth = BytesMut::new();
    let mut token = Vec::new();
    token.push(0);
    token.extend_from_slice(user.as_bytes());
    token.push(0);
    token.extend_from_slice(pass.as_bytes());
    put_bytes(&mut auth, Some(&token));
    let ar = c.rpc(encode_request(36, 0, 3, Some("t"), &auth)).await;
    let mut asrc = ar.freeze();
    assert_eq!(asrc.get_i32(), 3);
    assert_eq!(asrc.get_i16(), 0);
}

fn describe_all_body() -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body
}

fn describe_user_body(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn alter_upsert_sha256(name: &str, iterations: i32, salt: &[u8], salted: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 0);
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i8(1);
    body.put_i32(iterations);
    put_compact_bytes(&mut body, Some(salt));
    put_compact_bytes(&mut body, Some(salted));
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn alter_delete(name: &str, mechanism: i8) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, name);
    body.put_i8(mechanism);
    put_empty_tag_buffer(&mut body);
    put_compact_array_len(&mut body, 0);
    put_empty_tag_buffer(&mut body);
    body
}

fn read_describe_result(src: &mut impl Buf) -> (String, i16, Vec<(i8, i32)>) {
    let user = get_compact_string(src).unwrap();
    let err = src.get_i16();
    let _msg = get_compact_nullable_string(src).unwrap();
    let n = get_compact_array_len(src).unwrap().unwrap_or(0);
    let mut infos = Vec::with_capacity(n);
    for _ in 0..n {
        let mech = src.get_i8();
        let iter = src.get_i32();
        skip_tag_buffer(src).unwrap();
        infos.push((mech, iter));
    }
    skip_tag_buffer(src).unwrap();
    (user, err, infos)
}

fn skip_flex_header(src: &mut impl Buf, corr: i32) {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(src).unwrap();
}

#[tokio::test]
async fn api_versions_lists_scram_admin_50_51() {
    let base = unique_dir("v233", "api");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
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
    assert_eq!(found.len(), 45);
    assert_eq!(found.get(&50), Some(&(0, 0)));
    assert_eq!(found.get(&51), Some(&(0, 0)));

    server.abort();
}

#[tokio::test]
async fn describe_empty_users_after_native_create() {
    let base = unique_dir("v233", "desc");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;
    sasl_plain(&mut c, "alice", "s3cret").await;

    let resp = c
        .rpc(encode_request_flexible(
            50,
            0,
            10,
            Some("admin"),
            &describe_all_body(),
        ))
        .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 10);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0); // top-level
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let (user, err, infos) = read_describe_result(&mut src);
    assert_eq!(user, "alice");
    assert_eq!(err, 0);
    assert_eq!(infos, vec![(1, 4096), (2, 4096)]);

    server.abort();
}

#[tokio::test]
async fn alter_upsert_sha256_salted_then_describe() {
    let base = unique_dir("v233", "upsert");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let salt = b"0123456789abcdef";
    let salted = salted_password_for("pw", salt, 4096, ScramHash::Sha256);
    let resp = c
        .rpc(encode_request_flexible(
            51,
            0,
            11,
            Some("admin"),
            &alter_upsert_sha256("carol", 4096, salt, &salted),
        ))
        .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 11);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "carol");
    assert_eq!(src.get_i16(), 0);

    sasl_plain(&mut c, "carol", "pw").await;
    let resp = c
        .rpc(encode_request_flexible(
            50,
            0,
            12,
            Some("admin"),
            &describe_user_body("carol"),
        ))
        .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 12);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let (user, err, infos) = read_describe_result(&mut src);
    assert_eq!(user, "carol");
    assert_eq!(err, 0);
    assert_eq!(infos, vec![(1, 4096)]);

    server.abort();
}

#[tokio::test]
async fn alter_delete_mechanism_removes_user_if_last() {
    let base = unique_dir("v233", "del");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let mut c = KafkaClient::connect(&addr).await;

    let salt = b"0123456789abcdef";
    let salted = salted_password_for("pw", salt, 4096, ScramHash::Sha256);
    let resp = c
        .rpc(encode_request_flexible(
            51,
            0,
            13,
            Some("admin"),
            &alter_upsert_sha256("dave", 4096, salt, &salted),
        ))
        .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 13);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "dave");
    assert_eq!(src.get_i16(), 0);

    sasl_plain(&mut c, "dave", "pw").await;
    let resp = c
        .rpc(encode_request_flexible(
            51,
            0,
            14,
            Some("admin"),
            &alter_delete("dave", 1),
        ))
        .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 14);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), "dave");
    assert_eq!(src.get_i16(), 0);

    let resp = c
        .rpc(encode_request_flexible(
            50,
            0,
            15,
            Some("admin"),
            &describe_user_body("dave"),
        ))
        .await;
    let mut src = resp.freeze();
    skip_flex_header(&mut src, 15);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut src).unwrap();
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    let (user, err, infos) = read_describe_result(&mut src);
    assert_eq!(user, "dave");
    assert_eq!(err, 91); // RESOURCE_NOT_FOUND
    assert!(infos.is_empty());

    server.abort();
}

#[tokio::test]
async fn describe_and_alter_v1_unsupported() {
    let base = unique_dir("v233", "v1");
    let _g = Guard(base.clone());
    let broker = Arc::new(Broker::new(default_storage(base.join("n1"))));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    for key in [50i16, 51] {
        let resp = rpc(&addr, encode_request_flexible(key, 1, 99, Some("c"), &[])).await;
        let mut src = resp.freeze();
        assert_eq!(src.get_i32(), 99);
        skip_tag_buffer(&mut src).unwrap();
        assert_eq!(src.get_i16(), 35);
    }

    server.abort();
}
