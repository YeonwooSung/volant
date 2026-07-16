//! Phase 46: Kafka DescribeConfigs classic v0–3 + AlterConfigs v0–1.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_request, get_nullable_string, get_string, put_nullable_string, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p46-{label}-{}-{}",
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

#[tokio::test]
async fn api_versions_configs_classic_max() {
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
    assert_eq!(found.get(&32), Some(&(0, 3))); // DescribeConfigs
    assert_eq!(found.get(&33), Some(&(0, 1))); // AlterConfigs
    assert_eq!(found.get(&44), Some(&(0, 0))); // IncrementalAlterConfigs stays v0
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn alter_configs_v1_throttle() {
    let dir = temp_dir("alter");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    body.put_i32(1);
    body.put_i8(2);
    put_string(&mut body, "t");
    body.put_i32(1);
    put_string(&mut body, "retention.ms");
    put_nullable_string(&mut body, Some("120000"));
    body.put_u8(0);
    let resp = rpc(&addr, encode_request(33, 1, 2, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut src).unwrap(), None);
    assert_eq!(src.get_i8(), 2);
    assert_eq!(get_string(&mut src).unwrap(), "t");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_configs_v3_source_synonyms_docs() {
    let dir = temp_dir("desc3");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Set a config so source = TOPIC_CONFIG
    let mut abody = BytesMut::new();
    abody.put_i32(1);
    abody.put_i8(2);
    put_string(&mut abody, "orders");
    abody.put_i32(1);
    put_string(&mut abody, "retention.ms");
    put_nullable_string(&mut abody, Some("60000"));
    abody.put_u8(0);
    let _ = rpc(&addr, encode_request(33, 1, 3, Some("c"), &abody)).await;

    // DescribeConfigs v3 with include_synonyms + include_documentation
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    dbody.put_i8(2);
    put_string(&mut dbody, "orders");
    dbody.put_i32(-1);
    dbody.put_u8(1); // include_synonyms
    dbody.put_u8(1); // include_documentation
    let dresp = rpc(&addr, encode_request(32, 3, 4, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 4);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_nullable_string(&mut ds).unwrap(), None);
    assert_eq!(ds.get_i8(), 2);
    assert_eq!(get_string(&mut ds).unwrap(), "orders");
    let ncfg = ds.get_i32();
    assert!(ncfg >= 1);

    let mut found_retention = false;
    for _ in 0..ncfg {
        let k = get_string(&mut ds).unwrap();
        let v = get_nullable_string(&mut ds).unwrap();
        let _ro = ds.get_u8();
        let source = ds.get_i8();
        let _sens = ds.get_u8();
        let syn_n = ds.get_i32();
        assert_eq!(syn_n, 0); // empty synonyms
        let ctype = ds.get_i8();
        let doc = get_nullable_string(&mut ds).unwrap();
        if k == "retention.ms" {
            found_retention = true;
            assert_eq!(v.as_deref(), Some("60000"));
            assert_eq!(source, 1); // TOPIC_CONFIG
            assert_eq!(ctype, 5); // LONG
            assert!(doc.is_some());
        }
    }
    assert!(found_retention);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_configs_v0_is_default_field() {
    let dir = temp_dir("v0");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    dbody.put_i8(2);
    put_string(&mut dbody, "t");
    dbody.put_i32(1);
    put_string(&mut dbody, "cleanup.policy");
    let dresp = rpc(&addr, encode_request(32, 0, 5, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 5);
    assert_eq!(ds.get_i32(), 0); // throttle
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    let _ = get_nullable_string(&mut ds).unwrap();
    assert_eq!(ds.get_i8(), 2);
    assert_eq!(get_string(&mut ds).unwrap(), "t");
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(get_string(&mut ds).unwrap(), "cleanup.policy");
    let _v = get_nullable_string(&mut ds).unwrap();
    let _ro = ds.get_u8();
    let _is_default = ds.get_u8(); // v0 field present
    let _sens = ds.get_u8();
    // no synonyms / type / docs on v0
    assert_eq!(ds.remaining(), 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_configs_v1_config_source() {
    let dir = temp_dir("v1");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    dbody.put_i8(2);
    put_string(&mut dbody, "t");
    dbody.put_i32(-1);
    dbody.put_u8(0); // include_synonyms
    let dresp = rpc(&addr, encode_request(32, 1, 6, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    assert_eq!(ds.get_i32(), 6);
    assert_eq!(ds.get_i32(), 0);
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    let _ = get_nullable_string(&mut ds).unwrap();
    assert_eq!(ds.get_i8(), 2);
    assert_eq!(get_string(&mut ds).unwrap(), "t");
    let n = ds.get_i32();
    assert!(n >= 1);
    for _ in 0..n {
        let _k = get_string(&mut ds).unwrap();
        let _v = get_nullable_string(&mut ds).unwrap();
        let _ro = ds.get_u8();
        let source = ds.get_i8();
        assert!(source == 1 || source == 5, "source={source}");
        let _sens = ds.get_u8();
        assert_eq!(ds.get_i32(), 0); // synonyms
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
