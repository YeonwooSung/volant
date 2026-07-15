//! Phase 27: Kafka List/Describe/DeleteGroups, CreatePartitions, Describe/AlterConfigs.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, get_bytes, get_nullable_string, get_string,
    put_bytes, put_nullable_string, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p27-{label}-{}-{}",
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
async fn api_versions_includes_ops_keys() {
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
    let mut keys = Vec::new();
    for _ in 0..n {
        keys.push(src.get_i16());
        let _ = src.get_i16();
        let _ = src.get_i16();
    }
    for k in [15i16, 16, 32, 33, 37, 42] {
        assert!(keys.contains(&k), "missing api key {k}");
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_describe_delete_groups() {
    let dir = temp_dir("groups");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Join so group is live
    let sub = encode_consumer_subscription(&["t"]);
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, "ops-g");
    jbody.put_i32(10_000);
    put_string(&mut jbody, "");
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1);
    put_string(&mut jbody, "range");
    put_bytes(&mut jbody, Some(&sub));
    let jresp = rpc(&addr, encode_request(11, 0, 1, Some("c"), &jbody)).await;
    let mut js = jresp.freeze();
    js.advance(4);
    assert_eq!(js.get_i16(), 0);
    let _gen = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();

    // ListGroups
    let lresp = rpc(&addr, encode_request(16, 0, 2, Some("c"), &[])).await;
    let mut ls = lresp.freeze();
    ls.advance(4);
    assert_eq!(ls.get_i16(), 0);
    let gn = ls.get_i32();
    assert!(gn >= 1);
    let mut found = false;
    for _ in 0..gn {
        let gid = get_string(&mut ls).unwrap();
        let ptype = get_string(&mut ls).unwrap();
        if gid == "ops-g" {
            found = true;
            assert_eq!(ptype, "consumer");
        }
    }
    assert!(found, "ops-g not listed");

    // DescribeGroups
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    put_string(&mut dbody, "ops-g");
    let dresp = rpc(&addr, encode_request(15, 0, 3, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4);
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(get_string(&mut ds).unwrap(), "ops-g");
    assert_eq!(get_string(&mut ds).unwrap(), "Stable");
    assert_eq!(get_string(&mut ds).unwrap(), "consumer");
    let _proto = get_string(&mut ds).unwrap();
    assert_eq!(ds.get_i32(), 1); // members
    assert_eq!(get_string(&mut ds).unwrap(), member_id);
    let _ = get_string(&mut ds).unwrap(); // client_id
    let _ = get_string(&mut ds).unwrap(); // host
    let _ = get_bytes(&mut ds).unwrap();
    let _ = get_bytes(&mut ds).unwrap();

    // DeleteGroups while non-empty → 68
    let mut del = BytesMut::new();
    del.put_i32(1);
    put_string(&mut del, "ops-g");
    let delr = rpc(&addr, encode_request(42, 0, 4, Some("c"), &del)).await;
    let mut dels = delr.freeze();
    dels.advance(4);
    assert_eq!(dels.get_i32(), 1);
    assert_eq!(get_string(&mut dels).unwrap(), "ops-g");
    assert_eq!(dels.get_i16(), 68);

    // Leave then delete
    let mut leave = BytesMut::new();
    put_string(&mut leave, "ops-g");
    put_string(&mut leave, &member_id);
    let lr = rpc(&addr, encode_request(13, 0, 5, Some("c"), &leave)).await;
    let mut lrs = lr.freeze();
    lrs.advance(4);
    assert_eq!(lrs.get_i16(), 0);

    let delr2 = rpc(&addr, encode_request(42, 0, 6, Some("c"), &del)).await;
    let mut d2 = delr2.freeze();
    d2.advance(4);
    assert_eq!(d2.get_i32(), 1);
    assert_eq!(get_string(&mut d2).unwrap(), "ops-g");
    // may be 0 or 69 depending on offsets; after leave empty group may still be removed
    let err = d2.get_i16();
    assert!(err == 0 || err == 69, "delete after leave err={err}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_partitions_and_configs() {
    let dir = temp_dir("cfg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("orders", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreatePartitions → total 3
    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, "orders");
    body.put_i32(3); // total count
    body.put_i32(-1); // null assignments
    body.put_i32(5000); // timeout
    let resp = rpc(&addr, encode_request(37, 0, 1, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "orders");
    assert_eq!(src.get_i16(), 0);
    let _ = get_nullable_string(&mut src).unwrap();

    let meta = broker.metadata(Some(&[TopicName::new("orders")]));
    assert_eq!(meta.topics[0].partitions.len(), 3);

    // AlterConfigs
    let mut abody = BytesMut::new();
    abody.put_i32(1);
    abody.put_i8(2); // TOPIC
    put_string(&mut abody, "orders");
    abody.put_i32(2); // entries
    put_string(&mut abody, "retention.ms");
    put_nullable_string(&mut abody, Some("60000"));
    put_string(&mut abody, "cleanup.policy");
    put_nullable_string(&mut abody, Some("compact"));
    abody.put_u8(0); // validate_only = false
    let aresp = rpc(&addr, encode_request(33, 0, 2, Some("c"), &abody)).await;
    let mut asrc = aresp.freeze();
    asrc.advance(4);
    assert_eq!(asrc.get_i32(), 1);
    assert_eq!(asrc.get_i16(), 0);
    let _ = get_nullable_string(&mut asrc).unwrap();
    assert_eq!(asrc.get_i8(), 2);
    assert_eq!(get_string(&mut asrc).unwrap(), "orders");

    // DescribeConfigs
    let mut dbody = BytesMut::new();
    dbody.put_i32(1);
    dbody.put_i8(2);
    put_string(&mut dbody, "orders");
    dbody.put_i32(-1); // all keys
    let dresp = rpc(&addr, encode_request(32, 0, 3, Some("c"), &dbody)).await;
    let mut ds = dresp.freeze();
    ds.advance(4);
    assert_eq!(ds.get_i32(), 1);
    assert_eq!(ds.get_i16(), 0);
    assert_eq!(ds.get_i8(), 2);
    assert_eq!(get_string(&mut ds).unwrap(), "orders");
    let _ = get_nullable_string(&mut ds).unwrap();
    let ncfg = ds.get_i32();
    assert!(ncfg >= 2);
    let mut got_retention = false;
    let mut got_compact = false;
    for _ in 0..ncfg {
        let k = get_string(&mut ds).unwrap();
        let v = get_nullable_string(&mut ds).unwrap().unwrap_or_default();
        let _ro = ds.get_u8();
        let _def = ds.get_u8();
        let _sens = ds.get_u8();
        if k == "retention.ms" {
            assert_eq!(v, "60000");
            got_retention = true;
        }
        if k == "cleanup.policy" {
            assert_eq!(v, "compact");
            got_compact = true;
        }
    }
    assert!(got_retention && got_compact);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
