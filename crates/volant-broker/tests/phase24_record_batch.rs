//! Phase 24: Kafka RecordBatch (magic 2) produce/fetch.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_message_set, decode_records, encode_message_set, encode_record_batch, encode_request,
    get_bytes, get_string, put_bytes, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_core::{Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p24-{label}-{}-{}",
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
async fn api_versions_advertise_produce3_fetch4() {
    let dir = temp_dir("api-versions");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let req = encode_request(18, 0, 1, Some("test"), &[]);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut produce_max = None;
    let mut fetch_max = None;
    for _ in 0..n {
        let key = src.get_i16();
        let _min = src.get_i16();
        let max = src.get_i16();
        if key == 0 {
            produce_max = Some(max);
        }
        if key == 1 {
            fetch_max = Some(max);
        }
    }
    assert_eq!(produce_max, Some(13)); // Phase 71 TopicId
    assert_eq!(fetch_max, Some(13)); // Phase 68 TopicId

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_record_batch_fetch_v4_roundtrip() {
    let dir = temp_dir("rb-roundtrip");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("rb", 1).expect("create");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let records = vec![
        Record {
            offset: Offset::new(0),
            key: Some(Bytes::from_static(b"k")),
            value: Bytes::from_static(b"record-batch-value"),
            timestamp_ms: 1_700_000_000_100,
            headers: vec![("trace".into(), Bytes::from_static(b"abc"))],
        },
        Record {
            offset: Offset::new(1),
            key: None,
            value: Bytes::from_static(b"second"),
            timestamp_ms: 1_700_000_000_200,
            headers: vec![],
        },
    ];
    let batch = encode_record_batch(&records);
    assert_eq!(batch[16] as i8, 2);

    // ProduceRequest v0 with RecordBatch payload
    let mut body = BytesMut::new();
    body.put_i16(1); // acks
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, "rb");
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));

    let req = encode_request(0, 0, 9, Some("prod"), &body);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 9);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "rb");
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let err = src.get_i16();
    assert_eq!(err, 0, "produce error {err}");
    assert_eq!(src.get_i64(), 0);

    // FetchRequest v4
    let mut fbody = BytesMut::new();
    fbody.put_i32(-1); // replica_id
    fbody.put_i32(0); // max_wait
    fbody.put_i32(1); // min_bytes
    fbody.put_i32(1_048_576); // max_bytes (v3+)
    fbody.put_u8(0); // isolation_level (v4)
    fbody.put_i32(1);
    put_string(&mut fbody, "rb");
    fbody.put_i32(1);
    fbody.put_i32(0);
    fbody.put_i64(0);
    fbody.put_i32(1_000_000);

    let freq = encode_request(1, 4, 10, Some("fetch"), &fbody);
    let fresp = rpc(&addr, freq).await;
    let mut fsrc = fresp.freeze();
    assert_eq!(fsrc.get_i32(), 10); // corr
    assert_eq!(fsrc.get_i32(), 0); // throttle
    assert_eq!(fsrc.get_i32(), 1); // topics
    assert_eq!(get_string(&mut fsrc).unwrap(), "rb");
    assert_eq!(fsrc.get_i32(), 1);
    assert_eq!(fsrc.get_i32(), 0);
    let ferr = fsrc.get_i16();
    assert_eq!(ferr, 0, "fetch error {ferr}");
    let hwm = fsrc.get_i64();
    assert!(hwm >= 2, "hwm={hwm}");
    let lso = fsrc.get_i64();
    assert_eq!(lso, hwm);
    assert_eq!(fsrc.get_i32(), 0); // aborted txns
    let record_set = get_bytes(&mut fsrc).unwrap().unwrap_or_default();
    assert_eq!(record_set[16] as i8, 2, "fetch v4 must return RecordBatch");
    let msgs = decode_records(&record_set).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].value.as_ref(), b"record-batch-value");
    assert_eq!(msgs[0].key.as_ref().unwrap().as_ref(), b"k");
    assert_eq!(msgs[0].timestamp_ms, Some(1_700_000_000_100));
    assert_eq!(msgs[0].headers.len(), 1);
    assert_eq!(msgs[0].headers[0].0, "trace");
    assert_eq!(msgs[0].headers[0].1.as_ref(), b"abc");
    assert_eq!(msgs[1].value.as_ref(), b"second");

    // Native path sees the same data.
    let native = broker
        .fetch(&TopicName::new("rb"), PartitionId(0), Offset::new(0), 10)
        .unwrap();
    assert_eq!(native.len(), 2);
    assert_eq!(native[0].value.as_ref(), b"record-batch-value");
    assert_eq!(native[0].headers[0].0, "trace");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_record_batch_fetch_v0_messageset() {
    let dir = temp_dir("rb-ms-fetch");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("mixed", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let batch = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"x")),
        value: Bytes::from_static(b"from-rb"),
        timestamp_ms: 50,
        headers: vec![],
    }]);

    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, "mixed");
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&batch));
    let req = encode_request(0, 0, 1, Some("p"), &body);
    let resp = rpc(&addr, req).await;
    let mut src = resp.freeze();
    src.advance(4 + 4); // corr + topic count
    let _ = get_string(&mut src).unwrap();
    src.advance(4 + 4); // part count + id
    assert_eq!(src.get_i16(), 0);

    // Fetch v0 → MessageSet
    let mut fbody = BytesMut::new();
    fbody.put_i32(-1);
    fbody.put_i32(0);
    fbody.put_i32(1);
    fbody.put_i32(1);
    put_string(&mut fbody, "mixed");
    fbody.put_i32(1);
    fbody.put_i32(0);
    fbody.put_i64(0);
    fbody.put_i32(1_000_000);
    let fresp = rpc(&addr, encode_request(1, 0, 2, Some("f"), &fbody)).await;
    let mut fsrc = fresp.freeze();
    fsrc.advance(4); // corr
    fsrc.advance(4); // topics
    let _ = get_string(&mut fsrc).unwrap();
    fsrc.advance(4 + 4);
    assert_eq!(fsrc.get_i16(), 0);
    let _ = fsrc.get_i64();
    let set = get_bytes(&mut fsrc).unwrap().unwrap();
    assert!(matches!(set[16] as i8, 0 | 1));
    let msgs = decode_message_set(&set).unwrap();
    assert_eq!(msgs[0].value.as_ref(), b"from-rb");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn messageset_still_works_alongside_record_batch() {
    let dir = temp_dir("ms-still");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("legacy", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let set = encode_message_set(&[Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(b"legacy-ms"),
        timestamp_ms: 1,
        headers: vec![],
    }]);
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(1000);
    body.put_i32(1);
    put_string(&mut body, "legacy");
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(&set));
    let resp = rpc(&addr, encode_request(0, 0, 3, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    let _ = get_string(&mut src).unwrap();
    src.advance(4 + 4);
    assert_eq!(src.get_i16(), 0);

    // Produce v3 with RecordBatch after MessageSet
    let rb = encode_record_batch(&[Record {
        offset: Offset::new(0),
        key: Some(Bytes::from_static(b"k2")),
        value: Bytes::from_static(b"rb-after"),
        timestamp_ms: 2,
        headers: vec![],
    }]);
    let mut body3 = BytesMut::new();
    body3.put_i16(-1); // transactional_id null length
    // wait - put_nullable_string uses put_i16(-1)
    // already put -1 as i16 above for null transactional_id
    body3.put_i16(1); // acks
    body3.put_i32(1000);
    body3.put_i32(1);
    put_string(&mut body3, "legacy");
    body3.put_i32(1);
    body3.put_i32(0);
    put_bytes(&mut body3, Some(&rb));
    let resp3 = rpc(&addr, encode_request(0, 3, 4, Some("p3"), &body3)).await;
    let mut s3 = resp3.freeze();
    assert_eq!(s3.get_i32(), 4);
    assert_eq!(s3.get_i32(), 1);
    assert_eq!(get_string(&mut s3).unwrap(), "legacy");
    assert_eq!(s3.get_i32(), 1);
    assert_eq!(s3.get_i32(), 0);
    assert_eq!(s3.get_i16(), 0);
    let base = s3.get_i64();
    assert_eq!(base, 1); // second message
    assert_eq!(s3.get_i64(), -1); // log_append_time (v2+)
    assert_eq!(s3.get_i32(), 0); // throttle (v1+)

    let native = broker
        .fetch(
            &TopicName::new("legacy"),
            PartitionId(0),
            Offset::new(0),
            10,
        )
        .unwrap();
    assert_eq!(native.len(), 2);
    assert_eq!(native[0].value.as_ref(), b"legacy-ms");
    assert_eq!(native[1].value.as_ref(), b"rb-after");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
