//! v0.234: native Fetch honors group assignment trailer.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::Message;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_response, pack_request, FetchRecord, Request, Response};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-v234-{label}-{}-{}",
        std::process::id(),
        nanos
    ))
}

async fn start_broker(dir: std::path::PathBuf) -> (String, Arc<Broker>) {
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_listener(listener, b).await;
    });
    (format!("127.0.0.1:{}", addr.port()), broker)
}

async fn raw_fetch(
    addr: &str,
    topic: &str,
    partition: u32,
    group_id: &str,
    member_id: &str,
) -> (u16, Vec<FetchRecord>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = Request::Fetch {
        topic: topic.to_owned(),
        partition,
        from_offset: 0,
        max_messages: 16,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
    };
    let frame = pack_request(1, &req).expect("pack");
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out).expect("encode");
    stream.write_all(&out).await.expect("write");

    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        if let Some(resp_frame) = decode_frame(&mut buf).expect("decode frame") {
            match decode_response(resp_frame.header.opcode, &resp_frame.payload).expect("decode") {
                Response::Fetch {
                    error_code,
                    records,
                    ..
                } => return (error_code, records),
                other => panic!("unexpected fetch response {other:?}"),
            }
        }
        let n = stream.read_buf(&mut buf).await.expect("read");
        assert!(n > 0, "eof waiting for fetch");
    }
}

#[tokio::test]
async fn fetch_trailer_honors_assignment() {
    let dir = temp_dir("own");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let admin = Client::connect_addr(&addr).await.unwrap();
    admin.create_topic("events", 2).await.unwrap();
    admin
        .produce("events", Some(0), vec![Message::from_value("p0")])
        .await
        .unwrap();
    admin
        .produce("events", Some(1), vec![Message::from_value("p1")])
        .await
        .unwrap();

    let c1 = Client::connect_addr(&addr).await.unwrap();
    let j1 = c1
        .join_group("cg-v234", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert_eq!(j1.assignment.len(), 2);
    c1.sync_group("cg-v234", &j1.member_id, j1.generation)
        .await
        .unwrap();

    let (code, recs) = raw_fetch(&addr, "events", 0, "cg-v234", &j1.member_id).await;
    assert_eq!(code, 0);
    assert!(!recs.is_empty(), "owner fetch should return data");

    let c2 = Client::connect_addr(&addr).await.unwrap();
    let j2 = c2
        .join_group("cg-v234", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    let stolen = j2
        .assignment
        .iter()
        .find(|a| a.topic == "events")
        .expect("b assigned something");

    let (code, recs) = raw_fetch(&addr, "events", stolen.partition, "cg-v234", &j1.member_id).await;
    assert_eq!(code, 9);
    assert!(recs.is_empty());

    let (code, recs) = raw_fetch(&addr, "events", stolen.partition, "", "").await;
    assert_eq!(code, 0);
    assert!(!recs.is_empty(), "admin path stays unfiltered");

    let (code, recs) = raw_fetch(&addr, "events", 0, "cg-v234", "not-a-member").await;
    assert_eq!(code, 10);
    assert!(recs.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}
