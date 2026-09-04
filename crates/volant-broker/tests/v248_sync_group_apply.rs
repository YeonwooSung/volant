//! v0.248: apply SyncGroup assignment when it decodes.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::{boot_kafka, rpc, temp_dir};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_consumer_assignment, encode_consumer_assignment, encode_consumer_subscription,
    encode_request, get_bytes, get_string, put_bytes, put_string,
};
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_response, pack_request, Assignment, Request, Response};
use volant_storage::StorageConfig;

fn encode_native_assignment(parts: &[(String, u32)]) -> Bytes {
    let mut dst = BytesMut::new();
    dst.put_u32_le(parts.len() as u32);
    for (topic, p) in parts {
        let b = topic.as_bytes();
        dst.put_u16_le(b.len() as u16);
        dst.extend_from_slice(b);
        dst.put_u32_le(*p);
    }
    dst.freeze()
}

async fn start_native(dir: std::path::PathBuf) -> (String, Arc<Broker>) {
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

async fn raw_sync_group(
    addr: &str,
    group_id: &str,
    member_id: &str,
    generation: u32,
    assignment_bytes: Bytes,
) -> (u16, Vec<Assignment>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = Request::SyncGroup {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
        generation,
        assignment_bytes,
    };
    let frame = pack_request(1, &req).expect("pack");
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out).expect("encode");
    stream.write_all(&out).await.expect("write");

    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        if let Some(resp_frame) = decode_frame(&mut buf).expect("decode frame") {
            match decode_response(resp_frame.header.opcode, &resp_frame.payload).expect("decode") {
                Response::SyncGroup {
                    error_code,
                    assignment,
                } => return (error_code, assignment),
                other => panic!("unexpected sync_group response {other:?}"),
            }
        }
        let n = stream.read_buf(&mut buf).await.expect("read");
        assert!(n > 0, "eof waiting for sync_group");
    }
}

#[tokio::test]
async fn native_empty_bytes_keep_join_assignment() {
    let dir = temp_dir("v248", "native-empty");
    let (addr, _broker) = start_native(dir.clone()).await;
    let c = Client::connect_addr(&addr).await.unwrap();
    c.create_topic("events", 2).await.unwrap();
    let j = c
        .join_group("cg-empty", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert_eq!(j.assignment.len(), 2);

    let (code, asg) =
        raw_sync_group(&addr, "cg-empty", &j.member_id, j.generation, Bytes::new()).await;
    assert_eq!(code, 0);
    assert_eq!(asg, j.assignment);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn native_explicit_assignment_bytes_set_member() {
    let dir = temp_dir("v248", "native-apply");
    let (addr, broker) = start_native(dir.clone()).await;
    let c = Client::connect_addr(&addr).await.unwrap();
    c.create_topic("events", 2).await.unwrap();
    let j = c
        .join_group("cg-apply", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert_eq!(j.assignment.len(), 2);

    let want = vec![("events".into(), 1u32)];
    let (code, asg) = raw_sync_group(
        &addr,
        "cg-apply",
        &j.member_id,
        j.generation,
        encode_native_assignment(&want),
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(asg.len(), 1);
    assert_eq!(asg[0].topic, "events");
    assert_eq!(asg[0].partition, 1);
    assert_eq!(
        broker.groups().assignment("cg-apply", &j.member_id),
        Some(want)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn native_garbage_bytes_keep_join_assignment() {
    let dir = temp_dir("v248", "native-garbage");
    let (addr, broker) = start_native(dir.clone()).await;
    let c = Client::connect_addr(&addr).await.unwrap();
    c.create_topic("events", 2).await.unwrap();
    let j = c
        .join_group("cg-garb", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    let join_asg: Vec<(String, u32)> = j
        .assignment
        .iter()
        .map(|a| (a.topic.clone(), a.partition))
        .collect();

    let (code, asg) = raw_sync_group(
        &addr,
        "cg-garb",
        &j.member_id,
        j.generation,
        Bytes::from_static(b"\xff\x00not-an-assignment"),
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(asg, j.assignment);
    assert_eq!(
        broker.groups().assignment("cg-garb", &j.member_id),
        Some(join_asg)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn kafka_join(group: &str, topic: &str, corr: i32) -> BytesMut {
    let sub = encode_consumer_subscription(&[topic]);
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, group);
    jbody.put_i32(10_000);
    put_string(&mut jbody, "");
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1);
    put_string(&mut jbody, "range");
    put_bytes(&mut jbody, Some(&sub));
    encode_request(11, 0, corr, Some("c"), &jbody)
}

#[tokio::test]
async fn kafka_empty_assignments_keep_join() {
    let dir = temp_dir("v248", "kafka-empty");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join("cg-k-empty", "events", 10)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _protocol = get_string(&mut js).unwrap();
    let _leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    let _ = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_bytes(&mut js).unwrap();

    let join_asg = broker
        .groups()
        .assignment("cg-k-empty", &member_id)
        .unwrap();
    assert_eq!(join_asg.len(), 2);

    let mut sbody = BytesMut::new();
    put_string(&mut sbody, "cg-k-empty");
    sbody.put_i32(generation);
    put_string(&mut sbody, &member_id);
    sbody.put_i32(0);
    let sresp = rpc(&addr, encode_request(14, 0, 11, Some("c"), &sbody)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i16(), 0);
    let assign_bytes = get_bytes(&mut ss).unwrap().unwrap_or_default();
    let assignment = decode_consumer_assignment(&assign_bytes).unwrap();
    assert_eq!(assignment, join_asg);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn kafka_leader_payload_updates_member() {
    let dir = temp_dir("v248", "kafka-apply");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join("cg-k-apply", "events", 20)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 20);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _protocol = get_string(&mut js).unwrap();
    let _leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    let _ = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_bytes(&mut js).unwrap();

    let want = vec![("events".into(), 0u32)];
    let member_bytes = encode_consumer_assignment(&want);
    let mut sbody = BytesMut::new();
    put_string(&mut sbody, "cg-k-apply");
    sbody.put_i32(generation);
    put_string(&mut sbody, &member_id);
    sbody.put_i32(1);
    put_string(&mut sbody, &member_id);
    put_bytes(&mut sbody, Some(&member_bytes));
    let sresp = rpc(&addr, encode_request(14, 0, 21, Some("c"), &sbody)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 21);
    assert_eq!(ss.get_i16(), 0);
    let assign_bytes = get_bytes(&mut ss).unwrap().unwrap_or_default();
    let assignment = decode_consumer_assignment(&assign_bytes).unwrap();
    assert_eq!(assignment, want);
    assert_eq!(
        broker.groups().assignment("cg-k-apply", &member_id),
        Some(want)
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn kafka_garbage_bytes_keep_join() {
    let dir = temp_dir("v248", "kafka-garbage");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join("cg-k-garb", "events", 30)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 30);
    assert_eq!(js.get_i16(), 0);
    let generation = js.get_i32();
    let _protocol = get_string(&mut js).unwrap();
    let _leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    let _ = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_bytes(&mut js).unwrap();

    let join_asg = broker.groups().assignment("cg-k-garb", &member_id).unwrap();

    let mut sbody = BytesMut::new();
    put_string(&mut sbody, "cg-k-garb");
    sbody.put_i32(generation);
    put_string(&mut sbody, &member_id);
    sbody.put_i32(1);
    put_string(&mut sbody, &member_id);
    put_bytes(&mut sbody, Some(b"\xff\x00not-an-assignment"));
    let sresp = rpc(&addr, encode_request(14, 0, 31, Some("c"), &sbody)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 31);
    assert_eq!(ss.get_i16(), 0);
    let assign_bytes = get_bytes(&mut ss).unwrap().unwrap_or_default();
    let assignment = decode_consumer_assignment(&assign_bytes).unwrap();
    assert_eq!(assignment, join_asg);
    assert_eq!(
        broker.groups().assignment("cg-k-garb", &member_id),
        Some(join_asg)
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
