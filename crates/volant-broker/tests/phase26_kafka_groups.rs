//! Phase 26: Kafka consumer groups on the shim.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::codec::{
    decode_consumer_assignment, encode_consumer_subscription, encode_request, get_bytes, get_string,
    put_bytes, put_string,
};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p26-{label}-{}-{}",
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
async fn api_versions_includes_group_keys() {
    let dir = temp_dir("api");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
    let n = src.get_i32();
    let mut keys = Vec::new();
    for _ in 0..n {
        keys.push(src.get_i16());
        let _ = src.get_i16();
        let _ = src.get_i16();
    }
    for k in [8i16, 9, 10, 11, 12, 13, 14] {
        assert!(keys.contains(&k), "missing api key {k}");
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn find_coordinator_returns_broker() {
    let dir = temp_dir("fc");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_advertised("127.0.0.1", 19092);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let mut body = BytesMut::new();
    put_string(&mut body, "g1");
    let resp = rpc(&addr, encode_request(10, 0, 2, Some("c"), &body)).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    assert_eq!(src.get_i16(), 0);
    let node = src.get_i32();
    assert_eq!(node, 0);
    assert_eq!(get_string(&mut src).unwrap(), "127.0.0.1");
    assert_eq!(src.get_i32(), 19092);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn join_sync_heartbeat_offsets_leave() {
    let dir = temp_dir("lifecycle");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // JoinGroup v0
    let sub = encode_consumer_subscription(&["events"]);
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, "cg-1");
    jbody.put_i32(10_000); // session timeout
    put_string(&mut jbody, ""); // member_id empty
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1); // protocols
    put_string(&mut jbody, "range");
    put_bytes(&mut jbody, Some(&sub));

    let jresp = rpc(&addr, encode_request(11, 0, 10, Some("c"), &jbody)).await;
    let mut js = jresp.freeze();
    assert_eq!(js.get_i32(), 10);
    let jerr = js.get_i16();
    assert_eq!(jerr, 0, "join error {jerr}");
    let generation = js.get_i32();
    assert!(generation > 0);
    let protocol = get_string(&mut js).unwrap();
    assert_eq!(protocol, "range");
    let leader = get_string(&mut js).unwrap();
    let member_id = get_string(&mut js).unwrap();
    assert!(!member_id.is_empty());
    assert_eq!(leader, member_id); // sole member is leader
    let member_count = js.get_i32();
    assert_eq!(member_count, 1);
    assert_eq!(get_string(&mut js).unwrap(), member_id);
    let _ = get_bytes(&mut js).unwrap();

    // SyncGroup v0 — empty leader assignments (coordinator assigns)
    let mut sbody = BytesMut::new();
    put_string(&mut sbody, "cg-1");
    sbody.put_i32(generation);
    put_string(&mut sbody, &member_id);
    sbody.put_i32(0); // no assignments from leader
    let sresp = rpc(&addr, encode_request(14, 0, 11, Some("c"), &sbody)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i16(), 0);
    let assign_bytes = get_bytes(&mut ss).unwrap().unwrap_or_default();
    let assignment = decode_consumer_assignment(&assign_bytes).unwrap();
    assert_eq!(assignment.len(), 2, "both partitions to sole member: {assignment:?}");
    assert!(assignment.iter().all(|(t, _)| t == "events"));

    // Heartbeat
    let mut hbody = BytesMut::new();
    put_string(&mut hbody, "cg-1");
    hbody.put_i32(generation);
    put_string(&mut hbody, &member_id);
    let hresp = rpc(&addr, encode_request(12, 0, 12, Some("c"), &hbody)).await;
    let mut hs = hresp.freeze();
    assert_eq!(hs.get_i32(), 12);
    assert_eq!(hs.get_i16(), 0);

    // OffsetCommit v2
    let mut cbody = BytesMut::new();
    put_string(&mut cbody, "cg-1");
    cbody.put_i32(generation);
    put_string(&mut cbody, &member_id);
    cbody.put_i64(-1); // retention
    cbody.put_i32(1);
    put_string(&mut cbody, "events");
    cbody.put_i32(1);
    cbody.put_i32(0); // partition
    cbody.put_i64(42); // offset
    put_string(&mut cbody, "meta");
    let cresp = rpc(&addr, encode_request(8, 2, 13, Some("c"), &cbody)).await;
    let mut cs = cresp.freeze();
    assert_eq!(cs.get_i32(), 13);
    assert_eq!(cs.get_i32(), 1);
    assert_eq!(get_string(&mut cs).unwrap(), "events");
    assert_eq!(cs.get_i32(), 1);
    assert_eq!(cs.get_i32(), 0);
    assert_eq!(cs.get_i16(), 0);

    // OffsetFetch v1
    let mut fbody = BytesMut::new();
    put_string(&mut fbody, "cg-1");
    fbody.put_i32(1);
    put_string(&mut fbody, "events");
    fbody.put_i32(1);
    fbody.put_i32(0);
    let fresp = rpc(&addr, encode_request(9, 1, 14, Some("c"), &fbody)).await;
    let mut fs = fresp.freeze();
    assert_eq!(fs.get_i32(), 14);
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(get_string(&mut fs).unwrap(), "events");
    assert_eq!(fs.get_i32(), 1);
    assert_eq!(fs.get_i32(), 0);
    assert_eq!(fs.get_i64(), 42);
    assert_eq!(get_string(&mut fs).unwrap(), "meta");
    assert_eq!(fs.get_i16(), 0);

    // LeaveGroup
    let mut lbody = BytesMut::new();
    put_string(&mut lbody, "cg-1");
    put_string(&mut lbody, &member_id);
    let lresp = rpc(&addr, encode_request(13, 0, 15, Some("c"), &lbody)).await;
    let mut ls = lresp.freeze();
    assert_eq!(ls.get_i32(), 15);
    assert_eq!(ls.get_i16(), 0);

    // Heartbeat after leave → unknown member
    let mut h2 = BytesMut::new();
    put_string(&mut h2, "cg-1");
    h2.put_i32(generation);
    put_string(&mut h2, &member_id);
    let h2resp = rpc(&addr, encode_request(12, 0, 16, Some("c"), &h2)).await;
    let mut h2s = h2resp.freeze();
    assert_eq!(h2s.get_i32(), 16);
    assert_eq!(h2s.get_i16(), 25); // UNKNOWN_MEMBER_ID

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn two_members_split_partitions() {
    let dir = temp_dir("two");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    async fn join_member(addr: &str, corr: i32) -> (i32, String, Vec<(String, u32)>) {
        let sub = encode_consumer_subscription(&["t"]);
        let mut jbody = BytesMut::new();
        put_string(&mut jbody, "g2");
        jbody.put_i32(30_000);
        put_string(&mut jbody, "");
        put_string(&mut jbody, "consumer");
        jbody.put_i32(1);
        put_string(&mut jbody, "range");
        put_bytes(&mut jbody, Some(&sub));
        let jresp = rpc(addr, encode_request(11, 0, corr, Some("c"), &jbody)).await;
        let mut js = jresp.freeze();
        js.advance(4);
        assert_eq!(js.get_i16(), 0);
        let gen = js.get_i32();
        let _proto = get_string(&mut js).unwrap();
        let _leader = get_string(&mut js).unwrap();
        let mid = get_string(&mut js).unwrap();
        let mc = js.get_i32();
        for _ in 0..mc {
            let _ = get_string(&mut js).unwrap();
            let _ = get_bytes(&mut js).unwrap();
        }

        let mut sbody = BytesMut::new();
        put_string(&mut sbody, "g2");
        sbody.put_i32(gen);
        put_string(&mut sbody, &mid);
        sbody.put_i32(0);
        let sresp = rpc(addr, encode_request(14, 0, corr + 100, Some("c"), &sbody)).await;
        let mut ss = sresp.freeze();
        ss.advance(4);
        // May be rebalance if second member joined — retry once on rebalance.
        let err = ss.get_i16();
        if err == 27 {
            // rebalance: re-join
            return Box::pin(join_member(addr, corr + 200)).await;
        }
        assert_eq!(err, 0, "sync error {err}");
        let bytes = get_bytes(&mut ss).unwrap().unwrap_or_default();
        let asg = decode_consumer_assignment(&bytes).unwrap();
        (gen, mid, asg)
    }

    let (_g1, m1, a1) = join_member(&addr, 1).await;
    let (_g2, m2, a2) = join_member(&addr, 2).await;
    assert_ne!(m1, m2);

    // After both joined, re-sync member1 (rejoin) to get final assignment.
    let sub = encode_consumer_subscription(&["t"]);
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, "g2");
    jbody.put_i32(30_000);
    put_string(&mut jbody, &m1);
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1);
    put_string(&mut jbody, "range");
    put_bytes(&mut jbody, Some(&sub));
    let jresp = rpc(&addr, encode_request(11, 0, 50, Some("c"), &jbody)).await;
    let mut js = jresp.freeze();
    js.advance(4);
    assert_eq!(js.get_i16(), 0);
    let gen = js.get_i32();
    let _ = get_string(&mut js).unwrap();
    let _ = get_string(&mut js).unwrap();
    let mid = get_string(&mut js).unwrap();
    assert_eq!(mid, m1);
    let mc = js.get_i32();
    for _ in 0..mc {
        let _ = get_string(&mut js).unwrap();
        let _ = get_bytes(&mut js).unwrap();
    }
    let mut sbody = BytesMut::new();
    put_string(&mut sbody, "g2");
    sbody.put_i32(gen);
    put_string(&mut sbody, &m1);
    sbody.put_i32(0);
    let sresp = rpc(&addr, encode_request(14, 0, 51, Some("c"), &sbody)).await;
    let mut ss = sresp.freeze();
    ss.advance(4);
    assert_eq!(ss.get_i16(), 0);
    let a1_final = decode_consumer_assignment(
        &get_bytes(&mut ss).unwrap().unwrap_or_default(),
    )
    .unwrap();

    // Also refresh m2
    let mut j2 = BytesMut::new();
    put_string(&mut j2, "g2");
    j2.put_i32(30_000);
    put_string(&mut j2, &m2);
    put_string(&mut j2, "consumer");
    j2.put_i32(1);
    put_string(&mut j2, "range");
    put_bytes(&mut j2, Some(&sub));
    let j2r = rpc(&addr, encode_request(11, 0, 60, Some("c"), &j2)).await;
    let mut j2s = j2r.freeze();
    j2s.advance(4);
    assert_eq!(j2s.get_i16(), 0);
    let gen2 = j2s.get_i32();
    let _ = get_string(&mut j2s).unwrap();
    let _ = get_string(&mut j2s).unwrap();
    let mid2 = get_string(&mut j2s).unwrap();
    let mc2 = j2s.get_i32();
    for _ in 0..mc2 {
        let _ = get_string(&mut j2s).unwrap();
        let _ = get_bytes(&mut j2s).unwrap();
    }
    let mut s2 = BytesMut::new();
    put_string(&mut s2, "g2");
    s2.put_i32(gen2);
    put_string(&mut s2, &mid2);
    s2.put_i32(0);
    let s2r = rpc(&addr, encode_request(14, 0, 61, Some("c"), &s2)).await;
    let mut s2s = s2r.freeze();
    s2s.advance(4);
    assert_eq!(s2s.get_i16(), 0);
    let a2_final = decode_consumer_assignment(
        &get_bytes(&mut s2s).unwrap().unwrap_or_default(),
    )
    .unwrap();

    let mut all: Vec<u32> = a1_final
        .iter()
        .chain(a2_final.iter())
        .map(|(_, p)| *p)
        .collect();
    all.sort_unstable();
    assert_eq!(all, vec![0, 1], "partitions split across members: {a1_final:?} {a2_final:?}");
    assert!(
        a1_final.len() + a2_final.len() == 2,
        "no overlap expected: {a1_final:?} {a2_final:?}"
    );

    // silence unused
    let _ = (a1, a2);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
