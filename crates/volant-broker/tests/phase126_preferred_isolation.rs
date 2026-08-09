//! PreferredReadReplica × READ_COMMITTED (isolation=1) suppress.
//!
//! Split out of phase126 so the main preferred suite stays under ~1k lines.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{encode_request, get_bytes, get_string, put_string};
use volant_broker::{
    serve_kafka_listener, start_background_tasks, Broker, BrokerEndpoint, ClusterConfig,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p126-iso-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn cluster_config_racks(ports: [u16; 3], racks: [Option<&str>; 3]) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms: 2000,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=3)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: ports[(id - 1) as usize],
                rack: racks[(id - 1) as usize].map(|s| s.to_string()),
            })
            .collect(),
    }
}

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn catch_up_isr(leader: &Broker, topic: &str) {
    let t = TopicName::new(topic);
    let leo = leader.log_end_offset(&t, PartitionId(0)).unwrap();
    let isr = leader.local_partition_isr(&t, PartitionId(0)).unwrap();
    let lid = leader.node_id();
    for fid in isr {
        if fid != lid {
            leader
                .test_set_follower_leo(&t, PartitionId(0), fid, leo)
                .unwrap();
        }
    }
}

async fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes.iter().all(|n| n.partition_count_opt(topic).is_some()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("propagate failed");
}

fn fetch_body_v11(topic: &str, isolation: u8, rack: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // consumer
    body.put_i32(0);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(isolation);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i32(-1);
    body.put_i64(0);
    body.put_i64(-1);
    body.put_i32(1_000_000);
    body.put_i32(0);
    put_string(&mut body, rack);
    body
}

fn parse_fetch_v11_preferred(mut src: bytes::Bytes) -> (i64, i32, usize) {
    let _throttle = src.get_i32();
    let top_err = src.get_i16();
    let _session = src.get_i32();
    assert_eq!(src.get_i32(), 1);
    let _name = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let part_err = src.get_i16();
    let hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(src.get_i32(), 0);
    let preferred = src.get_i32();
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    assert_eq!(top_err, 0);
    assert_eq!(part_err, 0);
    (hwm, preferred, records.len())
}

async fn kafka_rpc(addr: &str, request: BytesMut) -> BytesMut {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
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
    panic!("no kafka response");
}

/// READ_COMMITTED suppresses preferred; READ_UNCOMMITTED still redirects.
#[tokio::test]
async fn read_committed_suppresses_preferred_redirect() {
    let base = unique_dir("rc-suppress");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let cfg = cluster_config_racks(
        [p1, p2, p3],
        [Some("rack-a"), Some("rack-a"), Some("rack-a")],
    );

    let mk = |id: u32| {
        let b = Broker::with_cluster(
            StorageConfig {
                data_dir: base.join(format!("n{id}")),
                flush_every_n: 1,
                ..StorageConfig::default()
            },
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", [p1, p2, p3][(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let _bg2 = start_background_tasks(Arc::clone(&b2));
    let _bg3 = start_background_tasks(Arc::clone(&b3));

    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_kafka_listener(l1, b).await.ok();
        })
    };
    let s2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            serve_kafka_listener(l2, b).await.ok();
        })
    };
    let s3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            serve_kafka_listener(l3, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    b1.create_topic("rc", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "rc").await;
    let topic = TopicName::new("rc");
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!("bad leader"),
    };
    let leader_addr = format!("127.0.0.1:{}", [p1, p2, p3][(leader_id - 1) as usize]);

    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value("hello-rc"));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, "rc");

    let expected = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(expected.is_some(), "expected preferred candidate");
    let expected_id = expected.unwrap() as i32;

    let resp_rc = kafka_rpc(
        &leader_addr,
        encode_request(1, 11, 5, Some("c"), &fetch_body_v11("rc", 1, "rack-a")),
    )
    .await;
    let mut src_rc = resp_rc.freeze();
    assert_eq!(src_rc.get_i32(), 5);
    let (hwm_rc, preferred_rc, rec_len_rc) = parse_fetch_v11_preferred(src_rc);
    assert!(hwm_rc > 0);
    assert_eq!(preferred_rc, -1, "READ_COMMITTED must suppress preferred");
    assert!(rec_len_rc > 0, "leader serves records when preferred suppressed");

    let resp_ru = kafka_rpc(
        &leader_addr,
        encode_request(1, 11, 6, Some("c"), &fetch_body_v11("rc", 0, "rack-a")),
    )
    .await;
    let mut src_ru = resp_ru.freeze();
    assert_eq!(src_ru.get_i32(), 6);
    let (hwm_ru, preferred_ru, rec_len_ru) = parse_fetch_v11_preferred(src_ru);
    assert!(hwm_ru > 0);
    assert_eq!(preferred_ru, expected_id, "READ_UNCOMMITTED still redirects");
    assert_eq!(rec_len_ru, 0);

    s1.abort();
    s2.abort();
    s3.abort();
}
