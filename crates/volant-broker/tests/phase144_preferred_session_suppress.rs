//! Phase 144: PreferredReadReplica × established fetch session suppress.
//!
//! When the client already has a non-zero fetch session id, Prefer redirects
//! are suppressed so the session stays on its owner (avoids 119 forward thrash).
//! Full fetch (`session_id == 0`) still prefers; READ_COMMITTED still uses the
//! Phase 140 RC suppress path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{encode_request, get_bytes, get_string, put_string};
use volant_broker::{
    serve_kafka_listener, start_background_tasks, BackgroundTasks, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p144-{label}-{}-{}",
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
        session_timeout_ms: 30_000,
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

/// Fetch v11 body: consumer replica_id=-1; configurable isolation/session/rack.
fn fetch_body_v11(
    topic: &str,
    isolation: u8,
    session_id: i32,
    session_epoch: i32,
    rack: &str,
) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(-1); // consumer
    body.put_i32(0); // max_wait
    body.put_i32(1); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_u8(isolation);
    body.put_i32(session_id);
    body.put_i32(session_epoch);
    body.put_i32(1); // topics
    put_string(&mut body, topic);
    body.put_i32(1); // partitions
    body.put_i32(0); // partition
    body.put_i32(-1); // current_leader_epoch
    body.put_i64(0); // fetch_offset
    body.put_i64(-1); // log_start
    body.put_i32(1_000_000); // partition_max_bytes
    body.put_i32(0); // forgotten
    put_string(&mut body, rack);
    body
}

/// (hwm, preferred_read_replica, records_len, resp_session_id)
fn parse_fetch_v11_preferred(mut src: bytes::Bytes) -> (i64, i32, usize, i32) {
    let _throttle = src.get_i32();
    let top_err = src.get_i16();
    let session = src.get_i32();
    assert_eq!(src.get_i32(), 1);
    let _name = get_string(&mut src).unwrap();
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let part_err = src.get_i16();
    let hwm = src.get_i64();
    let _lso = src.get_i64();
    let _log_start = src.get_i64();
    assert_eq!(src.get_i32(), 0); // aborted
    let preferred = src.get_i32();
    let records = get_bytes(&mut src).unwrap().unwrap_or_default();
    assert_eq!(top_err, 0);
    assert_eq!(part_err, 0);
    (hwm, preferred, records.len(), session)
}

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
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

struct ClusterHarness {
    _guard: Guard,
    leader: Arc<Broker>,
    leader_addr: String,
    servers: [tokio::task::JoinHandle<()>; 3],
    _bg: [BackgroundTasks; 3],
    /// Keep non-leader broker Arcs alive for the test duration.
    _nodes: [Arc<Broker>; 3],
}

/// 3-node same-rack cluster with produce + ISR LEO catch-up.
async fn setup_cluster(label: &str) -> ClusterHarness {
    let base = unique_dir(label);
    let guard = Guard(base.clone());

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
    let bg = [
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];

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

    b1.create_topic(label, 1).unwrap();
    {
        let nodes: [&Broker; 3] = [&b1, &b2, &b3];
        let src = nodes[0];
        let mut ok = false;
        for _ in 0..50 {
            let (_, gen, cid, topics) = src.cluster_state_snapshot();
            for n in nodes.iter().skip(1) {
                let _ = n.apply_cluster_state(gen, cid, &topics);
            }
            if nodes.iter().all(|n| n.partition_count_opt(label).is_some()) {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ok, "assignment did not propagate for {label}");
    }

    let topic = TopicName::new(label);
    let leader_id = b1.metadata(None).topics[0].partitions[0].leader;
    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        3 => Arc::clone(&b3),
        _ => panic!("bad leader"),
    };
    let leader_addr = format!("127.0.0.1:{}", [p1, p2, p3][(leader_id - 1) as usize]);

    let mut batch = MessageBatch::default();
    batch
        .messages
        .push(Message::from_value(format!("p144-{label}")));
    let (_, err) = leader
        .produce_with_acks(&topic, PartitionId(0), batch, 1, None)
        .unwrap();
    assert_eq!(err, 0);
    catch_up_isr(&leader, label);

    let expected = leader.select_preferred_read_replica(&topic, PartitionId(0), Some("rack-a"));
    assert!(
        expected.is_some(),
        "setup requires preferred candidate; leader={leader_id}"
    );

    ClusterHarness {
        _guard: guard,
        leader,
        leader_addr,
        servers: [s1, s2, s3],
        _bg: bg,
        _nodes: [b1, b2, b3],
    }
}

/// No session (session_id=0, FINAL epoch) + rack match → preferred still redirects.
#[tokio::test]
async fn no_session_rack_match_still_redirects() {
    let h = setup_cluster("nosess").await;
    let topic = "nosess";

    let before_redir = h.leader.preferred_replica_redirect_total();
    let before_sess = h.leader.preferred_replica_session_suppressed_total();
    let expected = h
        .leader
        .select_preferred_read_replica(&TopicName::new(topic), PartitionId(0), Some("rack-a"))
        .unwrap();

    // session_id=0, FINAL (-1): full fetch without creating a session.
    let resp = kafka_rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            1,
            Some("c"),
            &fetch_body_v11(topic, 0, 0, -1, "rack-a"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 1);
    let (_hwm, preferred, rec_len, _sess) = parse_fetch_v11_preferred(src);
    assert_eq!(preferred, expected as i32, "full fetch must preferred-redirect");
    assert_eq!(rec_len, 0, "preferred redirect uses empty records");
    assert!(
        h.leader.preferred_replica_redirect_total() > before_redir,
        "redirect counter must increment"
    );
    assert_eq!(
        h.leader.preferred_replica_session_suppressed_total(),
        before_sess,
        "session suppress must not fire on session_id=0"
    );

    for s in h.servers {
        s.abort();
    }
}

/// session_id == 0 full create (INITIAL epoch) still preferred-redirects when eligible.
#[tokio::test]
async fn session_id_zero_full_fetch_still_preferred() {
    let h = setup_cluster("full0").await;
    let topic = "full0";

    let before_redir = h.leader.preferred_replica_redirect_total();
    let before_sess = h.leader.preferred_replica_session_suppressed_total();
    let expected = h
        .leader
        .select_preferred_read_replica(&TopicName::new(topic), PartitionId(0), Some("rack-a"))
        .unwrap();

    // session_id=0, INITIAL (0): create path; preferred still allowed.
    let resp = kafka_rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            2,
            Some("c"),
            &fetch_body_v11(topic, 0, 0, 0, "rack-a"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 2);
    let (_hwm, preferred, rec_len, _sess) = parse_fetch_v11_preferred(src);
    assert_eq!(preferred, expected as i32);
    assert_eq!(rec_len, 0);
    assert!(h.leader.preferred_replica_redirect_total() > before_redir);
    assert_eq!(
        h.leader.preferred_replica_session_suppressed_total(),
        before_sess
    );

    for s in h.servers {
        s.abort();
    }
}

/// Established session (session_id != 0) → preferred stays -1; session suppress increments.
#[tokio::test]
async fn established_session_suppresses_preferred() {
    let h = setup_cluster("estsess").await;
    let topic = "estsess";

    // 1) Create session without preferred (empty rack) so leader serves + assigns id.
    let create = kafka_rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            10,
            Some("c"),
            &fetch_body_v11(topic, 0, 0, 0, ""), // INITIAL, no rack
        ),
    )
    .await;
    let mut src = create.freeze();
    assert_eq!(src.get_i32(), 10);
    let (hwm0, preferred0, rec0, session_id) = parse_fetch_v11_preferred(src);
    assert_eq!(preferred0, -1, "no rack → no preferred");
    assert!(rec0 > 0, "leader serves records on create");
    assert!(hwm0 > 0);
    assert!(session_id != 0, "session create must assign non-zero id");

    let before_sess = h.leader.preferred_replica_session_suppressed_total();
    let before_rc = h.leader.preferred_replica_suppressed_total();
    let before_redir = h.leader.preferred_replica_redirect_total();

    // 2) Incremental with matching rack — would prefer but session suppresses.
    let incr = kafka_rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            11,
            Some("c"),
            &fetch_body_v11(topic, 0, session_id, 1, "rack-a"), // expected epoch=1
        ),
    )
    .await;
    let mut src2 = incr.freeze();
    assert_eq!(src2.get_i32(), 11);
    let (_hwm, preferred, rec_len, resp_sess) = parse_fetch_v11_preferred(src2);
    assert_eq!(preferred, -1, "established session must suppress preferred");
    assert!(
        rec_len > 0,
        "leader serves records when preferred session-suppressed"
    );
    assert_eq!(resp_sess, session_id);

    assert!(
        h.leader.preferred_replica_session_suppressed_total() > before_sess,
        "session suppress metric must increment; before={before_sess} after={}",
        h.leader.preferred_replica_session_suppressed_total()
    );
    assert_eq!(
        h.leader.preferred_replica_suppressed_total(),
        before_rc,
        "RC suppress metric must not change on session path"
    );
    assert_eq!(
        h.leader.preferred_replica_redirect_total(),
        before_redir,
        "session suppress must not redirect"
    );

    for s in h.servers {
        s.abort();
    }
}

/// READ_COMMITTED still uses Phase 140 RC suppress (not session suppress).
#[tokio::test]
async fn read_committed_still_uses_rc_suppress() {
    let h = setup_cluster("rc144").await;
    let topic = "rc144";

    let before_rc = h.leader.preferred_replica_suppressed_total();
    let before_sess = h.leader.preferred_replica_session_suppressed_total();
    let before_redir = h.leader.preferred_replica_redirect_total();

    // session_id=0, FINAL, isolation=READ_COMMITTED.
    let resp = kafka_rpc(
        &h.leader_addr,
        encode_request(
            1,
            11,
            5,
            Some("c"),
            &fetch_body_v11(topic, 1, 0, -1, "rack-a"),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 5);
    let (hwm, preferred, rec_len, _) = parse_fetch_v11_preferred(src);
    assert!(hwm > 0);
    assert_eq!(preferred, -1, "READ_COMMITTED must suppress preferred");
    assert!(rec_len > 0, "leader serves when RC suppresses preferred");

    assert!(
        h.leader.preferred_replica_suppressed_total() > before_rc,
        "RC suppress counter must increment"
    );
    assert_eq!(
        h.leader.preferred_replica_session_suppressed_total(),
        before_sess,
        "session suppress must not fire on RC path (session_id=0)"
    );
    assert_eq!(
        h.leader.preferred_replica_redirect_total(),
        before_redir,
        "RC suppress must not redirect"
    );

    for s in h.servers {
        s.abort();
    }
}
