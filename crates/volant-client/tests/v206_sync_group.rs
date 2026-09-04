//! v0.206: native SyncGroup opcodes 116/117 (peek/confirm).

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::kafka::{ApiKey, SUPPORTED_APIS};
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Assignment, ErrorCode, Request, Response};
use volant_storage::StorageConfig;

const REBALANCE: u16 = ErrorCode::RebalanceInProgress as u16;
const UNKNOWN_MEMBER: u16 = ErrorCode::UnknownMemberId as u16;

struct SyncStub {
    addr: String,
    seen: Arc<Mutex<Vec<SeenSync>>>,
    replies: Arc<Mutex<Vec<SyncReply>>>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenSync {
    group_id: String,
    member_id: String,
    generation: u32,
    assignment_bytes_len: usize,
}

#[derive(Clone)]
struct SyncReply {
    error_code: u16,
    assignment: Vec<Assignment>,
}

impl SyncStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(Vec::new()));
        let seen_s = Arc::clone(&seen);
        let replies_s = Arc::clone(&replies);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let seen = Arc::clone(&seen_s);
                let replies = Arc::clone(&replies_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, seen, replies).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            seen,
            replies,
            server,
        }
    }

    fn queue(&self, error_code: u16, assignment: Vec<Assignment>) {
        self.replies.lock().expect("replies").push(SyncReply {
            error_code,
            assignment,
        });
    }

    fn seen(&self) -> Vec<SeenSync> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for SyncStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<SeenSync>>>,
    replies: Arc<Mutex<Vec<SyncReply>>>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            match decode_frame(&mut buf) {
                Ok(Some(frame)) => {
                    let corr = frame.header.correlation_id;
                    let req = match decode_request(frame.header.opcode, &frame.payload) {
                        Ok(r) => r,
                        Err(e) => {
                            write_resp(
                                &mut stream,
                                corr,
                                &Response::Error {
                                    code: 4,
                                    message: e.to_string(),
                                },
                            )
                            .await?;
                            continue;
                        }
                    };
                    let response = match req {
                        Request::SyncGroup {
                            group_id,
                            member_id,
                            generation,
                            assignment_bytes,
                        } => {
                            seen.lock().expect("seen").push(SeenSync {
                                group_id,
                                member_id,
                                generation,
                                assignment_bytes_len: assignment_bytes.len(),
                            });
                            let reply =
                                replies.lock().expect("replies").pop().unwrap_or(SyncReply {
                                    error_code: 0,
                                    assignment: vec![],
                                });
                            Response::SyncGroup {
                                error_code: reply.error_code,
                                assignment: reply.assignment,
                            }
                        }
                        other => Response::Error {
                            code: 4,
                            message: format!("unexpected {other:?}"),
                        },
                    };
                    write_resp(&mut stream, corr, &response).await?;
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e));
                }
            }
        }
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
    }
}

async fn write_resp(stream: &mut TcpStream, corr: u32, response: &Response) -> std::io::Result<()> {
    let packed = pack_response(corr, response)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut out = BytesMut::new();
    encode_frame(&packed, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&out).await
}

fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-v206-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn boot_broker(data_dir: std::path::PathBuf) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    let broker = std::sync::Arc::new(Broker::new(StorageConfig {
        data_dir,
        ..StorageConfig::default()
    }));
    let handle = tokio::spawn(async move {
        let _ = serve_listener(listener, broker).await;
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

#[tokio::test]
async fn sync_group_after_join_returns_same_partitions() {
    let dir = temp_data_dir("join");
    let (addr, server) = boot_broker(dir.clone()).await;
    let client = Client::connect_addr(&addr).await.expect("connect");
    client.create_topic("events", 4).await.expect("create");
    let joined = client
        .join_group("g", "", 10_000, vec!["events".into()])
        .await
        .expect("join");
    assert!(!joined.assignment.is_empty());
    let peeked = client
        .sync_group("g", &joined.member_id, joined.generation)
        .await
        .expect("sync");
    assert_eq!(peeked, joined.assignment);
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sync_group_unknown_member_is_10() {
    let dir = temp_data_dir("unknown");
    let (addr, server) = boot_broker(dir.clone()).await;
    let client = Client::connect_addr(&addr).await.expect("connect");
    client.create_topic("events", 1).await.expect("create");
    let joined = client
        .join_group("g", "", 10_000, vec!["events".into()])
        .await
        .expect("join");
    let err = client
        .sync_group("g", "nobody", joined.generation)
        .await
        .expect_err("unknown member");
    match err {
        Error::Protocol(m) => assert!(m.contains("error_code=10") || m.contains("10"), "{m}"),
        other => panic!("unexpected {other:?}"),
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sync_group_generation_mismatch_is_9() {
    let dir = temp_data_dir("gen");
    let (addr, server) = boot_broker(dir.clone()).await;
    let client = Client::connect_addr(&addr).await.expect("connect");
    client.create_topic("events", 1).await.expect("create");
    let joined = client
        .join_group("g", "", 10_000, vec!["events".into()])
        .await
        .expect("join");
    let err = client
        .sync_group("g", &joined.member_id, joined.generation.wrapping_add(1))
        .await
        .expect_err("generation mismatch");
    match err {
        Error::Protocol(m) => assert!(m.contains("error_code=9") || m.contains("9"), "{m}"),
        other => panic!("unexpected {other:?}"),
    }
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sync_group_fake_tcp_encodes_empty_assignment_bytes() {
    let stub = SyncStub::boot().await;
    stub.queue(
        0,
        vec![Assignment {
            topic: "events".into(),
            partition: 2,
        }],
    );
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let got = client.sync_group("g1", "m1", 3).await.expect("sync_group");
    assert_eq!(
        got,
        vec![Assignment {
            topic: "events".into(),
            partition: 2,
        }]
    );
    assert_eq!(
        stub.seen(),
        vec![SeenSync {
            group_id: "g1".into(),
            member_id: "m1".into(),
            generation: 3,
            assignment_bytes_len: 0,
        }]
    );
}

#[tokio::test]
async fn sync_group_fake_tcp_unknown_member_10() {
    let stub = SyncStub::boot().await;
    stub.queue(UNKNOWN_MEMBER, vec![]);
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let err = client
        .sync_group("g", "ghost", 1)
        .await
        .expect_err("unknown");
    match err {
        Error::Protocol(m) => assert!(m.contains("10"), "{m}"),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn sync_group_fake_tcp_generation_mismatch_9() {
    let stub = SyncStub::boot().await;
    stub.queue(REBALANCE, vec![]);
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let err = client
        .sync_group("g", "m1", 99)
        .await
        .expect_err("mismatch");
    match err {
        Error::Protocol(m) => assert!(m.contains("9"), "{m}"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn supported_apis_stays_49_sync_group_key_14() {
    assert_eq!(SUPPORTED_APIS.len(), 49);
    assert!(SUPPORTED_APIS
        .iter()
        .any(|(k, min, max)| *k == ApiKey::SyncGroup && *min == 0 && *max == 5));
}

#[tokio::test]
async fn second_join_is_9_until_sync_group() {
    let dir = temp_data_dir("fence");
    let (addr, server) = boot_broker(dir.clone()).await;
    let client = Client::connect_addr(&addr).await.expect("connect");
    client.create_topic("events", 4).await.expect("create");
    let first = client
        .join_group("g", "", 10_000, vec!["events".into()])
        .await
        .expect("first join");
    let err = client
        .join_group("g", "", 150, vec!["events".into()])
        .await
        .expect_err("second join fenced");
    match err {
        Error::Protocol(m) => assert!(m.contains("error_code=9") || m.contains("9"), "{m}"),
        other => panic!("unexpected {other:?}"),
    }
    let peeked = client
        .sync_group("g", &first.member_id, first.generation)
        .await
        .expect("sync");
    assert_eq!(peeked, first.assignment);
    let second = client
        .join_group("g", "", 10_000, vec!["events".into()])
        .await
        .expect("second join after sync");
    assert!(second.generation > first.generation);
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
