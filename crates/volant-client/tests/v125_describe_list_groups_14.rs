//! v0.125: Rust DescribeGroup / ListGroups NotController (14) redirect.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, BrokerInfo, ErrorCode, Request, Response};

const NOT_CONTROLLER: u16 = ErrorCode::NotController as u16;
const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const NOT_FOUND: u16 = ErrorCode::NotFound as u16;

struct GroupsStub {
    addr: String,
    port: u16,
    replies: Arc<Mutex<VecDeque<GroupReply>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    describe_n: Arc<AtomicU64>,
    list_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct GroupReply {
    code: u16,
    message: String,
    as_error: bool,
}

impl GroupsStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let replies = Arc::new(Mutex::new(VecDeque::new()));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let describe_n = Arc::new(AtomicU64::new(0));
        let list_n = Arc::new(AtomicU64::new(0));
        let metadata_n = Arc::new(AtomicU64::new(0));
        let replies_s = Arc::clone(&replies);
        let brokers_s = Arc::clone(&brokers);
        let describe_n_s = Arc::clone(&describe_n);
        let list_n_s = Arc::clone(&list_n);
        let metadata_n_s = Arc::clone(&metadata_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let replies = Arc::clone(&replies_s);
                let brokers = Arc::clone(&brokers_s);
                let describe_n = Arc::clone(&describe_n_s);
                let list_n = Arc::clone(&list_n_s);
                let metadata_n = Arc::clone(&metadata_n_s);
                tokio::spawn(async move {
                    let _ =
                        serve_stub(stream, replies, brokers, describe_n, list_n, metadata_n).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", sock.port()),
            port: sock.port(),
            replies,
            brokers,
            describe_n,
            list_n,
            metadata_n,
            server,
        }
    }

    fn queue_error(&self, code: u16, message: &str) {
        self.replies.lock().expect("replies").push_back(GroupReply {
            code,
            message: message.to_owned(),
            as_error: true,
        });
    }

    fn queue_typed(&self, code: u16) {
        self.replies.lock().expect("replies").push_back(GroupReply {
            code,
            message: String::new(),
            as_error: false,
        });
    }

    fn set_brokers(&self, brokers: Vec<BrokerInfo>) {
        *self.brokers.lock().expect("brokers") = brokers;
    }

    fn describe_count(&self) -> u64 {
        self.describe_n.load(Ordering::Relaxed)
    }

    fn list_count(&self) -> u64 {
        self.list_n.load(Ordering::Relaxed)
    }

    fn metadata_count(&self) -> u64 {
        self.metadata_n.load(Ordering::Relaxed)
    }
}

impl Drop for GroupsStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn broker(node_id: u32, host: &str, port: u16) -> BrokerInfo {
    BrokerInfo {
        node_id,
        host: host.into(),
        port,
    }
}

fn controller_meta(node_id: u32, host: &str, port: u16) -> Vec<BrokerInfo> {
    vec![broker(1, "127.0.0.1", 1), broker(node_id, host, port)]
}

fn other_broker_meta(current_port: u16, host: &str, port: u16) -> Vec<BrokerInfo> {
    vec![broker(1, "127.0.0.1", current_port), broker(2, host, port)]
}

fn surfaced_code(err: &Error) -> Option<u16> {
    match err {
        Error::Protocol(m) if m.contains("not controller") => Some(NOT_CONTROLLER),
        other => {
            let msg = other.to_string();
            let marker = "error_code=";
            let idx = msg.find(marker)?;
            msg[idx + marker.len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .ok()
        }
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    replies: Arc<Mutex<VecDeque<GroupReply>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    describe_n: Arc<AtomicU64>,
    list_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
) -> std::io::Result<()> {
    let mut buf = bytes::BytesMut::with_capacity(8 * 1024);
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
                        Request::DescribeGroup { group_id } => {
                            describe_n.fetch_add(1, Ordering::Relaxed);
                            match replies.lock().expect("replies").pop_front() {
                                Some(r) if r.as_error => Response::Error {
                                    code: r.code,
                                    message: r.message,
                                },
                                Some(r) => Response::DescribeGroup {
                                    error_code: r.code,
                                    group_id,
                                    generation: if r.code == 0 { 1 } else { 0 },
                                    members: vec![],
                                },
                                None => Response::DescribeGroup {
                                    error_code: 0,
                                    group_id,
                                    generation: 1,
                                    members: vec![],
                                },
                            }
                        }
                        Request::ListGroups => {
                            list_n.fetch_add(1, Ordering::Relaxed);
                            match replies.lock().expect("replies").pop_front() {
                                Some(r) if r.as_error => Response::Error {
                                    code: r.code,
                                    message: r.message,
                                },
                                Some(r) => Response::ListGroups {
                                    error_code: r.code,
                                    groups: vec![],
                                },
                                None => Response::ListGroups {
                                    error_code: 0,
                                    groups: vec![],
                                },
                            }
                        }
                        Request::Metadata { .. } => {
                            metadata_n.fetch_add(1, Ordering::Relaxed);
                            Response::Metadata {
                                brokers: brokers.lock().expect("brokers").clone(),
                                topics: vec![],
                                controller_id: 0,
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
    let mut out = bytes::BytesMut::new();
    encode_frame(&packed, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&out).await
}

async fn connect(addr: &str) -> Client {
    Client::connect_addr(addr).await.expect("connect")
}

async fn connect_redirects(addr: &str, max_redirects: u32) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        max_redirects,
        ..ClientConfig::default()
    })
    .await
    .expect("connect")
}

async fn connect_retries(addr: &str, max_retries: u32, retry_backoff_ms: u64) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        max_retries,
        retry_backoff_ms,
        ..ClientConfig::default()
    })
    .await
    .expect("connect")
}

#[tokio::test]
async fn describe_group_error_14_redirects_via_controller_id() {
    let leader = GroupsStub::boot().await;
    let follower = GroupsStub::boot().await;
    follower.queue_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let got = c.describe_group("g").await.expect("describe_group");
    assert_eq!(got.group_id, "g");
    assert_eq!(got.generation, 1);
    assert!(got.members.is_empty());
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.describe_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.describe_count(), 1);
    assert_eq!(leader.metadata_count(), 0);
}

#[tokio::test]
async fn list_groups_typed_14_no_hint_then_ok() {
    let leader = GroupsStub::boot().await;
    let follower = GroupsStub::boot().await;
    follower.queue_typed(NOT_CONTROLLER);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let groups = c.list_groups().await.expect("list_groups");
    assert!(groups.is_empty());
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.list_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.list_count(), 1);
}

#[tokio::test]
async fn describe_group_max_redirects_zero_raises_on_first_14() {
    let follower = GroupsStub::boot().await;
    follower.queue_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", 9));

    let c = connect_redirects(&follower.addr, 0).await;
    let err = c.describe_group("g").await.expect_err("should stay 14");
    assert_eq!(surfaced_code(&err), Some(NOT_CONTROLLER));
    assert_eq!(follower.describe_count(), 1);
    assert_eq!(follower.metadata_count(), 0);
}

#[tokio::test]
async fn describe_group_timeout_then_ok_still_retries() {
    let stub = GroupsStub::boot().await;
    stub.queue_typed(TIMEOUT);
    stub.queue_typed(0);

    let c = connect_retries(&stub.addr, 2, 0).await;
    let got = c.describe_group("g").await.expect("describe");
    assert_eq!(got.group_id, "g");
    assert_eq!(got.generation, 1);
    assert_eq!(stub.describe_count(), 2);
    assert_eq!(stub.metadata_count(), 0);
}

#[tokio::test]
async fn describe_group_not_found_is_not_retried() {
    let stub = GroupsStub::boot().await;
    stub.queue_typed(NOT_FOUND);

    let c = connect_retries(&stub.addr, 2, 0).await;
    let err = c.describe_group("g").await.expect_err("not found");
    assert_eq!(surfaced_code(&err), Some(NOT_FOUND));
    assert_eq!(stub.describe_count(), 1);
    assert_eq!(stub.metadata_count(), 0);
}
