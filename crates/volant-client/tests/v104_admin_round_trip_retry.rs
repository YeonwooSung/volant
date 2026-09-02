//! v0.104: Rust admin_round_trip transient retry (CreateTopic stub).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, BrokerInfo, ErrorCode, Request, Response};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const NOT_FOUND: u16 = ErrorCode::NotFound as u16;
const NOT_CONTROLLER: u16 = ErrorCode::NotController as u16;

struct AdminRetryStub {
    addr: String,
    port: u16,
    create_topic: Arc<Mutex<VecDeque<CreateTopicReply>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    create_topic_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct CreateTopicReply {
    code: u16,
    message: String,
    as_error: bool,
}

impl AdminRetryStub {
    async fn boot(codes: impl Into<Vec<u16>>) -> Self {
        let stub = Self::boot_empty().await;
        for code in codes.into() {
            stub.queue_create_topic(code);
        }
        stub
    }

    async fn boot_empty() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let create_topic = Arc::new(Mutex::new(VecDeque::new()));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let create_topic_n = Arc::new(AtomicU64::new(0));
        let metadata_n = Arc::new(AtomicU64::new(0));
        let create_topic_s = Arc::clone(&create_topic);
        let brokers_s = Arc::clone(&brokers);
        let create_topic_n_s = Arc::clone(&create_topic_n);
        let metadata_n_s = Arc::clone(&metadata_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let create_topic = Arc::clone(&create_topic_s);
                let brokers = Arc::clone(&brokers_s);
                let create_topic_n = Arc::clone(&create_topic_n_s);
                let metadata_n = Arc::clone(&metadata_n_s);
                tokio::spawn(async move {
                    let _ =
                        serve_stub(stream, create_topic, brokers, create_topic_n, metadata_n).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", sock.port()),
            port: sock.port(),
            create_topic,
            brokers,
            create_topic_n,
            metadata_n,
            server,
        }
    }

    fn queue_create_topic(&self, code: u16) {
        self.create_topic
            .lock()
            .expect("create_topic")
            .push_back(CreateTopicReply {
                code,
                message: String::new(),
                as_error: false,
            });
    }

    fn queue_create_topic_error(&self, code: u16, message: &str) {
        self.create_topic
            .lock()
            .expect("create_topic")
            .push_back(CreateTopicReply {
                code,
                message: message.to_owned(),
                as_error: true,
            });
    }

    fn set_brokers(&self, brokers: Vec<BrokerInfo>) {
        *self.brokers.lock().expect("brokers") = brokers;
    }

    fn create_topic_count(&self) -> u64 {
        self.create_topic_n.load(Ordering::Relaxed)
    }

    fn metadata_count(&self) -> u64 {
        self.metadata_n.load(Ordering::Relaxed)
    }
}

impl Drop for AdminRetryStub {
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

async fn serve_stub(
    mut stream: TcpStream,
    create_topic: Arc<Mutex<VecDeque<CreateTopicReply>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    create_topic_n: Arc<AtomicU64>,
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
                        Request::CreateTopic {
                            name, partitions, ..
                        } => {
                            create_topic_n.fetch_add(1, Ordering::Relaxed);
                            let reply = create_topic.lock().expect("create_topic").pop_front();
                            match reply {
                                Some(r) if r.as_error => Response::Error {
                                    code: r.code,
                                    message: r.message,
                                },
                                Some(r) => Response::CreateTopic {
                                    topic_id: if r.code == 0 { 1 } else { 0 },
                                    name,
                                    partitions: if r.code == 0 { partitions } else { 0 },
                                    error_code: r.code,
                                },
                                None => Response::CreateTopic {
                                    topic_id: 1,
                                    name,
                                    partitions,
                                    error_code: 0,
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

async fn connect(addr: &str, max_retries: u32, retry_backoff_ms: u64) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        max_retries,
        retry_backoff_ms,
        ..ClientConfig::default()
    })
    .await
    .expect("connect")
}

fn broker_code(err: &Error) -> Option<u16> {
    match err {
        Error::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => Some(TIMEOUT),
        Error::NotFound(m) if m.contains("error_code=2") => Some(NOT_FOUND),
        Error::Io(e) if e.to_string().contains("error_code=7") => Some(TIMEOUT),
        Error::Protocol(m) if m.contains("not controller") || m.contains("error_code=14") => {
            Some(NOT_CONTROLLER)
        }
        _ => None,
    }
}

fn client_max_retries_default() -> u32 {
    ClientConfig::default().max_retries
}

#[tokio::test]
async fn default_max_retries_zero_surfaces_timeout() {
    let stub = AdminRetryStub::boot([TIMEOUT]).await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    assert_eq!(client_max_retries_default(), 0);
    let err = client.create_topic("events", 1).await.expect_err("timeout");
    assert_eq!(broker_code(&err), Some(TIMEOUT));
    assert_eq!(stub.create_topic_count(), 1);
}

#[tokio::test]
async fn retries_create_topic_timeout_then_ok() {
    let stub = AdminRetryStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let topic_id = client
        .create_topic("events", 1)
        .await
        .expect("create_topic");
    assert_eq!(topic_id.0, 1);
    assert_eq!(stub.create_topic_count(), 2);
}

#[tokio::test]
async fn error_14_is_redirect_not_retry() {
    let leader = AdminRetryStub::boot_empty().await;
    let follower = AdminRetryStub::boot_empty().await;
    follower.queue_create_topic_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", leader.port));

    let client = Client::connect_addr(&follower.addr).await.expect("connect");
    assert_eq!(client_max_retries_default(), 0);
    let topic_id = client
        .create_topic("events", 1)
        .await
        .expect("create_topic");
    assert_eq!(topic_id.0, 1);
    assert_eq!(client.current_addr().await, leader.addr);
    assert_eq!(follower.create_topic_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.create_topic_count(), 1);
}

#[tokio::test]
async fn not_found_is_not_retried() {
    let stub = AdminRetryStub::boot([NOT_FOUND, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .create_topic("missing", 1)
        .await
        .expect_err("not found");
    assert_eq!(broker_code(&err), Some(NOT_FOUND));
    assert_eq!(stub.create_topic_count(), 1);
}

#[tokio::test]
async fn exhausted_retries_surface_timeout() {
    let stub = AdminRetryStub::boot([TIMEOUT, TIMEOUT, TIMEOUT]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .create_topic("events", 1)
        .await
        .expect_err("exhausted");
    assert_eq!(broker_code(&err), Some(TIMEOUT));
    assert_eq!(stub.create_topic_count(), 3);
}
