//! v0.94: Rust DescribeConfigs / AlterConfigs NotController (14) redirect.

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

struct AdminStub {
    addr: String,
    port: u16,
    describe: Arc<Mutex<VecDeque<DescribeReply>>>,
    alter: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    describe_n: Arc<AtomicU64>,
    alter_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    list_members_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct DescribeReply {
    code: u16,
    message: String,
    as_error: bool,
}

impl AdminStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let describe = Arc::new(Mutex::new(VecDeque::new()));
        let alter = Arc::new(Mutex::new(VecDeque::new()));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let describe_n = Arc::new(AtomicU64::new(0));
        let alter_n = Arc::new(AtomicU64::new(0));
        let metadata_n = Arc::new(AtomicU64::new(0));
        let list_members_n = Arc::new(AtomicU64::new(0));
        let describe_s = Arc::clone(&describe);
        let alter_s = Arc::clone(&alter);
        let brokers_s = Arc::clone(&brokers);
        let describe_n_s = Arc::clone(&describe_n);
        let alter_n_s = Arc::clone(&alter_n);
        let metadata_n_s = Arc::clone(&metadata_n);
        let list_members_n_s = Arc::clone(&list_members_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let describe = Arc::clone(&describe_s);
                let alter = Arc::clone(&alter_s);
                let brokers = Arc::clone(&brokers_s);
                let describe_n = Arc::clone(&describe_n_s);
                let alter_n = Arc::clone(&alter_n_s);
                let metadata_n = Arc::clone(&metadata_n_s);
                let list_members_n = Arc::clone(&list_members_n_s);
                tokio::spawn(async move {
                    let _ = serve_stub(
                        stream,
                        describe,
                        alter,
                        brokers,
                        describe_n,
                        alter_n,
                        metadata_n,
                        list_members_n,
                    )
                    .await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", sock.port()),
            port: sock.port(),
            describe,
            alter,
            brokers,
            describe_n,
            alter_n,
            metadata_n,
            list_members_n,
            server,
        }
    }

    fn queue_describe_error(&self, code: u16, message: &str) {
        self.describe
            .lock()
            .expect("describe")
            .push_back(DescribeReply {
                code,
                message: message.to_owned(),
                as_error: true,
            });
    }

    fn queue_describe(&self, code: u16) {
        self.describe
            .lock()
            .expect("describe")
            .push_back(DescribeReply {
                code,
                message: String::new(),
                as_error: false,
            });
    }

    fn queue_alter(&self, code: u16) {
        self.alter.lock().expect("alter").push_back(code);
    }

    fn set_brokers(&self, brokers: Vec<BrokerInfo>) {
        *self.brokers.lock().expect("brokers") = brokers;
    }

    fn describe_count(&self) -> u64 {
        self.describe_n.load(Ordering::Relaxed)
    }

    fn alter_count(&self) -> u64 {
        self.alter_n.load(Ordering::Relaxed)
    }

    fn metadata_count(&self) -> u64 {
        self.metadata_n.load(Ordering::Relaxed)
    }
}

impl Drop for AdminStub {
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

fn broker_code(err: &Error) -> Option<u16> {
    match err {
        Error::Protocol(m) if m.contains("not controller") || m.contains("error_code=14") => {
            Some(NOT_CONTROLLER)
        }
        _ => None,
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    describe: Arc<Mutex<VecDeque<DescribeReply>>>,
    alter: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    describe_n: Arc<AtomicU64>,
    alter_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    list_members_n: Arc<AtomicU64>,
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
                        Request::DescribeConfigs { topic } => {
                            describe_n.fetch_add(1, Ordering::Relaxed);
                            let reply = describe.lock().expect("describe").pop_front();
                            match reply {
                                Some(r) if r.as_error => Response::Error {
                                    code: r.code,
                                    message: r.message,
                                },
                                Some(r) => Response::DescribeConfigs {
                                    error_code: r.code,
                                    topic,
                                    topic_id: if r.code == 0 { 1 } else { 0 },
                                    partition_count: if r.code == 0 { 1 } else { 0 },
                                    configs: if r.code == 0 {
                                        vec![("retention.ms".into(), "86400000".into())]
                                    } else {
                                        vec![]
                                    },
                                },
                                None => Response::DescribeConfigs {
                                    error_code: 0,
                                    topic,
                                    topic_id: 1,
                                    partition_count: 1,
                                    configs: vec![("retention.ms".into(), "86400000".into())],
                                },
                            }
                        }
                        Request::AlterConfigs { topic, .. } => {
                            alter_n.fetch_add(1, Ordering::Relaxed);
                            let code = alter.lock().expect("alter").pop_front().unwrap_or(0);
                            Response::AlterConfigs {
                                error_code: code,
                                topic,
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
                        Request::ListMembers => {
                            list_members_n.fetch_add(1, Ordering::Relaxed);
                            Response::ListMembers {
                                error_code: 0,
                                generation: 0,
                                brokers: vec![],
                                live: vec![],
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

#[tokio::test]
async fn describe_configs_error_14_redirects_via_controller_id() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_describe_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let got = c
        .describe_configs("events")
        .await
        .expect("describe_configs");
    assert_eq!(got.topic, "events");
    assert_eq!(got.topic_id, 1);
    assert_eq!(got.partition_count, 1);
    assert_eq!(
        got.configs,
        vec![("retention.ms".to_string(), "86400000".to_string())]
    );
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.describe_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.describe_count(), 1);
    assert_eq!(follower.list_members_n.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn alter_configs_typed_14_no_hint_then_ok() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_alter(NOT_CONTROLLER);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    c.alter_configs("events", vec![("retention.ms".into(), "86400000".into())])
        .await
        .expect("alter_configs");
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.alter_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.alter_count(), 1);
}

#[tokio::test]
async fn describe_configs_max_redirects_zero_raises_on_first_14() {
    let follower = AdminStub::boot().await;
    follower.queue_describe(NOT_CONTROLLER);
    follower.set_brokers(controller_meta(2, "127.0.0.1", 9));

    let c = connect_redirects(&follower.addr, 0).await;
    let err = c
        .describe_configs("events")
        .await
        .expect_err("should stay 14");
    assert_eq!(broker_code(&err), Some(NOT_CONTROLLER));
    assert_eq!(follower.describe_count(), 1);
    assert_eq!(follower.metadata_count(), 0);
}
