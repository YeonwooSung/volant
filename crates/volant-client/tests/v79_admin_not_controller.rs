//! v0.79: Rust admin NotController (14) redirect via Metadata / first other broker.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, AclBinding, BrokerInfo, ErrorCode, Request, Response,
};

const NOT_CONTROLLER: u16 = ErrorCode::NotController as u16;

struct AdminStub {
    addr: String,
    port: u16,
    create_topic: Arc<Mutex<VecDeque<CreateTopicReply>>>,
    create_partitions: Arc<Mutex<VecDeque<u16>>>,
    create_acls: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    create_topic_n: Arc<AtomicU64>,
    create_partitions_n: Arc<AtomicU64>,
    create_acls_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    list_members_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct CreateTopicReply {
    code: u16,
    message: String,
    as_error: bool,
}

impl AdminStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let create_topic = Arc::new(Mutex::new(VecDeque::new()));
        let create_partitions = Arc::new(Mutex::new(VecDeque::new()));
        let create_acls = Arc::new(Mutex::new(VecDeque::new()));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let create_topic_n = Arc::new(AtomicU64::new(0));
        let create_partitions_n = Arc::new(AtomicU64::new(0));
        let create_acls_n = Arc::new(AtomicU64::new(0));
        let metadata_n = Arc::new(AtomicU64::new(0));
        let list_members_n = Arc::new(AtomicU64::new(0));
        let create_topic_s = Arc::clone(&create_topic);
        let create_partitions_s = Arc::clone(&create_partitions);
        let create_acls_s = Arc::clone(&create_acls);
        let brokers_s = Arc::clone(&brokers);
        let create_topic_n_s = Arc::clone(&create_topic_n);
        let create_partitions_n_s = Arc::clone(&create_partitions_n);
        let create_acls_n_s = Arc::clone(&create_acls_n);
        let metadata_n_s = Arc::clone(&metadata_n);
        let list_members_n_s = Arc::clone(&list_members_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let create_topic = Arc::clone(&create_topic_s);
                let create_partitions = Arc::clone(&create_partitions_s);
                let create_acls = Arc::clone(&create_acls_s);
                let brokers = Arc::clone(&brokers_s);
                let create_topic_n = Arc::clone(&create_topic_n_s);
                let create_partitions_n = Arc::clone(&create_partitions_n_s);
                let create_acls_n = Arc::clone(&create_acls_n_s);
                let metadata_n = Arc::clone(&metadata_n_s);
                let list_members_n = Arc::clone(&list_members_n_s);
                tokio::spawn(async move {
                    let _ = serve_stub(
                        stream,
                        create_topic,
                        create_partitions,
                        create_acls,
                        brokers,
                        create_topic_n,
                        create_partitions_n,
                        create_acls_n,
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
            create_topic,
            create_partitions,
            create_acls,
            brokers,
            create_topic_n,
            create_partitions_n,
            create_acls_n,
            metadata_n,
            list_members_n,
            server,
        }
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

    fn queue_create_partitions(&self, code: u16) {
        self.create_partitions
            .lock()
            .expect("create_partitions")
            .push_back(code);
    }

    fn queue_create_acls(&self, code: u16) {
        self.create_acls
            .lock()
            .expect("create_acls")
            .push_back(code);
    }

    fn set_brokers(&self, brokers: Vec<BrokerInfo>) {
        *self.brokers.lock().expect("brokers") = brokers;
    }

    fn create_topic_count(&self) -> u64 {
        self.create_topic_n.load(Ordering::Relaxed)
    }

    fn create_partitions_count(&self) -> u64 {
        self.create_partitions_n.load(Ordering::Relaxed)
    }

    fn create_acls_count(&self) -> u64 {
        self.create_acls_n.load(Ordering::Relaxed)
    }

    fn metadata_count(&self) -> u64 {
        self.metadata_n.load(Ordering::Relaxed)
    }

    fn list_members_count(&self) -> u64 {
        self.list_members_n.load(Ordering::Relaxed)
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

fn acl_entry() -> AclBinding {
    AclBinding {
        principal: "User:alice".into(),
        resource_type: 0,
        resource: "events".into(),
        operation: 3,
        permission: 1,
    }
}

fn broker_code(err: &Error) -> Option<u16> {
    match err {
        Error::Protocol(m) if m.contains("not controller") || m.contains("error_code=14") => {
            Some(NOT_CONTROLLER)
        }
        Error::NotFound(m) if m.contains("error_code=2") => Some(2),
        Error::Protocol(m) if m.contains("error_code=2") => Some(2),
        _ => None,
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    create_topic: Arc<Mutex<VecDeque<CreateTopicReply>>>,
    create_partitions: Arc<Mutex<VecDeque<u16>>>,
    create_acls: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    create_topic_n: Arc<AtomicU64>,
    create_partitions_n: Arc<AtomicU64>,
    create_acls_n: Arc<AtomicU64>,
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
                        Request::CreatePartitions { topic, total_count } => {
                            create_partitions_n.fetch_add(1, Ordering::Relaxed);
                            let code = create_partitions
                                .lock()
                                .expect("create_partitions")
                                .pop_front()
                                .unwrap_or(0);
                            Response::CreatePartitions {
                                error_code: code,
                                topic,
                                partitions: if code == 0 { total_count } else { 0 },
                            }
                        }
                        Request::CreateAcls { .. } => {
                            create_acls_n.fetch_add(1, Ordering::Relaxed);
                            let code = create_acls
                                .lock()
                                .expect("create_acls")
                                .pop_front()
                                .unwrap_or(0);
                            Response::CreateAcls { error_code: code }
                        }
                        Request::Metadata { .. } => {
                            metadata_n.fetch_add(1, Ordering::Relaxed);
                            Response::Metadata {
                                brokers: brokers.lock().expect("brokers").clone(),
                                topics: vec![],
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
async fn create_topic_error_14_redirects_via_controller_id() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_create_topic_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let topic_id = c.create_topic("events", 1).await.expect("create_topic");
    assert_eq!(topic_id.0, 1);
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.create_topic_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.create_topic_count(), 1);
    assert_eq!(follower.list_members_count(), 0);
}

#[tokio::test]
async fn create_partitions_error_14_no_hint_picks_other_broker() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_create_partitions(NOT_CONTROLLER);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let got = c
        .create_partitions("events", 4)
        .await
        .expect("create_partitions");
    assert_eq!(got, 4);
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.create_partitions_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.create_partitions_count(), 1);
}

#[tokio::test]
async fn max_redirects_zero_raises_on_first_14() {
    let follower = AdminStub::boot().await;
    follower.queue_create_topic_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", 9));

    let c = connect_redirects(&follower.addr, 0).await;
    let err = c
        .create_topic("events", 1)
        .await
        .expect_err("should stay 14");
    assert_eq!(broker_code(&err), Some(NOT_CONTROLLER));
    assert_eq!(follower.create_topic_count(), 1);
    assert_eq!(follower.metadata_count(), 0);
}

#[tokio::test]
async fn create_acls_error_14_then_ok() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_create_acls(NOT_CONTROLLER);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    c.create_acls(vec![acl_entry()]).await.expect("create_acls");
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.create_acls_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.create_acls_count(), 1);
}

#[tokio::test]
async fn helper_no_other_broker_raises_14() {
    let follower = AdminStub::boot().await;
    follower.queue_create_partitions(NOT_CONTROLLER);
    follower.set_brokers(vec![broker(1, "127.0.0.1", follower.port)]);

    let c = connect(&follower.addr).await;
    let err = c
        .create_partitions("events", 4)
        .await
        .expect_err("should stay 14");
    assert_eq!(broker_code(&err), Some(NOT_CONTROLLER));
    assert_eq!(c.current_addr().await, follower.addr);
    assert_eq!(follower.create_partitions_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
}

#[tokio::test]
async fn other_error_raises_immediately() {
    let follower = AdminStub::boot().await;
    follower.queue_create_partitions(2);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", 9));

    let c = connect(&follower.addr).await;
    let err = c
        .create_partitions("missing", 4)
        .await
        .expect_err("should stay 2");
    assert_eq!(broker_code(&err), Some(2));
    assert_eq!(follower.create_partitions_count(), 1);
    assert_eq!(follower.metadata_count(), 0);
}
