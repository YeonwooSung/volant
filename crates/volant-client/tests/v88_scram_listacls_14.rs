//! v0.88: Rust SCRAM-admin / ListAcls NotController (14) redirect.

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
    create_scram: Arc<Mutex<VecDeque<CreateScramReply>>>,
    delete_scram: Arc<Mutex<VecDeque<u16>>>,
    list_scram: Arc<Mutex<VecDeque<u16>>>,
    list_acls: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    create_scram_n: Arc<AtomicU64>,
    delete_scram_n: Arc<AtomicU64>,
    list_scram_n: Arc<AtomicU64>,
    list_acls_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    list_members_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct CreateScramReply {
    code: u16,
    message: String,
    as_error: bool,
}

impl AdminStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let create_scram = Arc::new(Mutex::new(VecDeque::new()));
        let delete_scram = Arc::new(Mutex::new(VecDeque::new()));
        let list_scram = Arc::new(Mutex::new(VecDeque::new()));
        let list_acls = Arc::new(Mutex::new(VecDeque::new()));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let create_scram_n = Arc::new(AtomicU64::new(0));
        let delete_scram_n = Arc::new(AtomicU64::new(0));
        let list_scram_n = Arc::new(AtomicU64::new(0));
        let list_acls_n = Arc::new(AtomicU64::new(0));
        let metadata_n = Arc::new(AtomicU64::new(0));
        let list_members_n = Arc::new(AtomicU64::new(0));
        let create_scram_s = Arc::clone(&create_scram);
        let delete_scram_s = Arc::clone(&delete_scram);
        let list_scram_s = Arc::clone(&list_scram);
        let list_acls_s = Arc::clone(&list_acls);
        let brokers_s = Arc::clone(&brokers);
        let create_scram_n_s = Arc::clone(&create_scram_n);
        let delete_scram_n_s = Arc::clone(&delete_scram_n);
        let list_scram_n_s = Arc::clone(&list_scram_n);
        let list_acls_n_s = Arc::clone(&list_acls_n);
        let metadata_n_s = Arc::clone(&metadata_n);
        let list_members_n_s = Arc::clone(&list_members_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let create_scram = Arc::clone(&create_scram_s);
                let delete_scram = Arc::clone(&delete_scram_s);
                let list_scram = Arc::clone(&list_scram_s);
                let list_acls = Arc::clone(&list_acls_s);
                let brokers = Arc::clone(&brokers_s);
                let create_scram_n = Arc::clone(&create_scram_n_s);
                let delete_scram_n = Arc::clone(&delete_scram_n_s);
                let list_scram_n = Arc::clone(&list_scram_n_s);
                let list_acls_n = Arc::clone(&list_acls_n_s);
                let metadata_n = Arc::clone(&metadata_n_s);
                let list_members_n = Arc::clone(&list_members_n_s);
                tokio::spawn(async move {
                    let _ = serve_stub(
                        stream,
                        create_scram,
                        delete_scram,
                        list_scram,
                        list_acls,
                        brokers,
                        create_scram_n,
                        delete_scram_n,
                        list_scram_n,
                        list_acls_n,
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
            create_scram,
            delete_scram,
            list_scram,
            list_acls,
            brokers,
            create_scram_n,
            delete_scram_n,
            list_scram_n,
            list_acls_n,
            metadata_n,
            list_members_n,
            server,
        }
    }

    fn queue_create_scram_error(&self, code: u16, message: &str) {
        self.create_scram
            .lock()
            .expect("create_scram")
            .push_back(CreateScramReply {
                code,
                message: message.to_owned(),
                as_error: true,
            });
    }

    fn queue_delete_scram(&self, code: u16) {
        self.delete_scram
            .lock()
            .expect("delete_scram")
            .push_back(code);
    }

    fn queue_list_scram(&self, code: u16) {
        self.list_scram.lock().expect("list_scram").push_back(code);
    }

    fn queue_list_acls(&self, code: u16) {
        self.list_acls.lock().expect("list_acls").push_back(code);
    }

    fn set_brokers(&self, brokers: Vec<BrokerInfo>) {
        *self.brokers.lock().expect("brokers") = brokers;
    }

    fn create_scram_count(&self) -> u64 {
        self.create_scram_n.load(Ordering::Relaxed)
    }

    fn delete_scram_count(&self) -> u64 {
        self.delete_scram_n.load(Ordering::Relaxed)
    }

    fn list_scram_count(&self) -> u64 {
        self.list_scram_n.load(Ordering::Relaxed)
    }

    fn list_acls_count(&self) -> u64 {
        self.list_acls_n.load(Ordering::Relaxed)
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
    create_scram: Arc<Mutex<VecDeque<CreateScramReply>>>,
    delete_scram: Arc<Mutex<VecDeque<u16>>>,
    list_scram: Arc<Mutex<VecDeque<u16>>>,
    list_acls: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    create_scram_n: Arc<AtomicU64>,
    delete_scram_n: Arc<AtomicU64>,
    list_scram_n: Arc<AtomicU64>,
    list_acls_n: Arc<AtomicU64>,
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
                        Request::CreateScramUser { .. } => {
                            create_scram_n.fetch_add(1, Ordering::Relaxed);
                            let reply = create_scram.lock().expect("create_scram").pop_front();
                            match reply {
                                Some(r) if r.as_error => Response::Error {
                                    code: r.code,
                                    message: r.message,
                                },
                                Some(r) => Response::CreateScramUser { error_code: r.code },
                                None => Response::CreateScramUser { error_code: 0 },
                            }
                        }
                        Request::DeleteScramUser { .. } => {
                            delete_scram_n.fetch_add(1, Ordering::Relaxed);
                            let code = delete_scram
                                .lock()
                                .expect("delete_scram")
                                .pop_front()
                                .unwrap_or(0);
                            Response::DeleteScramUser { error_code: code }
                        }
                        Request::ListScramUsers => {
                            list_scram_n.fetch_add(1, Ordering::Relaxed);
                            let code = list_scram
                                .lock()
                                .expect("list_scram")
                                .pop_front()
                                .unwrap_or(0);
                            Response::ListScramUsers {
                                error_code: code,
                                usernames: if code == 0 {
                                    vec!["alice".into()]
                                } else {
                                    vec![]
                                },
                            }
                        }
                        Request::ListAcls { .. } => {
                            list_acls_n.fetch_add(1, Ordering::Relaxed);
                            let code = list_acls
                                .lock()
                                .expect("list_acls")
                                .pop_front()
                                .unwrap_or(0);
                            Response::ListAcls {
                                error_code: code,
                                entries: vec![],
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
async fn create_scram_user_error_14_redirects_via_controller_id() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_create_scram_error(NOT_CONTROLLER, "not controller; controller_id=2");
    follower.set_brokers(controller_meta(2, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    c.create_scram_user("alice", "s3cret", 0)
        .await
        .expect("create_scram_user");
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.create_scram_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.create_scram_count(), 1);
    assert_eq!(follower.list_members_n.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn list_acls_typed_14_no_hint_then_ok() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_list_acls(NOT_CONTROLLER);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let listed = c.list_acls("", 255, "").await.expect("list_acls");
    assert!(listed.is_empty());
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.list_acls_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.list_acls_count(), 1);
}

#[tokio::test]
async fn delete_scram_user_max_redirects_zero_raises_on_first_14() {
    let follower = AdminStub::boot().await;
    follower.queue_delete_scram(NOT_CONTROLLER);
    follower.set_brokers(controller_meta(2, "127.0.0.1", 9));

    let c = connect_redirects(&follower.addr, 0).await;
    let err = c
        .delete_scram_user("alice")
        .await
        .expect_err("should stay 14");
    assert_eq!(broker_code(&err), Some(NOT_CONTROLLER));
    assert_eq!(follower.delete_scram_count(), 1);
    assert_eq!(follower.metadata_count(), 0);
}

#[tokio::test]
async fn list_scram_users_error_14_then_ok() {
    let leader = AdminStub::boot().await;
    let follower = AdminStub::boot().await;
    follower.queue_list_scram(NOT_CONTROLLER);
    follower.set_brokers(other_broker_meta(follower.port, "127.0.0.1", leader.port));

    let c = connect(&follower.addr).await;
    let names = c.list_scram_users().await.expect("list_scram_users");
    assert_eq!(names, vec!["alice".to_string()]);
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.list_scram_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.list_scram_count(), 1);
}
