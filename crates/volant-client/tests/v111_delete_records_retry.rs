//! v0.111: Rust DeleteRecords 13 redirect + transient retry.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, BrokerInfo, ErrorCode, PartitionInfo, Request, Response,
    TopicInfo,
};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const NOT_FOUND: u16 = ErrorCode::NotFound as u16;
const NOT_LEADER: u16 = ErrorCode::NotLeaderForPartition as u16;

struct DeleteRecordsStub {
    addr: String,
    port: u16,
    deletes: Arc<Mutex<VecDeque<DeleteReply>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
    delete_n: Arc<AtomicU64>,
    metadata_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct DeleteReply {
    code: u16,
    message: String,
    as_error: bool,
}

impl DeleteRecordsStub {
    async fn boot(codes: impl Into<Vec<u16>>) -> Self {
        let stub = Self::boot_empty().await;
        for code in codes.into() {
            stub.queue_delete(code);
        }
        stub
    }

    async fn boot_empty() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let deletes = Arc::new(Mutex::new(VecDeque::new()));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let topics = Arc::new(Mutex::new(Vec::new()));
        let delete_n = Arc::new(AtomicU64::new(0));
        let metadata_n = Arc::new(AtomicU64::new(0));
        let deletes_s = Arc::clone(&deletes);
        let brokers_s = Arc::clone(&brokers);
        let topics_s = Arc::clone(&topics);
        let delete_n_s = Arc::clone(&delete_n);
        let metadata_n_s = Arc::clone(&metadata_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let deletes = Arc::clone(&deletes_s);
                let brokers = Arc::clone(&brokers_s);
                let topics = Arc::clone(&topics_s);
                let delete_n = Arc::clone(&delete_n_s);
                let metadata_n = Arc::clone(&metadata_n_s);
                tokio::spawn(async move {
                    let _ =
                        serve_stub(stream, deletes, brokers, topics, delete_n, metadata_n).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", sock.port()),
            port: sock.port(),
            deletes,
            brokers,
            topics,
            delete_n,
            metadata_n,
            server,
        }
    }

    fn queue_delete(&self, code: u16) {
        self.deletes
            .lock()
            .expect("deletes")
            .push_back(DeleteReply {
                code,
                message: String::new(),
                as_error: false,
            });
    }

    fn set_leader_meta(&self, topic: &str, partition: u32, leader_id: u32, host: &str, port: u16) {
        *self.brokers.lock().expect("brokers") =
            vec![broker(1, "127.0.0.1", 1), broker(leader_id, host, port)];
        *self.topics.lock().expect("topics") = vec![TopicInfo {
            name: topic.into(),
            topic_id: 1,
            error_code: 0,
            partitions: vec![PartitionInfo {
                partition_id: partition,
                leader: leader_id,
                hwm: 0,
                replicas: vec![1, leader_id],
                isr: vec![leader_id],
                leader_epoch: 1,
            }],
        }];
    }

    fn delete_count(&self) -> u64 {
        self.delete_n.load(Ordering::Relaxed)
    }

    fn metadata_count(&self) -> u64 {
        self.metadata_n.load(Ordering::Relaxed)
    }
}

impl Drop for DeleteRecordsStub {
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

async fn serve_stub(
    mut stream: TcpStream,
    deletes: Arc<Mutex<VecDeque<DeleteReply>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
    delete_n: Arc<AtomicU64>,
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
                        Request::DeleteRecords {
                            topic, partition, ..
                        } => {
                            delete_n.fetch_add(1, Ordering::Relaxed);
                            let reply = deletes.lock().expect("deletes").pop_front();
                            match reply {
                                Some(r) if r.as_error => Response::Error {
                                    code: r.code,
                                    message: r.message,
                                },
                                Some(r) => Response::DeleteRecords {
                                    error_code: r.code,
                                    topic,
                                    partition,
                                    low_watermark: if r.code == 0 { 96 } else { 0 },
                                },
                                None => Response::DeleteRecords {
                                    error_code: 0,
                                    topic,
                                    partition,
                                    low_watermark: 96,
                                },
                            }
                        }
                        Request::Metadata { .. } => {
                            metadata_n.fetch_add(1, Ordering::Relaxed);
                            Response::Metadata {
                                brokers: brokers.lock().expect("brokers").clone(),
                                topics: topics.lock().expect("topics").clone(),
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

async fn connect_redirects(addr: &str, max_redirects: u32) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        max_redirects,
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
        Error::Protocol(m) if m.contains("not leader") || m.contains("error_code=13") => {
            Some(NOT_LEADER)
        }
        _ => None,
    }
}

fn client_max_retries_default() -> u32 {
    ClientConfig::default().max_retries
}

#[tokio::test]
async fn default_max_retries_zero_surfaces_timeout() {
    let stub = DeleteRecordsStub::boot([TIMEOUT]).await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    assert_eq!(client_max_retries_default(), 0);
    let err = client
        .delete_records("events", 0, 10)
        .await
        .expect_err("timeout");
    assert_eq!(broker_code(&err), Some(TIMEOUT));
    assert_eq!(stub.delete_count(), 1);
    assert_eq!(stub.metadata_count(), 0);
}

#[tokio::test]
async fn retries_delete_records_timeout_then_ok() {
    let stub = DeleteRecordsStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let got = client
        .delete_records("events", 2, 100)
        .await
        .expect("delete_records");
    assert_eq!(got.low_watermark, 96);
    assert_eq!(stub.delete_count(), 2);
    assert_eq!(stub.metadata_count(), 0);
}

#[tokio::test]
async fn error_13_is_redirect_not_retry() {
    let leader = DeleteRecordsStub::boot_empty().await;
    let follower = DeleteRecordsStub::boot_empty().await;
    follower.queue_delete(NOT_LEADER);
    follower.set_leader_meta("events", 2, 2, "127.0.0.1", leader.port);

    let client = Client::connect_addr(&follower.addr).await.expect("connect");
    assert_eq!(client_max_retries_default(), 0);
    let got = client
        .delete_records_with_wait_flag("events", 2, 100, 1)
        .await
        .expect("delete_records");
    assert_eq!(got.low_watermark, 96);
    assert_eq!(client.current_addr().await, leader.addr);
    assert_eq!(follower.delete_count(), 1);
    assert_eq!(follower.metadata_count(), 1);
    assert_eq!(leader.delete_count(), 1);
    assert_eq!(leader.metadata_count(), 0);
}

#[tokio::test]
async fn not_found_is_not_retried() {
    let stub = DeleteRecordsStub::boot([NOT_FOUND, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .delete_records("events", 0, 10)
        .await
        .expect_err("not found");
    assert_eq!(broker_code(&err), Some(NOT_FOUND));
    assert_eq!(stub.delete_count(), 1);
    assert_eq!(stub.metadata_count(), 0);
}

#[tokio::test]
async fn exhausted_retries_surface_timeout() {
    let stub = DeleteRecordsStub::boot([TIMEOUT, TIMEOUT, TIMEOUT]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .delete_records("events", 0, 10)
        .await
        .expect_err("exhausted");
    assert_eq!(broker_code(&err), Some(TIMEOUT));
    assert_eq!(stub.delete_count(), 3);
    assert_eq!(stub.metadata_count(), 0);
}

#[tokio::test]
async fn max_redirects_zero_raises_on_first_13() {
    let follower = DeleteRecordsStub::boot([NOT_LEADER]).await;
    follower.set_leader_meta("events", 0, 2, "127.0.0.1", 9);

    let client = connect_redirects(&follower.addr, 0).await;
    let err = client
        .delete_records("events", 0, 10)
        .await
        .expect_err("should stay 13");
    assert_eq!(broker_code(&err), Some(NOT_LEADER));
    assert_eq!(follower.delete_count(), 1);
    assert_eq!(follower.metadata_count(), 0);
}
