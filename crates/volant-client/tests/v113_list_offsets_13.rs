//! v0.113: Rust ListOffsets NotLeader (13) redirect.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, BrokerInfo, ErrorCode, OffsetListing, PartitionInfo, Request,
    Response, TopicInfo,
};

const NOT_LEADER: u16 = ErrorCode::NotLeaderForPartition as u16;
const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const NOT_FOUND: u16 = ErrorCode::NotFound as u16;

struct ListOffsetsStub {
    addr: String,
    port: u16,
    list_offsets: Arc<AtomicU64>,
    metadata: Arc<AtomicU64>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
    server: tokio::task::JoinHandle<()>,
}

impl ListOffsetsStub {
    async fn boot(codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let list_offsets = Arc::new(AtomicU64::new(0));
        let metadata = Arc::new(AtomicU64::new(0));
        let codes = Arc::new(Mutex::new(VecDeque::from(codes.into())));
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let topics = Arc::new(Mutex::new(Vec::new()));
        let lo = Arc::clone(&list_offsets);
        let meta_n = Arc::clone(&metadata);
        let queued = Arc::clone(&codes);
        let brokers_s = Arc::clone(&brokers);
        let topics_s = Arc::clone(&topics);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let lo = Arc::clone(&lo);
                let meta_n = Arc::clone(&meta_n);
                let queued = Arc::clone(&queued);
                let brokers = Arc::clone(&brokers_s);
                let topics = Arc::clone(&topics_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, lo, meta_n, queued, brokers, topics).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", sock.port()),
            port: sock.port(),
            list_offsets,
            metadata,
            brokers,
            topics,
            server,
        }
    }

    fn set_leader(&self, topic: &str, partition: u32, node_id: u32, host: &str, port: u16) {
        *self.brokers.lock().expect("brokers") = vec![
            broker(1, "127.0.0.1", self.port),
            broker(node_id, host, port),
        ];
        *self.topics.lock().expect("topics") = vec![TopicInfo {
            name: topic.into(),
            topic_id: 1,
            error_code: 0,
            partitions: vec![PartitionInfo {
                partition_id: partition,
                leader: node_id,
                hwm: 0,
                replicas: vec![node_id],
                isr: vec![node_id],
                leader_epoch: 0,
            }],
        }];
    }

    fn list_offsets_rpcs(&self) -> u64 {
        self.list_offsets.load(Ordering::Relaxed)
    }

    fn metadata_rpcs(&self) -> u64 {
        self.metadata.load(Ordering::Relaxed)
    }
}

impl Drop for ListOffsetsStub {
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
    list_offsets: Arc<AtomicU64>,
    metadata: Arc<AtomicU64>,
    codes: Arc<Mutex<VecDeque<u16>>>,
    brokers: Arc<Mutex<Vec<BrokerInfo>>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
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
                        Request::ListOffsets {
                            topic, partitions, ..
                        } => {
                            list_offsets.fetch_add(1, Ordering::Relaxed);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            let entries = if error_code == 0 {
                                let parts = if partitions.is_empty() {
                                    vec![0]
                                } else {
                                    partitions
                                };
                                parts
                                    .into_iter()
                                    .map(|partition| OffsetListing {
                                        partition,
                                        earliest: 0,
                                        latest: 5,
                                    })
                                    .collect()
                            } else {
                                vec![]
                            };
                            Response::ListOffsets {
                                error_code,
                                topic,
                                entries,
                            }
                        }
                        Request::Metadata { .. } => {
                            metadata.fetch_add(1, Ordering::Relaxed);
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
    let mut out = BytesMut::new();
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

fn surfaced_code(err: &Error) -> Option<u16> {
    let msg = err.to_string();
    let marker = "error_code=";
    let idx = msg.find(marker)?;
    msg[idx + marker.len()..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

#[tokio::test]
async fn first_13_redirects_via_metadata_leader_then_ok() {
    let leader = ListOffsetsStub::boot([0]).await;
    let follower = ListOffsetsStub::boot([NOT_LEADER]).await;
    follower.set_leader("t", 0, 2, "127.0.0.1", leader.port);

    let c = connect(&follower.addr).await;
    assert_eq!(ClientConfig::default().max_retries, 0);
    let result = c.list_offsets("t", vec![0]).await.expect("list_offsets");
    assert_eq!(result.topic, "t");
    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].partition, 0);
    assert_eq!(result.entries[0].earliest, 0);
    assert_eq!(result.entries[0].latest, 5);
    assert_eq!(c.current_addr().await, leader.addr);
    assert_eq!(follower.list_offsets_rpcs(), 1);
    assert_eq!(follower.metadata_rpcs(), 1);
    assert_eq!(leader.list_offsets_rpcs(), 1);
    assert_eq!(leader.metadata_rpcs(), 0);
}

#[tokio::test]
async fn max_redirects_zero_surfaces_13_without_metadata() {
    let follower = ListOffsetsStub::boot([NOT_LEADER]).await;
    follower.set_leader("t", 0, 2, "127.0.0.1", 9);

    let c = connect_redirects(&follower.addr, 0).await;
    let err = c
        .list_offsets("t", vec![0])
        .await
        .expect_err("should stay 13");
    assert_eq!(surfaced_code(&err), Some(NOT_LEADER));
    assert_eq!(follower.list_offsets_rpcs(), 1);
    assert_eq!(follower.metadata_rpcs(), 0);
}

#[tokio::test]
async fn timeout_then_ok_still_retries_without_metadata() {
    let stub = ListOffsetsStub::boot([TIMEOUT, 0]).await;
    let c = connect_retries(&stub.addr, 2, 0).await;
    let result = c.list_offsets("t", vec![0]).await.expect("list_offsets");
    assert_eq!(result.topic, "t");
    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].latest, 5);
    assert_eq!(stub.list_offsets_rpcs(), 2);
    assert_eq!(stub.metadata_rpcs(), 0);
}

#[tokio::test]
async fn not_found_is_not_redirected_or_retried() {
    let stub = ListOffsetsStub::boot([NOT_FOUND, 0]).await;
    let c = connect_retries(&stub.addr, 2, 0).await;
    let err = c.list_offsets("t", vec![0]).await.expect_err("not found");
    assert_eq!(surfaced_code(&err), Some(NOT_FOUND));
    assert_eq!(stub.list_offsets_rpcs(), 1);
    assert_eq!(stub.metadata_rpcs(), 0);
}
