//! v0.221: GroupConsumer retries Join on error 9 (SyncGroup fence).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, Assignment, ErrorCode, OffsetListing, Request, Response,
};

const REBALANCE: u16 = ErrorCode::RebalanceInProgress as u16;

struct JoinFenceStub {
    addr: String,
    joins: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
    heartbeats: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl JoinFenceStub {
    async fn boot(join_codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let joins = Arc::new(AtomicU64::new(0));
        let syncs = Arc::new(AtomicU64::new(0));
        let heartbeats = Arc::new(AtomicU64::new(0));
        let codes = Arc::new(Mutex::new(VecDeque::from(join_codes.into())));
        let jn = Arc::clone(&joins);
        let sy = Arc::clone(&syncs);
        let hb = Arc::clone(&heartbeats);
        let queued = Arc::clone(&codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let jn = Arc::clone(&jn);
                let sy = Arc::clone(&sy);
                let hb = Arc::clone(&hb);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, jn, sy, hb, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            joins,
            syncs,
            heartbeats,
            server,
        }
    }

    fn join_rpcs(&self) -> u64 {
        self.joins.load(Ordering::Relaxed)
    }

    fn sync_rpcs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    fn heartbeat_rpcs(&self) -> u64 {
        self.heartbeats.load(Ordering::Relaxed)
    }
}

impl Drop for JoinFenceStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    joins: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
    heartbeats: Arc<AtomicU64>,
    codes: Arc<Mutex<VecDeque<u16>>>,
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
                        Request::JoinGroup { .. } => {
                            joins.fetch_add(1, Ordering::Relaxed);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            Response::JoinGroup {
                                error_code,
                                generation: 1,
                                member_id: "m-1".into(),
                                assignment: vec![Assignment {
                                    topic: "t".into(),
                                    partition: 0,
                                }],
                                revoked: vec![],
                                members: vec![],
                            }
                        }
                        Request::SyncGroup { .. } => {
                            syncs.fetch_add(1, Ordering::Relaxed);
                            Response::SyncGroup {
                                error_code: 0,
                                assignment: vec![Assignment {
                                    topic: "t".into(),
                                    partition: 0,
                                }],
                            }
                        }
                        Request::Heartbeat { .. } => {
                            heartbeats.fetch_add(1, Ordering::Relaxed);
                            Response::Heartbeat { error_code: 0 }
                        }
                        Request::OffsetFetch { .. } => Response::OffsetFetch {
                            error_code: 0,
                            entries: vec![],
                        },
                        Request::ListOffsets { topic, partitions } => Response::ListOffsets {
                            error_code: 0,
                            topic,
                            entries: partitions
                                .into_iter()
                                .map(|partition| OffsetListing {
                                    partition,
                                    earliest: 0,
                                    latest: 0,
                                })
                                .collect(),
                        },
                        Request::LeaveGroup { .. } => Response::LeaveGroup { error_code: 0 },
                        Request::Metadata { .. } => Response::Metadata {
                            brokers: vec![],
                            topics: vec![],
                            controller_id: 0,
                        },
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

async fn connect(addr: &str, max_retries: u32, retry_backoff_ms: u64) -> Arc<Client> {
    Arc::new(
        Client::connect(ClientConfig {
            brokers: vec![addr.to_owned()],
            max_retries,
            retry_backoff_ms,
            ..ClientConfig::default()
        })
        .await
        .expect("connect"),
    )
}

#[tokio::test]
async fn group_join_retries_error_9_then_sync() {
    let stub = JoinFenceStub::boot([REBALANCE, 0]).await;
    let client = connect(&stub.addr, 1, 0).await;
    let g = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 10_000, false)
        .await
        .expect("join after fence retry");
    assert_eq!(g.member_id(), "m-1");
    assert_eq!(g.generation(), 1);
    assert_eq!(g.assignment(), vec![("t".into(), 0)]);
    assert_eq!(stub.join_rpcs(), 2);
    assert_eq!(stub.sync_rpcs(), 1);
    assert_eq!(g.heartbeat_count(), 0);
    assert_eq!(stub.heartbeat_rpcs(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn group_join_default_max_retries_does_not_retry_9() {
    let stub = JoinFenceStub::boot([REBALANCE, 0]).await;
    let client = connect(&stub.addr, 0, 0).await;
    let err = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 10_000, false)
        .await
        .expect_err("default max_retries=0");
    match err {
        volant_core::Error::Protocol(m) => {
            assert!(m.contains("error_code=9") || m.contains('9'), "{m}");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(stub.join_rpcs(), 1);
    assert_eq!(stub.sync_rpcs(), 0);
    assert_eq!(stub.heartbeat_rpcs(), 0);
}

#[tokio::test]
async fn client_join_retries_9_when_max_retries() {
    // v0.224: Client Join retries error 9 when max_retries > 0.
    let stub = JoinFenceStub::boot([REBALANCE, 0]).await;
    let client = connect(&stub.addr, 1, 0).await;
    let result = client
        .join_group("g", "m-rejoin", 10_000, vec!["t".into()])
        .await
        .expect("Client Join retries 9 when max_retries>0");
    assert_eq!(result.member_id, "m-1");
    assert_eq!(result.generation, 1);
    assert_eq!(stub.join_rpcs(), 2);
    assert_eq!(stub.sync_rpcs(), 0);
}
