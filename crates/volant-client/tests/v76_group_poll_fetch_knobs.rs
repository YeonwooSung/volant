//! v0.76: Rust GroupConsumer poll Fetch knobs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, Assignment, OffsetListing, Request, Response,
};

const POLL_MAX_MESSAGES: u32 = 100;
const POLL_MAX_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct FetchSnap {
    topic: String,
    partition: u32,
    max_messages: u32,
    max_bytes: u32,
}

struct GroupStub {
    addr: String,
    fetches: Arc<Mutex<Vec<FetchSnap>>>,
    joins: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl GroupStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let fetches = Arc::new(Mutex::new(Vec::new()));
        let joins = Arc::new(AtomicU64::new(0));
        let fetches_s = Arc::clone(&fetches);
        let joins_s = Arc::clone(&joins);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let fetches = Arc::clone(&fetches_s);
                let joins = Arc::clone(&joins_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, fetches, joins).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            fetches,
            joins,
            server,
        }
    }

    fn fetches(&self) -> Vec<FetchSnap> {
        self.fetches.lock().expect("fetches").clone()
    }

    fn join_count(&self) -> u64 {
        self.joins.load(Ordering::Relaxed)
    }
}

impl Drop for GroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    fetches: Arc<Mutex<Vec<FetchSnap>>>,
    joins: Arc<AtomicU64>,
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
                        Request::JoinGroup { .. } => {
                            joins.fetch_add(1, Ordering::Relaxed);
                            Response::JoinGroup {
                                error_code: 0,
                                generation: 1,
                                member_id: "m1".into(),
                                assignment: vec![Assignment {
                                    topic: "t".into(),
                                    partition: 0,
                                }],
                                revoked: vec![],
                                members: vec![],
                            }
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
                        Request::SyncGroup { .. } => Response::SyncGroup {
                            error_code: 0,
                            assignment: vec![],
                        },
                        Request::Heartbeat { .. } => Response::Heartbeat { error_code: 0 },
                        Request::LeaveGroup { .. } => Response::LeaveGroup { error_code: 0 },
                        Request::Fetch {
                            topic,
                            partition,
                            max_messages,
                            max_bytes,
                            ..
                        } => {
                            fetches.lock().expect("fetches").push(FetchSnap {
                                topic: topic.clone(),
                                partition,
                                max_messages,
                                max_bytes,
                            });
                            Response::Fetch {
                                topic,
                                partition,
                                high_watermark: 0,
                                error_code: 0,
                                records: vec![],
                            }
                        }
                        Request::OffsetCommit { .. } => Response::OffsetCommit { error_code: 0 },
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

async fn connect(stub: &GroupStub) -> Arc<Client> {
    Arc::new(Client::connect_addr(&stub.addr).await.expect("connect"))
}

async fn join_knobs(
    stub: &GroupStub,
    max_messages: u32,
    max_bytes: u32,
) -> volant_core::Result<GroupConsumer> {
    GroupConsumer::join_with_fetch_knobs(
        connect(stub).await,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        false,
        Duration::ZERO,
        "earliest",
        "broker",
        max_messages,
        max_bytes,
    )
    .await
}

#[tokio::test]
async fn default_join_poll_uses_historical_fetch_size() {
    let stub = GroupStub::boot().await;
    let client = connect(&stub).await;
    let mut g = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 10_000, false)
        .await
        .expect("join");
    assert_eq!(g.fetch_max_messages(), POLL_MAX_MESSAGES);
    assert_eq!(g.fetch_max_bytes(), POLL_MAX_BYTES);
    g.poll().await.expect("poll");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: POLL_MAX_MESSAGES,
            max_bytes: POLL_MAX_BYTES,
        }]
    );
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn join_fetch_max_messages_ten() {
    let stub = GroupStub::boot().await;
    let mut g = join_knobs(&stub, 10, POLL_MAX_BYTES).await.expect("join");
    assert_eq!(g.fetch_max_messages(), 10);
    assert_eq!(g.fetch_max_bytes(), POLL_MAX_BYTES);
    g.poll().await.expect("poll");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: 10,
            max_bytes: POLL_MAX_BYTES,
        }]
    );
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn join_fetch_max_bytes_4096() {
    let stub = GroupStub::boot().await;
    let mut g = join_knobs(&stub, POLL_MAX_MESSAGES, 4096)
        .await
        .expect("join");
    assert_eq!(g.fetch_max_messages(), POLL_MAX_MESSAGES);
    assert_eq!(g.fetch_max_bytes(), 4096);
    g.poll().await.expect("poll");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: POLL_MAX_MESSAGES,
            max_bytes: 4096,
        }]
    );
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn join_fetch_knobs_zero_clamps_to_defaults() {
    let stub = GroupStub::boot().await;
    let mut g = join_knobs(&stub, 0, 0).await.expect("join");
    assert_eq!(g.fetch_max_messages(), POLL_MAX_MESSAGES);
    assert_eq!(g.fetch_max_bytes(), POLL_MAX_BYTES);
    assert_eq!(stub.join_count(), 1);
    g.poll().await.expect("poll");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: POLL_MAX_MESSAGES,
            max_bytes: POLL_MAX_BYTES,
        }]
    );
    g.leave().await.expect("leave");
}
