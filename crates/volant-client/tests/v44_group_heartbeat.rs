//! v0.44: Rust GroupConsumer background heartbeat.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{heartbeat_interval, Client, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Assignment, Request, Response};

#[test]
fn heartbeat_interval_clamps() {
    assert_eq!(heartbeat_interval(0), Duration::from_millis(100));
    assert_eq!(heartbeat_interval(150), Duration::from_millis(100));
    assert_eq!(heartbeat_interval(900), Duration::from_millis(300));
    assert_eq!(heartbeat_interval(10_000), Duration::from_millis(3000));
}

struct GroupStub {
    addr: String,
    heartbeats: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl GroupStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let heartbeats = Arc::new(AtomicU64::new(0));
        let hb = Arc::clone(&heartbeats);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let hb = Arc::clone(&hb);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, hb).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            heartbeats,
            server,
        }
    }

    fn heartbeat_rpcs(&self) -> u64 {
        self.heartbeats.load(Ordering::Relaxed)
    }
}

impl Drop for GroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(mut stream: TcpStream, heartbeats: Arc<AtomicU64>) -> std::io::Result<()> {
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
                        Request::JoinGroup { .. } => Response::JoinGroup {
                            error_code: 0,
                            generation: 1,
                            member_id: "m1".into(),
                            assignment: vec![Assignment {
                                topic: "t".into(),
                                partition: 0,
                            }],
                            revoked: vec![],
                        },
                        Request::OffsetFetch { .. } => Response::OffsetFetch {
                            error_code: 0,
                            entries: vec![],
                        },
                        Request::Heartbeat { .. } => {
                            heartbeats.fetch_add(1, Ordering::Relaxed);
                            Response::Heartbeat { error_code: 0 }
                        }
                        Request::LeaveGroup { .. } => Response::LeaveGroup { error_code: 0 },
                        Request::Fetch {
                            topic, partition, ..
                        } => Response::Fetch {
                            topic,
                            partition,
                            high_watermark: 0,
                            error_code: 0,
                            records: vec![],
                        },
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
    let mut out = BytesMut::new();
    encode_frame(&packed, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&out).await
}

#[tokio::test]
async fn background_heartbeat_fires_without_poll() {
    let stub = GroupStub::boot().await;
    let client = Arc::new(Client::connect_addr(&stub.addr).await.expect("connect"));
    let g = GroupConsumer::join(client, "g", vec!["t".into()], 300)
        .await
        .expect("join");
    assert_eq!(g.heartbeat_count(), 0);
    assert_eq!(stub.heartbeat_rpcs(), 0);

    tokio::time::sleep(Duration::from_millis(350)).await;

    let consumer_n = g.heartbeat_count();
    let stub_n = stub.heartbeat_rpcs();
    assert!(
        consumer_n >= 1,
        "expected background heartbeat on consumer, got {consumer_n}"
    );
    assert!(stub_n >= 1, "expected Heartbeat RPC on stub, got {stub_n}");
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn heartbeat_false_no_background() {
    let stub = GroupStub::boot().await;
    let client = Arc::new(Client::connect_addr(&stub.addr).await.expect("connect"));
    let g = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 300, false)
        .await
        .expect("join");

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(g.heartbeat_count(), 0);
    assert_eq!(stub.heartbeat_rpcs(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn leave_stops_heartbeat_task() {
    let stub = GroupStub::boot().await;
    let client = Arc::new(Client::connect_addr(&stub.addr).await.expect("connect"));
    let g = GroupConsumer::join(client, "g", vec!["t".into()], 300)
        .await
        .expect("join");

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        stub.heartbeat_rpcs() >= 1,
        "need at least one heartbeat before leave"
    );

    g.leave().await.expect("leave");
    let after_leave = stub.heartbeat_rpcs();

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        stub.heartbeat_rpcs(),
        after_leave,
        "no heartbeats after leave"
    );
}
