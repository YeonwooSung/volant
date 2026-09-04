//! v0.205: JoinGroup retry when member_id or group_instance_id is set.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, ErrorCode, Request, Response};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;

struct JoinGroupStub {
    addr: String,
    joins: Arc<AtomicU64>,
    heartbeats: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl JoinGroupStub {
    async fn boot(codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let joins = Arc::new(AtomicU64::new(0));
        let heartbeats = Arc::new(AtomicU64::new(0));
        let codes = Arc::new(Mutex::new(VecDeque::from(codes.into())));
        let jn = Arc::clone(&joins);
        let hb = Arc::clone(&heartbeats);
        let queued = Arc::clone(&codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let jn = Arc::clone(&jn);
                let hb = Arc::clone(&hb);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, jn, hb, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            joins,
            heartbeats,
            server,
        }
    }

    fn join_rpcs(&self) -> u64 {
        self.joins.load(Ordering::Relaxed)
    }

    fn heartbeat_rpcs(&self) -> u64 {
        self.heartbeats.load(Ordering::Relaxed)
    }
}

impl Drop for JoinGroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    joins: Arc<AtomicU64>,
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
                                assignment: vec![],
                                revoked: vec![],
                                members: vec![],
                            }
                        }
                        Request::Heartbeat { .. } => {
                            heartbeats.fetch_add(1, Ordering::Relaxed);
                            Response::Heartbeat { error_code: 0 }
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
async fn empty_member_and_instance_is_one_shot() {
    let stub = JoinGroupStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .join_group("g", "", 10_000, vec!["t".into()])
        .await
        .expect_err("empty first join is not retried");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.join_rpcs(), 1);
    assert_eq!(stub.heartbeat_rpcs(), 0);
}

#[tokio::test]
async fn stored_member_id_retries_timeout_then_ok() {
    let stub = JoinGroupStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let result = client
        .join_group("g", "m-rejoin", 10_000, vec!["t".into()])
        .await
        .expect("join");
    assert_eq!(result.member_id, "m-1");
    assert_eq!(result.generation, 1);
    assert_eq!(stub.join_rpcs(), 2);
    assert_eq!(stub.heartbeat_rpcs(), 0);
}

#[tokio::test]
async fn static_instance_retries_timeout_then_ok() {
    let stub = JoinGroupStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let result = client
        .join_group_with_instance("g", "", 10_000, vec!["t".into()], "inst-1")
        .await
        .expect("join");
    assert_eq!(result.member_id, "m-1");
    assert_eq!(result.generation, 1);
    assert_eq!(stub.join_rpcs(), 2);
    assert_eq!(stub.heartbeat_rpcs(), 0);
}
