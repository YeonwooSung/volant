//! v0.87: Rust Client LeaveGroup retry on transient errors.

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
const REBALANCE: u16 = ErrorCode::RebalanceInProgress as u16;
const UNKNOWN_MEMBER: u16 = ErrorCode::UnknownMemberId as u16;

struct LeaveGroupStub {
    addr: String,
    leaves: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl LeaveGroupStub {
    async fn boot(codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let leaves = Arc::new(AtomicU64::new(0));
        let codes = Arc::new(Mutex::new(VecDeque::from(codes.into())));
        let lv = Arc::clone(&leaves);
        let queued = Arc::clone(&codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let lv = Arc::clone(&lv);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, lv, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            leaves,
            server,
        }
    }

    fn leave_rpcs(&self) -> u64 {
        self.leaves.load(Ordering::Relaxed)
    }
}

impl Drop for LeaveGroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    leaves: Arc<AtomicU64>,
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
                        Request::LeaveGroup { .. } => {
                            leaves.fetch_add(1, Ordering::Relaxed);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            Response::LeaveGroup { error_code }
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

fn client_max_retries_default() -> u32 {
    ClientConfig::default().max_retries
}

#[tokio::test]
async fn default_max_retries_zero_surfaces_timeout() {
    let stub = LeaveGroupStub::boot([TIMEOUT]).await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    assert_eq!(client_max_retries_default(), 0);
    let err = client.leave_group("g", "m1").await.expect_err("timeout");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.leave_rpcs(), 1);
}

#[tokio::test]
async fn retries_timeout_then_ok() {
    let stub = LeaveGroupStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client.leave_group("g", "m1").await.expect("leave");
    assert_eq!(stub.leave_rpcs(), 2);
}

#[tokio::test]
async fn unknown_member_is_success() {
    let stub = LeaveGroupStub::boot([UNKNOWN_MEMBER]).await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    client.leave_group("g", "m1").await.expect("already left");
    assert_eq!(stub.leave_rpcs(), 1);
}

#[tokio::test]
async fn retries_timeout_then_unknown_member_success() {
    let stub = LeaveGroupStub::boot([TIMEOUT, UNKNOWN_MEMBER]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client.leave_group("g", "m1").await.expect("already left");
    assert_eq!(stub.leave_rpcs(), 2);
}

#[tokio::test]
async fn rebalance_is_not_retried() {
    let stub = LeaveGroupStub::boot([REBALANCE, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client.leave_group("g", "m1").await.expect_err("rebalance");
    assert_eq!(surfaced_code(&err), Some(REBALANCE));
    assert_eq!(stub.leave_rpcs(), 1);
}
