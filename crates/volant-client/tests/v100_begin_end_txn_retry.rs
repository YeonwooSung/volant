//! v0.100: Rust Client BeginTxn / EndTxn retry on transient errors.

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
const INVALID_TXN_STATE: u16 = ErrorCode::InvalidTxnState as u16;

struct TxnStub {
    addr: String,
    begins: Arc<AtomicU64>,
    ends: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl TxnStub {
    async fn boot(begin_codes: impl Into<Vec<u16>>, end_codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let begins = Arc::new(AtomicU64::new(0));
        let ends = Arc::new(AtomicU64::new(0));
        let begin_codes = Arc::new(Mutex::new(VecDeque::from(begin_codes.into())));
        let end_codes = Arc::new(Mutex::new(VecDeque::from(end_codes.into())));
        let begins_s = Arc::clone(&begins);
        let ends_s = Arc::clone(&ends);
        let begin_queued = Arc::clone(&begin_codes);
        let end_queued = Arc::clone(&end_codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let begins = Arc::clone(&begins_s);
                let ends = Arc::clone(&ends_s);
                let begin_queued = Arc::clone(&begin_queued);
                let end_queued = Arc::clone(&end_queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, begins, ends, begin_queued, end_queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            begins,
            ends,
            server,
        }
    }

    fn begin_rpcs(&self) -> u64 {
        self.begins.load(Ordering::Relaxed)
    }

    fn end_rpcs(&self) -> u64 {
        self.ends.load(Ordering::Relaxed)
    }
}

impl Drop for TxnStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    begins: Arc<AtomicU64>,
    ends: Arc<AtomicU64>,
    begin_codes: Arc<Mutex<VecDeque<u16>>>,
    end_codes: Arc<Mutex<VecDeque<u16>>>,
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
                        Request::InitProducerId { .. } => Response::InitProducerId {
                            producer_id: 1,
                            epoch: 1,
                            error_code: 0,
                        },
                        Request::BeginTxn { .. } => {
                            begins.fetch_add(1, Ordering::Relaxed);
                            let error_code = begin_codes
                                .lock()
                                .expect("begin_codes")
                                .pop_front()
                                .unwrap_or(0);
                            Response::BeginTxn { error_code }
                        }
                        Request::EndTxn { .. } => {
                            ends.fetch_add(1, Ordering::Relaxed);
                            let error_code = end_codes
                                .lock()
                                .expect("end_codes")
                                .pop_front()
                                .unwrap_or(0);
                            Response::EndTxn {
                                error_code,
                                results: vec![],
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

async fn connect(addr: &str, max_retries: u32, retry_backoff_ms: u64) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        transactional_id: Some("txn-1".into()),
        max_retries,
        retry_backoff_ms,
        ..ClientConfig::default()
    })
    .await
    .expect("connect")
}

async fn connect_default(addr: &str) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        transactional_id: Some("txn-1".into()),
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
async fn default_max_retries_zero_surfaces_begin_timeout() {
    let stub = TxnStub::boot([TIMEOUT], []).await;
    let client = connect_default(&stub.addr).await;
    assert_eq!(client_max_retries_default(), 0);
    let err = client.begin_transaction().await.expect_err("timeout");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.begin_rpcs(), 1);
    assert_eq!(stub.end_rpcs(), 0);
}

#[tokio::test]
async fn retries_end_timeout_then_ok() {
    let stub = TxnStub::boot([], [TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client.begin_transaction().await.expect("begin");
    client.commit_transaction(Vec::new()).await.expect("commit");
    assert_eq!(stub.begin_rpcs(), 1);
    assert_eq!(stub.end_rpcs(), 2);
}

#[tokio::test]
async fn invalid_txn_state_is_not_retried() {
    let stub = TxnStub::boot([INVALID_TXN_STATE, 0], []).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .begin_transaction()
        .await
        .expect_err("invalid txn state");
    assert_eq!(surfaced_code(&err), Some(INVALID_TXN_STATE));
    assert_eq!(stub.begin_rpcs(), 1);
}

#[tokio::test]
async fn exhausted_end_retries_surface_timeout() {
    let stub = TxnStub::boot([], [TIMEOUT, TIMEOUT, TIMEOUT]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client.begin_transaction().await.expect("begin");
    let err = client
        .commit_transaction(Vec::new())
        .await
        .expect_err("exhausted");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.begin_rpcs(), 1);
    assert_eq!(stub.end_rpcs(), 3);
}
