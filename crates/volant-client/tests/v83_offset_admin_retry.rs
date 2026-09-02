//! v0.83: Rust Client OffsetCommit / OffsetFetch / DeleteOffsets retry.

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
    decode_request, pack_response, ErrorCode, OffsetCommitEntry, OffsetEntry, Request, Response,
};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const NOT_FOUND: u16 = ErrorCode::NotFound as u16;

struct OffsetAdminStub {
    addr: String,
    commits: Arc<AtomicU64>,
    fetches: Arc<AtomicU64>,
    deletes: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl OffsetAdminStub {
    async fn boot(codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let commits = Arc::new(AtomicU64::new(0));
        let fetches = Arc::new(AtomicU64::new(0));
        let deletes = Arc::new(AtomicU64::new(0));
        let codes = Arc::new(Mutex::new(VecDeque::from(codes.into())));
        let commits_s = Arc::clone(&commits);
        let fetches_s = Arc::clone(&fetches);
        let deletes_s = Arc::clone(&deletes);
        let queued = Arc::clone(&codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let commits = Arc::clone(&commits_s);
                let fetches = Arc::clone(&fetches_s);
                let deletes = Arc::clone(&deletes_s);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, commits, fetches, deletes, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            commits,
            fetches,
            deletes,
            server,
        }
    }

    fn commit_rpcs(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }

    fn fetch_rpcs(&self) -> u64 {
        self.fetches.load(Ordering::Relaxed)
    }

    fn delete_rpcs(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
    }
}

impl Drop for OffsetAdminStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    commits: Arc<AtomicU64>,
    fetches: Arc<AtomicU64>,
    deletes: Arc<AtomicU64>,
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
                        Request::OffsetCommit { .. } => {
                            commits.fetch_add(1, Ordering::Relaxed);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            Response::OffsetCommit { error_code }
                        }
                        Request::OffsetFetch { .. } => {
                            fetches.fetch_add(1, Ordering::Relaxed);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            Response::OffsetFetch {
                                error_code,
                                entries: vec![],
                            }
                        }
                        Request::DeleteOffsets { .. } => {
                            deletes.fetch_add(1, Ordering::Relaxed);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            Response::DeleteOffsets {
                                error_code,
                                deleted_count: 0,
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
        max_retries,
        retry_backoff_ms,
        ..ClientConfig::default()
    })
    .await
    .expect("connect")
}

fn commit_entry() -> OffsetCommitEntry {
    OffsetCommitEntry {
        topic: "t".into(),
        partition: 0,
        offset: 5,
        metadata: String::new(),
    }
}

fn fetch_entry() -> OffsetEntry {
    OffsetEntry {
        topic: "t".into(),
        partition: 0,
    }
}

fn broker_code(err: &Error) -> Option<u16> {
    match err {
        Error::Io(e) if e.kind() == std::io::ErrorKind::TimedOut => Some(TIMEOUT),
        Error::NotFound(m) if m.contains("error_code=2") => Some(NOT_FOUND),
        Error::Io(e) if e.to_string().contains("error_code=7") => Some(TIMEOUT),
        _ => None,
    }
}

fn client_max_retries_default() -> u32 {
    ClientConfig::default().max_retries
}

#[tokio::test]
async fn default_max_retries_zero_surfaces_timeout() {
    let stub = OffsetAdminStub::boot([TIMEOUT]).await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    assert_eq!(client_max_retries_default(), 0);
    let err = client
        .commit_offsets("g", "", 0, vec![commit_entry()])
        .await
        .expect_err("timeout");
    assert_eq!(broker_code(&err), Some(TIMEOUT));
    assert_eq!(stub.commit_rpcs(), 1);
}

#[tokio::test]
async fn retries_commit_timeout_then_ok() {
    let stub = OffsetAdminStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client
        .commit_offsets("g", "", 0, vec![commit_entry()])
        .await
        .expect("commit");
    assert_eq!(stub.commit_rpcs(), 2);
}

#[tokio::test]
async fn retries_fetch_timeout_then_ok() {
    let stub = OffsetAdminStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client
        .fetch_offsets("g", vec![fetch_entry()])
        .await
        .expect("fetch");
    assert_eq!(stub.fetch_rpcs(), 2);
}

#[tokio::test]
async fn retries_delete_timeout_then_ok() {
    let stub = OffsetAdminStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client.delete_offsets("g", vec![]).await.expect("delete");
    assert_eq!(stub.delete_rpcs(), 2);
}

#[tokio::test]
async fn not_found_is_not_retried() {
    let stub = OffsetAdminStub::boot([NOT_FOUND, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .commit_offsets("g", "", 0, vec![commit_entry()])
        .await
        .expect_err("not found");
    assert_eq!(broker_code(&err), Some(NOT_FOUND));
    assert_eq!(stub.commit_rpcs(), 1);
}

#[tokio::test]
async fn exhausted_retries_surface_timeout() {
    let stub = OffsetAdminStub::boot([TIMEOUT, TIMEOUT, TIMEOUT]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .commit_offsets("g", "", 0, vec![commit_entry()])
        .await
        .expect_err("exhausted");
    assert_eq!(broker_code(&err), Some(TIMEOUT));
    assert_eq!(stub.commit_rpcs(), 3);
}
