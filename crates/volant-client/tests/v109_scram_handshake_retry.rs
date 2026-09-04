//! v0.109: Rust Client SCRAM handshake retry on transient errors.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::scram::client_proof_and_server_sig;
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, ErrorCode, Request, Response};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const AUTH_FAILED: u16 = ErrorCode::AuthenticationFailed as u16;

const USER: &str = "alice";
const PASS: &str = "s3cret";
const SALT: &[u8] = b"v109-salt-16byte";
const ITERATIONS: u32 = 1;

struct ScramStub {
    addr: String,
    firsts: Arc<AtomicU64>,
    finals: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl ScramStub {
    async fn boot(first_codes: impl Into<Vec<u16>>, final_codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let firsts = Arc::new(AtomicU64::new(0));
        let finals = Arc::new(AtomicU64::new(0));
        let first_codes = Arc::new(Mutex::new(VecDeque::from(first_codes.into())));
        let final_codes = Arc::new(Mutex::new(VecDeque::from(final_codes.into())));
        let firsts_s = Arc::clone(&firsts);
        let finals_s = Arc::clone(&finals);
        let first_q = Arc::clone(&first_codes);
        let final_q = Arc::clone(&final_codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let firsts = Arc::clone(&firsts_s);
                let finals = Arc::clone(&finals_s);
                let first_q = Arc::clone(&first_q);
                let final_q = Arc::clone(&final_q);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, firsts, finals, first_q, final_q).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            firsts,
            finals,
            server,
        }
    }

    fn first_rpcs(&self) -> u64 {
        self.firsts.load(Ordering::Relaxed)
    }

    fn final_rpcs(&self) -> u64 {
        self.finals.load(Ordering::Relaxed)
    }
}

impl Drop for ScramStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    firsts: Arc<AtomicU64>,
    finals: Arc<AtomicU64>,
    first_codes: Arc<Mutex<VecDeque<u16>>>,
    final_codes: Arc<Mutex<VecDeque<u16>>>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    let mut last_first: Option<(String, String, String)> = None;
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
                        Request::ScramFirst {
                            username,
                            client_nonce,
                            hash: _,
                        } => {
                            firsts.fetch_add(1, Ordering::Relaxed);
                            let error_code = first_codes
                                .lock()
                                .expect("first_codes")
                                .pop_front()
                                .unwrap_or(0);
                            if error_code != 0 {
                                last_first = None;
                                Response::ScramFirst {
                                    error_code,
                                    combined_nonce: String::new(),
                                    salt: Bytes::new(),
                                    iterations: 0,
                                }
                            } else {
                                let combined_nonce = format!("{client_nonce}srv");
                                last_first = Some((username, client_nonce, combined_nonce.clone()));
                                Response::ScramFirst {
                                    error_code: 0,
                                    combined_nonce,
                                    salt: Bytes::from_static(SALT),
                                    iterations: ITERATIONS,
                                }
                            }
                        }
                        Request::ScramFinal { .. } => {
                            finals.fetch_add(1, Ordering::Relaxed);
                            let error_code = final_codes
                                .lock()
                                .expect("final_codes")
                                .pop_front()
                                .unwrap_or(0);
                            if error_code != 0 {
                                Response::ScramFinal {
                                    error_code,
                                    server_signature: Bytes::new(),
                                }
                            } else {
                                let (username, client_nonce, combined_nonce) =
                                    last_first.clone().unwrap_or_else(|| {
                                        (USER.to_owned(), String::new(), String::new())
                                    });
                                let (_, sig) = client_proof_and_server_sig(
                                    &username,
                                    PASS,
                                    &client_nonce,
                                    &combined_nonce,
                                    SALT,
                                    ITERATIONS,
                                )
                                .expect("server signature");
                                Response::ScramFinal {
                                    error_code: 0,
                                    server_signature: Bytes::from(sig),
                                }
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

async fn connect(addr: &str, max_retries: u32, retry_backoff_ms: u64) -> Result<Client, Error> {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        scram_username: Some(USER.into()),
        scram_password: Some(PASS.into()),
        max_retries,
        retry_backoff_ms,
        ..ClientConfig::default()
    })
    .await
}

async fn connect_default(addr: &str) -> Result<Client, Error> {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        scram_username: Some(USER.into()),
        scram_password: Some(PASS.into()),
        ..ClientConfig::default()
    })
    .await
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
async fn default_max_retries_zero_surfaces_first_timeout() {
    let stub = ScramStub::boot([TIMEOUT], []).await;
    assert_eq!(client_max_retries_default(), 0);
    let err = connect_default(&stub.addr).await.expect_err("timeout");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.first_rpcs(), 1);
    assert_eq!(stub.final_rpcs(), 0);
}

#[tokio::test]
async fn retries_first_timeout_then_ok() {
    let stub = ScramStub::boot([TIMEOUT, 0], [0]).await;
    connect(&stub.addr, 2, 0).await.expect("connect");
    assert_eq!(stub.first_rpcs(), 2);
    assert_eq!(stub.final_rpcs(), 1);
}

#[tokio::test]
async fn final_timeout_restarts_handshake() {
    let stub = ScramStub::boot([0, 0], [TIMEOUT, 0]).await;
    connect(&stub.addr, 2, 0).await.expect("connect");
    assert_eq!(stub.first_rpcs(), 2);
    assert_eq!(stub.final_rpcs(), 2);
}

#[tokio::test]
async fn first_auth_failed_is_not_retried() {
    let stub = ScramStub::boot([AUTH_FAILED, 0], [0]).await;
    let err = connect(&stub.addr, 2, 0).await.expect_err("auth failed");
    assert_eq!(surfaced_code(&err), Some(AUTH_FAILED));
    assert_eq!(stub.first_rpcs(), 1);
    assert_eq!(stub.final_rpcs(), 0);
}
