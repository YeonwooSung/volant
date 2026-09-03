//! v0.107: Rust Client shared-token Auth retry on transient errors.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Error;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, ErrorCode, Request, Response};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const AUTH_FAILED: u16 = ErrorCode::AuthenticationFailed as u16;

struct AuthStub {
    addr: String,
    auths: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl AuthStub {
    async fn boot(auth_codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let auths = Arc::new(AtomicU64::new(0));
        let auth_codes = Arc::new(Mutex::new(VecDeque::from(auth_codes.into())));
        let auths_s = Arc::clone(&auths);
        let queued = Arc::clone(&auth_codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let auths = Arc::clone(&auths_s);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, auths, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            auths,
            server,
        }
    }

    fn auth_rpcs(&self) -> u64 {
        self.auths.load(Ordering::Relaxed)
    }
}

impl Drop for AuthStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    auths: Arc<AtomicU64>,
    auth_codes: Arc<Mutex<VecDeque<u16>>>,
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
                        Request::Auth { .. } => {
                            auths.fetch_add(1, Ordering::Relaxed);
                            let error_code = auth_codes
                                .lock()
                                .expect("auth_codes")
                                .pop_front()
                                .unwrap_or(0);
                            Response::Auth { error_code }
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

async fn connect_with_token(
    addr: &str,
    token: &str,
    max_retries: u32,
    retry_backoff_ms: u64,
) -> Result<Client, Error> {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        auth_token: Some(token.to_owned()),
        max_retries,
        retry_backoff_ms,
        ..ClientConfig::default()
    })
    .await
}

async fn connect_default_token(addr: &str) -> Result<Client, Error> {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        auth_token: Some("tok".into()),
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
async fn default_max_retries_zero_surfaces_auth_timeout() {
    let stub = AuthStub::boot([TIMEOUT]).await;
    assert_eq!(client_max_retries_default(), 0);
    let err = connect_default_token(&stub.addr)
        .await
        .expect_err("timeout");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.auth_rpcs(), 1);
}

#[tokio::test]
async fn retries_auth_timeout_then_ok() {
    let stub = AuthStub::boot([TIMEOUT, 0]).await;
    let _client = connect_with_token(&stub.addr, "tok", 2, 0)
        .await
        .expect("connect");
    assert_eq!(stub.auth_rpcs(), 2);
}

#[tokio::test]
async fn authentication_failed_is_not_retried() {
    let stub = AuthStub::boot([AUTH_FAILED, 0]).await;
    let err = connect_with_token(&stub.addr, "tok", 2, 0)
        .await
        .expect_err("auth failed");
    assert_eq!(surfaced_code(&err), Some(AUTH_FAILED));
    assert_eq!(stub.auth_rpcs(), 1);
}

#[tokio::test]
async fn exhausted_auth_retries_surface_timeout() {
    let stub = AuthStub::boot([TIMEOUT, TIMEOUT, TIMEOUT]).await;
    let err = connect_with_token(&stub.addr, "tok", 2, 0)
        .await
        .expect_err("exhausted");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.auth_rpcs(), 3);
}

#[tokio::test]
async fn no_auth_token_skips_auth() {
    let stub = AuthStub::boot([TIMEOUT]).await;
    let _client = Client::connect(ClientConfig {
        brokers: vec![stub.addr.clone()],
        ..ClientConfig::default()
    })
    .await
    .expect("connect");
    assert_eq!(stub.auth_rpcs(), 0);
}
