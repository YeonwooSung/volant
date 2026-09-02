//! v0.102: Rust Client InitProducerId retry on transient errors.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::{Error, Message};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, ErrorCode, Request, Response};

const TIMEOUT: u16 = ErrorCode::Timeout as u16;
const UNKNOWN_PRODUCER_ID: u16 = ErrorCode::UnknownProducerId as u16;

struct InitStub {
    addr: String,
    inits: Arc<AtomicU64>,
    produces: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl InitStub {
    async fn boot(init_codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let inits = Arc::new(AtomicU64::new(0));
        let produces = Arc::new(AtomicU64::new(0));
        let init_codes = Arc::new(Mutex::new(VecDeque::from(init_codes.into())));
        let inits_s = Arc::clone(&inits);
        let produces_s = Arc::clone(&produces);
        let queued = Arc::clone(&init_codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let inits = Arc::clone(&inits_s);
                let produces = Arc::clone(&produces_s);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, inits, produces, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            inits,
            produces,
            server,
        }
    }

    fn init_rpcs(&self) -> u64 {
        self.inits.load(Ordering::Relaxed)
    }

    fn produce_rpcs(&self) -> u64 {
        self.produces.load(Ordering::Relaxed)
    }
}

impl Drop for InitStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    inits: Arc<AtomicU64>,
    produces: Arc<AtomicU64>,
    init_codes: Arc<Mutex<VecDeque<u16>>>,
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
                        Request::InitProducerId { .. } => {
                            inits.fetch_add(1, Ordering::Relaxed);
                            let error_code = init_codes
                                .lock()
                                .expect("init_codes")
                                .pop_front()
                                .unwrap_or(0);
                            Response::InitProducerId {
                                producer_id: 42,
                                epoch: 1,
                                error_code,
                            }
                        }
                        Request::Produce {
                            topic,
                            partition,
                            messages,
                            ..
                        } => {
                            produces.fetch_add(1, Ordering::Relaxed);
                            Response::Produce {
                                topic,
                                partition: partition.max(0) as u32,
                                base_offset: 0,
                                count: messages.len() as u32,
                                error_code: 0,
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
        enable_idempotence: true,
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
        enable_idempotence: true,
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

fn one_message() -> Vec<Message> {
    vec![Message::from_value(Bytes::from_static(b"v102"))]
}

#[tokio::test]
async fn default_max_retries_zero_surfaces_init_timeout() {
    let stub = InitStub::boot([TIMEOUT]).await;
    let client = connect_default(&stub.addr).await;
    assert_eq!(client_max_retries_default(), 0);
    let err = client
        .produce("t", Some(0), one_message())
        .await
        .expect_err("timeout");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.init_rpcs(), 1);
    assert_eq!(stub.produce_rpcs(), 0);
}

#[tokio::test]
async fn retries_init_timeout_then_ok() {
    let stub = InitStub::boot([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let produced = client
        .produce("t", Some(0), one_message())
        .await
        .expect("produce");
    assert_eq!(produced.count, 1);
    assert_eq!(stub.init_rpcs(), 2);
    assert_eq!(stub.produce_rpcs(), 1);
}

#[tokio::test]
async fn unknown_producer_id_on_init_is_not_retried() {
    let stub = InitStub::boot([UNKNOWN_PRODUCER_ID, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .produce("t", Some(0), one_message())
        .await
        .expect_err("unknown producer id");
    assert_eq!(surfaced_code(&err), Some(UNKNOWN_PRODUCER_ID));
    assert_eq!(stub.init_rpcs(), 1);
    assert_eq!(stub.produce_rpcs(), 0);
}

#[tokio::test]
async fn exhausted_init_retries_surface_timeout() {
    let stub = InitStub::boot([TIMEOUT, TIMEOUT, TIMEOUT]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let err = client
        .produce("t", Some(0), one_message())
        .await
        .expect_err("exhausted");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.init_rpcs(), 3);
    assert_eq!(stub.produce_rpcs(), 0);
}

#[tokio::test]
async fn already_initialized_skips_second_init() {
    let stub = InitStub::boot([0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    client
        .produce("t", Some(0), one_message())
        .await
        .expect("first produce");
    client
        .produce("t", Some(0), one_message())
        .await
        .expect("second produce");
    assert_eq!(stub.init_rpcs(), 1);
    assert_eq!(stub.produce_rpcs(), 2);
}
