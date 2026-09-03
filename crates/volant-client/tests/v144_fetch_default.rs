//! v0.144: Rust ClientConfig Fetch knobs + fetch_default.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Offset;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response};

const DEFAULT_FETCH_MAX_MESSAGES: u32 = 128;
const DEFAULT_FETCH_MAX_BYTES: u32 = 4 * 1024 * 1024;
const DEFAULT_FETCH_MAX_WAIT_MS: u32 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
struct FetchSnap {
    topic: String,
    partition: u32,
    max_messages: u32,
    max_bytes: u32,
    max_wait_ms: u32,
}

struct FetchStub {
    addr: String,
    fetches: Arc<Mutex<Vec<FetchSnap>>>,
    server: tokio::task::JoinHandle<()>,
}

impl FetchStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let fetches = Arc::new(Mutex::new(Vec::new()));
        let fetches_s = Arc::clone(&fetches);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let fetches = Arc::clone(&fetches_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, fetches).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            fetches,
            server,
        }
    }

    fn fetches(&self) -> Vec<FetchSnap> {
        self.fetches.lock().expect("fetches").clone()
    }
}

impl Drop for FetchStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    fetches: Arc<Mutex<Vec<FetchSnap>>>,
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
                        Request::Fetch {
                            topic,
                            partition,
                            max_messages,
                            max_bytes,
                            max_wait_ms,
                            ..
                        } => {
                            fetches.lock().expect("fetches").push(FetchSnap {
                                topic: topic.clone(),
                                partition,
                                max_messages,
                                max_bytes,
                                max_wait_ms,
                            });
                            Response::Fetch {
                                topic,
                                partition,
                                high_watermark: 0,
                                error_code: 0,
                                records: vec![],
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
    let mut out = bytes::BytesMut::new();
    encode_frame(&packed, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&out).await
}

async fn connect(stub: &FetchStub) -> Client {
    Client::connect_addr(&stub.addr).await.expect("connect")
}

async fn connect_cfg(stub: &FetchStub, cfg: ClientConfig) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![stub.addr.clone()],
        ..cfg
    })
    .await
    .expect("connect")
}

#[tokio::test]
async fn fetch_default_uses_client_config_defaults() {
    let stub = FetchStub::boot().await;
    let client = connect(&stub).await;
    client
        .fetch_default("t", 0, Offset::ZERO)
        .await
        .expect("fetch_default");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: DEFAULT_FETCH_MAX_MESSAGES,
            max_bytes: DEFAULT_FETCH_MAX_BYTES,
            max_wait_ms: DEFAULT_FETCH_MAX_WAIT_MS,
        }]
    );
}

#[tokio::test]
async fn fetch_default_uses_configured_knobs() {
    let stub = FetchStub::boot().await;
    let client = connect_cfg(
        &stub,
        ClientConfig {
            fetch_max_messages: 10,
            fetch_max_bytes: 4096,
            fetch_max_wait_ms: 100,
            ..ClientConfig::default()
        },
    )
    .await;
    client
        .fetch_default("t", 0, Offset::ZERO)
        .await
        .expect("fetch_default");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: 10,
            max_bytes: 4096,
            max_wait_ms: 100,
        }]
    );
}

#[tokio::test]
async fn existing_fetch_still_hardcodes_max_bytes() {
    let stub = FetchStub::boot().await;
    let client = connect_cfg(
        &stub,
        ClientConfig {
            fetch_max_messages: 10,
            fetch_max_bytes: 4096,
            fetch_max_wait_ms: 100,
            ..ClientConfig::default()
        },
    )
    .await;
    client
        .fetch("t", 0, Offset::ZERO, 7, 0)
        .await
        .expect("fetch");
    assert_eq!(
        stub.fetches(),
        vec![FetchSnap {
            topic: "t".into(),
            partition: 0,
            max_messages: 7,
            max_bytes: DEFAULT_FETCH_MAX_BYTES,
            max_wait_ms: 0,
        }]
    );
}
