//! v0.114: Rust Client Metadata topic filter (`metadata_topics`).

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

struct MetaStub {
    addr: String,
    metadata: Arc<AtomicU64>,
    seen_topics: Arc<Mutex<Vec<Vec<String>>>>,
    server: tokio::task::JoinHandle<()>,
}

impl MetaStub {
    async fn boot() -> Self {
        Self::boot_codes(Vec::new()).await
    }

    async fn boot_codes(codes: impl Into<Vec<u16>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let metadata = Arc::new(AtomicU64::new(0));
        let seen_topics = Arc::new(Mutex::new(Vec::new()));
        let codes = Arc::new(Mutex::new(VecDeque::from(codes.into())));
        let metadata_s = Arc::clone(&metadata);
        let seen_s = Arc::clone(&seen_topics);
        let queued = Arc::clone(&codes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let metadata = Arc::clone(&metadata_s);
                let seen = Arc::clone(&seen_s);
                let queued = Arc::clone(&queued);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, metadata, seen, queued).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            metadata,
            seen_topics,
            server,
        }
    }

    fn metadata_rpcs(&self) -> u64 {
        self.metadata.load(Ordering::Relaxed)
    }

    fn seen_topics(&self) -> Vec<Vec<String>> {
        self.seen_topics.lock().expect("seen").clone()
    }
}

impl Drop for MetaStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    metadata: Arc<AtomicU64>,
    seen: Arc<Mutex<Vec<Vec<String>>>>,
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
                        Request::Metadata { topics } => {
                            metadata.fetch_add(1, Ordering::Relaxed);
                            seen.lock().expect("seen").push(topics);
                            let error_code = codes.lock().expect("codes").pop_front().unwrap_or(0);
                            // Native Metadata has no top-level error_code.
                            if error_code == 0 {
                                Response::Metadata {
                                    brokers: vec![],
                                    topics: vec![],
                                    controller_id: 0,
                                }
                            } else {
                                Response::Error {
                                    code: error_code,
                                    message: format!("error_code={error_code}"),
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
async fn metadata_sends_empty_topics_list() {
    let stub = MetaStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let meta = client.metadata().await.expect("metadata");
    assert!(meta.brokers.is_empty());
    assert!(meta.topics.is_empty());
    assert_eq!(stub.metadata_rpcs(), 1);
    assert_eq!(stub.seen_topics(), vec![Vec::<String>::new()]);
}

#[tokio::test]
async fn metadata_topics_encodes_named_filter() {
    let stub = MetaStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let meta = client
        .metadata_topics(vec!["events".into()])
        .await
        .expect("metadata_topics");
    assert!(meta.brokers.is_empty());
    assert_eq!(stub.metadata_rpcs(), 1);
    assert_eq!(stub.seen_topics(), vec![vec!["events".to_string()]]);
}

#[tokio::test]
async fn metadata_topics_empty_matches_metadata() {
    let stub = MetaStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let _ = client.metadata().await.expect("metadata");
    let _ = client
        .metadata_topics(Vec::new())
        .await
        .expect("metadata_topics empty");
    assert_eq!(stub.metadata_rpcs(), 2);
    let seen = stub.seen_topics();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1]);
    assert!(seen[0].is_empty());
}

#[tokio::test]
async fn metadata_still_retries_timeout() {
    let stub = MetaStub::boot_codes([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let meta = client.metadata().await.expect("metadata");
    assert!(meta.brokers.is_empty());
    assert!(meta.topics.is_empty());
    assert_eq!(stub.metadata_rpcs(), 2);
    assert_eq!(
        stub.seen_topics(),
        vec![Vec::<String>::new(), Vec::<String>::new()]
    );
}

#[tokio::test]
async fn metadata_topics_inherits_retry() {
    let stub = MetaStub::boot_codes([TIMEOUT, 0]).await;
    let client = connect(&stub.addr, 2, 0).await;
    let meta = client
        .metadata_topics(vec!["events".into()])
        .await
        .expect("metadata_topics");
    assert!(meta.brokers.is_empty());
    assert_eq!(stub.metadata_rpcs(), 2);
    assert_eq!(
        stub.seen_topics(),
        vec![vec!["events".to_string()], vec!["events".to_string()]]
    );
}

#[tokio::test]
async fn default_max_retries_zero_surfaces_timeout() {
    let stub = MetaStub::boot_codes([TIMEOUT]).await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    assert_eq!(ClientConfig::default().max_retries, 0);
    let err = client
        .metadata_topics(vec!["events".into()])
        .await
        .expect_err("timeout");
    assert_eq!(surfaced_code(&err), Some(TIMEOUT));
    assert_eq!(stub.metadata_rpcs(), 1);
}
