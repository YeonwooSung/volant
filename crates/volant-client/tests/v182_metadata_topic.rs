//! v0.182: Rust Client Metadata one-topic named helper (`metadata_topic`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response};

struct MetaStub {
    addr: String,
    metadata: Arc<AtomicU64>,
    seen_topics: Arc<Mutex<Vec<Vec<String>>>>,
    server: tokio::task::JoinHandle<()>,
}

impl MetaStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let metadata = Arc::new(AtomicU64::new(0));
        let seen_topics = Arc::new(Mutex::new(Vec::new()));
        let metadata_s = Arc::clone(&metadata);
        let seen_s = Arc::clone(&seen_topics);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let metadata = Arc::clone(&metadata_s);
                let seen = Arc::clone(&seen_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, metadata, seen).await;
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
                            Response::Metadata {
                                brokers: vec![],
                                topics: vec![],
                                controller_id: 0,
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

#[tokio::test]
async fn metadata_topic_encodes_one_name() {
    let stub = MetaStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let meta = client.metadata_topic("events").await.expect("metadata_topic");
    assert!(meta.brokers.is_empty());
    assert!(meta.topics.is_empty());
    assert_eq!(stub.metadata_rpcs(), 1);
    assert_eq!(stub.seen_topics(), vec![vec!["events".to_string()]]);
}

#[tokio::test]
async fn metadata_topic_matches_metadata_topics_one() {
    let stub = MetaStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let _ = client.metadata_topic("events").await.expect("metadata_topic");
    let _ = client
        .metadata_topics(vec!["events".into()])
        .await
        .expect("metadata_topics");
    assert_eq!(stub.metadata_rpcs(), 2);
    let seen = stub.seen_topics();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[0], vec!["events".to_string()]);
}
