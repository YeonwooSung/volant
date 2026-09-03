//! v0.159: Rust Client OffsetFetch all-group named helper.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, OffsetEntry, OffsetFetchEntry, Request, Response,
};

fn two_topic_entries() -> Vec<OffsetFetchEntry> {
    vec![
        OffsetFetchEntry {
            topic: "t".into(),
            partition: 0,
            offset: 5,
            metadata: "consumer-1".into(),
        },
        OffsetFetchEntry {
            topic: "u".into(),
            partition: 1,
            offset: 9,
            metadata: String::new(),
        },
    ]
}

struct OffsetFetchStub {
    addr: String,
    seen_entries: Arc<Mutex<Vec<Vec<OffsetEntry>>>>,
    server: tokio::task::JoinHandle<()>,
}

impl OffsetFetchStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let seen_entries = Arc::new(Mutex::new(Vec::new()));
        let seen_s = Arc::clone(&seen_entries);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let seen = Arc::clone(&seen_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, seen).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            seen_entries,
            server,
        }
    }

    fn seen_entries(&self) -> Vec<Vec<OffsetEntry>> {
        self.seen_entries.lock().expect("seen").clone()
    }
}

impl Drop for OffsetFetchStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<Vec<OffsetEntry>>>>,
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
                        Request::OffsetFetch { entries, .. } => {
                            seen.lock().expect("seen").push(entries);
                            Response::OffsetFetch {
                                error_code: 0,
                                entries: two_topic_entries(),
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
async fn fetch_offsets_all_matches_empty_fetch_offsets() {
    let stub = OffsetFetchStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let named = client
        .fetch_offsets_all("g")
        .await
        .expect("fetch_offsets_all");
    let empty = client
        .fetch_offsets("g", vec![])
        .await
        .expect("fetch_offsets");
    assert_eq!(named, empty);
    assert_eq!(named, two_topic_entries());
    assert_eq!(named[0].metadata, "consumer-1");
    assert_eq!(
        stub.seen_entries(),
        vec![Vec::<OffsetEntry>::new(), Vec::<OffsetEntry>::new()]
    );
}
