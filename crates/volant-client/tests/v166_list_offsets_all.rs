//! v0.166: Rust Client ListOffsets all-partition named helper.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, OffsetListing, Request, Response};

fn sample_listings() -> Vec<OffsetListing> {
    vec![
        OffsetListing {
            partition: 0,
            earliest: 0,
            latest: 5,
        },
        OffsetListing {
            partition: 1,
            earliest: 1,
            latest: 8,
        },
    ]
}

struct ListOffsetsStub {
    addr: String,
    seen: Arc<Mutex<Vec<(String, Vec<u32>)>>>,
    server: tokio::task::JoinHandle<()>,
}

impl ListOffsetsStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_s = Arc::clone(&seen);
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
            seen,
            server,
        }
    }

    fn seen(&self) -> Vec<(String, Vec<u32>)> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for ListOffsetsStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<(String, Vec<u32>)>>>,
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
                        Request::ListOffsets {
                            topic, partitions, ..
                        } => {
                            seen.lock().expect("seen").push((topic.clone(), partitions));
                            Response::ListOffsets {
                                error_code: 0,
                                topic,
                                entries: sample_listings(),
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
async fn list_offsets_all_encodes_empty_partitions() {
    let stub = ListOffsetsStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let named = client
        .list_offsets_all("t")
        .await
        .expect("list_offsets_all");
    let empty = client
        .list_offsets("t", vec![])
        .await
        .expect("list_offsets");
    assert_eq!(named.topic, "t");
    assert_eq!(empty.topic, "t");
    assert_eq!(named.entries.len(), 2);
    assert_eq!(named.entries[0].partition, 0);
    assert_eq!(named.entries[0].earliest, 0);
    assert_eq!(named.entries[0].latest, 5);
    assert_eq!(named.entries[1].partition, 1);
    assert_eq!(empty.entries.len(), named.entries.len());
    assert_eq!(
        stub.seen(),
        vec![
            ("t".into(), Vec::<u32>::new()),
            ("t".into(), Vec::<u32>::new()),
        ]
    );
}
