//! v0.168: Rust Client ReassignPartitions all-partition named helper.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response, REASSIGN_ALL_PARTITIONS};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenReassign {
    topic: String,
    partition: u32,
    replicas: Vec<u32>,
}

struct ReassignStub {
    addr: String,
    seen: Arc<Mutex<Vec<SeenReassign>>>,
    server: tokio::task::JoinHandle<()>,
}

impl ReassignStub {
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

    fn seen(&self) -> Vec<SeenReassign> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for ReassignStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<SeenReassign>>>,
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
                        Request::ReassignPartitions {
                            topic,
                            partition,
                            replicas,
                        } => {
                            seen.lock().expect("seen").push(SeenReassign {
                                topic,
                                partition,
                                replicas,
                            });
                            Response::ReassignPartitions {
                                error_code: 0,
                                generation: 1,
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
async fn reassign_partitions_all_encodes_sentinel_and_replicas() {
    let stub = ReassignStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let gen = client
        .reassign_partitions_all("t", &[1, 2])
        .await
        .expect("reassign_partitions_all");
    assert_eq!(gen, 1);
    assert_eq!(
        stub.seen(),
        vec![SeenReassign {
            topic: "t".into(),
            partition: REASSIGN_ALL_PARTITIONS,
            replicas: vec![1, 2],
        }]
    );
}

#[tokio::test]
async fn reassign_partitions_all_empty_replicas_auto_place() {
    let stub = ReassignStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let gen = client
        .reassign_partitions_all("t", &[])
        .await
        .expect("reassign_partitions_all empty");
    assert_eq!(gen, 1);
    assert_eq!(
        stub.seen(),
        vec![SeenReassign {
            topic: "t".into(),
            partition: REASSIGN_ALL_PARTITIONS,
            replicas: vec![],
        }]
    );
}
