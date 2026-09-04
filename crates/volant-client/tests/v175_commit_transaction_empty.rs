//! v0.175: Rust Client commit_transaction_empty named helper.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response, TxnOffsetCommit};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenEndTxn {
    committed: bool,
    offsets: Vec<TxnOffsetCommit>,
}

struct EndTxnStub {
    addr: String,
    seen: Arc<Mutex<Vec<SeenEndTxn>>>,
    server: tokio::task::JoinHandle<()>,
}

impl EndTxnStub {
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

    fn seen(&self) -> Vec<SeenEndTxn> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for EndTxnStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<SeenEndTxn>>>,
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
                        Request::InitProducerId { .. } => Response::InitProducerId {
                            producer_id: 1,
                            epoch: 1,
                            error_code: 0,
                        },
                        Request::EndTxn {
                            committed,
                            offsets,
                            ..
                        } => {
                            seen.lock().expect("seen").push(SeenEndTxn {
                                committed,
                                offsets,
                            });
                            Response::EndTxn {
                                error_code: 0,
                                results: vec![],
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
async fn commit_transaction_empty_sends_committed_end_txn_with_no_offsets() {
    let stub = EndTxnStub::boot().await;
    let client = Client::connect(ClientConfig {
        brokers: vec![stub.addr.clone()],
        transactional_id: Some("txn-1".into()),
        enable_idempotence: true,
        ..ClientConfig::default()
    })
    .await
    .expect("connect");
    client.init_producer_id().await.expect("init");
    let results = client
        .commit_transaction_empty()
        .await
        .expect("commit_transaction_empty");
    assert!(results.is_empty());
    assert_eq!(
        stub.seen(),
        vec![SeenEndTxn {
            committed: true,
            offsets: Vec::new(),
        }]
    );
}
