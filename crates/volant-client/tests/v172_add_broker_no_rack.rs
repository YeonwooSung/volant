//! v0.172: Rust Client AddBroker no-rack named helper.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenAddBroker {
    id: u32,
    host: String,
    port: u16,
    rack: Option<String>,
}

struct AddBrokerStub {
    addr: String,
    seen: Arc<Mutex<Vec<SeenAddBroker>>>,
    server: tokio::task::JoinHandle<()>,
}

impl AddBrokerStub {
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

    fn seen(&self) -> Vec<SeenAddBroker> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for AddBrokerStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<SeenAddBroker>>>,
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
                        Request::AddBroker {
                            id,
                            host,
                            port,
                            rack,
                        } => {
                            seen.lock().expect("seen").push(SeenAddBroker {
                                id,
                                host,
                                port,
                                rack,
                            });
                            Response::AddBroker {
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
async fn add_broker_no_rack_encodes_none_flag_zero() {
    let stub = AddBrokerStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let gen = client
        .add_broker_no_rack(2, "10.0.0.2", 9092)
        .await
        .expect("add_broker_no_rack");
    assert_eq!(gen, 1);
    assert_eq!(
        stub.seen(),
        vec![SeenAddBroker {
            id: 2,
            host: "10.0.0.2".into(),
            port: 9092,
            rack: None,
        }]
    );
}
