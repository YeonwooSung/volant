//! v0.151: Rust public InitProducerId wrapping ensure_producer_id.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_core::Message;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, RequestOpcode, Response};

struct InitStub {
    addr: String,
    inits: Arc<AtomicU64>,
    produces: Arc<AtomicU64>,
    opcodes: Arc<Mutex<Vec<u16>>>,
    server: tokio::task::JoinHandle<()>,
}

impl InitStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let inits = Arc::new(AtomicU64::new(0));
        let produces = Arc::new(AtomicU64::new(0));
        let opcodes = Arc::new(Mutex::new(Vec::new()));
        let inits_s = Arc::clone(&inits);
        let produces_s = Arc::clone(&produces);
        let opcodes_s = Arc::clone(&opcodes);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let inits = Arc::clone(&inits_s);
                let produces = Arc::clone(&produces_s);
                let opcodes = Arc::clone(&opcodes_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, inits, produces, opcodes).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            inits,
            produces,
            opcodes,
            server,
        }
    }

    fn init_rpcs(&self) -> u64 {
        self.inits.load(Ordering::Relaxed)
    }

    fn produce_rpcs(&self) -> u64 {
        self.produces.load(Ordering::Relaxed)
    }

    fn opcodes(&self) -> Vec<u16> {
        self.opcodes.lock().expect("opcodes").clone()
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
    opcodes: Arc<Mutex<Vec<u16>>>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            match decode_frame(&mut buf) {
                Ok(Some(frame)) => {
                    let corr = frame.header.correlation_id;
                    opcodes.lock().expect("opcodes").push(frame.header.opcode);
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
                            Response::InitProducerId {
                                producer_id: 42,
                                epoch: 1,
                                error_code: 0,
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

async fn connect(addr: &str, enable_idempotence: bool) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![addr.to_owned()],
        enable_idempotence,
        ..ClientConfig::default()
    })
    .await
    .expect("connect")
}

fn one_message(value: &'static [u8]) -> Vec<Message> {
    vec![Message::from_value(Bytes::from_static(value))]
}

#[tokio::test]
async fn first_init_producer_id_sends_opcode_and_returns_pid() {
    let stub = InitStub::boot().await;
    let client = connect(&stub.addr, false).await;
    let (pid, epoch) = client.init_producer_id().await.expect("init");
    assert_eq!((pid, epoch), (42, 1));
    assert_eq!(stub.init_rpcs(), 1);
    assert_eq!(stub.opcodes(), vec![RequestOpcode::InitProducerId as u16]);
}

#[tokio::test]
async fn second_init_producer_id_is_noop() {
    let stub = InitStub::boot().await;
    let client = connect(&stub.addr, false).await;
    let first = client.init_producer_id().await.expect("first");
    let second = client.init_producer_id().await.expect("second");
    assert_eq!(first, (42, 1));
    assert_eq!(second, (42, 1));
    assert_eq!(stub.init_rpcs(), 1);
    assert_eq!(stub.opcodes(), vec![RequestOpcode::InitProducerId as u16]);
}

#[tokio::test]
async fn idempotent_produce_still_inits_once() {
    let stub = InitStub::boot().await;
    let client = connect(&stub.addr, true).await;
    client
        .produce("t", Some(0), one_message(b"a"))
        .await
        .expect("first produce");
    client
        .produce("t", Some(0), one_message(b"b"))
        .await
        .expect("second produce");
    assert_eq!(stub.init_rpcs(), 1);
    assert_eq!(stub.produce_rpcs(), 2);
    assert_eq!(
        stub.opcodes(),
        vec![
            RequestOpcode::InitProducerId as u16,
            RequestOpcode::Produce as u16,
            RequestOpcode::Produce as u16,
        ]
    );
}
