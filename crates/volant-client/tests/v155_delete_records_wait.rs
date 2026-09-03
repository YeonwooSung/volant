//! v0.155: Client::delete_records uses ClientConfig.delete_records_wait.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, ClientConfig};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response};

struct DeleteRecordsStub {
    addr: String,
    waits: Arc<Mutex<Vec<u8>>>,
    delete_n: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl DeleteRecordsStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let sock = listener.local_addr().expect("local_addr");
        let waits = Arc::new(Mutex::new(Vec::new()));
        let delete_n = Arc::new(AtomicU64::new(0));
        let waits_s = Arc::clone(&waits);
        let delete_n_s = Arc::clone(&delete_n);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let waits = Arc::clone(&waits_s);
                let delete_n = Arc::clone(&delete_n_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, waits, delete_n).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", sock.port()),
            waits,
            delete_n,
            server,
        }
    }

    fn waits(&self) -> Vec<u8> {
        self.waits.lock().expect("waits").clone()
    }

    fn delete_count(&self) -> u64 {
        self.delete_n.load(Ordering::Relaxed)
    }
}

impl Drop for DeleteRecordsStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    waits: Arc<Mutex<Vec<u8>>>,
    delete_n: Arc<AtomicU64>,
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
                        Request::DeleteRecords {
                            topic,
                            partition,
                            wait_majority,
                            ..
                        } => {
                            delete_n.fetch_add(1, Ordering::Relaxed);
                            waits.lock().expect("waits").push(wait_majority);
                            Response::DeleteRecords {
                                error_code: 0,
                                topic,
                                partition,
                                low_watermark: 96,
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

async fn connect(stub: &DeleteRecordsStub) -> Client {
    Client::connect_addr(&stub.addr).await.expect("connect")
}

async fn connect_cfg(stub: &DeleteRecordsStub, cfg: ClientConfig) -> Client {
    Client::connect(ClientConfig {
        brokers: vec![stub.addr.clone()],
        ..cfg
    })
    .await
    .expect("connect")
}

#[tokio::test]
async fn default_delete_records_sends_wait_majority_zero() {
    let stub = DeleteRecordsStub::boot().await;
    let client = connect(&stub).await;
    assert_eq!(ClientConfig::default().delete_records_wait, 0);
    let got = client
        .delete_records("events", 0, 10)
        .await
        .expect("delete_records");
    assert_eq!(got.low_watermark, 96);
    assert_eq!(stub.waits(), vec![0]);
    assert_eq!(stub.delete_count(), 1);
}

#[tokio::test]
async fn delete_records_uses_configured_wait() {
    let stub = DeleteRecordsStub::boot().await;
    let client = connect_cfg(
        &stub,
        ClientConfig {
            delete_records_wait: 1,
            ..ClientConfig::default()
        },
    )
    .await;
    let got = client
        .delete_records("events", 2, 100)
        .await
        .expect("delete_records");
    assert_eq!(got.low_watermark, 96);
    assert_eq!(stub.waits(), vec![1]);
    assert_eq!(stub.delete_count(), 1);
}

#[tokio::test]
async fn delete_records_with_wait_flag_ignores_config() {
    let stub = DeleteRecordsStub::boot().await;
    let client = connect_cfg(
        &stub,
        ClientConfig {
            delete_records_wait: 1,
            ..ClientConfig::default()
        },
    )
    .await;
    let got = client
        .delete_records_with_wait_flag("events", 2, 100, 2)
        .await
        .expect("delete_records_with_wait_flag");
    assert_eq!(got.low_watermark, 96);
    assert_eq!(stub.waits(), vec![2]);
    assert_eq!(stub.delete_count(), 1);
}
