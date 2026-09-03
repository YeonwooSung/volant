//! v0.170: Rust Client create_acl / delete_acl single-binding wrappers.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, AclBinding, Request, Response};

fn sample_entry() -> AclBinding {
    AclBinding {
        principal: "alice".into(),
        resource_type: 0,
        resource: "t".into(),
        operation: 0,
        permission: 1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SeenAcls {
    Create(Vec<AclBinding>),
    Delete(Vec<AclBinding>),
}

struct AclStub {
    addr: String,
    seen: Arc<Mutex<Vec<SeenAcls>>>,
    server: tokio::task::JoinHandle<()>,
}

impl AclStub {
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

    fn seen(&self) -> Vec<SeenAcls> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for AclStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<SeenAcls>>>,
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
                        Request::CreateAcls { entries } => {
                            seen.lock()
                                .expect("seen")
                                .push(SeenAcls::Create(entries));
                            Response::CreateAcls { error_code: 0 }
                        }
                        Request::DeleteAcls { entries } => {
                            let removed = entries.len() as u32;
                            seen.lock()
                                .expect("seen")
                                .push(SeenAcls::Delete(entries));
                            Response::DeleteAcls {
                                error_code: 0,
                                removed,
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
async fn create_acl_encodes_one_binding() {
    let stub = AclStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let entry = sample_entry();
    client
        .create_acl(entry.clone())
        .await
        .expect("create_acl");
    assert_eq!(stub.seen(), vec![SeenAcls::Create(vec![entry])]);
}

#[tokio::test]
async fn delete_acl_encodes_one_binding() {
    let stub = AclStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let entry = sample_entry();
    let removed = client
        .delete_acl(entry.clone())
        .await
        .expect("delete_acl");
    assert_eq!(removed, 1);
    assert_eq!(stub.seen(), vec![SeenAcls::Delete(vec![entry])]);
}

#[tokio::test]
async fn create_acls_batch_still_encodes_all_bindings() {
    let stub = AclStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let a = sample_entry();
    let mut b = sample_entry();
    b.principal = "bob".into();
    client
        .create_acls(vec![a.clone(), b.clone()])
        .await
        .expect("create_acls");
    assert_eq!(stub.seen(), vec![SeenAcls::Create(vec![a, b])]);
}
