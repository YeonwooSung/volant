//! v0.162: Rust Client ListAcls unfiltered named helper.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, AclBinding, Request, Response};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListAclsFilter {
    principal: String,
    resource_type: u8,
    resource: String,
}

fn sample_bindings() -> Vec<AclBinding> {
    vec![AclBinding {
        principal: "alice".into(),
        resource_type: 0,
        resource: "t".into(),
        operation: 0,
        permission: 1,
    }]
}

struct ListAclsStub {
    addr: String,
    seen_filters: Arc<Mutex<Vec<ListAclsFilter>>>,
    server: tokio::task::JoinHandle<()>,
}

impl ListAclsStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let seen_filters = Arc::new(Mutex::new(Vec::new()));
        let seen_s = Arc::clone(&seen_filters);
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
            seen_filters,
            server,
        }
    }

    fn seen_filters(&self) -> Vec<ListAclsFilter> {
        self.seen_filters.lock().expect("seen").clone()
    }
}

impl Drop for ListAclsStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<ListAclsFilter>>>,
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
                        Request::ListAcls {
                            principal,
                            resource_type,
                            resource,
                        } => {
                            seen.lock().expect("seen").push(ListAclsFilter {
                                principal,
                                resource_type,
                                resource,
                            });
                            Response::ListAcls {
                                error_code: 0,
                                entries: sample_bindings(),
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
async fn list_acls_all_encodes_empty_filters() {
    let stub = ListAclsStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let listed = client.list_acls_all().await.expect("list_acls_all");
    assert_eq!(listed, sample_bindings());
    assert_eq!(
        stub.seen_filters(),
        vec![ListAclsFilter {
            principal: String::new(),
            resource_type: 255,
            resource: String::new(),
        }]
    );
}
