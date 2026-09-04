//! v0.210: empty first JoinGroup generates a client member_id.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, Request, Response};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenJoin {
    member_id: String,
    group_instance_id: String,
}

struct JoinStub {
    addr: String,
    seen: Arc<Mutex<Vec<SeenJoin>>>,
    server: tokio::task::JoinHandle<()>,
}

impl JoinStub {
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

    fn seen(&self) -> Vec<SeenJoin> {
        self.seen.lock().expect("seen").clone()
    }
}

impl Drop for JoinStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    seen: Arc<Mutex<Vec<SeenJoin>>>,
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
                        Request::JoinGroup {
                            member_id,
                            group_instance_id,
                            ..
                        } => {
                            seen.lock().expect("seen").push(SeenJoin {
                                member_id: member_id.clone(),
                                group_instance_id,
                            });
                            Response::JoinGroup {
                                error_code: 0,
                                generation: 1,
                                member_id,
                                assignment: vec![],
                                revoked: vec![],
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
async fn empty_first_join_encodes_non_empty_member_id() {
    let stub = JoinStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let result = client
        .join_group("g", "", 10_000, vec!["t".into()])
        .await
        .expect("join");
    let seen = stub.seen();
    assert_eq!(seen.len(), 1);
    assert!(
        !seen[0].member_id.is_empty(),
        "empty first join must generate a member_id"
    );
    assert_eq!(seen[0].group_instance_id, "");
    uuid::Uuid::parse_str(&seen[0].member_id).expect("generated member_id is a UUID");
    assert_eq!(result.member_id, seen[0].member_id);
}

#[tokio::test]
async fn static_instance_sends_empty_member_id() {
    let stub = JoinStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let _ = client
        .join_group_with_instance("g", "", 10_000, vec!["t".into()], "inst-1")
        .await
        .expect("join");
    assert_eq!(
        stub.seen(),
        vec![SeenJoin {
            member_id: String::new(),
            group_instance_id: "inst-1".into(),
        }]
    );
}

#[tokio::test]
async fn explicit_member_id_unchanged() {
    let stub = JoinStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    let _ = client
        .join_group("g", "m-explicit", 10_000, vec!["t".into()])
        .await
        .expect("join");
    assert_eq!(
        stub.seen(),
        vec![SeenJoin {
            member_id: "m-explicit".into(),
            group_instance_id: String::new(),
        }]
    );
}
