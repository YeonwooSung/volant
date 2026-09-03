//! v0.153: Rust single-entry OffsetCommit wrappers.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::Client;
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{decode_request, pack_response, OffsetCommitEntry, Request, Response};

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitSnap {
    group_id: String,
    member_id: String,
    generation: u32,
    entries: Vec<OffsetCommitEntry>,
}

struct OffsetCommitStub {
    addr: String,
    commits: Arc<Mutex<Vec<CommitSnap>>>,
    server: tokio::task::JoinHandle<()>,
}

impl OffsetCommitStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let commits = Arc::new(Mutex::new(Vec::new()));
        let commits_s = Arc::clone(&commits);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let commits = Arc::clone(&commits_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, commits).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            commits,
            server,
        }
    }

    fn commits(&self) -> Vec<CommitSnap> {
        self.commits.lock().expect("commits").clone()
    }
}

impl Drop for OffsetCommitStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    commits: Arc<Mutex<Vec<CommitSnap>>>,
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
                        Request::OffsetCommit {
                            group_id,
                            member_id,
                            generation,
                            entries,
                        } => {
                            commits.lock().expect("commits").push(CommitSnap {
                                group_id,
                                member_id,
                                generation,
                                entries,
                            });
                            Response::OffsetCommit { error_code: 0 }
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

fn one_entry(metadata: &str) -> Vec<OffsetCommitEntry> {
    vec![OffsetCommitEntry {
        topic: "t".into(),
        partition: 0,
        offset: 5,
        metadata: metadata.into(),
    }]
}

#[tokio::test]
async fn commit_offset_is_admin_empty_metadata() {
    let stub = OffsetCommitStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    client
        .commit_offset("g", "t", 0, 5)
        .await
        .expect("commit_offset");
    assert_eq!(
        stub.commits(),
        vec![CommitSnap {
            group_id: "g".into(),
            member_id: String::new(),
            generation: 0,
            entries: one_entry(""),
        }]
    );
}

#[tokio::test]
async fn commit_offset_meta_sends_metadata_admin_path() {
    let stub = OffsetCommitStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    client
        .commit_offset_meta("g", "t", 0, 5, "consumer-1")
        .await
        .expect("commit_offset_meta");
    assert_eq!(
        stub.commits(),
        vec![CommitSnap {
            group_id: "g".into(),
            member_id: String::new(),
            generation: 0,
            entries: one_entry("consumer-1"),
        }]
    );
}

#[tokio::test]
async fn commit_offset_member_sends_member_and_generation() {
    let stub = OffsetCommitStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    client
        .commit_offset_member("g", "t", 0, 5, "m1", 3)
        .await
        .expect("commit_offset_member");
    assert_eq!(
        stub.commits(),
        vec![CommitSnap {
            group_id: "g".into(),
            member_id: "m1".into(),
            generation: 3,
            entries: one_entry(""),
        }]
    );
}

#[tokio::test]
async fn commit_offset_member_meta_sends_all_fields() {
    let stub = OffsetCommitStub::boot().await;
    let client = Client::connect_addr(&stub.addr).await.expect("connect");
    client
        .commit_offset_member_meta("g", "t", 0, 5, "m1", 3, "consumer-1")
        .await
        .expect("commit_offset_member_meta");
    assert_eq!(
        stub.commits(),
        vec![CommitSnap {
            group_id: "g".into(),
            member_id: "m1".into(),
            generation: 3,
            entries: one_entry("consumer-1"),
        }]
    );
}
