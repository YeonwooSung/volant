//! v0.60: Rust GroupConsumer opt-in auto-commit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, Assignment, FetchRecord, OffsetCommitEntry, OffsetListing,
    Request, Response,
};

#[derive(Clone, Debug)]
struct CommitSnap {
    member_id: String,
    generation: u32,
    entries: Vec<OffsetCommitEntry>,
}

struct GroupStub {
    addr: String,
    commits: Arc<Mutex<Vec<CommitSnap>>>,
    leaves: Arc<AtomicU64>,
    records: Arc<Mutex<HashMap<(String, u32), Vec<FetchRecord>>>>,
    server: tokio::task::JoinHandle<()>,
}

impl GroupStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let commits = Arc::new(Mutex::new(Vec::new()));
        let leaves = Arc::new(AtomicU64::new(0));
        let records = Arc::new(Mutex::new(HashMap::new()));
        let commits_s = Arc::clone(&commits);
        let leaves_s = Arc::clone(&leaves);
        let records_s = Arc::clone(&records);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let commits = Arc::clone(&commits_s);
                let leaves = Arc::clone(&leaves_s);
                let records = Arc::clone(&records_s);
                tokio::spawn(async move {
                    let _ = serve_stub(stream, commits, leaves, records).await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            commits,
            leaves,
            records,
            server,
        }
    }

    fn push_record(&self, topic: &str, partition: u32, rec: FetchRecord) {
        self.records
            .lock()
            .expect("records")
            .entry((topic.to_string(), partition))
            .or_default()
            .push(rec);
    }

    fn commit_count(&self) -> usize {
        self.commits.lock().expect("commits").len()
    }

    fn last_commit(&self) -> CommitSnap {
        self.commits
            .lock()
            .expect("commits")
            .last()
            .cloned()
            .expect("commit")
    }

    fn leave_count(&self) -> u64 {
        self.leaves.load(Ordering::Relaxed)
    }
}

impl Drop for GroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn rec(offset: u64, value: &[u8]) -> FetchRecord {
    FetchRecord {
        offset,
        timestamp_ms: 0,
        key: None,
        value: Bytes::copy_from_slice(value),
        headers: vec![],
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    commits: Arc<Mutex<Vec<CommitSnap>>>,
    leaves: Arc<AtomicU64>,
    records: Arc<Mutex<HashMap<(String, u32), Vec<FetchRecord>>>>,
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
                        Request::JoinGroup { .. } => Response::JoinGroup {
                            error_code: 0,
                            generation: 1,
                            member_id: "m1".into(),
                            assignment: vec![Assignment {
                                topic: "t".into(),
                                partition: 0,
                            }],
                            revoked: vec![],
                        },
                        Request::OffsetFetch { .. } => Response::OffsetFetch {
                            error_code: 0,
                            entries: vec![],
                        },
                        Request::ListOffsets { topic, partitions } => Response::ListOffsets {
                            error_code: 0,
                            topic,
                            entries: partitions
                                .into_iter()
                                .map(|partition| OffsetListing {
                                    partition,
                                    earliest: 0,
                                    latest: 0,
                                })
                                .collect(),
                        },
                        Request::Heartbeat { .. } => Response::Heartbeat { error_code: 0 },
                        Request::LeaveGroup { .. } => {
                            leaves.fetch_add(1, Ordering::Relaxed);
                            Response::LeaveGroup { error_code: 0 }
                        }
                        Request::Fetch {
                            topic,
                            partition,
                            from_offset,
                            ..
                        } => {
                            let recs = records
                                .lock()
                                .expect("records")
                                .get(&(topic.clone(), partition))
                                .map(|v| {
                                    v.iter()
                                        .filter(|r| r.offset >= from_offset)
                                        .cloned()
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            Response::Fetch {
                                topic,
                                partition,
                                high_watermark: recs
                                    .last()
                                    .map(|r| r.offset.saturating_add(1))
                                    .unwrap_or(from_offset),
                                error_code: 0,
                                records: recs,
                            }
                        }
                        Request::OffsetCommit {
                            member_id,
                            generation,
                            entries,
                            ..
                        } => {
                            commits.lock().expect("commits").push(CommitSnap {
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
    let mut out = bytes::BytesMut::new();
    encode_frame(&packed, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&out).await
}

async fn connect(stub: &GroupStub) -> Arc<Client> {
    Arc::new(Client::connect_addr(&stub.addr).await.expect("connect"))
}

#[tokio::test]
async fn poll_does_not_autocommit_by_default() {
    let stub = GroupStub::boot().await;
    stub.push_record("t", 0, rec(0, b"a"));
    let client = connect(&stub).await;
    let mut g = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 10_000, false)
        .await
        .expect("join");
    let recs = g.poll().await.expect("poll");
    assert_eq!(recs.len(), 1);
    assert_eq!(stub.commit_count(), 0);
    g.leave().await.expect("leave");
    assert_eq!(stub.commit_count(), 0);
    assert_eq!(stub.leave_count(), 1);
}

#[tokio::test]
async fn auto_commit_interval_zero_commits_after_poll() {
    let stub = GroupStub::boot().await;
    stub.push_record("t", 0, rec(0, b"a"));
    let client = connect(&stub).await;
    let mut g = GroupConsumer::join_with_auto_commit(
        client,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        true,
        Duration::ZERO,
    )
    .await
    .expect("join");
    let recs = g.poll().await.expect("poll");
    assert_eq!(recs.len(), 1);
    assert_eq!(stub.commit_count(), 1);
    let commit = stub.last_commit();
    assert_eq!(commit.member_id, "m1");
    assert_eq!(commit.generation, 1);
    assert_eq!(commit.entries.len(), 1);
    assert_eq!(commit.entries[0].topic, "t");
    assert_eq!(commit.entries[0].partition, 0);
    assert_eq!(commit.entries[0].offset, 1);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn auto_commit_interval_first_poll_only() {
    let stub = GroupStub::boot().await;
    stub.push_record("t", 0, rec(0, b"a"));
    let client = connect(&stub).await;
    let mut g = GroupConsumer::join_with_auto_commit(
        client,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        true,
        Duration::from_secs(10),
    )
    .await
    .expect("join");
    assert_eq!(g.poll().await.expect("poll 1").len(), 1);
    assert_eq!(stub.commit_count(), 1);
    stub.push_record("t", 0, rec(1, b"b"));
    assert_eq!(g.poll().await.expect("poll 2").len(), 1);
    assert_eq!(stub.commit_count(), 1);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn auto_commit_leave_commits_pending_then_leaves() {
    let stub = GroupStub::boot().await;
    stub.push_record("t", 0, rec(0, b"a"));
    let client = connect(&stub).await;
    let mut g = GroupConsumer::join_with_auto_commit(
        client,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        true,
        Duration::from_secs(10),
    )
    .await
    .expect("join");
    g.poll().await.expect("poll 1");
    assert_eq!(stub.commit_count(), 1);
    stub.push_record("t", 0, rec(1, b"b"));
    g.poll().await.expect("poll 2");
    assert_eq!(stub.commit_count(), 1);
    g.leave().await.expect("leave");
    assert_eq!(stub.commit_count(), 2);
    let commit = stub.last_commit();
    assert_eq!(commit.member_id, "m1");
    assert_eq!(commit.generation, 1);
    assert_eq!(commit.entries[0].offset, 2);
    assert_eq!(stub.leave_count(), 1);
}

#[tokio::test]
async fn explicit_commit_resets_autocommit_clock() {
    let stub = GroupStub::boot().await;
    stub.push_record("t", 0, rec(0, b"a"));
    let client = connect(&stub).await;
    let mut g = GroupConsumer::join_with_auto_commit(
        client,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        true,
        Duration::from_secs(10),
    )
    .await
    .expect("join");
    g.poll().await.expect("poll 1");
    assert_eq!(stub.commit_count(), 1);
    g.commit().await.expect("explicit commit");
    assert_eq!(stub.commit_count(), 2);
    stub.push_record("t", 0, rec(1, b"b"));
    g.poll().await.expect("poll 2");
    assert_eq!(stub.commit_count(), 2);
    g.leave().await.expect("leave");
}
