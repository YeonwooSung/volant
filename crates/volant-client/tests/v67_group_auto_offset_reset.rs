//! v0.67: Rust GroupConsumer auto_offset_reset.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, Assignment, OffsetFetchEntry, OffsetListing, Request, Response,
};

const OFFSET_UNKNOWN: u64 = u64::MAX;

struct GroupStub {
    addr: String,
    assignment: Arc<Mutex<Vec<Assignment>>>,
    offset_fetch: Arc<Mutex<Vec<OffsetFetchEntry>>>,
    list_offset_entries: Arc<Mutex<HashMap<(String, u32), (u64, u64)>>>,
    list_offsets_calls: Arc<Mutex<Vec<(String, Vec<u32>)>>>,
    offset_fetch_calls: Arc<AtomicU64>,
    joins: Arc<AtomicU64>,
    server: tokio::task::JoinHandle<()>,
}

impl GroupStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let assignment = Arc::new(Mutex::new(vec![Assignment {
            topic: "t".into(),
            partition: 0,
        }]));
        let offset_fetch = Arc::new(Mutex::new(Vec::new()));
        let list_offset_entries = Arc::new(Mutex::new(HashMap::new()));
        let list_offsets_calls = Arc::new(Mutex::new(Vec::new()));
        let offset_fetch_calls = Arc::new(AtomicU64::new(0));
        let joins = Arc::new(AtomicU64::new(0));
        let assignment_s = Arc::clone(&assignment);
        let offset_fetch_s = Arc::clone(&offset_fetch);
        let list_offset_entries_s = Arc::clone(&list_offset_entries);
        let list_offsets_calls_s = Arc::clone(&list_offsets_calls);
        let offset_fetch_calls_s = Arc::clone(&offset_fetch_calls);
        let joins_s = Arc::clone(&joins);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let assignment = Arc::clone(&assignment_s);
                let offset_fetch = Arc::clone(&offset_fetch_s);
                let list_offset_entries = Arc::clone(&list_offset_entries_s);
                let list_offsets_calls = Arc::clone(&list_offsets_calls_s);
                let offset_fetch_calls = Arc::clone(&offset_fetch_calls_s);
                let joins = Arc::clone(&joins_s);
                tokio::spawn(async move {
                    let _ = serve_stub(
                        stream,
                        assignment,
                        offset_fetch,
                        list_offset_entries,
                        list_offsets_calls,
                        offset_fetch_calls,
                        joins,
                    )
                    .await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            assignment,
            offset_fetch,
            list_offset_entries,
            list_offsets_calls,
            offset_fetch_calls,
            joins,
            server,
        }
    }

    fn set_assignment(&self, assignment: Vec<Assignment>) {
        *self.assignment.lock().expect("assignment") = assignment;
    }

    fn set_offset_fetch(&self, entries: Vec<OffsetFetchEntry>) {
        *self.offset_fetch.lock().expect("offset_fetch") = entries;
    }

    fn set_committed(&self, topic: &str, partition: u32, offset: u64) {
        self.set_offset_fetch(vec![OffsetFetchEntry {
            topic: topic.into(),
            partition,
            offset,
            metadata: String::new(),
        }]);
    }

    fn set_list_offset(&self, topic: &str, partition: u32, earliest: u64, latest: u64) {
        self.list_offset_entries
            .lock()
            .expect("list_offset_entries")
            .insert((topic.to_string(), partition), (earliest, latest));
    }

    fn list_offsets_calls(&self) -> Vec<(String, Vec<u32>)> {
        self.list_offsets_calls.lock().expect("calls").clone()
    }

    fn offset_fetch_count(&self) -> u64 {
        self.offset_fetch_calls.load(Ordering::Relaxed)
    }

    fn join_count(&self) -> u64 {
        self.joins.load(Ordering::Relaxed)
    }
}

impl Drop for GroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    assignment: Arc<Mutex<Vec<Assignment>>>,
    offset_fetch: Arc<Mutex<Vec<OffsetFetchEntry>>>,
    list_offset_entries: Arc<Mutex<HashMap<(String, u32), (u64, u64)>>>,
    list_offsets_calls: Arc<Mutex<Vec<(String, Vec<u32>)>>>,
    offset_fetch_calls: Arc<AtomicU64>,
    joins: Arc<AtomicU64>,
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
                        Request::JoinGroup { .. } => {
                            joins.fetch_add(1, Ordering::Relaxed);
                            Response::JoinGroup {
                                error_code: 0,
                                generation: 1,
                                member_id: "m1".into(),
                                assignment: assignment.lock().expect("assignment").clone(),
                                revoked: vec![],
                            }
                        }
                        Request::OffsetFetch { .. } => {
                            offset_fetch_calls.fetch_add(1, Ordering::Relaxed);
                            Response::OffsetFetch {
                                error_code: 0,
                                entries: offset_fetch.lock().expect("offset_fetch").clone(),
                            }
                        }
                        Request::ListOffsets { topic, partitions } => {
                            list_offsets_calls
                                .lock()
                                .expect("calls")
                                .push((topic.clone(), partitions.clone()));
                            let map = list_offset_entries.lock().expect("list_offset_entries");
                            let entries = partitions
                                .iter()
                                .filter_map(|p| {
                                    map.get(&(topic.clone(), *p)).map(|(earliest, latest)| {
                                        OffsetListing {
                                            partition: *p,
                                            earliest: *earliest,
                                            latest: *latest,
                                        }
                                    })
                                })
                                .collect();
                            Response::ListOffsets {
                                error_code: 0,
                                topic,
                                entries,
                            }
                        }
                        Request::Heartbeat { .. } => Response::Heartbeat { error_code: 0 },
                        Request::LeaveGroup { .. } => Response::LeaveGroup { error_code: 0 },
                        Request::Fetch {
                            topic, partition, ..
                        } => Response::Fetch {
                            topic,
                            partition,
                            high_watermark: 0,
                            error_code: 0,
                            records: vec![],
                        },
                        Request::OffsetCommit { .. } => Response::OffsetCommit { error_code: 0 },
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

async fn join_reset(stub: &GroupStub, reset: &str) -> volant_core::Result<GroupConsumer> {
    GroupConsumer::join_with_auto_offset_reset(
        connect(stub).await,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        false,
        Duration::ZERO,
        reset,
    )
    .await
}

fn pos(g: &GroupConsumer, topic: &str, partition: u32) -> Option<u64> {
    g.positions().get(&(topic.to_string(), partition)).copied()
}

#[tokio::test]
async fn default_join_offset_fetch_miss_is_zero_without_list_offsets() {
    let stub = GroupStub::boot().await;
    let client = connect(&stub).await;
    let g = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 10_000, false)
        .await
        .expect("join");
    assert_eq!(g.auto_offset_reset(), "earliest");
    assert_eq!(pos(&g, "t", 0), Some(0));
    assert!(stub.list_offsets_calls().is_empty());
    assert_eq!(stub.offset_fetch_count(), 1);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn latest_unknown_uses_list_offsets_leo() {
    let stub = GroupStub::boot().await;
    stub.set_list_offset("t", 0, 0, 5);
    let g = join_reset(&stub, "latest").await.expect("join");
    assert_eq!(g.auto_offset_reset(), "latest");
    assert_eq!(pos(&g, "t", 0), Some(5));
    assert_eq!(stub.list_offsets_calls(), vec![("t".into(), vec![0])]);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn latest_offset_unknown_sentinel_uses_list_offsets() {
    let stub = GroupStub::boot().await;
    stub.set_committed("t", 0, OFFSET_UNKNOWN);
    stub.set_list_offset("t", 0, 0, 5);
    let g = join_reset(&stub, "latest").await.expect("join");
    assert_eq!(pos(&g, "t", 0), Some(5));
    assert_eq!(stub.list_offsets_calls(), vec![("t".into(), vec![0])]);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn none_unknown_errors_without_list_offsets() {
    let stub = GroupStub::boot().await;
    let err = join_reset(&stub, "none").await.expect_err("none");
    assert!(err.to_string().contains("auto_offset_reset"), "err={err}");
    assert!(stub.list_offsets_calls().is_empty());
    assert_eq!(stub.join_count(), 1);
}

#[tokio::test]
async fn invalid_reset_string_fails_before_join_group() {
    let stub = GroupStub::boot().await;
    let err = join_reset(&stub, "banana").await.expect_err("banana");
    assert!(
        err.to_string().contains("unknown auto_offset_reset"),
        "err={err}"
    );
    assert_eq!(stub.join_count(), 0);
    assert_eq!(stub.offset_fetch_count(), 0);
    assert!(stub.list_offsets_calls().is_empty());
}

#[tokio::test]
async fn committed_offset_used_regardless_of_reset_policy() {
    for policy in ["earliest", "latest", "none"] {
        let stub = GroupStub::boot().await;
        stub.set_committed("t", 0, 3);
        stub.set_list_offset("t", 0, 0, 9);
        let g = join_reset(&stub, policy)
            .await
            .unwrap_or_else(|e| panic!("join {policy}: {e}"));
        assert_eq!(pos(&g, "t", 0), Some(3), "policy={policy}");
        assert!(
            stub.list_offsets_calls().is_empty(),
            "policy={policy} issued ListOffsets"
        );
        g.leave().await.expect("leave");
    }
}

#[tokio::test]
async fn latest_list_offsets_missing_partition_errors() {
    let stub = GroupStub::boot().await;
    let err = join_reset(&stub, "latest")
        .await
        .expect_err("missing partition");
    assert!(
        err.to_string().contains("list_offsets missing partition"),
        "err={err}"
    );
    assert_eq!(stub.list_offsets_calls(), vec![("t".into(), vec![0])]);
}

#[tokio::test]
async fn latest_empty_assignment_skips_list_offsets() {
    let stub = GroupStub::boot().await;
    stub.set_assignment(vec![]);
    let g = join_reset(&stub, "latest").await.expect("join");
    assert!(g.positions().is_empty());
    assert!(g.assignment().is_empty());
    assert!(stub.list_offsets_calls().is_empty());
    assert_eq!(stub.offset_fetch_count(), 0);
    g.leave().await.expect("leave");
}
