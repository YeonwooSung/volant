//! v0.208: GroupConsumer SyncGroup peek after JoinGroup (including rejoin).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, Assignment, ErrorCode, GroupMemberInfo, OffsetListing,
    PartitionInfo, Request, Response, TopicInfo,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenSync {
    group_id: String,
    member_id: String,
    generation: u32,
    assignment_bytes_len: usize,
}

struct GroupStub {
    addr: String,
    join_assignment: Arc<Mutex<Vec<Assignment>>>,
    sync_assignment: Arc<Mutex<Vec<Assignment>>>,
    sync_error: Arc<Mutex<Option<u16>>>,
    member_id: Arc<Mutex<String>>,
    generation: Arc<Mutex<u32>>,
    describe_members: Arc<Mutex<Vec<GroupMemberInfo>>>,
    heartbeat_rebalance: Arc<Mutex<bool>>,
    joins: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
    describes: Arc<AtomicU64>,
    heartbeats: Arc<AtomicU64>,
    seen_syncs: Arc<Mutex<Vec<SeenSync>>>,
    server: tokio::task::JoinHandle<()>,
}

impl GroupStub {
    async fn boot() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let addr = listener.local_addr().expect("local_addr");
        let join_assignment = Arc::new(Mutex::new(vec![Assignment {
            topic: "t".into(),
            partition: 0,
        }]));
        let sync_assignment = Arc::new(Mutex::new(Vec::new()));
        let sync_error = Arc::new(Mutex::new(None));
        let member_id = Arc::new(Mutex::new("m1".into()));
        let generation = Arc::new(Mutex::new(1u32));
        let topics = Arc::new(Mutex::new(vec![topic_info("t", 4)]));
        let describe_members = Arc::new(Mutex::new(Vec::new()));
        let heartbeat_rebalance = Arc::new(Mutex::new(false));
        let joins = Arc::new(AtomicU64::new(0));
        let syncs = Arc::new(AtomicU64::new(0));
        let describes = Arc::new(AtomicU64::new(0));
        let heartbeats = Arc::new(AtomicU64::new(0));
        let seen_syncs = Arc::new(Mutex::new(Vec::new()));
        let join_assignment_s = Arc::clone(&join_assignment);
        let sync_assignment_s = Arc::clone(&sync_assignment);
        let sync_error_s = Arc::clone(&sync_error);
        let member_id_s = Arc::clone(&member_id);
        let generation_s = Arc::clone(&generation);
        let topics_s = Arc::clone(&topics);
        let describe_members_s = Arc::clone(&describe_members);
        let heartbeat_rebalance_s = Arc::clone(&heartbeat_rebalance);
        let joins_s = Arc::clone(&joins);
        let syncs_s = Arc::clone(&syncs);
        let describes_s = Arc::clone(&describes);
        let heartbeats_s = Arc::clone(&heartbeats);
        let seen_syncs_s = Arc::clone(&seen_syncs);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let join_assignment = Arc::clone(&join_assignment_s);
                let sync_assignment = Arc::clone(&sync_assignment_s);
                let sync_error = Arc::clone(&sync_error_s);
                let member_id = Arc::clone(&member_id_s);
                let generation = Arc::clone(&generation_s);
                let topics = Arc::clone(&topics_s);
                let describe_members = Arc::clone(&describe_members_s);
                let heartbeat_rebalance = Arc::clone(&heartbeat_rebalance_s);
                let joins = Arc::clone(&joins_s);
                let syncs = Arc::clone(&syncs_s);
                let describes = Arc::clone(&describes_s);
                let heartbeats = Arc::clone(&heartbeats_s);
                let seen_syncs = Arc::clone(&seen_syncs_s);
                tokio::spawn(async move {
                    let _ = serve_stub(
                        stream,
                        join_assignment,
                        sync_assignment,
                        sync_error,
                        member_id,
                        generation,
                        topics,
                        describe_members,
                        heartbeat_rebalance,
                        joins,
                        syncs,
                        describes,
                        heartbeats,
                        seen_syncs,
                    )
                    .await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            join_assignment,
            sync_assignment,
            sync_error,
            member_id,
            generation,
            describe_members,
            heartbeat_rebalance,
            joins,
            syncs,
            describes,
            heartbeats,
            seen_syncs,
            server,
        }
    }

    fn set_join_assignment(&self, assignment: Vec<Assignment>) {
        *self.join_assignment.lock().expect("join_assignment") = assignment;
    }

    fn set_sync_assignment(&self, assignment: Vec<Assignment>) {
        *self.sync_assignment.lock().expect("sync_assignment") = assignment;
    }

    fn set_sync_error(&self, code: u16) {
        *self.sync_error.lock().expect("sync_error") = Some(code);
    }

    fn set_member_id(&self, member_id: &str) {
        *self.member_id.lock().expect("member_id") = member_id.to_string();
    }

    fn set_generation(&self, generation: u32) {
        *self.generation.lock().expect("generation") = generation;
    }

    fn set_describe_members(&self, members: Vec<GroupMemberInfo>) {
        *self.describe_members.lock().expect("describe_members") = members;
    }

    fn set_heartbeat_rebalance(&self, rebalance: bool) {
        *self
            .heartbeat_rebalance
            .lock()
            .expect("heartbeat_rebalance") = rebalance;
    }

    fn join_count(&self) -> u64 {
        self.joins.load(Ordering::Relaxed)
    }

    fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    fn describe_count(&self) -> u64 {
        self.describes.load(Ordering::Relaxed)
    }

    fn heartbeat_rpcs(&self) -> u64 {
        self.heartbeats.load(Ordering::Relaxed)
    }

    fn seen_syncs(&self) -> Vec<SeenSync> {
        self.seen_syncs.lock().expect("seen_syncs").clone()
    }
}

impl Drop for GroupStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn topic_info(name: &str, n: u32) -> TopicInfo {
    TopicInfo {
        name: name.into(),
        topic_id: 1,
        error_code: 0,
        partitions: (0..n)
            .map(|i| PartitionInfo {
                partition_id: i,
                leader: 0,
                hwm: 0,
                replicas: vec![],
                isr: vec![],
                leader_epoch: 0,
            })
            .collect(),
    }
}

fn member(id: &str, topics: &[&str]) -> GroupMemberInfo {
    GroupMemberInfo {
        member_id: id.into(),
        topics: topics.iter().map(|t| (*t).to_string()).collect(),
        assignment: vec![],
    }
}

fn asgn(topic: &str, partition: u32) -> Assignment {
    Assignment {
        topic: topic.into(),
        partition,
    }
}

async fn serve_stub(
    mut stream: TcpStream,
    join_assignment: Arc<Mutex<Vec<Assignment>>>,
    sync_assignment: Arc<Mutex<Vec<Assignment>>>,
    sync_error: Arc<Mutex<Option<u16>>>,
    member_id: Arc<Mutex<String>>,
    generation: Arc<Mutex<u32>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
    describe_members: Arc<Mutex<Vec<GroupMemberInfo>>>,
    heartbeat_rebalance: Arc<Mutex<bool>>,
    joins: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
    describes: Arc<AtomicU64>,
    heartbeats: Arc<AtomicU64>,
    seen_syncs: Arc<Mutex<Vec<SeenSync>>>,
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
                                generation: *generation.lock().expect("generation"),
                                member_id: member_id.lock().expect("member_id").clone(),
                                assignment: join_assignment
                                    .lock()
                                    .expect("join_assignment")
                                    .clone(),
                                revoked: vec![],
                                members: vec![],
                            }
                        }
                        Request::SyncGroup {
                            group_id,
                            member_id,
                            generation,
                            assignment_bytes,
                        } => {
                            syncs.fetch_add(1, Ordering::Relaxed);
                            seen_syncs.lock().expect("seen_syncs").push(SeenSync {
                                group_id,
                                member_id,
                                generation,
                                assignment_bytes_len: assignment_bytes.len(),
                            });
                            if let Some(code) = *sync_error.lock().expect("sync_error") {
                                Response::SyncGroup {
                                    error_code: code,
                                    assignment: vec![],
                                }
                            } else {
                                Response::SyncGroup {
                                    error_code: 0,
                                    assignment: sync_assignment
                                        .lock()
                                        .expect("sync_assignment")
                                        .clone(),
                                }
                            }
                        }
                        Request::DescribeGroup { group_id } => {
                            describes.fetch_add(1, Ordering::Relaxed);
                            Response::DescribeGroup {
                                error_code: 0,
                                group_id,
                                generation: *generation.lock().expect("generation"),
                                members: describe_members.lock().expect("describe_members").clone(),
                            }
                        }
                        Request::Metadata { .. } => Response::Metadata {
                            brokers: vec![],
                            topics: topics.lock().expect("topics").clone(),
                            controller_id: 0,
                        },
                        Request::OffsetFetch { .. } => Response::OffsetFetch {
                            error_code: 0,
                            entries: vec![],
                        },
                        Request::ListOffsets {
                            topic, partitions, ..
                        } => Response::ListOffsets {
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
                        Request::Heartbeat { .. } => {
                            heartbeats.fetch_add(1, Ordering::Relaxed);
                            let code = if *heartbeat_rebalance.lock().expect("heartbeat_rebalance")
                            {
                                *heartbeat_rebalance.lock().expect("heartbeat_rebalance") = false;
                                ErrorCode::RebalanceInProgress as u16
                            } else {
                                0
                            };
                            Response::Heartbeat { error_code: code }
                        }
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

async fn join_broker(stub: &GroupStub) -> volant_core::Result<GroupConsumer> {
    GroupConsumer::join_with_heartbeat(connect(stub).await, "g", vec!["t".into()], 10_000, false)
        .await
}

async fn join_range(stub: &GroupStub) -> volant_core::Result<GroupConsumer> {
    GroupConsumer::join_with_assignor(
        connect(stub).await,
        "g",
        vec!["t".into()],
        10_000,
        "",
        false,
        false,
        Duration::ZERO,
        "earliest",
        "range",
    )
    .await
}

#[tokio::test]
async fn join_issues_sync_group_and_uses_nonempty() {
    let stub = GroupStub::boot().await;
    stub.set_member_id("m-sync");
    stub.set_generation(3);
    stub.set_join_assignment(vec![asgn("t", 0)]);
    stub.set_sync_assignment(vec![asgn("t", 2), asgn("t", 3)]);
    let g = join_broker(&stub).await.expect("join");
    assert_eq!(g.member_id(), "m-sync");
    assert_eq!(g.generation(), 3);
    assert_eq!(g.assignment(), vec![("t".into(), 2), ("t".into(), 3)]);
    assert_eq!(stub.join_count(), 1);
    assert_eq!(stub.sync_count(), 1);
    assert_eq!(g.heartbeat_count(), 0);
    assert_eq!(
        stub.seen_syncs(),
        vec![SeenSync {
            group_id: "g".into(),
            member_id: "m-sync".into(),
            generation: 3,
            assignment_bytes_len: 0,
        }]
    );
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn sync_group_empty_keeps_join_assignment() {
    let stub = GroupStub::boot().await;
    stub.set_join_assignment(vec![asgn("t", 0)]);
    stub.set_sync_assignment(vec![]);
    let g = join_broker(&stub).await.expect("join");
    assert_eq!(g.assignment(), vec![("t".into(), 0)]);
    assert_eq!(stub.sync_count(), 1);
    assert_eq!(g.heartbeat_count(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn sync_group_error_keeps_join_assignment() {
    let stub = GroupStub::boot().await;
    stub.set_join_assignment(vec![asgn("t", 1)]);
    stub.set_sync_error(ErrorCode::UnknownMemberId as u16);
    let g = join_broker(&stub).await.expect("join");
    assert_eq!(g.assignment(), vec![("t".into(), 1)]);
    assert_eq!(stub.sync_count(), 1);
    assert_eq!(g.heartbeat_count(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn range_override_still_runs_after_sync() {
    let stub = GroupStub::boot().await;
    stub.set_member_id("m-a");
    stub.set_join_assignment(vec![asgn("t", 0)]);
    stub.set_sync_assignment(vec![asgn("t", 9)]);
    stub.set_describe_members(vec![member("m-a", &["t"]), member("m-b", &["t"])]);
    let g = join_range(&stub).await.expect("join");
    assert_eq!(g.assignor(), "range");
    assert_eq!(g.assignment(), vec![("t".into(), 0), ("t".into(), 1)]);
    assert_eq!(stub.sync_count(), 1);
    assert_eq!(stub.describe_count(), 1);
    assert_eq!(g.heartbeat_count(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn rejoin_issues_sync_group() {
    let stub = GroupStub::boot().await;
    stub.set_join_assignment(vec![asgn("t", 0)]);
    stub.set_sync_assignment(vec![asgn("t", 0)]);
    let mut g = join_broker(&stub).await.expect("join");
    assert_eq!(stub.sync_count(), 1);
    stub.set_heartbeat_rebalance(true);
    stub.set_sync_assignment(vec![asgn("t", 1)]);
    stub.set_generation(2);
    g.poll().await.expect("poll rejoin");
    assert_eq!(stub.join_count(), 2);
    assert_eq!(stub.sync_count(), 2);
    assert_eq!(g.assignment(), vec![("t".into(), 1)]);
    assert_eq!(g.generation(), 2);
    assert_eq!(g.heartbeat_count(), 1);
    assert_eq!(stub.heartbeat_rpcs(), 1);
    g.leave().await.expect("leave");
}
