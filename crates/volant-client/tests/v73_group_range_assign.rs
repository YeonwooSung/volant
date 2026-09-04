//! v0.73: Rust GroupConsumer range assignor via DescribeGroup.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_client::{Client, GroupConsumer};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, Assignment, GroupMemberInfo, OffsetListing, PartitionInfo,
    Request, Response, TopicInfo,
};

struct GroupStub {
    addr: String,
    assignment: Arc<Mutex<Vec<Assignment>>>,
    member_id: Arc<Mutex<String>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
    describe_members: Arc<Mutex<Vec<GroupMemberInfo>>>,
    describe_error: Arc<Mutex<bool>>,
    join_members: Arc<Mutex<Vec<String>>>,
    joins: Arc<AtomicU64>,
    describes: Arc<AtomicU64>,
    metadatas: Arc<AtomicU64>,
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
        let member_id = Arc::new(Mutex::new("m1".into()));
        let topics = Arc::new(Mutex::new(vec![topic_info("t", 4)]));
        let describe_members = Arc::new(Mutex::new(Vec::new()));
        let describe_error = Arc::new(Mutex::new(false));
        let join_members = Arc::new(Mutex::new(Vec::new()));
        let joins = Arc::new(AtomicU64::new(0));
        let describes = Arc::new(AtomicU64::new(0));
        let metadatas = Arc::new(AtomicU64::new(0));
        let assignment_s = Arc::clone(&assignment);
        let member_id_s = Arc::clone(&member_id);
        let topics_s = Arc::clone(&topics);
        let describe_members_s = Arc::clone(&describe_members);
        let describe_error_s = Arc::clone(&describe_error);
        let join_members_s = Arc::clone(&join_members);
        let joins_s = Arc::clone(&joins);
        let describes_s = Arc::clone(&describes);
        let metadatas_s = Arc::clone(&metadatas);
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let assignment = Arc::clone(&assignment_s);
                let member_id = Arc::clone(&member_id_s);
                let topics = Arc::clone(&topics_s);
                let describe_members = Arc::clone(&describe_members_s);
                let describe_error = Arc::clone(&describe_error_s);
                let join_members = Arc::clone(&join_members_s);
                let joins = Arc::clone(&joins_s);
                let describes = Arc::clone(&describes_s);
                let metadatas = Arc::clone(&metadatas_s);
                tokio::spawn(async move {
                    let _ = serve_stub(
                        stream,
                        assignment,
                        member_id,
                        topics,
                        describe_members,
                        describe_error,
                        join_members,
                        joins,
                        describes,
                        metadatas,
                    )
                    .await;
                });
            }
        });
        tokio::task::yield_now().await;
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            assignment,
            member_id,
            topics,
            describe_members,
            describe_error,
            join_members,
            joins,
            describes,
            metadatas,
            server,
        }
    }

    fn set_assignment(&self, assignment: Vec<Assignment>) {
        *self.assignment.lock().expect("assignment") = assignment;
    }

    fn set_member_id(&self, member_id: &str) {
        *self.member_id.lock().expect("member_id") = member_id.to_string();
    }

    fn set_partitions(&self, topic: &str, n: u32) {
        *self.topics.lock().expect("topics") = vec![topic_info(topic, n)];
    }

    fn set_describe_members(&self, members: Vec<GroupMemberInfo>) {
        *self.describe_members.lock().expect("describe_members") = members;
    }

    fn set_describe_error(&self, error: bool) {
        *self.describe_error.lock().expect("describe_error") = error;
    }

    fn set_join_members(&self, members: Vec<String>) {
        *self.join_members.lock().expect("join_members") = members;
    }

    fn join_count(&self) -> u64 {
        self.joins.load(Ordering::Relaxed)
    }

    fn describe_count(&self) -> u64 {
        self.describes.load(Ordering::Relaxed)
    }

    fn metadata_count(&self) -> u64 {
        self.metadatas.load(Ordering::Relaxed)
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

async fn serve_stub(
    mut stream: TcpStream,
    assignment: Arc<Mutex<Vec<Assignment>>>,
    member_id: Arc<Mutex<String>>,
    topics: Arc<Mutex<Vec<TopicInfo>>>,
    describe_members: Arc<Mutex<Vec<GroupMemberInfo>>>,
    describe_error: Arc<Mutex<bool>>,
    join_members: Arc<Mutex<Vec<String>>>,
    joins: Arc<AtomicU64>,
    describes: Arc<AtomicU64>,
    metadatas: Arc<AtomicU64>,
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
                                member_id: member_id.lock().expect("member_id").clone(),
                                assignment: assignment.lock().expect("assignment").clone(),
                                revoked: vec![],
                                members: join_members.lock().expect("join_members").clone(),
                            }
                        }
                        Request::DescribeGroup { group_id } => {
                            describes.fetch_add(1, Ordering::Relaxed);
                            if *describe_error.lock().expect("describe_error") {
                                Response::DescribeGroup {
                                    error_code: 2,
                                    group_id,
                                    generation: 0,
                                    members: vec![],
                                }
                            } else {
                                Response::DescribeGroup {
                                    error_code: 0,
                                    group_id,
                                    generation: 1,
                                    members: describe_members
                                        .lock()
                                        .expect("describe_members")
                                        .clone(),
                                }
                            }
                        }
                        Request::Metadata { .. } => {
                            metadatas.fetch_add(1, Ordering::Relaxed);
                            Response::Metadata {
                                brokers: vec![],
                                topics: topics.lock().expect("topics").clone(),
                                controller_id: 0,
                            }
                        }
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

async fn join_assignor(stub: &GroupStub, assignor: &str) -> volant_core::Result<GroupConsumer> {
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
        assignor,
    )
    .await
}

#[tokio::test]
async fn range_describe_two_members_splits_half() {
    for (id, want) in [
        ("m-a", vec![("t".into(), 0), ("t".into(), 1)]),
        ("m-b", vec![("t".into(), 2), ("t".into(), 3)]),
    ] {
        let stub = GroupStub::boot().await;
        stub.set_member_id(id);
        stub.set_partitions("t", 4);
        stub.set_describe_members(vec![member("m-a", &["t"]), member("m-b", &["t"])]);
        let g = join_assignor(&stub, "range").await.expect("join");
        assert_eq!(g.assignor(), "range");
        assert_eq!(g.member_id(), id);
        assert_eq!(g.assignment(), want, "member={id}");
        assert_eq!(stub.describe_count(), 1);
        assert_eq!(stub.metadata_count(), 1);
        g.leave().await.expect("leave");
    }
}

#[tokio::test]
async fn range_describe_error_uses_join_group_assignment() {
    let stub = GroupStub::boot().await;
    stub.set_member_id("m-a");
    stub.set_assignment(vec![Assignment {
        topic: "t".into(),
        partition: 0,
    }]);
    stub.set_partitions("t", 4);
    stub.set_describe_error(true);
    let g = join_assignor(&stub, "range").await.expect("join");
    assert_eq!(g.assignor(), "range");
    assert_eq!(g.assignment(), vec![("t".into(), 0)]);
    assert_eq!(stub.describe_count(), 1);
    assert_eq!(stub.join_count(), 1);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn range_describe_error_empty_join_assignment_solos() {
    let stub = GroupStub::boot().await;
    stub.set_member_id("m-a");
    stub.set_assignment(vec![]);
    stub.set_partitions("t", 4);
    stub.set_describe_error(true);
    let g = join_assignor(&stub, "range").await.expect("join");
    assert_eq!(
        g.assignment(),
        vec![
            ("t".into(), 0),
            ("t".into(), 1),
            ("t".into(), 2),
            ("t".into(), 3)
        ]
    );
    assert_eq!(stub.describe_count(), 1);
    assert_eq!(stub.metadata_count(), 1);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn default_join_does_not_describe_group() {
    let stub = GroupStub::boot().await;
    let client = connect(&stub).await;
    let g = GroupConsumer::join_with_heartbeat(client, "g", vec!["t".into()], 10_000, false)
        .await
        .expect("join");
    assert_eq!(g.assignor(), "broker");
    assert_eq!(g.assignment(), vec![("t".into(), 0)]);
    assert_eq!(stub.describe_count(), 0);
    assert_eq!(stub.metadata_count(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn range_describe_omits_self_still_includes() {
    let stub = GroupStub::boot().await;
    stub.set_member_id("m-b");
    stub.set_partitions("t", 4);
    stub.set_describe_members(vec![member("m-a", &["t"])]);
    let g = join_assignor(&stub, "range").await.expect("join");
    assert_eq!(g.assignment(), vec![("t".into(), 2), ("t".into(), 3)]);
    assert_eq!(stub.describe_count(), 1);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn invalid_assignor_fails_before_join_group() {
    let stub = GroupStub::boot().await;
    let err = join_assignor(&stub, "banana").await.expect_err("banana");
    assert!(err.to_string().contains("unknown assignor"), "err={err}");
    assert!(err.to_string().contains("banana"), "err={err}");
    assert_eq!(stub.join_count(), 0);
    assert_eq!(stub.describe_count(), 0);
    assert_eq!(stub.metadata_count(), 0);
}

#[tokio::test]
async fn empty_assignor_is_broker() {
    let stub = GroupStub::boot().await;
    let g = join_assignor(&stub, "").await.expect("join");
    assert_eq!(g.assignor(), "broker");
    assert_eq!(g.assignment(), vec![("t".into(), 0)]);
    assert_eq!(stub.describe_count(), 0);
    assert_eq!(stub.metadata_count(), 0);
    g.leave().await.expect("leave");
}

#[tokio::test]
async fn range_join_members_skips_describe_group() {
    for (id, want) in [
        ("m-a", vec![("t".into(), 0), ("t".into(), 1)]),
        ("m-b", vec![("t".into(), 2), ("t".into(), 3)]),
    ] {
        let stub = GroupStub::boot().await;
        stub.set_member_id(id);
        stub.set_partitions("t", 4);
        stub.set_join_members(vec!["m-a".into(), "m-b".into()]);
        stub.set_describe_error(true);
        let g = join_assignor(&stub, "range").await.expect("join");
        assert_eq!(g.assignment(), want, "member={id}");
        assert_eq!(stub.describe_count(), 0);
        assert_eq!(stub.metadata_count(), 1);
        g.leave().await.expect("leave");
    }
}
