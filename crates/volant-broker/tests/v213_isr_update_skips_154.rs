//! v0.213 — IsrUpdate must not fan out homemade 154 when openraft is on.
//!
//! CreateTopic already prefers openraft via `maybe_fanout_assignment_consensus`.
//! IsrUpdate used to call `fanout_metadata_raft_append` whenever
//! `VOLANT_METADATA_RAFT` was on, leaking opcode 98 even with both flags set.

#[path = "common/mod.rs"]
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use common::cluster::{
    bind_port0, cluster_config, cluster_config_n2, default_storage, unique_dir, Guard,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use volant_broker::net::dispatch_request;
use volant_broker::{serve_listener, Broker};
use volant_protocol::{
    codec::{decode_frame, encode_frame},
    decode_request, pack_response, Request, RequestOpcode, Response,
};

fn set_openraft_env(on: bool) {
    if on {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "1");
    } else {
        std::env::set_var("VOLANT_OPENRAFT_METADATA", "0");
    }
}

/// Dummy peer that records inbound native opcodes and acks 98/96.
async fn spawn_opcode_spy() -> (u16, Arc<Mutex<Vec<u16>>>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_task = Arc::clone(&seen);
    let handle = tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            let seen = Arc::clone(&seen_task);
            tokio::spawn(async move {
                let mut buf = BytesMut::with_capacity(8 * 1024);
                loop {
                    match decode_frame(&mut buf) {
                        Ok(Some(frame)) => {
                            seen.lock().expect("opcodes").push(frame.header.opcode);
                            let resp = match decode_request(frame.header.opcode, &frame.payload) {
                                Ok(Request::MetadataRaftAppend {
                                    term,
                                    entries,
                                    prev_log_index,
                                    ..
                                }) => {
                                    let match_index =
                                        entries.last().map(|e| e.index).unwrap_or(prev_log_index);
                                    Response::MetadataRaftAppend {
                                        term,
                                        success: 1,
                                        match_index,
                                    }
                                }
                                Ok(Request::AssignmentConsensusNote { generation, .. }) => {
                                    Response::AssignmentConsensusNote {
                                        error_code: 0,
                                        generation,
                                    }
                                }
                                _ => Response::Error {
                                    code: 0,
                                    message: String::new(),
                                },
                            };
                            let packed =
                                pack_response(frame.header.correlation_id, &resp).expect("pack");
                            let mut out = BytesMut::new();
                            encode_frame(&packed, &mut out).expect("encode");
                            if stream.write_all(&out).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {
                            let n = match stream.read_buf(&mut buf).await {
                                Ok(n) => n,
                                Err(_) => break,
                            };
                            if n == 0 {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });
    (port, seen, handle)
}

fn isr_update_from_metadata(b: &Broker, topic: &str) -> Request {
    let meta = b.metadata(None);
    let t = meta
        .topics
        .iter()
        .find(|t| t.name.as_str() == topic)
        .expect("topic metadata");
    let p = &t.partitions[0];
    Request::IsrUpdate {
        topic: topic.into(),
        partition: p.partition_id.0,
        leader_id: p.leader,
        leader_epoch: p.leader_epoch,
        isr: p.isr.clone(),
        generation_hint: b.generation(),
    }
}

/// Openraft off + 154 on: successful IsrUpdate still fans out opcode 98.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isr_update_uses_154_when_openraft_off() {
    set_openraft_env(false);
    let base = unique_dir("v213", "154-only");
    let _g = Guard(base.clone());

    let (spy_port, seen, spy) = spawn_opcode_spy().await;
    let (_l1, p1) = bind_port0().await;
    let cfg = cluster_config_n2([p1, spy_port]);
    let b1 = {
        let b = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b.set_metadata_raft_enabled(true);
        b.set_metadata_raft_wait_commit(false);
        b.set_assignment_consensus_enabled(true);
        Arc::new(b)
    };
    assert!(
        !b1.openraft_metadata_enabled(),
        "154-only case must keep openraft off"
    );
    assert!(b1.metadata_raft_enabled());
    assert!(b1.is_controller());

    b1.create_topic("t154", 1).unwrap();
    let before = b1.metadata_raft().last_index();
    let resp = dispatch_request(&b1, isr_update_from_metadata(&b1, "t154")).await;
    match resp {
        Response::IsrUpdate { error_code, .. } => {
            assert_eq!(error_code, 0, "controller IsrUpdate must succeed");
        }
        other => panic!("unexpected IsrUpdate response: {other:?}"),
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut saw_98 = false;
    while tokio::time::Instant::now() < deadline {
        let ops = seen.lock().expect("opcodes").clone();
        if ops.contains(&(RequestOpcode::MetadataRaftAppend as u16)) {
            saw_98 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        saw_98,
        "openraft off + 154 on must still send opcode 98; seen={:?}",
        seen.lock().expect("opcodes")
    );
    assert!(
        b1.metadata_raft().last_index() > before,
        "homemade 154 must append locally when openraft is off"
    );

    spy.abort();
}

async fn wait_agreed_leader(nodes: &[Arc<Broker>], timeout: Duration) -> u32 {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = Vec::new();
    while tokio::time::Instant::now() < deadline {
        last = nodes.iter().map(|n| n.controller_id()).collect();
        if last.iter().all(|id| *id != 0) && last.iter().all(|id| *id == last[0]) {
            let leader = last[0];
            if nodes.iter().any(|n| n.node_id() == leader) {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no agreed openraft leader within {timeout:?}; last={last:?}");
}

/// Openraft on + 154 on: IsrUpdate must not append homemade 154 / send 98.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isr_update_skips_154_when_openraft_on() {
    set_openraft_env(true);
    let base = unique_dir("v213", "both");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);
    let mk = |id: u32| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        b.set_metadata_raft_enabled(true);
        b.set_metadata_raft_wait_commit(false);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    assert!(b1.openraft_metadata_enabled());
    assert!(b1.metadata_raft_enabled());

    let servers: Vec<_> = [
        (l1, Arc::clone(&b1)),
        (l2, Arc::clone(&b2)),
        (l3, Arc::clone(&b3)),
    ]
    .into_iter()
    .map(|(listener, b)| {
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        })
    })
    .collect();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let nodes = [Arc::clone(&b1), Arc::clone(&b2), Arc::clone(&b3)];
    let leader_id = wait_agreed_leader(&nodes, Duration::from_secs(8)).await;
    let leader = nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader node")
        .clone();
    assert!(leader.is_controller());

    leader.create_topic("t213", 1).unwrap();
    let before_idx = leader.metadata_raft().last_index();
    let before_ok = leader.metadata_raft_append_success_total();

    let meta = leader.metadata(None);
    let p = &meta.topics[0].partitions[0];
    let shrunk = vec![p.leader];
    let resp = dispatch_request(
        &leader,
        Request::IsrUpdate {
            topic: "t213".into(),
            partition: p.partition_id.0,
            leader_id: p.leader,
            leader_epoch: p.leader_epoch,
            isr: shrunk.clone(),
            generation_hint: leader.generation(),
        },
    )
    .await;
    match resp {
        Response::IsrUpdate { error_code, .. } => {
            assert_eq!(error_code, 0, "openraft leader must accept IsrUpdate");
        }
        other => panic!("unexpected IsrUpdate response: {other:?}"),
    }

    assert_eq!(
        leader.metadata_raft().last_index(),
        before_idx,
        "both flags on must not append homemade 154"
    );
    assert_eq!(
        leader.metadata_raft_append_success_total(),
        before_ok,
        "both flags on must not count a homemade 154 majority"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut follower_isr = None;
    while tokio::time::Instant::now() < deadline {
        let follower = nodes.iter().find(|n| n.node_id() != leader_id).unwrap();
        if let Some(asg) = follower.clone_live_assignment() {
            if let Some(t) = asg.topics.get("t213") {
                if let Some(part) = t.partitions.get(&0) {
                    if part.isr == shrunk {
                        follower_isr = Some(part.isr.clone());
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        follower_isr.as_deref(),
        Some(shrunk.as_slice()),
        "openraft SetAssignment (108) must install the ISR on a follower"
    );

    for s in servers {
        s.abort();
    }
}
