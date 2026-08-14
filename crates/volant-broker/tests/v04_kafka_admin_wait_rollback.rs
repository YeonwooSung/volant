//! v0.4: Kafka CreateTopics shares native assignment wait/rollback.
//!
//! Wait/committed-only off is unchanged (`phase25_kafka_admin`): single-node
//! create succeeds with no fan-out wait.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{bind_port0, cluster_config_n2, default_storage, unique_dir, Guard};
use common::{boot_kafka, rpc};
use volant_broker::kafka::codec::{encode_request, get_string, put_string};
use volant_broker::{serve_listener, start_background_tasks, Broker};

fn assignment_json_has_topic(data_dir: &std::path::Path, topic: &str) -> bool {
    let path = data_dir.join("cluster").join("assignment.json");
    if !path.is_file() {
        return false;
    }
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    raw.contains(&format!("\"{topic}\"")) || raw.contains(&format!("\"name\": \"{topic}\""))
}

/// CreateTopics v1 body (error_message, no throttle) matching `phase25_kafka_admin`.
fn create_topics_v1(name: &str) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1); // topic count
    put_string(&mut body, name);
    body.put_i32(1); // num_partitions
    body.put_i16(1); // replication_factor (ignored)
    body.put_i32(0); // replica assignments
    body.put_i32(0); // configs
    body.put_i32(5000); // timeout
    body.put_u8(0); // validate_only
    body
}

/// N=2 one-dead: wait-on Kafka CreateTopics returns 19 and does not leave the
/// topic on disk; wait-off retry succeeds (not already-exists).
#[tokio::test]
async fn n2_one_dead_kafka_create_topics_wait_rolls_back() {
    let base = unique_dir("v04", "kt-rollback");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    // Peer never listens — 96/97 RPC fails (connection refused).
    let p2 = p1.saturating_add(100).max(33_000);
    let cfg = cluster_config_n2([p1, p2]);
    let data_dir = base.join("n1");
    let b1 = {
        let b = Broker::with_cluster(default_storage(data_dir.clone()), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };

    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    let (kaddr, kserver) = boot_kafka(Arc::clone(&b1)).await;

    // 1. Wait on → CreateTopics v1 "kt" → Kafka 19; disk has no "kt".
    b1.set_assignment_consensus_wait(true);
    let resp = rpc(
        &kaddr,
        encode_request(19, 1, 10, Some("admin"), &create_topics_v1("kt")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "kt");
    assert_eq!(
        src.get_i16(),
        19,
        "CreateTopics wait must surface Kafka NotEnoughReplicas (19)"
    );
    assert!(
        !assignment_json_has_topic(&data_dir, "kt"),
        "wait-fail CreateTopics must not leave kt on disk"
    );

    // 2. Wait off → CreateTopics "kt" → 0 (retry is not already-exists).
    b1.set_assignment_consensus_wait(false);
    let resp = rpc(
        &kaddr,
        encode_request(19, 1, 11, Some("admin"), &create_topics_v1("kt")),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 11);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), "kt");
    assert_eq!(
        src.get_i16(),
        0,
        "retry CreateTopics after rollback must succeed (not already-exists)"
    );
    assert!(
        assignment_json_has_topic(&data_dir, "kt"),
        "wait-off CreateTopics must write kt"
    );

    kserver.abort();
    s1.abort();
}
