//! v0.6: Kafka DeleteRecords per-request wait tag (flex v2 only).
//!
//! Request-level TAG_BUFFER tag **0** is `wait_majority` `u8` (same 0/1/2
//! semantics as the native trailer). Not a Kafka standard field; v0–1 stay
//! env/broker only.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use common::cluster::{bind_port0, cluster_config_n2, unique_dir, Guard};
use common::{boot_kafka, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_string, get_string,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_string,
    put_unsigned_varint, skip_tag_buffer,
};
use volant_broker::{serve_listener, start_background_tasks, BackgroundTasks, Broker};
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn small_seg_storage(data_dir: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        data_dir,
        flush_every_n: 1,
        segment_size: 256,
        ..StorageConfig::default()
    }
}

fn big(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

fn fill_local(broker: &Broker, topic: &str, n: u32) {
    let name = TopicName::new(topic);
    let pid = PartitionId(0);
    for i in 0..n {
        let mut batch = MessageBatch::default();
        batch
            .messages
            .push(Message::from_value(big(&format!("m{i}"), 180)));
        let (_, err) = broker
            .produce_with_acks(&name, pid, batch, 1, None)
            .expect("produce");
        assert_eq!(err, 0, "produce acks=1 should succeed on leader");
    }
}

fn assert_is_leader(broker: &Broker, topic: &str) {
    let name = TopicName::new(topic);
    assert!(
        broker.is_partition_leader(&name, PartitionId(0)),
        "node {} must lead {topic}/0",
        broker.node_id()
    );
}

fn earliest(broker: &Broker, topic: &str) -> u64 {
    broker
        .list_offsets(topic, &[0])
        .unwrap()
        .first()
        .map(|e| e.1)
        .unwrap_or(0)
}

fn put_tag_buffer(dst: &mut BytesMut, tags: &[(u32, &[u8])]) {
    put_unsigned_varint(dst, tags.len() as u32);
    for (id, body) in tags {
        put_unsigned_varint(dst, *id);
        put_unsigned_varint(dst, body.len() as u32);
        dst.extend_from_slice(body);
    }
}

/// DeleteRecords v2 body. `wait` is tag 0; extra tags are appended (sorted by id).
fn delete_records_v2(
    topic: &str,
    partition: i32,
    offset: i64,
    wait: Option<u8>,
    extra: &[(u32, &[u8])],
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, 1);
    body.put_i32(partition);
    body.put_i64(offset);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body.put_i32(5000);
    let wait_buf = wait.map(|f| [f]);
    let mut tags: Vec<(u32, &[u8])> = Vec::new();
    if let Some(ref buf) = wait_buf {
        tags.push((0, buf.as_slice()));
    }
    tags.extend_from_slice(extra);
    tags.sort_by_key(|(id, _)| *id);
    put_tag_buffer(&mut body, &tags);
    body
}

fn delete_records_classic(topic: &str, partition: i32, offset: i64) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(partition);
    body.put_i64(offset);
    body.put_i32(5000);
    body
}

fn parse_v2_partition(resp: BytesMut, corr: i32, topic: &str) -> (i64, i16) {
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), topic);
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(src.get_i32(), 0);
    let low = src.get_i64();
    let err = src.get_i16();
    (low, err)
}

fn parse_classic_partition(resp: BytesMut, corr: i32, topic: &str) -> (i64, i16) {
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle (v0–1 encoder always writes it)
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    let low = src.get_i64();
    let err = src.get_i16();
    (low, err)
}

struct N2Solo {
    broker: Arc<Broker>,
    kaddr: String,
    _guard: Guard,
    native: tokio::task::JoinHandle<()>,
    kafka: tokio::task::JoinHandle<()>,
    bg: Option<BackgroundTasks>,
}

impl N2Solo {
    async fn boot(label: &str, wait_knob: bool) -> Self {
        let base = unique_dir("v06", label);
        let guard = Guard(base.clone());
        let (l1, p1) = bind_port0().await;
        let p2 = p1.saturating_add(100).max(34_000);
        let cfg = cluster_config_n2([p1, p2]);
        let broker = {
            let b = Broker::with_cluster(small_seg_storage(base.join("n1")), 1, cfg).unwrap();
            b.set_advertised("127.0.0.1", p1);
            b.set_delete_records_wait_majority(wait_knob);
            // v0.29: keep v0.6 wait-off / force-off cases on the irreversible path.
            // Production equivalent: VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1
            b.set_delete_records_allow_irreversible(true);
            Arc::new(b)
        };
        let bg = start_background_tasks(Arc::clone(&broker));
        let b = Arc::clone(&broker);
        let native = tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        let (kaddr, kafka) = boot_kafka(Arc::clone(&broker)).await;
        Self {
            broker,
            kaddr,
            _guard: guard,
            native,
            kafka,
            bg: Some(bg),
        }
    }

    async fn shutdown(mut self) {
        self.kafka.abort();
        self.native.abort();
        if let Some(bg) = self.bg.take() {
            bg.shutdown().await;
        }
    }
}

/// v2 no tag → broker knob (default wait-off). N=2 one-dead still succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_no_tag_uses_broker_knob_wait_off() {
    let cluster = N2Solo::boot("no-tag", false).await;
    assert!(!cluster.broker.delete_records_wait_majority());

    // "off" → N=2 leader broker 1.
    cluster.broker.create_topic("off", 1).unwrap();
    assert_is_leader(&cluster.broker, "off");
    fill_local(&cluster.broker, "off", 40);

    let before_ok = cluster.broker.delete_records_majority_wait_success_total();
    let before_fail = cluster.broker.delete_records_majority_wait_fail_total();
    let earliest_before = earliest(&cluster.broker, "off");

    let resp = rpc(
        &cluster.kaddr,
        encode_request_flexible(
            21,
            2,
            10,
            Some("v06"),
            &delete_records_v2("off", 0, 15, None, &[]),
        ),
    )
    .await;
    let (low, err) = parse_v2_partition(resp, 10, "off");
    assert_eq!(err, 0, "v2 no tag + wait-off must succeed (got {err})");
    assert!(
        low > earliest_before as i64,
        "wait-off must truncate: low={low} before={earliest_before}"
    );
    assert!(
        earliest(&cluster.broker, "off") > earliest_before,
        "log_start must advance on wait-off"
    );
    assert_eq!(
        cluster.broker.delete_records_majority_wait_success_total(),
        before_ok
    );
    assert_eq!(
        cluster.broker.delete_records_majority_wait_fail_total(),
        before_fail
    );

    cluster.shutdown().await;
}

/// v2 tag 0 = 1 (force on) + N=2 one-dead → Kafka 19, no local truncate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_tag_force_on_n2_dead_peer_no_truncate() {
    let cluster = N2Solo::boot("force-on", false).await;
    assert!(!cluster.broker.delete_records_wait_majority());

    // "wait" → N=2 leader broker 1.
    cluster.broker.create_topic("wait", 1).unwrap();
    assert_is_leader(&cluster.broker, "wait");
    fill_local(&cluster.broker, "wait", 40);

    let earliest_before = earliest(&cluster.broker, "wait");
    let before_fail = cluster.broker.delete_records_majority_wait_fail_total();
    let before_ok = cluster.broker.delete_records_majority_wait_success_total();

    let resp = rpc(
        &cluster.kaddr,
        encode_request_flexible(
            21,
            2,
            11,
            Some("v06"),
            &delete_records_v2("wait", 0, 15, Some(1), &[]),
        ),
    )
    .await;
    let (low, err) = parse_v2_partition(resp, 11, "wait");
    assert_eq!(
        err, 19,
        "force-on + N=2 one-dead must surface Kafka 19 (got {err})"
    );
    assert_eq!(
        low, earliest_before as i64,
        "response low must equal pre-request log_start"
    );
    assert_eq!(
        earliest(&cluster.broker, "wait"),
        earliest_before,
        "Phase 148: wait fail must not truncate"
    );
    assert!(
        cluster.broker.delete_records_majority_wait_fail_total() > before_fail,
        "force-on wait fail must increment fail metric"
    );
    assert_eq!(
        cluster.broker.delete_records_majority_wait_success_total(),
        before_ok
    );

    cluster.shutdown().await;
}

/// v2 tag 0 = 2 (force off) even when broker knob is wait-on → local truncate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_tag_force_off_overrides_wait_on_knob() {
    let cluster = N2Solo::boot("force-off", true).await;
    assert!(cluster.broker.delete_records_wait_majority());

    // "v06a" → N=2 leader broker 1.
    cluster.broker.create_topic("v06a", 1).unwrap();
    assert_is_leader(&cluster.broker, "v06a");
    fill_local(&cluster.broker, "v06a", 40);

    let earliest_before = earliest(&cluster.broker, "v06a");
    let before_ok = cluster.broker.delete_records_majority_wait_success_total();
    let before_fail = cluster.broker.delete_records_majority_wait_fail_total();

    let resp = rpc(
        &cluster.kaddr,
        encode_request_flexible(
            21,
            2,
            12,
            Some("v06"),
            &delete_records_v2("v06a", 0, 15, Some(2), &[]),
        ),
    )
    .await;
    let (low, err) = parse_v2_partition(resp, 12, "v06a");
    assert_eq!(
        err, 0,
        "force-off must truncate without majority (got {err})"
    );
    assert!(
        low > earliest_before as i64,
        "force-off must advance low: low={low} before={earliest_before}"
    );
    assert!(
        earliest(&cluster.broker, "v06a") > earliest_before,
        "log_start must advance on force-off"
    );
    assert_eq!(
        cluster.broker.delete_records_majority_wait_success_total(),
        before_ok,
        "force-off must not touch wait success metric"
    );
    assert_eq!(
        cluster.broker.delete_records_majority_wait_fail_total(),
        before_fail,
        "force-off must not touch wait fail metric"
    );

    cluster.shutdown().await;
}

/// v1 (no tags) still uses the broker knob only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v1_uses_broker_knob_only() {
    let cluster = N2Solo::boot("classic", true).await;
    assert!(cluster.broker.delete_records_wait_majority());

    // "v06c" → N=2 leader broker 1.
    cluster.broker.create_topic("v06c", 1).unwrap();
    assert_is_leader(&cluster.broker, "v06c");
    fill_local(&cluster.broker, "v06c", 40);

    let earliest_before = earliest(&cluster.broker, "v06c");

    let resp = rpc(
        &cluster.kaddr,
        encode_request(
            21,
            1,
            13,
            Some("v06"),
            &delete_records_classic("v06c", 0, 15),
        ),
    )
    .await;
    let (low, err) = parse_classic_partition(resp, 13, "v06c");
    assert_eq!(
        err, 19,
        "v1 + wait-on knob + N=2 one-dead must surface 19 (got {err})"
    );
    assert_eq!(low, earliest_before as i64);
    assert_eq!(
        earliest(&cluster.broker, "v06c"),
        earliest_before,
        "v1 wait-on fail must not truncate"
    );

    cluster.shutdown().await;
}

/// Unknown tag id is ignored; request still decodes (wait-off default).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v2_unknown_tag_ignored() {
    let cluster = N2Solo::boot("unk-tag", false).await;

    // "events" → N=2 leader broker 1.
    cluster.broker.create_topic("events", 1).unwrap();
    assert_is_leader(&cluster.broker, "events");
    fill_local(&cluster.broker, "events", 40);

    let earliest_before = earliest(&cluster.broker, "events");
    let unknown: &[u8] = &[0xab, 0xcd];
    let resp = rpc(
        &cluster.kaddr,
        encode_request_flexible(
            21,
            2,
            14,
            Some("v06"),
            &delete_records_v2("events", 0, 15, None, &[(7, unknown)]),
        ),
    )
    .await;
    let (low, err) = parse_v2_partition(resp, 14, "events");
    assert_eq!(err, 0, "unknown tag must not fail decode (got {err})");
    assert!(
        low > earliest_before as i64,
        "unknown tag + wait-off must truncate"
    );

    cluster.shutdown().await;
}
