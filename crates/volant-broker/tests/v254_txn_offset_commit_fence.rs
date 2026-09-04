//! v0.254: TxnOffsetCommit v3+ generation/member fence.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, rpc, temp_dir};
use volant_broker::kafka::codec::{
    encode_consumer_subscription, encode_request, encode_request_flexible, get_compact_array_len,
    get_compact_string, get_string, put_bytes, put_compact_array_len, put_compact_nullable_string,
    put_compact_string, put_empty_tag_buffer, put_nullable_string, put_string, skip_tag_buffer,
};
use volant_broker::{Broker, OFFSET_UNKNOWN};
use volant_storage::StorageConfig;

const REBALANCE_IN_PROGRESS: i16 = 27;
const UNKNOWN_MEMBER_ID: i16 = 25;

fn kafka_join(group: &str, topic: &str, corr: i32) -> BytesMut {
    let sub = encode_consumer_subscription(&[topic]);
    let mut jbody = BytesMut::new();
    put_string(&mut jbody, group);
    jbody.put_i32(10_000); // session_timeout
    jbody.put_i32(150); // rebalance_timeout — keep sequential Joins off the 10s park
    put_string(&mut jbody, "");
    put_string(&mut jbody, "consumer");
    jbody.put_i32(1);
    put_string(&mut jbody, "range");
    put_bytes(&mut jbody, Some(&sub));
    encode_request(11, 1, corr, Some("c"), &jbody)
}

fn parse_join(mut src: bytes::Bytes, corr: i32) -> (i32, String) {
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i16(), 0);
    let generation = src.get_i32();
    let _protocol = get_string(&mut src).unwrap();
    let _leader = get_string(&mut src).unwrap();
    let member_id = get_string(&mut src).unwrap();
    (generation, member_id)
}

fn kafka_sync(group: &str, member_id: &str, generation: i32, corr: i32) -> BytesMut {
    let mut sbody = BytesMut::new();
    put_string(&mut sbody, group);
    sbody.put_i32(generation);
    put_string(&mut sbody, member_id);
    sbody.put_i32(0);
    encode_request(14, 0, corr, Some("c"), &sbody)
}

fn init_txn_body(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body
}

fn add_partitions_body(txn_id: &str, pid: i64, epoch: i16, topic: &str, parts: &[i32]) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(parts.len() as i32);
    for p in parts {
        body.put_i32(*p);
    }
    body
}

fn end_txn_body(txn_id: &str, pid: i64, epoch: i16, committed: bool) -> BytesMut {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_u8(if committed { 1 } else { 0 });
    body
}

fn txn_offset_commit_v3(
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    generation: i32,
    member: &str,
    topic: &str,
    parts: &[(i32, i64)],
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_string(&mut body, txn_id);
    put_compact_string(&mut body, group);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(generation);
    put_compact_string(&mut body, member);
    put_compact_nullable_string(&mut body, None); // instance id — parsed, ignored
    put_compact_array_len(&mut body, 1);
    put_compact_string(&mut body, topic);
    put_compact_array_len(&mut body, parts.len());
    for (partition, offset) in parts {
        body.put_i32(*partition);
        body.put_i64(*offset);
        body.put_i32(-1); // leader_epoch
        put_compact_nullable_string(&mut body, Some(""));
        put_empty_tag_buffer(&mut body);
    }
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

async fn init_pid(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let resp = rpc(
        addr,
        encode_request(22, 0, corr, Some("p"), &init_txn_body(txn_id)),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i16(), 0);
    let pid = src.get_i64();
    let epoch = src.get_i16();
    (pid, epoch)
}

async fn add_partitions(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, topic: &str) {
    let resp = rpc(
        addr,
        encode_request(
            24,
            0,
            corr,
            Some("p"),
            &add_partitions_body(txn_id, pid, epoch, topic, &[0, 1]),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(src.get_i32(), 1); // topics
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 2); // partitions
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i16(), 0);
}

async fn end_txn_commit(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16) {
    let resp = rpc(
        addr,
        encode_request(
            26,
            0,
            corr,
            Some("p"),
            &end_txn_body(txn_id, pid, epoch, true),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
}

fn parse_toc_v3_errors(mut src: bytes::Bytes, corr: i32, topic: &str) -> Vec<(i32, i16)> {
    assert_eq!(src.get_i32(), corr);
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.get_i32(), 0); // throttle
    assert_eq!(get_compact_array_len(&mut src).unwrap(), Some(1));
    assert_eq!(get_compact_string(&mut src).unwrap(), topic);
    let n = get_compact_array_len(&mut src).unwrap().unwrap();
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let p = src.get_i32();
        let e = src.get_i16();
        skip_tag_buffer(&mut src).unwrap();
        out.push((p, e));
    }
    skip_tag_buffer(&mut src).unwrap();
    skip_tag_buffer(&mut src).unwrap();
    out
}

async fn toc_v3(
    addr: &str,
    corr: i32,
    txn_id: &str,
    group: &str,
    pid: i64,
    epoch: i16,
    generation: i32,
    member: &str,
    topic: &str,
    parts: &[(i32, i64)],
) -> Vec<(i32, i16)> {
    let resp = rpc(
        addr,
        encode_request_flexible(
            28,
            3,
            corr,
            Some("p"),
            &txn_offset_commit_v3(txn_id, group, pid, epoch, generation, member, topic, parts),
        ),
    )
    .await;
    parse_toc_v3_errors(resp.freeze(), corr, topic)
}

fn fetch_offset(broker: &Broker, group: &str, topic: &str, partition: u32) -> u64 {
    broker
        .groups()
        .fetch_offsets(group, &[(topic.into(), partition)])
        .unwrap()
        .entries[0]
        .offset
}

#[tokio::test]
async fn join_without_sync_is_rebalance_27_offsets_not_stored() {
    let dir = temp_dir("v254", "no-sync");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join("cg-fence", "events", 1)).await;
    let (generation, member_id) = parse_join(jresp.freeze(), 1);

    let errs = toc_v3(
        &addr,
        2,
        "txn-fence",
        "cg-fence",
        1,
        0,
        generation,
        &member_id,
        "events",
        &[(0, 42), (1, 43)],
    )
    .await;
    assert_eq!(errs.len(), 2);
    assert!(
        errs.iter().all(|(_, e)| *e == REBALANCE_IN_PROGRESS),
        "expected 27 on every partition, got {errs:?}"
    );
    assert_eq!(
        fetch_offset(&broker, "cg-fence", "events", 0),
        OFFSET_UNKNOWN
    );
    assert_eq!(
        fetch_offset(&broker, "cg-fence", "events", 1),
        OFFSET_UNKNOWN
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn after_sync_matching_member_gen_is_0() {
    let dir = temp_dir("v254", "after-sync");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join("cg-ok", "events", 10)).await;
    let (generation, member_id) = parse_join(jresp.freeze(), 10);

    let sresp = rpc(&addr, kafka_sync("cg-ok", &member_id, generation, 11)).await;
    let mut ss = sresp.freeze();
    assert_eq!(ss.get_i32(), 11);
    assert_eq!(ss.get_i16(), 0);

    let (pid, epoch) = init_pid(&addr, 12, "txn-ok").await;
    add_partitions(&addr, 13, "txn-ok", pid, epoch, "events").await;

    let errs = toc_v3(
        &addr,
        14,
        "txn-ok",
        "cg-ok",
        pid,
        epoch,
        generation,
        &member_id,
        "events",
        &[(0, 7), (1, 8)],
    )
    .await;
    assert_eq!(errs, vec![(0, 0), (1, 0)]);

    end_txn_commit(&addr, 15, "txn-ok", pid, epoch).await;
    assert_eq!(fetch_offset(&broker, "cg-ok", "events", 0), 7);
    assert_eq!(fetch_offset(&broker, "cg-ok", "events", 1), 8);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn empty_member_v3_skips_fence() {
    let dir = temp_dir("v254", "empty-member");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // Join without Sync so a member fence would have been 27.
    let jresp = rpc(&addr, kafka_join("cg-admin", "events", 20)).await;
    let (_generation, _member_id) = parse_join(jresp.freeze(), 20);

    let (pid, epoch) = init_pid(&addr, 21, "txn-admin").await;
    add_partitions(&addr, 22, "txn-admin", pid, epoch, "events").await;

    let errs = toc_v3(
        &addr,
        23,
        "txn-admin",
        "cg-admin",
        pid,
        epoch,
        -1,
        "",
        "events",
        &[(0, 11), (1, 12)],
    )
    .await;
    assert_eq!(errs, vec![(0, 0), (1, 0)]);

    end_txn_commit(&addr, 24, "txn-admin", pid, epoch).await;
    assert_eq!(fetch_offset(&broker, "cg-admin", "events", 0), 11);
    assert_eq!(fetch_offset(&broker, "cg-admin", "events", 1), 12);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unknown_member_is_25() {
    let dir = temp_dir("v254", "unknown-member");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("events", 2).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let jresp = rpc(&addr, kafka_join("cg-unk", "events", 30)).await;
    let (generation, _member_id) = parse_join(jresp.freeze(), 30);

    let errs = toc_v3(
        &addr,
        31,
        "txn-unk",
        "cg-unk",
        1,
        0,
        generation,
        "nobody",
        "events",
        &[(0, 1), (1, 2)],
    )
    .await;
    assert_eq!(errs.len(), 2);
    assert!(
        errs.iter().all(|(_, e)| *e == UNKNOWN_MEMBER_ID),
        "expected 25 on every partition, got {errs:?}"
    );
    assert_eq!(fetch_offset(&broker, "cg-unk", "events", 0), OFFSET_UNKNOWN);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
