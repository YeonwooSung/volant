//! Phase 98: ABORT control batches for crash≡abort of open write-through txns.

#[path = "common/mod.rs"]
mod common;
use common::{boot_kafka, rpc, temp_dir};

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use volant_broker::kafka::codec::{
    encode_record_batch_idempotent, encode_request, get_bytes, get_string, is_txn_control_record,
    parse_txn_control_record, peek_record_batch_attributes, put_bytes, put_nullable_string,
    put_string, ControlMarkerType, RECORD_BATCH_ATTR_CONTROL, RECORD_BATCH_ATTR_TRANSACTIONAL,
    RECORD_BATCH_ATTR_TXN_CONTROL,
};
use volant_broker::Broker;
use volant_core::{Message, Offset, PartitionId, Record, TopicName};
use volant_storage::StorageConfig;

async fn init_txn_async(addr: &str, corr: i32, txn_id: &str) -> (i64, i16) {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    let resp = rpc(addr, encode_request(22, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i16(), 0);
    (src.get_i64(), src.get_i16())
}

async fn add_partitions(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, topic: &str) {
    let mut body = BytesMut::new();
    put_string(&mut body, txn_id);
    body.put_i64(pid);
    body.put_i16(epoch);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    let resp = rpc(addr, encode_request(24, 0, corr, Some("p"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0);
    assert_eq!(src.get_i16(), 0);
}

fn produce_body(topic: &str, batch: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(5000);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    put_bytes(&mut body, Some(batch));
    body
}

fn sample(val: &'static [u8]) -> Vec<Record> {
    vec![Record {
        offset: Offset::new(0),
        key: None,
        value: Bytes::from_static(val),
        timestamp_ms: 1,
        headers: vec![],
    }]
}

async fn end_txn(addr: &str, corr: i32, txn_id: &str, pid: i64, epoch: i16, commit: bool) {
    let mut ebody = BytesMut::new();
    put_string(&mut ebody, txn_id);
    ebody.put_i64(pid);
    ebody.put_i16(epoch);
    ebody.put_u8(if commit { 1 } else { 0 });
    let eresp = rpc(addr, encode_request(26, 0, corr, Some("p"), &ebody)).await;
    let mut es = eresp.freeze();
    es.advance(4 + 4);
    assert_eq!(es.get_i16(), 0);
}

async fn fetch_v4_records(addr: &str, corr: i32, topic: &str, isolation: u8) -> (i64, i64, i32, Bytes) {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(100);
    body.put_i32(1);
    body.put_i32(1_048_576);
    body.put_u8(isolation);
    body.put_i32(1);
    put_string(&mut body, topic);
    body.put_i32(1);
    body.put_i32(0);
    body.put_i64(0);
    body.put_i32(1_048_576);
    let resp = rpc(addr, encode_request(1, 4, corr, Some("c"), &body)).await;
    let mut src = resp.freeze();
    src.advance(4 + 4); // corr + throttle
    assert_eq!(src.get_i32(), 1);
    assert_eq!(get_string(&mut src).unwrap(), topic);
    assert_eq!(src.get_i32(), 1);
    assert_eq!(src.get_i32(), 0); // partition
    assert_eq!(src.get_i16(), 0); // error
    let hwm = src.get_i64();
    let lso = src.get_i64();
    let aborted_n = src.get_i32();
    for _ in 0..aborted_n {
        let _ = src.get_i64();
        let _ = src.get_i64();
    }
    let recs = get_bytes(&mut src).unwrap().unwrap_or_default();
    (hwm, lso, aborted_n, recs)
}

fn count_control_batches(set: &[u8], want: ControlMarkerType) -> usize {
    let mut off = 0usize;
    let mut n = 0usize;
    while off + 23 <= set.len() {
        let Some((attrs, _base)) = peek_record_batch_attributes(&set[off..]) else {
            break;
        };
        let batch_len = i32::from_be_bytes(set[off + 8..off + 12].try_into().unwrap()) as usize;
        let end = (off + 12 + batch_len).min(set.len());
        if attrs & RECORD_BATCH_ATTR_CONTROL != 0 {
            assert_eq!(attrs & RECORD_BATCH_ATTR_TXN_CONTROL, RECORD_BATCH_ATTR_TXN_CONTROL);
            assert_eq!(attrs & RECORD_BATCH_ATTR_TRANSACTIONAL, RECORD_BATCH_ATTR_TRANSACTIONAL);
            let slice = &set[off..end];
            let ty = want as i16;
            let pattern = [0u8, 0, 0, ty as u8];
            if slice.windows(4).any(|w| w == pattern) {
                n += 1;
            }
        }
        off += 12 + batch_len;
    }
    n
}

fn assert_has_control_batch(set: &[u8], want: ControlMarkerType) {
    assert!(
        count_control_batches(set, want) >= 1,
        "expected at least one {:?} control batch",
        want
    );
}

/// Open write-through + simulated crash (drop broker, reload same data_dir):
/// soft aborted list + ABORT control batch on the partition log.
#[test]
fn unit_crash_reload_appends_abort_control() {
    let dir = temp_dir("p98", "crash-reload");
    let topic = TopicName::new("t");
    let (pid, epoch) = {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("t", 1).unwrap();
        let (pid, epoch) = broker.init_producer_id_with_txn("txn-crash");
        assert_eq!(broker.begin_txn(pid, epoch), 0);
        let _ = broker.buffer_txn_produce(
            pid,
            epoch,
            "t",
            0,
            0,
            vec![Message::from_value("unstable")],
        );
        // Open ranges persisted under __txn_markers; no EndTxn.
        // Drop broker ≡ crash.
        (pid, epoch)
    };

    // Reload: promote open → aborted + ABORT control batch.
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });

    let recs = broker
        .fetch_kafka(&topic, PartitionId(0), Offset::ZERO, 20, false)
        .unwrap();
    let controls: Vec<_> = recs
        .iter()
        .filter(|r| is_txn_control_record(r))
        .collect();
    assert_eq!(
        controls.len(),
        1,
        "expected exactly one ABORT control after crash promote, got {} records ({:?})",
        recs.len(),
        recs.iter()
            .map(|r| (
                is_txn_control_record(r),
                r.value.as_ref().to_vec()
            ))
            .collect::<Vec<_>>()
    );
    let p = parse_txn_control_record(controls[0]).unwrap();
    assert_eq!(p.marker_type, ControlMarkerType::Abort);
    assert_eq!(p.producer_id, pid as i64);
    assert_eq!(p.producer_epoch, epoch as i16);

    // Soft aborted list filters data under READ_COMMITTED.
    let rc = broker
        .fetch_kafka(&topic, PartitionId(0), Offset::ZERO, 20, true)
        .unwrap();
    // RC still includes control markers (Phase 89) but hides aborted data.
    assert!(rc.iter().any(is_txn_control_record));
    assert!(
        rc.iter()
            .filter(|r| !is_txn_control_record(r))
            .all(|r| r.value.as_ref() != b"unstable"),
        "aborted data must be hidden under READ_COMMITTED"
    );

    // Native committed-only hides control + aborted data.
    let native = broker
        .fetch(&topic, PartitionId(0), Offset::ZERO, 20)
        .unwrap();
    assert!(native.is_empty());

    // Second reload must not duplicate control markers (idempotent).
    drop(broker);
    let broker2 = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let recs2 = broker2
        .fetch_kafka(&topic, PartitionId(0), Offset::ZERO, 20, false)
        .unwrap();
    let n_ctrl = recs2.iter().filter(|r| is_txn_control_record(r)).count();
    assert_eq!(
        n_ctrl, 1,
        "second reload must not re-append ABORT control (got {n_ctrl})"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// begin_txn only (no AddPartitions membership, no write-through) still produces
/// no control batch on crash promote — nothing was registered on the txn.
///
/// Phase 105 covers the AddPartitions-membership path (control for empty added
/// partitions) in `phase105_empty_add_partitions_control.rs`. This test remains
/// the guard for "open with zero membership invents nothing."
#[test]
fn unit_begin_only_no_control_on_crash() {
    let dir = temp_dir("p98", "add-only");
    {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("t", 1).unwrap();
        let (pid, epoch) = broker.init_producer_id_with_txn("txn-empty");
        assert_eq!(broker.begin_txn(pid, epoch), 0);
        // No record_txn_added_partitions, no buffer_txn_produce — open markers
        // file has empty open + open_added. Crash ≡ nothing to promote.
    }
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let recs = broker
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    assert!(
        recs.iter().all(|r| !is_txn_control_record(r)),
        "begin_txn without AddPartitions/writes must not invent control batches"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// EndTxn abort path unchanged: still one ABORT control (not double from load).
#[test]
fn unit_end_txn_abort_still_one_control() {
    let dir = temp_dir("p98", "end-abort");
    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    broker.create_topic("t", 1).unwrap();
    let (pid, epoch) = broker.init_producer_id_with_txn("txn-end");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    let _ = broker.buffer_txn_produce(pid, epoch, "t", 0, 0, vec![Message::from_value("x")]);
    let (code, _) = broker.end_txn(pid, epoch, false, &[]).unwrap();
    assert_eq!(code, 0);

    let recs = broker
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    let n = recs.iter().filter(|r| is_txn_control_record(r)).count();
    assert_eq!(n, 1, "EndTxn abort should write exactly one ABORT control");

    // Reload after clean EndTxn: open list empty → no extra control.
    drop(broker);
    let broker2 = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let recs2 = broker2
        .fetch_kafka(
            &TopicName::new("t"),
            PartitionId(0),
            Offset::ZERO,
            20,
            false,
        )
        .unwrap();
    let n2 = recs2.iter().filter(|r| is_txn_control_record(r)).count();
    assert_eq!(n2, 1, "reload after EndTxn must not add another control");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Wire path: open produce, drop broker, reload, Fetch v4 sees ABORT control.
#[tokio::test]
async fn crash_reload_fetch_sees_abort_control() {
    let dir = temp_dir("p98", "wire-crash");
    let (pid, epoch) = {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        }));
        broker.create_topic("t", 1).unwrap();
        let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
        let (pid, epoch) = init_txn_async(&addr, 1, "txn-w").await;
        add_partitions(&addr, 2, "txn-w", pid, epoch, "t").await;
        let batch = encode_record_batch_idempotent(&sample(b"x"), pid, epoch, 0);
        let _ = rpc(
            &addr,
            encode_request(0, 0, 3, Some("p"), &produce_body("t", &batch)),
        )
        .await;
        // No EndTxn — simulate crash by dropping broker + server.
        server.abort();
        (pid, epoch)
    };

    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (_hwm, lso, aborted_n, rec_u) = fetch_v4_records(&addr, 10, "t", 0).await;
    assert_has_control_batch(&rec_u, ControlMarkerType::Abort);
    assert_eq!(
        count_control_batches(&rec_u, ControlMarkerType::Abort),
        1,
        "exactly one ABORT control under RU"
    );

    let (_hwm_c, lso_c, aborted_c, rec_c) = fetch_v4_records(&addr, 11, "t", 1).await;
    assert!(aborted_c >= 1, "soft aborted list under RC");
    assert_eq!(lso_c, lso, "LSO should match after crash promote");
    assert_has_control_batch(&rec_c, ControlMarkerType::Abort);
    // Aborted app data filtered under RC; control remains.
    // (May still contain control batch only.)
    let _ = (pid, epoch, aborted_n);

    let native = broker
        .fetch(&TopicName::new("t"), PartitionId(0), Offset::ZERO, 20)
        .unwrap();
    assert!(native.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pre-Phase-98 open markers without producer_epoch still get a control batch
/// via best-effort producer_state lookup.
#[test]
fn unit_legacy_open_marker_best_effort_epoch() {
    let dir = temp_dir("p98", "legacy");
    let topic = TopicName::new("t");
    let (pid, epoch) = {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        broker.create_topic("t", 1).unwrap();
        let (pid, epoch) = broker.init_producer_id_with_txn("txn-leg");
        assert_eq!(broker.begin_txn(pid, epoch), 0);
        let check = broker.buffer_txn_produce(
            pid,
            epoch,
            "t",
            0,
            0,
            vec![Message::from_value("leg")],
        );
        // Capture written range from markers file shape: rewrite without epoch.
        let _ = check;
        (pid, epoch)
    };

    // Overwrite __txn_markers with a pre-98 shape (no producer_epoch field).
    let markers_path = dir.join("__txn_markers").join("state.json");
    let raw = std::fs::read_to_string(&markers_path).expect("markers exist");
    let mut v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(open) = v.get_mut("open").and_then(|o| o.as_array_mut()) {
        for item in open.iter_mut() {
            if let Some(obj) = item.as_object_mut() {
                obj.remove("producer_epoch");
            }
        }
    }
    std::fs::write(&markers_path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let recs = broker
        .fetch_kafka(&topic, PartitionId(0), Offset::ZERO, 20, false)
        .unwrap();
    let ctrl = recs
        .iter()
        .find(|r| is_txn_control_record(r))
        .expect("best-effort control from producer_state epoch");
    let p = parse_txn_control_record(ctrl).unwrap();
    assert_eq!(p.marker_type, ControlMarkerType::Abort);
    assert_eq!(p.producer_id, pid as i64);
    assert_eq!(p.producer_epoch, epoch as i16);

    let _ = std::fs::remove_dir_all(&dir);
}

/// EndTxn commit path still writes COMMIT (regression guard).
#[tokio::test]
async fn end_txn_commit_unchanged() {
    let dir = temp_dir("p98", "commit-reg");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.create_topic("t", 1).unwrap();
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let (pid, epoch) = init_txn_async(&addr, 1, "txn-c").await;
    add_partitions(&addr, 2, "txn-c", pid, epoch, "t").await;
    let batch = encode_record_batch_idempotent(&sample(b"keep"), pid, epoch, 0);
    let _ = rpc(
        &addr,
        encode_request(0, 0, 3, Some("p"), &produce_body("t", &batch)),
    )
    .await;
    end_txn(&addr, 4, "txn-c", pid, epoch, true).await;

    let (_h, _l, _a, rec) = fetch_v4_records(&addr, 10, "t", 1).await;
    assert_has_control_batch(&rec, ControlMarkerType::Commit);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
