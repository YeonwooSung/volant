//! Broker-level durable produce / fetch integration tests (Phase 1).

use std::path::{Path, PathBuf};

use bytes::Bytes;
use volant_broker::Broker;
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
use volant_storage::{PartitionLog, StorageConfig};

/// Unique temporary data directory for isolation across parallel tests.
fn unique_data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-broker-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).expect("create temp data_dir");
    dir
}

fn test_storage(label: &str) -> (TempCleanup, StorageConfig) {
    let data_dir = unique_data_dir(label);
    let config = StorageConfig {
        data_dir: data_dir.clone(),
        // Explicit flush in tests; durable engine may also honor flush_every_n.
        flush_every_n: 0,
        ..StorageConfig::default()
    };
    (TempCleanup(data_dir), config)
}

struct TempCleanup(PathBuf);

impl TempCleanup {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn msg_with_headers(
    key: &str,
    value: &str,
    headers: Vec<(&str, &str)>,
    timestamp_ms: Option<i64>,
) -> Message {
    Message {
        key: Some(Bytes::from(key.to_owned())),
        value: Bytes::from(value.to_owned()),
        timestamp_ms,
        headers: headers
            .into_iter()
            .map(|(n, v)| (n.to_owned(), Bytes::from(v.to_owned())))
            .collect(),
    }
}

#[test]
fn produce_and_fetch_preserves_order_keys_and_headers() {
    let (_cleanup, config) = test_storage("produce-fetch");
    let broker = Broker::new(config);
    let topic = TopicName::new("orders");
    broker.create_topic(topic.clone(), 1).unwrap();
    let partition = PartitionId(0);

    let batch = MessageBatch {
        messages: vec![
            msg_with_headers(
                "k0",
                "payload-0",
                vec![("h1", "v1"), ("trace", "abc")],
                Some(1_700_000_000_000),
            ),
            msg_with_headers("k1", "payload-1", vec![("h1", "v2")], Some(1_700_000_000_001)),
            msg_with_headers("k2", "payload-2", vec![], Some(1_700_000_000_002)),
        ],
    };

    let produced = broker.produce(&topic, partition, batch).unwrap();
    assert_eq!(produced.len(), 3);
    for (i, rec) in produced.iter().enumerate() {
        assert_eq!(rec.offset.raw(), i as u64);
    }

    broker.flush(&topic, partition).unwrap();

    let fetched = broker.fetch(&topic, partition, Offset::ZERO, 100).unwrap();
    assert_eq!(fetched.len(), 3, "fetch from 0 should return all produced records");

    assert_eq!(fetched[0].offset.raw(), 0);
    assert_eq!(fetched[0].key.as_ref().map(|b| b.as_ref()), Some(b"k0".as_slice()));
    assert_eq!(fetched[0].value.as_ref(), b"payload-0");
    assert_eq!(fetched[0].timestamp_ms, 1_700_000_000_000);
    assert_eq!(fetched[0].headers.len(), 2);
    assert_eq!(fetched[0].headers[0].0, "h1");
    assert_eq!(fetched[0].headers[0].1.as_ref(), b"v1");
    assert_eq!(fetched[0].headers[1].0, "trace");
    assert_eq!(fetched[0].headers[1].1.as_ref(), b"abc");

    assert_eq!(fetched[1].offset.raw(), 1);
    assert_eq!(fetched[1].key.as_ref().map(|b| b.as_ref()), Some(b"k1".as_slice()));
    assert_eq!(fetched[1].value.as_ref(), b"payload-1");
    assert_eq!(fetched[1].headers.len(), 1);

    assert_eq!(fetched[2].offset.raw(), 2);
    assert_eq!(fetched[2].key.as_ref().map(|b| b.as_ref()), Some(b"k2".as_slice()));
    assert_eq!(fetched[2].value.as_ref(), b"payload-2");
    assert!(fetched[2].headers.is_empty());

    // Produce more and fetch from a mid offset.
    let more = MessageBatch {
        messages: vec![
            Message::with_key("k3", "payload-3"),
            Message::with_key("k4", "payload-4"),
        ],
    };
    let produced_more = broker.produce(&topic, partition, more).unwrap();
    assert_eq!(produced_more[0].offset.raw(), 3);
    assert_eq!(produced_more[1].offset.raw(), 4);
    broker.flush(&topic, partition).unwrap();

    let mid = broker
        .fetch(&topic, partition, Offset::new(2), 100)
        .unwrap();
    assert_eq!(mid.len(), 3, "fetch from mid offset should return tail");
    assert_eq!(mid[0].offset.raw(), 2);
    assert_eq!(mid[0].value.as_ref(), b"payload-2");
    assert_eq!(mid[1].offset.raw(), 3);
    assert_eq!(mid[1].value.as_ref(), b"payload-3");
    assert_eq!(mid[2].offset.raw(), 4);
    assert_eq!(mid[2].value.as_ref(), b"payload-4");

    // max_messages limit
    let limited = broker
        .fetch(&topic, partition, Offset::new(1), 2)
        .unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].offset.raw(), 1);
    assert_eq!(limited[1].offset.raw(), 2);
}

#[test]
fn durable_reopen_via_broker_preserves_records() {
    let (cleanup, config) = test_storage("durable-broker");
    let data_dir = cleanup.path().to_path_buf();
    let topic = TopicName::new("durable-events");
    let partition = PartitionId(0);

    {
        let broker = Broker::new(config);
        broker.create_topic(topic.clone(), 1).unwrap();

        for i in 0..5 {
            let msg = msg_with_headers(
                &format!("key-{i}"),
                &format!("value-{i}"),
                vec![("idx", &format!("{i}"))],
                Some(1_800_000_000_000 + i as i64),
            );
            let rec = broker.produce_one(&topic, partition, msg).unwrap();
            assert_eq!(rec.offset.raw(), i as u64);
        }
        broker.flush(&topic, partition).unwrap();
        // Broker + open PartitionLogs drop here.
    }

    // Reopen same data_dir; topic metadata is in-memory so recreate the topic
    // (PartitionLog::open must recover segments from disk).
    let config = StorageConfig {
        data_dir: data_dir.clone(),
        flush_every_n: 0,
        ..StorageConfig::default()
    };
    let broker = Broker::new(config);
    broker.create_topic(topic.clone(), 1).unwrap();

    let records = broker.fetch(&topic, partition, Offset::ZERO, 100).unwrap();
    assert_eq!(
        records.len(),
        5,
        "records must survive broker drop + reopen of the same data_dir"
    );
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.offset.raw(), i as u64);
        assert_eq!(
            rec.key.as_ref().map(|b| b.as_ref()),
            Some(format!("key-{i}").as_bytes())
        );
        assert_eq!(rec.value.as_ref(), format!("value-{i}").as_bytes());
        assert_eq!(rec.timestamp_ms, 1_800_000_000_000 + i as i64);
        assert_eq!(rec.headers.len(), 1);
        assert_eq!(rec.headers[0].0, "idx");
        assert_eq!(rec.headers[0].1.as_ref(), format!("{i}").as_bytes());
    }
}

#[test]
fn durable_reopen_via_partition_log() {
    let (cleanup, mut config) = test_storage("durable-log");
    // Point the log directly at the unique temp dir (no topic/partition nesting).
    config.data_dir = cleanup.path().to_path_buf();

    {
        let mut log = PartitionLog::open(config.clone()).unwrap();
        for i in 0..4 {
            let rec = log
                .append(Message::with_key(format!("k{i}"), format!("v{i}")))
                .unwrap();
            assert_eq!(rec.offset.raw(), i as u64);
        }
        log.flush().unwrap();
        assert_eq!(log.high_watermark().raw(), 4);
        // Drop log handles / file descriptors.
    }

    let log = PartitionLog::open(config).unwrap();
    assert_eq!(
        log.high_watermark().raw(),
        4,
        "high watermark must recover after reopen"
    );
    let records = log.read(Offset::ZERO, 100).unwrap();
    assert_eq!(records.len(), 4);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.offset.raw(), i as u64);
        assert_eq!(
            rec.key.as_ref().map(|b| b.as_ref()),
            Some(format!("k{i}").as_bytes())
        );
        assert_eq!(rec.value.as_ref(), format!("v{i}").as_bytes());
    }

    // Mid-offset read after recovery.
    let mid = log.read(Offset::new(2), 10).unwrap();
    assert_eq!(mid.len(), 2);
    assert_eq!(mid[0].offset.raw(), 2);
    assert_eq!(mid[1].offset.raw(), 3);
}
