use std::path::PathBuf;

use volant_broker::Broker;
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
use volant_storage::StorageConfig;

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

fn test_storage(label: &str) -> (PathBuf, StorageConfig) {
    let data_dir = unique_data_dir(label);
    let config = StorageConfig {
        data_dir: data_dir.clone(),
        ..StorageConfig::default()
    };
    (data_dir, config)
}

#[test]
fn create_topic_and_produce() {
    let (data_dir, config) = test_storage("inprocess-produce");
    let _cleanup = TempCleanup(data_dir);

    let broker = Broker::new(config);
    let topic = TopicName::new("events");
    let id = broker.create_topic(topic.clone(), 2).unwrap();
    assert_eq!(id.0, 1);

    let r0 = broker
        .produce_one(&topic, PartitionId(0), Message::from_value("a"))
        .unwrap();
    let r1 = broker
        .produce_one(&topic, PartitionId(0), Message::from_value("b"))
        .unwrap();
    assert_eq!(r0.offset.raw(), 0);
    assert_eq!(r1.offset.raw(), 1);

    assert!(broker.create_topic(topic, 1).is_err());
}

/// Produce a MessageBatch of N → contiguous offsets and HWM advanced by N.
#[test]
fn batch_produce_contiguous_offsets_and_hwm() {
    let (data_dir, config) = test_storage("batch-coalesce");
    let _cleanup = TempCleanup(data_dir);

    let broker = Broker::new(config);
    let topic = TopicName::new("batch-events");
    broker.create_topic(topic.clone(), 1).unwrap();
    let partition = PartitionId(0);

    let hwm_before = broker.high_watermark(&topic, partition).unwrap();
    assert_eq!(hwm_before, 0);

    const N: usize = 16;
    let batch = MessageBatch {
        messages: (0..N)
            .map(|i| Message::with_key(format!("k{i}"), format!("v{i}")))
            .collect(),
    };

    let produced = broker.produce(&topic, partition, batch).unwrap();
    assert_eq!(produced.len(), N);
    for (i, rec) in produced.iter().enumerate() {
        assert_eq!(
            rec.offset.raw(),
            i as u64,
            "offsets must be contiguous starting at 0"
        );
        assert_eq!(rec.value.as_ref(), format!("v{i}").as_bytes());
    }

    let hwm_after = broker.high_watermark(&topic, partition).unwrap();
    assert_eq!(
        hwm_after,
        hwm_before + N as u64,
        "single partition HWM must advance by N after one coalesced produce"
    );

    // Fetch confirms the batch is readable end-to-end.
    let fetched = broker
        .fetch(&topic, partition, Offset::ZERO, N + 10)
        .unwrap();
    assert_eq!(fetched.len(), N);
    for (i, rec) in fetched.iter().enumerate() {
        assert_eq!(rec.offset.raw(), i as u64);
        assert_eq!(
            rec.key.as_ref().map(|b| b.as_ref()),
            Some(format!("k{i}").as_bytes())
        );
    }

    // Second batch continues contiguously from previous HWM.
    let batch2 = MessageBatch {
        messages: vec![
            Message::from_value("tail-a"),
            Message::from_value("tail-b"),
            Message::from_value("tail-c"),
        ],
    };
    let more = broker.produce(&topic, partition, batch2).unwrap();
    assert_eq!(more.len(), 3);
    assert_eq!(more[0].offset.raw(), N as u64);
    assert_eq!(more[1].offset.raw(), N as u64 + 1);
    assert_eq!(more[2].offset.raw(), N as u64 + 2);
    assert_eq!(
        broker.high_watermark(&topic, partition).unwrap(),
        N as u64 + 3
    );
}

/// Multi-message produce increments `messages_coalesced`; single-message does not.
#[test]
fn batch_produce_coalesce_metric() {
    let (data_dir, config) = test_storage("batch-metric");
    let _cleanup = TempCleanup(data_dir);

    let broker = Broker::new(config);
    let topic = TopicName::new("metrics");
    broker.create_topic(topic.clone(), 1).unwrap();
    let partition = PartitionId(0);

    assert_eq!(broker.messages_coalesced(), 0);

    broker
        .produce_one(&topic, partition, Message::from_value("solo"))
        .unwrap();
    assert_eq!(
        broker.messages_coalesced(),
        0,
        "single-message produce must not count as coalesced"
    );

    let batch = MessageBatch {
        messages: vec![
            Message::from_value("a"),
            Message::from_value("b"),
            Message::from_value("c"),
            Message::from_value("d"),
            Message::from_value("e"),
        ],
    };
    broker.produce(&topic, partition, batch).unwrap();
    assert_eq!(broker.messages_coalesced(), 5);

    let batch2 = MessageBatch {
        messages: vec![Message::from_value("x"), Message::from_value("y")],
    };
    broker.produce(&topic, partition, batch2).unwrap();
    assert_eq!(broker.messages_coalesced(), 7);

    // Empty batch is a no-op for the metric.
    broker
        .produce(&topic, partition, MessageBatch::default())
        .unwrap();
    assert_eq!(broker.messages_coalesced(), 7);
}

struct TempCleanup(PathBuf);

impl Drop for TempCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
