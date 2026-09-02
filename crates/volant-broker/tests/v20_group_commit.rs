//! v0.20 broker wiring: env overlay + produce durability via group-commit.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use volant_broker::net::render_metrics;
use volant_broker::Broker;
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-broker-v20-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn batch(s: &str) -> MessageBatch {
    let mut b = MessageBatch::default();
    b.messages.push(Message::from_value(s.to_owned()));
    b
}

#[test]
fn produce_group_commit_durable_on_reopen() {
    let dir = temp_dir("reopen");
    let topic = TopicName::new("gc");
    {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            group_commit_max_ms: 20,
            group_commit_max_records: 8,
            ..StorageConfig::default()
        }));
        assert!(broker.group_commit_enabled());
        broker.create_topic(topic.clone(), 1).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let a = {
            let broker = Arc::clone(&broker);
            let topic = topic.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                broker.produce(&topic, PartitionId(0), batch("one"))
            })
        };
        let b = {
            let broker = Arc::clone(&broker);
            let topic = topic.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                broker.produce(&topic, PartitionId(0), batch("two"))
            })
        };
        a.join().unwrap().unwrap();
        b.join().unwrap().unwrap();

        let (flushes, records) = broker.group_commit_stats();
        assert!(flushes >= 1, "expected at least one group-commit flush");
        assert!(
            records >= 2,
            "both produces should be covered, got {records}"
        );

        let text = render_metrics(&broker);
        assert!(text.contains("volant_group_commit_flushes_total"));
        assert!(text.contains("volant_group_commit_records_total"));
    }

    let broker = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    let got = broker
        .fetch(&topic, PartitionId(0), Offset::ZERO, 10)
        .unwrap();
    assert_eq!(got.len(), 2);
    let mut values: Vec<_> = got.iter().map(|r| r.value.as_ref().to_vec()).collect();
    values.sort();
    assert_eq!(values, [b"one".to_vec(), b"two".to_vec()]);

    let _ = std::fs::remove_dir_all(&dir);
}
