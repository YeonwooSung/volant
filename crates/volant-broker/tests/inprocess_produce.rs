use std::path::PathBuf;

use volant_broker::Broker;
use volant_core::{Message, PartitionId, TopicName};
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

struct TempCleanup(PathBuf);

impl Drop for TempCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
