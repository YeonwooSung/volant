use std::path::PathBuf;

use volant_broker::{partition_for_key, Broker};
use volant_core::TopicName;
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-broker-ps-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct TempCleanup(PathBuf);
impl Drop for TempCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn keyed_partition_stable_across_calls() {
    let dir = unique_dir("keyed");
    let _c = TempCleanup(dir.clone());
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    });
    let topic = TopicName::new("events");
    broker.create_topic(topic.clone(), 8).unwrap();

    let a = broker
        .select_partition(&topic, Some(b"customer-9"))
        .unwrap();
    let b = broker
        .select_partition(&topic, Some(b"customer-9"))
        .unwrap();
    assert_eq!(a, b);

    // Matches murmur2 helper directly.
    let expected = partition_for_key(b"customer-9", 8);
    assert_eq!(a.0, expected);
}

#[test]
fn null_key_round_robin() {
    let dir = unique_dir("rr");
    let _c = TempCleanup(dir.clone());
    let broker = Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    });
    let topic = TopicName::new("events");
    broker.create_topic(topic.clone(), 4).unwrap();

    let mut parts = Vec::new();
    for _ in 0..8 {
        parts.push(broker.select_partition(&topic, None).unwrap().0);
    }
    // First 4 should be 0,1,2,3 in order.
    assert_eq!(&parts[..4], &[0, 1, 2, 3]);
    assert_eq!(&parts[4..], &[0, 1, 2, 3]);
}
