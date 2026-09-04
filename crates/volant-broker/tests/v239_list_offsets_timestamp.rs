//! v0.239: native ListOffsets timestamp trailer.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::{Error, Message, PartitionId, TopicName};
use volant_protocol::{LIST_OFFSETS_EARLIEST, LIST_OFFSETS_LATEST};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-v239-{label}-{}-{}",
        std::process::id(),
        nanos
    ))
}

fn storage(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        flush_every_n: 1,
        ..StorageConfig::default()
    }
}

fn msg_at(value: &str, timestamp_ms: i64) -> Message {
    Message {
        key: None,
        value: value.as_bytes().to_vec().into(),
        timestamp_ms: Some(timestamp_ms),
        headers: Vec::new(),
    }
}

#[test]
fn list_offsets_at_between_records() {
    let dir = temp_dir("scan");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("t");
    let broker = Broker::new(storage(&dir));
    broker.create_topic(topic.clone(), 1).unwrap();

    broker
        .produce_one(&topic, PartitionId(0), msg_at("a", 1000))
        .unwrap();
    broker
        .produce_one(&topic, PartitionId(0), msg_at("b", 2000))
        .unwrap();
    broker.flush(&topic, PartitionId(0)).unwrap();

    let between = broker.list_offsets_at("t", &[0], 1500).unwrap();
    assert_eq!(between, vec![(0, 0, 1)]);

    let earliest = broker
        .list_offsets_at("t", &[0], LIST_OFFSETS_EARLIEST)
        .unwrap();
    assert_eq!(earliest, vec![(0, 0, 0)]);

    let latest = broker
        .list_offsets_at("t", &[0], LIST_OFFSETS_LATEST)
        .unwrap();
    assert_eq!(latest, vec![(0, 0, 2)]);
    assert_eq!(broker.list_offsets("t", &[0]).unwrap(), latest);

    let none = broker.list_offsets_at("t", &[0], 3000).unwrap();
    assert_eq!(none, vec![(0, 0, 2)]);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn list_offsets_at_invalid_timestamp() {
    let dir = temp_dir("bad");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("t");
    let broker = Broker::new(storage(&dir));
    broker.create_topic(topic, 1).unwrap();

    let err = broker.list_offsets_at("t", &[0], -3).unwrap_err();
    match err {
        Error::InvalidArgument(m) => {
            assert!(m.contains("timestamp"), "{m}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_offsets_at_via_client() {
    let dir = temp_dir("wire");
    let _ = std::fs::remove_dir_all(&dir);
    let broker = Arc::new(Broker::new(storage(&dir)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_listener(listener, b).await;
    });

    let client = Client::connect_addr(&format!("127.0.0.1:{}", addr.port()))
        .await
        .unwrap();
    client.create_topic("wire", 1).await.unwrap();
    client
        .produce("wire", Some(0), vec![msg_at("a", 1000), msg_at("b", 2000)])
        .await
        .unwrap();

    let between = client.list_offsets_at("wire", vec![0], 1500).await.unwrap();
    assert_eq!(between.entries.len(), 1);
    assert_eq!(between.entries[0].earliest, 0);
    assert_eq!(between.entries[0].latest, 1);

    let earliest = client
        .list_offsets_at("wire", vec![0], LIST_OFFSETS_EARLIEST)
        .await
        .unwrap();
    assert_eq!(earliest.entries[0].latest, 0);

    let latest = client
        .list_offsets_at("wire", vec![0], LIST_OFFSETS_LATEST)
        .await
        .unwrap();
    assert_eq!(latest.entries[0].latest, 2);
    let default = client.list_offsets("wire", vec![0]).await.unwrap();
    assert_eq!(default.entries[0].latest, 2);

    let err = client
        .list_offsets_at("wire", vec![0], -3)
        .await
        .unwrap_err();
    match err {
        volant_core::Error::InvalidArgument(m) => {
            assert!(m.contains("error_code=3") || m.contains("Invalid"), "{m}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
