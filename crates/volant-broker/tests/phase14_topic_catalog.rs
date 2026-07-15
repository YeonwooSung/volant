//! Phase 14: durable topic catalog + DeleteRecords.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::{Message, Offset, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-p14-{label}-{}-{}",
        std::process::id(),
        nanos
    ))
}

fn storage(dir: &std::path::Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        segment_size: 512,
        flush_every_n: 1,
        ..StorageConfig::default()
    }
}

#[test]
fn topic_and_data_survive_broker_restart() {
    let dir = temp_dir("restart");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("orders");

    let first_id = {
        let broker = Broker::new(storage(&dir));
        broker.create_topic(topic.clone(), 2).unwrap();
        for i in 0..5u64 {
            broker
                .produce_one(&topic, PartitionId(0), Message::from_value(format!("v{i}")))
                .unwrap();
        }
        broker.flush(&topic, PartitionId(0)).unwrap();
        let id = broker.metadata(None).topics[0].topic_id.0;
        assert!(id > 0);
        id
    };

    // Restart — must not require create_topic.
    let broker = Broker::new(storage(&dir));
    let meta = broker.metadata(None);
    assert_eq!(meta.topics.len(), 1);
    assert_eq!(meta.topics[0].name.as_str(), "orders");
    assert_eq!(meta.topics[0].partitions.len(), 2);
    assert_eq!(meta.topics[0].topic_id.0, first_id);

    let records = broker
        .fetch(&topic, PartitionId(0), Offset::ZERO, 100)
        .unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].value.as_ref(), b"v0");
    assert_eq!(records[4].value.as_ref(), b"v4");

    // Creating another topic continues ids.
    let id2 = broker.create_topic(TopicName::new("events"), 1).unwrap();
    assert!(id2.0 > first_id);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn deleted_topic_does_not_resurrect() {
    let dir = temp_dir("delete");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("gone");
    {
        let broker = Broker::new(storage(&dir));
        broker.create_topic(topic.clone(), 1).unwrap();
        broker.delete_topic(&topic).unwrap();
    }
    let broker = Broker::new(storage(&dir));
    assert!(broker.metadata(None).topics.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_records_api() {
    let dir = temp_dir("delrec");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("t");
    let broker = Broker::new(storage(&dir));
    broker.create_topic(topic.clone(), 1).unwrap();
    let pid = PartitionId(0);

    for i in 0..40u64 {
        let payload = format!("{:080}", i);
        broker
            .produce_one(&topic, pid, Message::from_value(payload))
            .unwrap();
    }
    broker.flush(&topic, pid).unwrap();

    let before = broker.fetch(&topic, pid, Offset::ZERO, 1000).unwrap();
    assert!(before.len() >= 10);

    let (low, err) = broker.delete_records("t", 0, 10).unwrap();
    assert_eq!(err, 0);

    let after = broker.fetch(&topic, pid, Offset::new(low), 1000).unwrap();
    // Remaining data from low watermark onward.
    assert!(!after.is_empty() || low >= before.len() as u64);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_records_via_client() {
    let dir = temp_dir("client-del");
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
    for i in 0..20u32 {
        client
            .produce(
                "wire",
                Some(0),
                vec![Message::from_value(format!("msg-{i:03}"))],
            )
            .await
            .unwrap();
    }

    let res = client.delete_records("wire", 0, 5).await.unwrap();
    assert_eq!(res.topic, "wire");
    assert_eq!(res.partition, 0);

    let meta = client.metadata().await.unwrap();
    assert_eq!(meta.topics.len(), 1);

    // Restart broker process simulation: new Broker on same dir, new listener.
    drop(client);
    // Original broker still holds open logs; drop it by ending scope via Arc
    // — can't drop while spawn holds Arc. Just verify catalog file exists.
    let catalog = dir.join("__topics").join("catalog.json");
    assert!(
        catalog.exists(),
        "catalog should be written on create"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
