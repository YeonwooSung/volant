//! Phase 15: CreatePartitions + ListOffsets.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::{Message, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-p15-{label}-{}-{}",
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

#[test]
fn create_partitions_and_survive_restart() {
    let dir = temp_dir("add");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("orders");

    {
        let broker = Broker::new(storage(&dir));
        broker.create_topic(topic.clone(), 2).unwrap();
        assert_eq!(broker.metadata(None).topics[0].partitions.len(), 2);

        let n = broker.create_partitions("orders", 4).unwrap();
        assert_eq!(n, 4);
        assert_eq!(broker.metadata(None).topics[0].partitions.len(), 4);

        // Produce to a newly added partition.
        broker
            .produce_one(&topic, PartitionId(3), Message::from_value("new-p"))
            .unwrap();
        broker.flush(&topic, PartitionId(3)).unwrap();

        // Cannot shrink or no-op increase.
        assert!(broker.create_partitions("orders", 4).is_err());
        assert!(broker.create_partitions("orders", 2).is_err());
    }

    let broker = Broker::new(storage(&dir));
    assert_eq!(broker.metadata(None).topics[0].partitions.len(), 4);
    let recs = broker
        .fetch(
            &topic,
            PartitionId(3),
            volant_core::Offset::ZERO,
            10,
        )
        .unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_ref(), b"new-p");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn list_offsets_earliest_latest() {
    let dir = temp_dir("off");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("t");
    let broker = Broker::new(storage(&dir));
    broker.create_topic(topic.clone(), 2).unwrap();

    for i in 0..5u64 {
        broker
            .produce_one(
                &topic,
                PartitionId(0),
                Message::from_value(format!("m{i}")),
            )
            .unwrap();
    }
    broker.flush(&topic, PartitionId(0)).unwrap();

    let all = broker.list_offsets("t", &[]).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0], (0, 0, 5));
    assert_eq!(all[1], (1, 0, 0));

    let one = broker.list_offsets("t", &[0]).unwrap();
    assert_eq!(one, vec![(0, 0, 5)]);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn create_partitions_list_offsets_via_client() {
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
        .produce("wire", Some(0), vec![Message::from_value("a")])
        .await
        .unwrap();
    client
        .produce("wire", Some(0), vec![Message::from_value("b")])
        .await
        .unwrap();

    let n = client.create_partitions("wire", 3).await.unwrap();
    assert_eq!(n, 3);

    let meta = client.metadata().await.unwrap();
    assert_eq!(meta.topics[0].partitions.len(), 3);

    let off = client.list_offsets("wire", vec![]).await.unwrap();
    assert_eq!(off.entries.len(), 3);
    assert_eq!(off.entries[0].earliest, 0);
    assert_eq!(off.entries[0].latest, 2);
    assert_eq!(off.entries[1].latest, 0);

    let filtered = client.list_offsets("wire", vec![0]).await.unwrap();
    assert_eq!(filtered.entries.len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}
