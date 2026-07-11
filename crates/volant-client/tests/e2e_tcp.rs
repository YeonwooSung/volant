//! End-to-end TCP tests: create → produce → fetch over localhost.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::{Message, Offset};
use volant_storage::StorageConfig;

fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-e2e-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn boot_server(data_dir: std::path::PathBuf) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir,
        ..StorageConfig::default()
    }));
    let handle = tokio::spawn(async move {
        let _ = serve_listener(listener, broker).await;
    });
    // Tiny yield so accept loop is scheduled.
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

#[tokio::test]
async fn create_produce_fetch() {
    let dir = temp_data_dir("cpf");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.expect("connect");
    let id = client
        .create_topic("events", 1)
        .await
        .expect("create_topic");
    assert!(id.0 >= 1);

    let produced = client
        .produce(
            "events",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"hello-e2e"))],
        )
        .await
        .expect("produce");
    assert_eq!(produced.partition, 0);
    assert_eq!(produced.count, 1);
    assert_eq!(produced.base_offset, 0);

    let fetched = client
        .fetch("events", 0, Offset::ZERO, 10, 0)
        .await
        .expect("fetch");
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_ref(), b"hello-e2e");
    assert_eq!(fetched.records[0].offset, 0);
    assert!(fetched.high_watermark >= 1);

    // Metadata lists the topic.
    let meta = client.metadata().await.expect("metadata");
    assert!(meta.topics.iter().any(|t| t.name == "events"));

    // Delete cleans up.
    client.delete_topic("events").await.expect("delete");
    let meta2 = client.metadata().await.expect("metadata after delete");
    assert!(!meta2.topics.iter().any(|t| t.name == "events"));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn multi_partition_key_stickiness() {
    let dir = temp_data_dir("sticky");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.expect("connect");
    client
        .create_topic("keyed", 8)
        .await
        .expect("create multi-partition topic");

    let key = Bytes::from_static(b"user-42");
    let mut partitions = Vec::new();
    for i in 0..5 {
        let mut msg = Message::from_value(Bytes::from(format!("v{i}")));
        msg.key = Some(key.clone());
        let res = client
            .produce("keyed", None, vec![msg])
            .await
            .expect("produce keyed");
        partitions.push(res.partition);
    }

    // Same key always lands on the same partition.
    let first = partitions[0];
    assert!(
        partitions.iter().all(|&p| p == first),
        "expected sticky partition, got {partitions:?}"
    );

    // Different keys may map differently (not required, but produce still works).
    let mut other = Message::from_value(Bytes::from_static(b"other"));
    other.key = Some(Bytes::from_static(b"user-99"));
    let other_res = client
        .produce("keyed", None, vec![other])
        .await
        .expect("produce other key");
    // Fetch from the sticky partition and confirm our values are there.
    let fetched = client
        .fetch("keyed", first, Offset::ZERO, 20, 0)
        .await
        .expect("fetch sticky partition");
    assert!(fetched.records.len() >= 5);
    assert!(fetched
        .records
        .iter()
        .all(|r| r.key.as_ref().map(|k| k.as_ref()) == Some(key.as_ref())));

    // Ensure broker accepted the other produce regardless of partition.
    let _ = other_res;

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_without_key_round_robin() {
    let dir = temp_data_dir("rr");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.expect("connect");
    client.create_topic("rr", 3).await.expect("create");

    let mut seen = std::collections::HashSet::new();
    for i in 0..9 {
        let res = client
            .produce(
                "rr",
                None,
                vec![Message::from_value(Bytes::from(format!("m{i}")))],
            )
            .await
            .expect("produce");
        seen.insert(res.partition);
    }
    // With 9 null-key produces across 3 partitions, round-robin should hit all.
    assert_eq!(seen.len(), 3, "expected all partitions used, got {seen:?}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
