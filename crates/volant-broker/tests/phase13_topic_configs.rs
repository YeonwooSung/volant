//! Phase 13: topic configs, describe/alter, retention.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker, KEY_RETENTION_BYTES, KEY_SEGMENT_BYTES};
use volant_client::Client;
use volant_core::Message;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-p13-{label}-{}-{}",
        std::process::id(),
        nanos
    ))
}

async fn start_broker(dir: std::path::PathBuf) -> (String, Arc<Broker>) {
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_listener(listener, b).await;
    });
    (format!("127.0.0.1:{}", addr.port()), broker)
}

#[tokio::test]
async fn create_describe_alter_configs() {
    let dir = temp_dir("cfg");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.unwrap();
    client
        .create_topic_with_configs(
            "events",
            2,
            vec![
                ("retention.ms".into(), "60000".into()),
                (KEY_SEGMENT_BYTES.into(), "65536".into()),
            ],
        )
        .await
        .unwrap();

    let desc = client.describe_configs("events").await.unwrap();
    assert_eq!(desc.partition_count, 2);
    assert_eq!(desc.topic_id, 1);
    let map: std::collections::HashMap<_, _> = desc.configs.into_iter().collect();
    assert_eq!(map.get("retention.ms").map(String::as_str), Some("60000"));
    assert_eq!(map.get(KEY_SEGMENT_BYTES).map(String::as_str), Some("65536"));
    assert_eq!(map.get(KEY_RETENTION_BYTES).map(String::as_str), Some(""));

    client
        .alter_configs(
            "events",
            vec![(KEY_RETENTION_BYTES.into(), "1024".into())],
        )
        .await
        .unwrap();
    let desc = client.describe_configs("events").await.unwrap();
    let map: std::collections::HashMap<_, _> = desc.configs.into_iter().collect();
    assert_eq!(map.get(KEY_RETENTION_BYTES).map(String::as_str), Some("1024"));

    // Clear
    client
        .alter_configs("events", vec![("retention.ms".into(), "".into())])
        .await
        .unwrap();
    let desc = client.describe_configs("events").await.unwrap();
    let map: std::collections::HashMap<_, _> = desc.configs.into_iter().collect();
    assert_eq!(map.get("retention.ms").map(String::as_str), Some(""));

    assert!(client.describe_configs("missing").await.is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn retention_bytes_drops_old_segments() {
    let dir = temp_dir("ret");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, broker) = start_broker(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.unwrap();
    // Tiny segments + small retention so we drop sealed segments.
    client
        .create_topic_with_configs(
            "t",
            1,
            vec![
                (KEY_SEGMENT_BYTES.into(), "80".into()),
                (KEY_RETENTION_BYTES.into(), "200".into()),
            ],
        )
        .await
        .unwrap();

    // Produce enough to force multiple segment rolls.
    for i in 0..40u32 {
        let payload = format!("msg-{i:04}-{}", "x".repeat(32));
        client
            .produce(
                "t",
                Some(0),
                vec![Message::from_value(Bytes::from(payload))],
            )
            .await
            .unwrap();
    }

    // Force retention now (background is 5s).
    broker.apply_retention_all().unwrap();

    // Re-open partition log from disk path to inspect size/segments.
    let part_dir = dir.join("t").join("0");
    let log = volant_storage::PartitionLog::open(StorageConfig {
        data_dir: part_dir,
        segment_size: 80,
        retention_bytes: Some(200),
        ..StorageConfig::default()
    })
    .unwrap();
    // After retention, total size should be constrained (active segment may exceed).
    // At least we should have fewer than 40 segments.
    assert!(
        log.segment_count() < 30,
        "expected retention to drop segments, got {}",
        log.segment_count()
    );
    // HWM / LEO still advanced.
    assert!(log.log_end_offset().raw() >= 40);

    let _ = addr;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_config_survives_store_reload() {
    let dir = temp_dir("dur");
    let _ = std::fs::remove_dir_all(&dir);
    {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let entries = vec![("retention.ms".into(), "1234".into())];
        broker
            .create_topic_with_configs("ev", 1, &entries)
            .unwrap();
    }
    let broker2 = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    // Topic itself is not auto-reloaded on single-node, but config file is.
    // Recreate topic name isn't needed — describe fails without live topic.
    // Load via alter after recreate would merge; check store directly.
    let store = volant_broker::TopicConfigStore::open(&dir).unwrap();
    let cfg = store.load("ev").unwrap();
    assert_eq!(cfg.retention_ms, Some(1234));

    let _ = broker2;
    let _ = std::fs::remove_dir_all(&dir);
}
