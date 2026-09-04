//! v0.240: native ListOffsets isolation trailer (latest = LSO).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker, IdempotentCheck};
use volant_client::Client;
use volant_core::{Error, Message, PartitionId, TopicName};
use volant_protocol::{LIST_OFFSETS_LATEST, LIST_OFFSETS_READ_COMMITTED};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-v240-{label}-{}-{}",
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
fn open_txn_uncommitted_latest_ahead_of_lso() {
    let dir = temp_dir("lso");
    let _ = std::fs::remove_dir_all(&dir);
    let topic = TopicName::new("t");
    let broker = Broker::new(storage(&dir));
    broker.create_topic(topic.clone(), 1).unwrap();
    broker
        .produce_one(&topic, PartitionId(0), Message::from_value("seed"))
        .unwrap();
    broker.flush(&topic, PartitionId(0)).unwrap();

    let (pid, epoch) = broker.init_producer_id_with_txn("txn-lo");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(pid, epoch, "t", 0, 0, vec![Message::from_value("unstable")]) {
        IdempotentCheck::Accept { .. } => {}
        other => panic!("produce rejected: {other:?}"),
    }

    let uncommitted = broker.list_offsets("t", &[0]).unwrap();
    let committed = broker
        .list_offsets_isolated("t", &[0], LIST_OFFSETS_LATEST, LIST_OFFSETS_READ_COMMITTED)
        .unwrap();
    assert_eq!(uncommitted.len(), 1);
    assert_eq!(committed.len(), 1);
    assert!(
        uncommitted[0].2 > committed[0].2,
        "uncommitted latest {} should exceed committed LSO {}",
        uncommitted[0].2,
        committed[0].2
    );
    assert_eq!(committed[0].2, broker.last_stable_offset("t", 0));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn isolation_2_is_invalid() {
    let dir = temp_dir("bad");
    let _ = std::fs::remove_dir_all(&dir);
    let broker = Broker::new(storage(&dir));
    broker.create_topic(TopicName::new("t"), 1).unwrap();

    let err = broker
        .list_offsets_isolated("t", &[0], LIST_OFFSETS_LATEST, 2)
        .unwrap_err();
    match err {
        Error::InvalidArgument(m) => {
            assert!(m.contains("isolation"), "{m}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn isolation_via_client() {
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
        .produce("wire", Some(0), vec![Message::from_value("seed")])
        .await
        .unwrap();

    let (pid, epoch) = broker.init_producer_id_with_txn("txn-wire");
    assert_eq!(broker.begin_txn(pid, epoch), 0);
    match broker.buffer_txn_produce(
        pid,
        epoch,
        "wire",
        0,
        0,
        vec![Message::from_value("unstable")],
    ) {
        IdempotentCheck::Accept { .. } => {}
        other => panic!("produce rejected: {other:?}"),
    }

    let uncommitted = client.list_offsets("wire", vec![0]).await.unwrap();
    let committed = client
        .list_offsets_committed("wire", vec![0])
        .await
        .unwrap();
    assert!(
        uncommitted.entries[0].latest > committed.entries[0].latest,
        "uncommitted {} committed {}",
        uncommitted.entries[0].latest,
        committed.entries[0].latest
    );
    assert_eq!(
        committed.entries[0].latest,
        broker.last_stable_offset("wire", 0)
    );

    let err = client
        .list_offsets_at_isolated("wire", vec![0], LIST_OFFSETS_LATEST, 2)
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
