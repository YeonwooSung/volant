//! v0.238: native SCRAM-SHA-512 handshake (ScramFirst hash trailer 2).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, ClientConfig};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-v238-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

#[tokio::test]
async fn scram_sha512_handshake_succeeds() {
    let dir = temp_dir("hs");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    let (addr, server) = boot(Arc::clone(&broker)).await;

    let client = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        scram_username: Some("alice".into()),
        scram_password: Some("s3cret".into()),
        scram_hash: volant_protocol::SCRAM_HASH_SHA512,
        ..ClientConfig::default()
    })
    .await
    .expect("sha512 scram");
    client.create_topic("t", 1).await.expect("create");

    let via_helper = Client::connect_scram_sha512(&addr, "alice", "s3cret")
        .await
        .expect("connect_scram_sha512");
    let names = via_helper.list_scram_users().await.expect("list");
    assert!(names.iter().any(|n| n == "alice"));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
