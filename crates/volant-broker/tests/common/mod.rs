#![allow(dead_code)]
//! Shared helpers for Kafka phase integration tests.
//!
//! Include from a test file with:
//! ```ignore
//! #[path = "common/mod.rs"]
//! mod common;
//! use common::{boot_kafka, rpc, temp_dir};
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::{serve_kafka_listener, Broker};
use volant_storage::StorageConfig;

/// Unique temp data directory for a phase test (`prefix` e.g. `"p76"`, `label` e.g. `"toc"`).
pub fn temp_dir(prefix: &str, label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-{prefix}-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Broker with default storage config under a unique temp dir.
#[allow(dead_code)]
pub fn broker_temp(prefix: &str, label: &str) -> (PathBuf, Arc<Broker>) {
    let dir = temp_dir(prefix, label);
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    (dir, broker)
}

/// Bind an ephemeral Kafka shim listener and spawn the accept loop.
pub async fn boot_kafka(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_kafka_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

/// Send one size-prefixed Kafka request body and return the response body (no size prefix).
pub async fn rpc(addr: &str, request: BytesMut) -> BytesMut {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&request).await.unwrap();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        if buf.len() >= 4 {
            let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if buf.len() >= 4 + size {
                let _ = buf.split_to(4);
                return buf.split_to(size);
            }
        }
    }
    panic!("connection closed without full kafka response");
}
