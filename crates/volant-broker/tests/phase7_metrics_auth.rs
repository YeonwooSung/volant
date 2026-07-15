//! Phase 7: metrics HTTP smoke + shared-token auth over TCP.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::{run_metrics_server, serve_listener, Broker};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset};
use volant_protocol::{ErrorCode, Request, Response};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p7-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot(auth: Option<&str>) -> (String, Arc<Broker>, tokio::task::JoinHandle<()>) {
    // Keep data_dir path owned by the broker for the life of the test; do not
    // delete it while the server task may still create topics.
    let dir = temp_dir("boot");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    }));
    if let Some(t) = auth {
        broker.set_auth_token(Some(t.to_owned()));
    }
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move {
        let _ = serve_listener(listener, b).await;
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), broker, handle)
}

#[tokio::test]
async fn metrics_endpoint_contains_volant_prefix() {
    let dir = temp_dir("metrics");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    }));
    let mlistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let maddr = mlistener.local_addr().unwrap();
    drop(mlistener);

    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = run_metrics_server(maddr, b).await;
    });
    // Give the metrics server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Also run a broker so we can produce once.
    let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let baddr = blistener.local_addr().unwrap();
    let b2 = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_listener(blistener, b2).await;
    });
    tokio::task::yield_now().await;

    let client = Client::connect_addr(format!("127.0.0.1:{}", baddr.port()))
        .await
        .unwrap();
    client.create_topic("m", 1).await.unwrap();
    client
        .produce(
            "m",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"hello-metrics"))],
        )
        .await
        .unwrap();

    // Scrape metrics.
    let mut stream = TcpStream::connect(maddr).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("volant_produce_requests_total"),
        "missing produce counter: {text}"
    );
    assert!(
        text.contains("volant_build_info"),
        "missing build_info: {text}"
    );
    assert!(
        text.contains("volant_connections_accepted_total"),
        "missing connections: {text}"
    );
    // At least one produce ok.
    assert!(
        text.contains("volant_produce_requests_total{result=\"ok\"}"),
        "missing produce ok: {text}"
    );
}

#[tokio::test]
async fn auth_required_without_token() {
    let (addr, _broker, _h) = boot(Some("correct-token")).await;
    // Connect without auth — create_topic must fail with AuthenticationRequired.
    let client = Client::connect_addr(&addr).await.expect("tcp connect");
    let err = client
        .create_topic("nope", 1)
        .await
        .expect_err("must require auth");
    let msg = err.to_string();
    assert!(
        msg.contains("authentication")
            || msg.contains(&format!("{}", ErrorCode::AuthenticationRequired as u16))
            || msg.to_lowercase().contains("auth"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn auth_wrong_token_fails() {
    let (addr, _broker, _h) = boot(Some("correct-token")).await;
    let err = Client::connect_with_auth(&addr, "wrong-token")
        .await
        .expect_err("wrong token must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("auth") || msg.contains("17") || msg.contains("fail"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn auth_success_produce_fetch() {
    let (addr, _broker, _h) = boot(Some("s3cret")).await;
    let client = Client::connect_with_auth(&addr, "s3cret")
        .await
        .expect("auth connect");
    client.create_topic("events", 1).await.expect("create");
    let pr = client
        .produce(
            "events",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"auth-ok"))],
        )
        .await
        .expect("produce");
    assert_eq!(pr.count, 1);
    let fr = client
        .fetch("events", 0, Offset::ZERO, 10, 0)
        .await
        .expect("fetch");
    assert_eq!(fr.records.len(), 1);
    assert_eq!(fr.records[0].value.as_ref(), b"auth-ok");
}

#[tokio::test]
async fn auth_disabled_works_without_token() {
    let (addr, _broker, _h) = boot(None).await;
    let client = Client::connect_addr(&addr).await.unwrap();
    client.create_topic("plain", 1).await.unwrap();
}

#[tokio::test]
async fn metrics_unit_render() {
    // Pure unit-style via public Metrics.
    let m = volant_broker::Metrics::new();
    m.record_produce(true, 2, 40);
    m.record_fetch(true, 1, 10);
    m.record_connection();
    let text = m.render_prometheus(3, 6, 2, "0.1.0-test", &[]);
    assert!(text.starts_with("# HELP") || text.contains("volant_"));
    assert!(text.contains("volant_topics 3"));
    assert!(text.contains("volant_partitions 6"));
}

// Silence unused import warnings if any feature-gated paths change.
#[allow(dead_code)]
fn _touch_protocol() {
    let _ = Request::Auth {
        token: "x".into(),
    };
    let _ = Response::Auth { error_code: 0 };
    let _ = ClientConfig::default();
}
