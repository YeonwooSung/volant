//! v0.8: cross-app EOS fencing via `application_id`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::{Message, Offset};
use volant_storage::StorageConfig;
use volant_stream::{
    app_fence_transactional_id, ProcessingGuarantee, SourceConfig, StreamApp, StreamBuilder,
    APP_FENCE_TXN_SUFFIX,
};

// ── helpers ──────────────────────────────────────────────────────────────

fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-stream-v08-{}-{}-{}",
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
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

async fn produce_line(client: &Client, topic: &str, line: &str) {
    client
        .produce(
            topic,
            Some(0),
            vec![Message::from_value(Bytes::from(line.to_owned()))],
        )
        .await
        .expect("produce");
}

fn is_fenced_err(err: &volant_core::Error) -> bool {
    let msg = err.to_string();
    msg.contains("fenced")
        || msg.contains("error_code=19")
        || msg.to_ascii_lowercase().contains("epoch")
}

async fn step_until_committed(app: &mut StreamApp, client: &Client, group: &str, topic: &str) {
    for _ in 0..8 {
        app.step().await.expect("eos step");
        tokio::task::yield_now().await;
        let offs = client.fetch_offsets(group, vec![]).await.expect("offsets");
        if offs
            .iter()
            .any(|e| e.topic == topic && e.partition == 0 && e.offset >= 1)
        {
            return;
        }
    }
    panic!("expected group {group} to commit an offset on {topic}");
}

// ── unit ─────────────────────────────────────────────────────────────────

#[test]
fn fence_transactional_id_format() {
    assert_eq!(APP_FENCE_TXN_SUFFIX, "::__volant_app_fence");
    assert_eq!(
        app_fence_transactional_id("word-count"),
        "word-count::__volant_app_fence"
    );
    assert_eq!(
        app_fence_transactional_id("app-1"),
        "app-1::__volant_app_fence"
    );
}

#[test]
fn builder_exactly_once_has_no_application_id() {
    let topo = StreamBuilder::new("eos")
        .source_topic("in", SourceConfig::new("g"))
        .map(|r| Ok(r))
        .sink_topic("out")
        .exactly_once("txn-app-1")
        .build()
        .expect("build");
    assert_eq!(
        topo.processing_guarantee,
        ProcessingGuarantee::ExactlyOnce {
            transactional_id: "txn-app-1".into(),
            application_id: None,
        }
    );
    assert_eq!(topo.processing_guarantee.application_id(), None);
}

#[test]
fn builder_exactly_once_app_sets_both() {
    let topo = StreamBuilder::new("eos")
        .source_topic("in", SourceConfig::new("g"))
        .map(|r| Ok(r))
        .sink_topic("out")
        .exactly_once_app("my-app", "tid-a")
        .build()
        .expect("build");
    assert_eq!(
        topo.processing_guarantee,
        ProcessingGuarantee::ExactlyOnce {
            transactional_id: "tid-a".into(),
            application_id: Some("my-app".into()),
        }
    );
    assert_eq!(topo.processing_guarantee.application_id(), Some("my-app"));
}

#[test]
fn builder_application_id_chain() {
    let topo = StreamBuilder::new("eos")
        .source_topic("in", SourceConfig::new("g"))
        .map(|r| Ok(r))
        .sink_topic("out")
        .exactly_once("tid-a")
        .application_id("chained-app")
        .build()
        .expect("build");
    assert_eq!(
        topo.processing_guarantee,
        ProcessingGuarantee::ExactlyOnce {
            transactional_id: "tid-a".into(),
            application_id: Some("chained-app".into()),
        }
    );
}

#[test]
fn builder_empty_application_id_is_absent() {
    let topo = StreamBuilder::new("eos")
        .source_topic("in", SourceConfig::new("g"))
        .map(|r| Ok(r))
        .sink_topic("out")
        .exactly_once_app("", "tid-a")
        .build()
        .expect("build");
    assert_eq!(
        topo.processing_guarantee,
        ProcessingGuarantee::ExactlyOnce {
            transactional_id: "tid-a".into(),
            application_id: None,
        }
    );
}

// ── live e2e ─────────────────────────────────────────────────────────────

/// Regression: `exactly_once(tid)` without application_id still produce + commit.
#[tokio::test]
async fn exactly_once_without_application_id_still_works() {
    let dir = temp_data_dir("regression");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    client.create_topic("in-reg", 1).await.expect("create in");
    client.create_topic("out-reg", 1).await.expect("create out");

    produce_line(&client, "in-reg", "hello").await;

    let topology = StreamBuilder::new("reg")
        .source_topic("in-reg", SourceConfig::new("reg-cg"))
        .map(|r| Ok(r))
        .sink_topic("out-reg")
        .exactly_once("reg-tid")
        .build()
        .expect("build");

    match &topology.processing_guarantee {
        ProcessingGuarantee::ExactlyOnce {
            transactional_id,
            application_id,
        } => {
            assert_eq!(transactional_id, "reg-tid");
            assert_eq!(application_id, &None);
        }
        other => panic!("expected ExactlyOnce, got {other:?}"),
    }

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");

    for _ in 0..6 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    let fetched = client
        .fetch("out-reg", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch");
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_ref(), b"hello");

    let offs = client
        .fetch_offsets("reg-cg", vec![])
        .await
        .expect("offsets");
    let line_off = offs
        .iter()
        .find(|e| e.topic == "in-reg" && e.partition == 0)
        .expect("in-reg offset");
    assert_eq!(line_off.offset, 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Same application_id + different transactional_id: B fences A; B can step.
#[tokio::test]
async fn same_application_id_different_tid_fences_first() {
    let dir = temp_data_dir("same-app");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    for topic in ["in-a", "out-a", "in-b", "out-b"] {
        client.create_topic(topic, 1).await.expect("create topic");
    }

    produce_line(&client, "in-a", "from-a-1").await;
    produce_line(&client, "in-b", "from-b-1").await;

    let topo_a = StreamBuilder::new("app-a")
        .source_topic("in-a", SourceConfig::new("cg-a"))
        .map(|r| Ok(r))
        .sink_topic("out-a")
        .exactly_once_app("shared-app", "tid-a")
        .build()
        .expect("build a");

    let mut app_a = StreamApp::start(Arc::clone(&client), topo_a)
        .await
        .expect("start a");
    step_until_committed(&mut app_a, &client, "cg-a", "in-a").await;

    // More input so A has work after B starts (fence is checked even on empty).
    produce_line(&client, "in-a", "from-a-2").await;

    let topo_b = StreamBuilder::new("app-b")
        .source_topic("in-b", SourceConfig::new("cg-b"))
        .map(|r| Ok(r))
        .sink_topic("out-b")
        .exactly_once_app("shared-app", "tid-b")
        .build()
        .expect("build b");

    let mut app_b = StreamApp::start(Arc::clone(&client), topo_b)
        .await
        .expect("start b");

    let err = app_a
        .step()
        .await
        .expect_err("app A must be fenced after B starts");
    assert!(
        is_fenced_err(&err),
        "expected fenced / invalid-epoch error, got {err}"
    );

    step_until_committed(&mut app_b, &client, "cg-b", "in-b").await;

    let fetched = client
        .fetch("out-b", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch b");
    assert!(
        !fetched.records.is_empty(),
        "app B should produce after fencing A"
    );

    let _ = app_a.shutdown().await;
    app_b.shutdown().await.expect("shutdown b");
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Different application_id: two apps do not fence each other.
#[tokio::test]
async fn different_application_id_does_not_fence() {
    let dir = temp_data_dir("diff-app");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    for topic in ["in-a", "out-a", "in-b", "out-b"] {
        client.create_topic(topic, 1).await.expect("create topic");
    }

    produce_line(&client, "in-a", "alpha").await;
    produce_line(&client, "in-b", "beta").await;

    let topo_a = StreamBuilder::new("app-a")
        .source_topic("in-a", SourceConfig::new("cg-a"))
        .map(|r| Ok(r))
        .sink_topic("out-a")
        .exactly_once_app("app-one", "tid-a")
        .build()
        .expect("build a");
    let topo_b = StreamBuilder::new("app-b")
        .source_topic("in-b", SourceConfig::new("cg-b"))
        .map(|r| Ok(r))
        .sink_topic("out-b")
        .exactly_once_app("app-two", "tid-b")
        .build()
        .expect("build b");

    let mut app_a = StreamApp::start(Arc::clone(&client), topo_a)
        .await
        .expect("start a");
    step_until_committed(&mut app_a, &client, "cg-a", "in-a").await;

    let mut app_b = StreamApp::start(Arc::clone(&client), topo_b)
        .await
        .expect("start b");
    step_until_committed(&mut app_b, &client, "cg-b", "in-b").await;

    // Further empty / extra steps on A must still succeed (not fenced).
    produce_line(&client, "in-a", "alpha-2").await;
    app_a.step().await.expect("app A still live after B");
    app_b.step().await.expect("app B still live");

    let fetched_a = client
        .fetch("out-a", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch a");
    let fetched_b = client
        .fetch("out-b", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch b");
    assert!(!fetched_a.records.is_empty(), "app A produced");
    assert!(!fetched_b.records.is_empty(), "app B produced");

    app_a.shutdown().await.expect("shutdown a");
    app_b.shutdown().await.expect("shutdown b");
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
