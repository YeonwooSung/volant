//! Phase 151: stream exactly-once (EOS) MVP tests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, FetchResult};
use volant_core::{Message, Offset, Record, Result};
use volant_storage::StorageConfig;
use volant_stream::{
    process_pipeline, record_from_value, ProcessingGuarantee, SourceConfig, StreamApp,
    StreamBuilder,
};

// ── helpers ──────────────────────────────────────────────────────────────

fn line_record(text: &str) -> Record {
    record_from_value(Bytes::from(text.to_owned()), 0)
}

fn split_words(record: Record) -> Result<Vec<Record>> {
    let text = String::from_utf8_lossy(&record.value);
    let mut out = Vec::new();
    for raw in text.split_whitespace() {
        let word: String = raw
            .chars()
            .filter(|c| c.is_alphanumeric())
            .flat_map(|c| c.to_lowercase())
            .collect();
        if word.is_empty() {
            continue;
        }
        out.push(Record {
            offset: Offset::ZERO,
            key: Some(Bytes::from(word)),
            value: Bytes::from_static(b"1"),
            timestamp_ms: record.timestamp_ms,
            headers: Vec::new(),
        });
    }
    Ok(out)
}

fn final_counts(fetched: &FetchResult) -> HashMap<String, u64> {
    let mut map = HashMap::new();
    for r in &fetched.records {
        let key = r
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .unwrap_or_default();
        let n = std::str::from_utf8(&r.value)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        map.insert(key, n);
    }
    map
}

fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-stream-p151-{}-{}-{}",
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

// ── unit / offline ────────────────────────────────────────────────────────

#[test]
fn builder_exactly_once_sets_guarantee() {
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
        }
    );
}

#[test]
fn builder_default_is_at_least_once() {
    let topo = StreamBuilder::new("alo")
        .source_topic("in", SourceConfig::new("g"))
        .sink_topic("out")
        .build()
        .expect("build");
    assert_eq!(topo.processing_guarantee, ProcessingGuarantee::AtLeastOnce);
}

#[test]
fn offline_process_still_works() {
    let mut pipeline = StreamBuilder::new("offline")
        .flat_map(split_words)
        .reduce_count()
        .build_pipeline();
    let emitted = process_pipeline(&mut pipeline, vec![line_record("hello hello world")], None)
        .expect("process");
    let mut counts = HashMap::new();
    for r in &emitted {
        let key = r
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .unwrap_or_default();
        let n = std::str::from_utf8(&r.value)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        counts.insert(key, n);
    }
    assert_eq!(counts.get("hello"), Some(&2));
    assert_eq!(counts.get("world"), Some(&1));
}

// ── live e2e ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn alo_path_regression() {
    let dir = temp_data_dir("alo");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    client.create_topic("lines", 1).await.expect("create lines");
    client
        .create_topic("counts", 1)
        .await
        .expect("create counts");

    for line in ["a a", "b"] {
        client
            .produce(
                "lines",
                Some(0),
                vec![Message::from_value(Bytes::from(line))],
            )
            .await
            .expect("produce");
    }

    let topology = StreamBuilder::new("alo-wc")
        .source_topic("lines", SourceConfig::new("alo-cg"))
        .flat_map(split_words)
        .reduce_count()
        .sink_topic("counts")
        .build()
        .expect("build");

    assert_eq!(
        topology.processing_guarantee,
        ProcessingGuarantee::AtLeastOnce
    );

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");
    assert_eq!(
        app.processing_guarantee(),
        &ProcessingGuarantee::AtLeastOnce
    );

    for _ in 0..5 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    let fetched = client
        .fetch("counts", 0, Offset::ZERO, 1000, 0)
        .await
        .expect("fetch");
    let counts = final_counts(&fetched);
    assert_eq!(counts.get("a"), Some(&2), "counts={counts:?}");
    assert_eq!(counts.get("b"), Some(&1), "counts={counts:?}");

    // Group offsets should be committed (ALO path).
    let offs = client
        .fetch_offsets("alo-cg", vec![])
        .await
        .expect("fetch_offsets");
    let line_off = offs
        .iter()
        .find(|e| e.topic == "lines" && e.partition == 0)
        .expect("lines offset entry");
    assert!(
        line_off.offset >= 2,
        "expected committed offset ≥ 2, got {}",
        line_off.offset
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn eos_path_produce_and_commit_offsets() {
    let dir = temp_data_dir("eos");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    client
        .create_topic("lines-eos", 1)
        .await
        .expect("create lines");
    client
        .create_topic("counts-eos", 1)
        .await
        .expect("create counts");

    for line in ["the quick brown fox", "the fox jumps", "quick quick fox"] {
        client
            .produce(
                "lines-eos",
                Some(0),
                vec![Message::from_value(Bytes::from(line))],
            )
            .await
            .expect("produce line");
    }

    let topology = StreamBuilder::new("eos-wc")
        .source_topic("lines-eos", SourceConfig::new("eos-cg"))
        .flat_map(split_words)
        .reduce_count()
        .sink_topic("counts-eos")
        .exactly_once("stream-eos-txn-1")
        .build()
        .expect("build");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start eos");
    match app.processing_guarantee() {
        ProcessingGuarantee::ExactlyOnce { transactional_id } => {
            assert_eq!(transactional_id, "stream-eos-txn-1");
        }
        other => panic!("expected ExactlyOnce, got {other:?}"),
    }

    for _ in 0..8 {
        app.step().await.expect("eos step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    // Sink output present and correct after txn commits.
    let fetched = client
        .fetch("counts-eos", 0, Offset::ZERO, 1000, 0)
        .await
        .expect("fetch counts");
    assert!(
        !fetched.records.is_empty(),
        "expected sink output after EOS steps"
    );
    let counts = final_counts(&fetched);
    assert_eq!(counts.get("the"), Some(&2), "counts={counts:?}");
    assert_eq!(counts.get("quick"), Some(&3), "counts={counts:?}");
    assert_eq!(counts.get("fox"), Some(&3), "counts={counts:?}");
    assert_eq!(counts.get("brown"), Some(&1), "counts={counts:?}");
    assert_eq!(counts.get("jumps"), Some(&1), "counts={counts:?}");

    // Consumer group offsets committed atomically with the txn.
    let offs = client
        .fetch_offsets("eos-cg", vec![])
        .await
        .expect("fetch_offsets");
    let line_off = offs
        .iter()
        .find(|e| e.topic == "lines-eos" && e.partition == 0)
        .expect("lines-eos offset entry");
    assert_eq!(
        line_off.offset, 3,
        "expected next offset 3 after 3 input records, got {}",
        line_off.offset
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn eos_empty_step_noops() {
    let dir = temp_data_dir("empty");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    client.create_topic("empty-in", 1).await.expect("create in");
    client
        .create_topic("empty-out", 1)
        .await
        .expect("create out");

    let topology = StreamBuilder::new("empty-eos")
        .source_topic("empty-in", SourceConfig::new("empty-cg"))
        .map(|r| Ok(r))
        .sink_topic("empty-out")
        .exactly_once("stream-eos-empty")
        .build()
        .expect("build");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");

    // No input records → empty steps must succeed without error / txn.
    for _ in 0..3 {
        app.step().await.expect("empty step");
    }

    let fetched = client
        .fetch("empty-out", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch");
    assert!(
        fetched.records.is_empty(),
        "empty steps must not produce sink records"
    );

    // Offsets should remain uncommitted (no txn ran).
    let offs = client
        .fetch_offsets("empty-cg", vec![])
        .await
        .expect("fetch_offsets");
    assert!(
        offs.is_empty() || offs.iter().all(|e| e.offset == u64::MAX || e.offset == 0),
        "empty EOS steps should not commit meaningful offsets: {offs:?}"
    );

    app.shutdown().await.expect("shutdown");
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn start_exactly_once_api() {
    let dir = temp_data_dir("start-api");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    client.create_topic("api-in", 1).await.expect("create in");
    client.create_topic("api-out", 1).await.expect("create out");

    client
        .produce(
            "api-in",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"x"))],
        )
        .await
        .expect("produce");

    // Topology without exactly_once flag; start_exactly_once overrides.
    let topology = StreamBuilder::new("api")
        .source_topic("api-in", SourceConfig::new("api-cg"))
        .map(|r| Ok(r))
        .sink_topic("api-out")
        .build()
        .expect("build");

    let mut app = StreamApp::start_exactly_once(Arc::clone(&client), topology, "api-txn")
        .await
        .expect("start_exactly_once");
    assert!(matches!(
        app.processing_guarantee(),
        ProcessingGuarantee::ExactlyOnce { .. }
    ));

    for _ in 0..4 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    let fetched = client
        .fetch("api-out", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch");
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(fetched.records[0].value.as_ref(), b"x");

    let offs = client
        .fetch_offsets("api-cg", vec![])
        .await
        .expect("offsets");
    let line_off = offs
        .iter()
        .find(|e| e.topic == "api-in" && e.partition == 0)
        .expect("api-in offset");
    assert_eq!(line_off.offset, 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
