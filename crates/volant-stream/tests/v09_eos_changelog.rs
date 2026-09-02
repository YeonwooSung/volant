//! v0.9 — EOS changelog-backed durable state (txn 2PC MVP).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, FetchResult, TransactionalProducer};
use volant_core::{Message, Offset, Record, Result};
use volant_storage::StorageConfig;
use volant_stream::{
    changelog_message, ensure_changelog_topic, produce_changelog_in_txn, replay_changelog,
    DurableStore, KeyValueStore, SourceConfig, StreamApp, StreamBuilder, CHANGELOG_HEADER,
    CHANGELOG_VERSION, DEFAULT_CHANGELOG_TOPIC,
};

// ── helpers ──────────────────────────────────────────────────────────────

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

fn store_counts(store: &DurableStore) -> HashMap<String, u64> {
    let mut map = HashMap::new();
    for (k, v) in store.iter() {
        let key = String::from_utf8_lossy(&k).into_owned();
        let n = std::str::from_utf8(&v)
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
        "volant-stream-v09-{}-{}-{}",
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

async fn produce_lines(client: &Client, topic: &str, lines: &[&str]) {
    for line in lines {
        client
            .produce(
                topic,
                Some(0),
                vec![Message::from_value(Bytes::from((*line).to_owned()))],
            )
            .await
            .expect("produce line");
    }
}

// ── unit ─────────────────────────────────────────────────────────────────

#[test]
fn builder_changelog_default_off() {
    let topo = StreamBuilder::new("off")
        .source_topic("in", SourceConfig::new("g"))
        .sink_topic("out")
        .exactly_once("txn")
        .build()
        .expect("build");
    assert!(topo.changelog_topic.is_none());
}

#[test]
fn builder_changelog_default_name() {
    let topo = StreamBuilder::new("def")
        .source_topic("in", SourceConfig::new("g"))
        .sink_topic("out")
        .exactly_once("txn")
        .changelog()
        .build()
        .expect("build");
    assert_eq!(
        topo.changelog_topic.as_deref(),
        Some(DEFAULT_CHANGELOG_TOPIC)
    );
}

#[test]
fn builder_changelog_explicit_topic() {
    let topo = StreamBuilder::new("ex")
        .source_topic("in", SourceConfig::new("g"))
        .sink_topic("out")
        .exactly_once("txn")
        .changelog_topic("myapp__changelog")
        .build()
        .expect("build");
    assert_eq!(topo.changelog_topic.as_deref(), Some("myapp__changelog"));
}

#[test]
fn changelog_message_format() {
    let put = changelog_message(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")));
    assert_eq!(put.key.as_deref(), Some(b"k".as_ref()));
    assert_eq!(put.value.as_ref(), b"v");
    assert_eq!(
        put.headers,
        vec![(
            CHANGELOG_HEADER.to_string(),
            Bytes::from_static(CHANGELOG_VERSION)
        )]
    );
    let del = changelog_message(Bytes::from_static(b"k"), None);
    assert!(del.value.is_empty());
}

#[test]
fn staged_changelog_without_checkpoint_empty() {
    let dir = temp_data_dir("staged-unit");
    let mut store = DurableStore::open(&dir).expect("open");
    store.put(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
    assert!(store.staged_changelog().is_empty());
    store.begin_checkpoint();
    store.put(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
    let deltas = store.staged_changelog();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].0.as_ref(), b"b");
    assert_eq!(deltas[0].1.as_deref(), Some(b"2".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 1. Regression: EOS + DurableStore, no changelog (Phase 153) ──────────

#[tokio::test]
async fn eos_durable_without_changelog_still_commits_local() {
    let dir = temp_data_dir("reg");
    let state = dir.join("state");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    client.create_topic("reg-in", 1).await.expect("in");
    client.create_topic("reg-out", 1).await.expect("out");
    produce_lines(&client, "reg-in", &["hello hello world"]).await;

    let topology = StreamBuilder::new("reg")
        .state_dir(&state)
        .source_topic("reg-in", SourceConfig::new("reg-cg"))
        .flat_map(split_words)
        .reduce_count_durable()
        .expect("durable reduce")
        .sink_topic("reg-out")
        .exactly_once("v09-reg-txn")
        .build()
        .expect("build");
    assert!(topology.changelog_topic.is_none());

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");
    assert!(app.changelog_topic().is_none());
    for _ in 0..6 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    let store = DurableStore::open(&state).expect("reopen");
    let counts = store_counts(&store);
    assert_eq!(counts.get("hello"), Some(&2), "counts={counts:?}");
    assert_eq!(counts.get("world"), Some(&1), "counts={counts:?}");

    let fetched = client
        .fetch("reg-out", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch sink");
    let sink = final_counts(&fetched);
    assert_eq!(sink.get("hello"), Some(&2));
    assert_eq!(sink.get("world"), Some(&1));

    // Default changelog topic was never created.
    let meta = client.metadata().await.expect("meta");
    assert!(
        !meta
            .topics
            .iter()
            .any(|t| t.name == DEFAULT_CHANGELOG_TOPIC),
        "changelog must stay opt-in"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 2. Happy path: changelog put + sink after successful EOS step ────────

#[tokio::test]
async fn eos_changelog_happy_path() {
    let dir = temp_data_dir("happy");
    let state = dir.join("state");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    client.create_topic("hp-in", 1).await.expect("in");
    client.create_topic("hp-out", 1).await.expect("out");
    produce_lines(&client, "hp-in", &["hello hello world"]).await;

    let cl_topic = "happy__changelog";
    let topology = StreamBuilder::new("happy")
        .state_dir(&state)
        .source_topic("hp-in", SourceConfig::new("hp-cg"))
        .flat_map(split_words)
        .reduce_count_durable()
        .expect("durable reduce")
        .sink_topic("hp-out")
        .exactly_once("v09-happy-txn")
        .changelog_topic(cl_topic)
        .build()
        .expect("build");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");
    assert_eq!(app.changelog_topic(), Some(cl_topic));
    for _ in 0..6 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    let sink = client
        .fetch("hp-out", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch sink");
    let sink_counts = final_counts(&sink);
    assert_eq!(sink_counts.get("hello"), Some(&2), "sink={sink_counts:?}");
    assert_eq!(sink_counts.get("world"), Some(&1), "sink={sink_counts:?}");

    let cl = client
        .fetch(cl_topic, 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch changelog");
    assert!(
        !cl.records.is_empty(),
        "changelog must have committed puts after EOS step"
    );
    let cl_counts = final_counts(&cl);
    assert_eq!(cl_counts.get("hello"), Some(&2), "changelog={cl_counts:?}");
    assert_eq!(cl_counts.get("world"), Some(&1), "changelog={cl_counts:?}");
    for r in &cl.records {
        assert!(
            r.headers
                .iter()
                .any(|(k, v)| k == CHANGELOG_HEADER && v.as_ref() == CHANGELOG_VERSION),
            "missing volant-changelog=1 header: {:?}",
            r.headers
        );
    }

    let store = DurableStore::open(&state).expect("reopen");
    let local = store_counts(&store);
    assert_eq!(local.get("hello"), Some(&2));
    assert_eq!(local.get("world"), Some(&1));

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 3. Abort / empty step: no changelog records ──────────────────────────

#[tokio::test]
async fn empty_step_writes_no_changelog() {
    let dir = temp_data_dir("empty");
    let state = dir.join("state");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    client.create_topic("em-in", 1).await.expect("in");
    client.create_topic("em-out", 1).await.expect("out");
    let cl_topic = "empty__changelog";

    let topology = StreamBuilder::new("empty")
        .state_dir(&state)
        .source_topic("em-in", SourceConfig::new("em-cg"))
        .flat_map(split_words)
        .reduce_count_durable()
        .expect("durable reduce")
        .sink_topic("em-out")
        .exactly_once("v09-empty-txn")
        .changelog_topic(cl_topic)
        .build()
        .expect("build");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");
    for _ in 0..3 {
        app.step().await.expect("empty step");
    }
    app.shutdown().await.expect("shutdown");

    let cl = client
        .fetch(cl_topic, 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch changelog");
    assert!(
        cl.records.is_empty(),
        "empty steps must not append changelog records: {:?}",
        cl.records
    );
    let sink = client
        .fetch("em-out", 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch sink");
    assert!(sink.records.is_empty());

    let store = DurableStore::open(&state).expect("reopen");
    assert!(
        store.is_empty(),
        "empty steps must not commit durable state"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 4. Replay: fresh DurableStore dir from changelog ─────────────────────

#[tokio::test]
async fn replay_rebuilds_fresh_store() {
    let dir = temp_data_dir("replay");
    let state = dir.join("state");
    let fresh = dir.join("fresh");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    client.create_topic("rp-in", 1).await.expect("in");
    client.create_topic("rp-out", 1).await.expect("out");
    produce_lines(&client, "rp-in", &["alpha beta alpha"]).await;

    let cl_topic = "replay__changelog";
    let topology = StreamBuilder::new("replay")
        .state_dir(&state)
        .source_topic("rp-in", SourceConfig::new("rp-cg"))
        .flat_map(split_words)
        .reduce_count_durable()
        .expect("durable reduce")
        .sink_topic("rp-out")
        .exactly_once("v09-replay-txn")
        .changelog_topic(cl_topic)
        .build()
        .expect("build");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");
    for _ in 0..6 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    let original = DurableStore::open(&state).expect("original");
    let expected = store_counts(&original);
    assert_eq!(expected.get("alpha"), Some(&2));
    assert_eq!(expected.get("beta"), Some(&1));
    drop(original);

    let replayed = DurableStore::open_with_changelog(&fresh, &client, cl_topic)
        .await
        .expect("open_with_changelog");
    assert_eq!(store_counts(&replayed), expected);

    // Helper path: empty store + replay_changelog.
    let helper_dir = dir.join("helper");
    let mut helper = DurableStore::open(&helper_dir).expect("helper open");
    replay_changelog(&mut helper, &client, cl_topic)
        .await
        .expect("replay_changelog");
    assert_eq!(store_counts(&helper), expected);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 5. Txn fail / abort: staged local + changelog not committed ──────────

#[tokio::test]
async fn txn_abort_hides_changelog_and_staging() {
    let dir = temp_data_dir("abort");
    let state = dir.join("state");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    let cl_topic = "abort__changelog";
    ensure_changelog_topic(&client, cl_topic)
        .await
        .expect("ensure changelog");

    // Stage locally, produce changelog in a txn, then abort.
    {
        let mut store = DurableStore::open(&state).expect("open");
        store.begin_checkpoint();
        store.put(Bytes::from_static(b"ghost"), Bytes::from_static(b"9"));
        assert_eq!(store.get(b"ghost").as_deref(), Some(b"9".as_ref()));

        let mut tp = TransactionalProducer::connect(vec![addr.clone()], "v09-abort-txn")
            .await
            .expect("txn connect");
        tp.begin().await.expect("begin");
        produce_changelog_in_txn(
            &tp,
            cl_topic,
            &[(Bytes::from_static(b"ghost"), Some(Bytes::from_static(b"9")))],
        )
        .await
        .expect("produce changelog");
        tp.abort().await.expect("abort txn");
        store.abort_checkpoint();
        assert_eq!(store.get(b"ghost"), None);
    }

    let cl = client
        .fetch(cl_topic, 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch changelog");
    assert!(
        cl.records.is_empty(),
        "native committed-only fetch must hide aborted changelog: {:?}",
        cl.records
    );
    let store = DurableStore::open(&state).expect("reopen");
    assert_eq!(store.get(b"ghost"), None);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fenced_endtxn_aborts_state_and_changelog() {
    let dir = temp_data_dir("fence");
    let state = dir.join("state");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    client.create_topic("fn-in", 1).await.expect("in");
    client.create_topic("fn-out", 1).await.expect("out");
    produce_lines(&client, "fn-in", &["one"]).await;

    let cl_topic = "fence__changelog";
    let txn_id = "v09-fence-txn";
    let topology = StreamBuilder::new("fence")
        .state_dir(&state)
        .source_topic("fn-in", SourceConfig::new("fn-cg"))
        .flat_map(split_words)
        .reduce_count_durable()
        .expect("durable reduce")
        .sink_topic("fn-out")
        .exactly_once(txn_id)
        .changelog_topic(cl_topic)
        .build()
        .expect("build");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start");

    // First EOS step inits the transactional producer and commits "one".
    let mut committed = false;
    for _ in 0..8 {
        app.step().await.expect("pre-fence step");
        let sink = client
            .fetch("fn-out", 0, Offset::ZERO, 100, 0)
            .await
            .expect("fetch sink");
        if !sink.records.is_empty() {
            committed = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(committed, "pre-fence EOS step should commit sink output");

    // Fence: InitProducerId bumps epoch and invalidates the app producer.
    let mut fencer = TransactionalProducer::connect(vec![addr.clone()], txn_id)
        .await
        .expect("fence connect");
    fencer.begin().await.expect("fence begin");

    produce_lines(&client, "fn-in", &["two"]).await;
    let mut saw_err = false;
    for _ in 0..8 {
        match app.step().await {
            Ok(()) => tokio::task::yield_now().await,
            Err(_) => {
                saw_err = true;
                break;
            }
        }
    }
    assert!(saw_err, "fenced EOS step must fail after new input");
    let _ = fencer.abort().await;
    app.shutdown().await.expect("shutdown");

    let store = DurableStore::open(&state).expect("reopen");
    let local = store_counts(&store);
    assert_eq!(local.get("one"), Some(&1), "committed pre-fence state kept");
    assert!(
        !local.contains_key("two"),
        "fenced step must abort staged key 'two', got {local:?}"
    );

    let cl = client
        .fetch(cl_topic, 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch changelog");
    let cl_counts = final_counts(&cl);
    assert_eq!(cl_counts.get("one"), Some(&1));
    assert!(
        !cl_counts.contains_key("two"),
        "fenced txn must not commit changelog for 'two': {cl_counts:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn produce_then_fence_endtxn_hides_changelog() {
    let dir = temp_data_dir("fence-end");
    let (addr, server) = boot_server(dir.join("broker")).await;
    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));

    let cl_topic = "fence-end__changelog";
    ensure_changelog_topic(&client, cl_topic)
        .await
        .expect("ensure");

    let txn_id = "v09-fence-end-txn";
    let mut tp = TransactionalProducer::connect(vec![addr.clone()], txn_id)
        .await
        .expect("txn");
    tp.begin().await.expect("begin");
    produce_changelog_in_txn(
        &tp,
        cl_topic,
        &[(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")))],
    )
    .await
    .expect("produce in txn");

    // New owner of transactional_id fences the open txn (invalid epoch).
    let mut fencer = TransactionalProducer::connect(vec![addr.clone()], txn_id)
        .await
        .expect("fence connect");
    fencer.begin().await.expect("fence begin");
    let commit = tp.commit().await;
    assert!(commit.is_err(), "EndTxn after fence must fail: {commit:?}");
    let _ = fencer.abort().await;

    let cl = client
        .fetch(cl_topic, 0, Offset::ZERO, 100, 0)
        .await
        .expect("fetch");
    assert!(
        cl.records.is_empty(),
        "uncommitted/fenced changelog must be hidden: {:?}",
        cl.records
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
