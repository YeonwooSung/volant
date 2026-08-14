//! Offline word-count pipeline + optional live broker e2e.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::Client;
use volant_core::{Message, Offset, Record, Result};
use volant_storage::StorageConfig;
use volant_stream::{
    count_reduce, filter, flat_map, foreach, map, process_pipeline, record_from_value, Pipeline,
    SourceConfig, StreamApp, StreamBuilder, TumblingWindow,
};

// ── helpers ──────────────────────────────────────────────────────────────

fn line_record(text: &str) -> Record {
    record_from_value(Bytes::from(text.to_owned()), 0)
}

/// Split a line into (word, "1") records. Keys are lowercased alphanumeric tokens.
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

fn word_count_pipeline() -> Pipeline {
    Pipeline::new()
        .then(flat_map(split_words))
        .then(count_reduce())
}

/// Collapse running reduce emissions to final count per key.
fn final_counts(emitted: &[Record]) -> HashMap<String, u64> {
    let mut map = HashMap::new();
    for r in emitted {
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

// ── unit-style operator tests ────────────────────────────────────────────

#[test]
fn map_filter_foreach_flat_map() {
    let mut pipeline = Pipeline::new()
        .then(map(|mut r| {
            r.value = Bytes::from(format!("x-{}", String::from_utf8_lossy(&r.value)));
            Ok(r)
        }))
        .then(filter(|r| r.value.as_ref() != b"x-skip"))
        .then(foreach(|_| {
            // side-effect only; covered further in foreach_side_effect
        }))
        .then(flat_map(|r| {
            Ok(vec![
                r.clone(),
                Record {
                    offset: Offset::ZERO,
                    key: r.key.clone(),
                    value: Bytes::from_static(b"dup"),
                    timestamp_ms: r.timestamp_ms,
                    headers: Vec::new(),
                },
            ])
        }));

    let out = pipeline
        .process(vec![
            line_record("a"),
            line_record("skip"),
            line_record("b"),
        ])
        .expect("process");
    // a → x-a → kept → 2; skip → x-skip → filtered; b → x-b → 2
    assert_eq!(out.len(), 4);
    assert_eq!(out[0].value.as_ref(), b"x-a");
    assert_eq!(out[1].value.as_ref(), b"dup");
}

#[test]
fn foreach_side_effect() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    N.store(0, Ordering::SeqCst);
    let mut pipeline = Pipeline::new().then(foreach(|_| {
        N.fetch_add(1, Ordering::SeqCst);
    }));
    let out = pipeline
        .process(vec![line_record("a"), line_record("b")])
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(N.load(Ordering::SeqCst), 2);
}

#[test]
fn reduce_counts_keys() {
    let mut pipeline = Pipeline::new().then(count_reduce());
    let inputs = vec![
        Record {
            offset: Offset::ZERO,
            key: Some(Bytes::from_static(b"foo")),
            value: Bytes::from_static(b"1"),
            timestamp_ms: 0,
            headers: vec![],
        },
        Record {
            offset: Offset::ZERO,
            key: Some(Bytes::from_static(b"bar")),
            value: Bytes::from_static(b"1"),
            timestamp_ms: 0,
            headers: vec![],
        },
        Record {
            offset: Offset::ZERO,
            key: Some(Bytes::from_static(b"foo")),
            value: Bytes::from_static(b"1"),
            timestamp_ms: 0,
            headers: vec![],
        },
    ];
    let out = pipeline.process(inputs).unwrap();
    let counts = final_counts(&out);
    assert_eq!(counts.get("foo"), Some(&2));
    assert_eq!(counts.get("bar"), Some(&1));
}

#[test]
fn tumbling_window_emits_at_boundary() {
    let mut pipeline = Pipeline::new().then(TumblingWindow::new(1000));
    // window [0,1000): two foos
    let r1 = Record {
        offset: Offset::ZERO,
        key: Some(Bytes::from_static(b"foo")),
        value: Bytes::from_static(b"1"),
        timestamp_ms: 100,
        headers: vec![],
    };
    let r2 = Record {
        offset: Offset::ZERO,
        key: Some(Bytes::from_static(b"foo")),
        value: Bytes::from_static(b"1"),
        timestamp_ms: 200,
        headers: vec![],
    };
    // advance into next window → should emit previous
    let r3 = Record {
        offset: Offset::ZERO,
        key: Some(Bytes::from_static(b"bar")),
        value: Bytes::from_static(b"1"),
        timestamp_ms: 1500,
        headers: vec![],
    };
    let mut out = pipeline.process(vec![r1, r2, r3]).unwrap();
    // emit closed window for foo count=2
    let closed: Vec<_> = out
        .iter()
        .filter(|r| r.key.as_ref().map(|k| k.as_ref()) == Some(b"foo".as_ref()))
        .collect();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].value.as_ref(), b"2");

    // punctuate to flush remaining bar window
    out = pipeline.punctuate(2000).unwrap();
    let bars: Vec<_> = out
        .iter()
        .filter(|r| r.key.as_ref().map(|k| k.as_ref()) == Some(b"bar".as_ref()))
        .collect();
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].value.as_ref(), b"1");
}

// ── offline word-count (required) ────────────────────────────────────────

#[test]
fn offline_word_count_pipeline() {
    let mut pipeline = word_count_pipeline();
    let lines = vec![
        line_record("the quick brown fox"),
        line_record("the fox jumps"),
        line_record("quick quick fox"),
    ];
    let emitted = process_pipeline(&mut pipeline, lines, None).expect("word count");
    let counts = final_counts(&emitted);

    assert_eq!(counts.get("the"), Some(&2), "counts={counts:?}");
    assert_eq!(counts.get("quick"), Some(&3), "counts={counts:?}");
    assert_eq!(counts.get("brown"), Some(&1), "counts={counts:?}");
    assert_eq!(counts.get("fox"), Some(&3), "counts={counts:?}");
    assert_eq!(counts.get("jumps"), Some(&1), "counts={counts:?}");
}

#[test]
fn stream_builder_offline_pipeline() {
    let pipeline = StreamBuilder::new("word-count")
        .flat_map(split_words)
        .reduce_count()
        .build_pipeline();
    let mut pipeline = pipeline;
    let emitted = pipeline
        .process(vec![line_record("hello hello world")])
        .unwrap();
    let counts = final_counts(&emitted);
    assert_eq!(counts.get("hello"), Some(&2));
    assert_eq!(counts.get("world"), Some(&1));
}

// ── live e2e (boot broker) ───────────────────────────────────────────────

fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-stream-e2e-{}-{}-{}",
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

#[tokio::test]
async fn live_word_count_source_sink() {
    let dir = temp_data_dir("wc");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Arc::new(Client::connect_addr(&addr).await.expect("connect"));
    client.create_topic("lines", 1).await.expect("create lines");
    client
        .create_topic("counts", 1)
        .await
        .expect("create counts");

    // Produce input lines.
    for line in ["the quick brown fox", "the fox jumps", "quick quick fox"] {
        client
            .produce(
                "lines",
                Some(0),
                vec![Message::from_value(Bytes::from(line))],
            )
            .await
            .expect("produce line");
    }

    let topology = StreamBuilder::new("word-count")
        .source_topic("lines", SourceConfig::new("wc-app"))
        .flat_map(split_words)
        .reduce_count()
        .sink_topic("counts")
        .build()
        .expect("build topology");

    let mut app = StreamApp::start(Arc::clone(&client), topology)
        .await
        .expect("start app");

    // A few poll cycles to drain input.
    for _ in 0..5 {
        app.step().await.expect("step");
        tokio::task::yield_now().await;
    }
    app.shutdown().await.expect("shutdown");

    // Fetch counts topic and take last value per key.
    let fetched = client
        .fetch("counts", 0, Offset::ZERO, 1000, 0)
        .await
        .expect("fetch counts");
    assert!(!fetched.records.is_empty(), "expected sink output records");

    let mut counts: HashMap<String, u64> = HashMap::new();
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
        counts.insert(key, n);
    }

    assert_eq!(counts.get("the"), Some(&2), "counts={counts:?}");
    assert_eq!(counts.get("quick"), Some(&3), "counts={counts:?}");
    assert_eq!(counts.get("fox"), Some(&3), "counts={counts:?}");
    assert_eq!(counts.get("brown"), Some(&1), "counts={counts:?}");
    assert_eq!(counts.get("jumps"), Some(&1), "counts={counts:?}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
