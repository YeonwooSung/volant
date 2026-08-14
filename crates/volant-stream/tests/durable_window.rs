//! Durable tumbling window buckets: survive process restart in one app.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use volant_core::{Offset, Record};
use volant_stream::{
    DurableStore, KeyValueStore, Operator, Pipeline, StreamStateError, TumblingWindow,
};

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-durable-window-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn rec(key: &[u8], value: &str, ts: i64) -> Record {
    Record {
        offset: Offset::ZERO,
        key: Some(Bytes::copy_from_slice(key)),
        value: Bytes::from(value.to_owned()),
        timestamp_ms: ts,
        headers: vec![],
    }
}

// ── 1. Write → drop → reopen same dir restores open buckets ──────────────

#[test]
fn durable_window_survives_restart() {
    let dir = temp_dir("restart");
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("open");
        assert!(w.process(rec(b"foo", "1", 100)).unwrap().is_empty());
        assert!(w.process(rec(b"foo", "1", 200)).unwrap().is_empty());
        let buckets = w.buckets();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].0, 0);
        assert_eq!(buckets[0].1.as_ref(), b"foo");
        assert_eq!(buckets[0].2, 2);
        assert_eq!(w.max_event_ms(), 200);
    }
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("reopen");
        let buckets = w.buckets();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].1.as_ref(), b"foo");
        assert_eq!(buckets[0].2, 2);
        assert_eq!(w.max_event_ms(), 200);

        // Advance into the next window: restored bucket must emit.
        let out = w.process(rec(b"bar", "1", 1500)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key.as_deref(), Some(b"foo".as_ref()));
        assert_eq!(out[0].value.as_ref(), b"2");
        assert_eq!(out[0].timestamp_ms, 0);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 2. Punctuate after reopen flushes restored buckets ───────────────────

#[test]
fn durable_window_punctuate_after_reopen() {
    let dir = temp_dir("punctuate");
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("open");
        w.process(rec(b"foo", "3", 100)).unwrap();
    }
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("reopen");
        let out = w.punctuate(2000).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key.as_deref(), Some(b"foo".as_ref()));
        assert_eq!(out[0].value.as_ref(), b"3");
        assert!(w.buckets().is_empty());
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 3. Checkpoint abort: window state does not advance on disk ───────────

#[test]
fn durable_window_checkpoint_abort() {
    let dir = temp_dir("ckpt-abort");
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("open");
        w.process(rec(b"foo", "1", 100)).unwrap();

        w.begin_checkpoint();
        w.process(rec(b"foo", "1", 200)).unwrap();
        assert_eq!(w.buckets()[0].2, 2);
        w.abort_checkpoint();
        assert_eq!(w.buckets()[0].2, 1);
        assert_eq!(w.max_event_ms(), 100);
    }
    {
        let w = TumblingWindow::durable(1000, &dir).expect("reopen window");
        assert_eq!(w.buckets().len(), 1);
        assert_eq!(w.buckets()[0].2, 1);
        assert_eq!(w.max_event_ms(), 100);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 4. Checkpoint commit persists buckets ────────────────────────────────

#[test]
fn durable_window_checkpoint_commit() {
    let dir = temp_dir("ckpt-commit");
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("open");
        w.begin_checkpoint();
        w.process(rec(b"foo", "2", 100)).unwrap();
        w.commit_checkpoint().expect("commit");
    }
    {
        let w = TumblingWindow::durable(1000, &dir).expect("reopen");
        assert_eq!(w.buckets().len(), 1);
        assert_eq!(w.buckets()[0].2, 2);
        assert_eq!(w.max_event_ms(), 100);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 5. Pipeline fan-out + abort leaves disk pre-step ─────────────────────

#[test]
fn pipeline_durable_window_checkpoint_abort() {
    let dir = temp_dir("pipe-abort");
    {
        let mut seed = TumblingWindow::durable(1000, &dir).expect("seed");
        seed.process(rec(b"x", "5", 100)).unwrap();
    }
    {
        let mut pipe = Pipeline::new().then(TumblingWindow::durable(1000, &dir).expect("window"));
        pipe.begin_checkpoint();
        let out = pipe.process(vec![rec(b"x", "1", 200)]).expect("process");
        assert!(out.is_empty());
        pipe.abort_checkpoint();
    }
    {
        let w = TumblingWindow::durable(1000, &dir).expect("reopen");
        assert_eq!(w.buckets()[0].2, 5);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 6. Default in-memory path still works (no durability) ────────────────

#[test]
fn memory_window_still_default() {
    let mut w = TumblingWindow::new(1000);
    assert!(w.process(rec(b"foo", "1", 100)).unwrap().is_empty());
    assert_eq!(w.buckets()[0].2, 1);
    // MemoryStore checkpoints are no-ops; abort does not roll back.
    w.begin_checkpoint();
    w.process(rec(b"foo", "1", 200)).unwrap();
    w.abort_checkpoint();
    assert_eq!(w.buckets()[0].2, 2);
}

// ── 7. Injected DurableStore via with_store ──────────────────────────────

#[test]
fn with_store_durable() {
    let dir = temp_dir("with-store");
    {
        let store = DurableStore::open(&dir).expect("open store");
        let mut w = TumblingWindow::with_store(1000, store);
        w.process(rec(b"k", "1", 50)).unwrap();
        assert_eq!(w.store().len(), 3); // bucket + max_event + size
    }
    {
        let store = DurableStore::open(&dir).expect("reopen store");
        assert!(store.get(b"\x00max_event_ms").is_some());
        assert!(store.get(b"\x00size_ms").is_some());
        let w = TumblingWindow::with_store(1000, store);
        assert_eq!(w.buckets()[0].2, 1);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 8. Reopen with a different size_ms is an error ───────────────────────

#[test]
fn durable_window_size_mismatch() {
    let dir = temp_dir("size-mismatch");
    {
        let mut w = TumblingWindow::durable(1000, &dir).expect("open");
        w.process(rec(b"foo", "1", 100)).unwrap();
    }
    let err = match TumblingWindow::durable(500, &dir) {
        Ok(_) => panic!("expected size mismatch, durable(500) succeeded"),
        Err(e) => e,
    };
    match err {
        StreamStateError::WindowSizeMismatch { stored, requested } => {
            assert_eq!(stored, 1000);
            assert_eq!(requested, 500);
        }
        other => panic!("expected WindowSizeMismatch, got {other}"),
    }
    // Matching size still opens.
    let w = TumblingWindow::durable(1000, &dir).expect("reopen matching");
    assert_eq!(w.buckets()[0].2, 1);
    let _ = std::fs::remove_dir_all(&dir);
}
