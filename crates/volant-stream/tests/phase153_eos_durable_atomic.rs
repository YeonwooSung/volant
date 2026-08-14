//! Phase 153 — EOS + durable stream state atomic boundary (checkpoint staging).

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use volant_core::{Offset, Record};
use volant_stream::{
    count_reduce_durable, DurableStore, KeyValueStore, MemoryStore, Operator, Pipeline,
};

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-phase153-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn count_rec(key: &[u8], value: &str) -> Record {
    Record {
        offset: Offset::ZERO,
        key: Some(Bytes::copy_from_slice(key)),
        value: Bytes::from(value.to_owned()),
        timestamp_ms: 0,
        headers: vec![],
    }
}

// ── 1. Staging abort: get sees staged; abort → gone; reopen disk unchanged ─

#[test]
fn staging_abort_discards_and_leaves_disk() {
    let dir = temp_dir("abort");
    {
        let mut store = DurableStore::open(&dir).expect("open");
        store.put(Bytes::from_static(b"seed"), Bytes::from_static(b"1"));

        store.begin_checkpoint();
        assert!(store.in_checkpoint());
        store.put(Bytes::from_static(b"staged"), Bytes::from_static(b"2"));
        store.put(Bytes::from_static(b"seed"), Bytes::from_static(b"99")); // overwrite staged
        assert_eq!(store.get(b"staged").as_deref(), Some(b"2".as_ref()));
        assert_eq!(store.get(b"seed").as_deref(), Some(b"99".as_ref()));
        assert_eq!(store.len(), 2);

        store.abort_checkpoint();
        assert!(!store.in_checkpoint());
        assert_eq!(store.get(b"staged"), None);
        assert_eq!(store.get(b"seed").as_deref(), Some(b"1".as_ref()));
        assert_eq!(store.len(), 1);
    }
    // Reopen: disk never saw staged keys.
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"staged"), None);
    assert_eq!(store.get(b"seed").as_deref(), Some(b"1".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 2. Staging commit: after commit_checkpoint, reopen sees data ─────────

#[test]
fn staging_commit_persists_on_reopen() {
    let dir = temp_dir("commit");
    {
        let mut store = DurableStore::open(&dir).expect("open");
        store.begin_checkpoint();
        store.put(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        store.put(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        store.delete(b"missing"); // no-op on disk after commit
        store.commit_checkpoint().expect("commit");
        assert!(!store.in_checkpoint());
        assert_eq!(store.get(b"a").as_deref(), Some(b"1".as_ref()));
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"a").as_deref(), Some(b"1".as_ref()));
    assert_eq!(store.get(b"b").as_deref(), Some(b"2".as_ref()));
    assert_eq!(store.len(), 2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 3. Outside checkpoint: immediate put (ALO path) ──────────────────────

#[test]
fn outside_checkpoint_still_immediate() {
    let dir = temp_dir("immediate");
    {
        let mut store = DurableStore::open(&dir).expect("open");
        assert!(!store.in_checkpoint());
        store.put(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"k").as_deref(), Some(b"v".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 4. Reduce + checkpoint: abort after process leaves disk pre-step ─────

#[test]
fn reduce_checkpoint_abort_no_durable_advance() {
    let dir = temp_dir("reduce-abort");
    {
        let mut reduce = count_reduce_durable(&dir).expect("open reduce");
        // Seed committed state outside checkpoint (ALO-style).
        reduce
            .process(count_rec(b"hello", "1"))
            .expect("seed process");
        assert_eq!(reduce.get(b"hello").as_deref(), Some(b"1".as_ref()));

        // Simulate EOS step process under checkpoint, then crash/abort before EndTxn.
        reduce.begin_checkpoint();
        reduce
            .process(count_rec(b"hello", "1"))
            .expect("staged process");
        assert_eq!(reduce.get(b"hello").as_deref(), Some(b"2".as_ref()));
        reduce.abort_checkpoint();
        assert_eq!(reduce.get(b"hello").as_deref(), Some(b"1".as_ref()));
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"hello").as_deref(), Some(b"1".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 5. Reduce + checkpoint commit advances durable state ─────────────────

#[test]
fn reduce_checkpoint_commit_advances() {
    let dir = temp_dir("reduce-commit");
    {
        let mut reduce = count_reduce_durable(&dir).expect("open reduce");
        reduce.begin_checkpoint();
        reduce.process(count_rec(b"world", "3")).expect("process");
        assert_eq!(reduce.get(b"world").as_deref(), Some(b"3".as_ref()));
        reduce.commit_checkpoint().expect("commit");
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"world").as_deref(), Some(b"3".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 6. Pipeline fan-out: begin → process → abort leaves disk pre-step ────

#[test]
fn pipeline_checkpoint_order_abort() {
    let dir = temp_dir("pipe-abort");
    // Seed on disk (store dropped before pipeline reopens the same path).
    {
        let mut seed = count_reduce_durable(&dir).expect("seed reduce");
        seed.process(count_rec(b"x", "5")).expect("seed");
    }
    {
        let mut pipe = Pipeline::new().then(count_reduce_durable(&dir).expect("reduce"));
        pipe.begin_checkpoint();
        let out = pipe.process(vec![count_rec(b"x", "1")]).expect("process");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value.as_ref(), b"6");
        pipe.abort_checkpoint();
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"x").as_deref(), Some(b"5".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 7. Pipeline: begin → process → commit_checkpoint persists ────────────

#[test]
fn pipeline_checkpoint_order_commit() {
    let dir = temp_dir("pipe-commit");
    {
        let mut pipe = Pipeline::new().then(count_reduce_durable(&dir).expect("reduce"));
        pipe.begin_checkpoint();
        let out = pipe
            .process(vec![count_rec(b"y", "2"), count_rec(b"y", "3")])
            .expect("process");
        assert_eq!(out.last().unwrap().value.as_ref(), b"5");
        pipe.commit_checkpoint().expect("commit");
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"y").as_deref(), Some(b"5".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 8. MemoryStore checkpoints are no-ops (ephemeral) ────────────────────

#[test]
fn memory_store_checkpoint_noop() {
    let mut store = MemoryStore::new();
    store.begin_checkpoint();
    assert!(!store.in_checkpoint()); // default trait: always false
    store.put(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
    store.abort_checkpoint();
    // Abort is no-op; memory put already applied.
    assert_eq!(store.get(b"k").as_deref(), Some(b"v".as_ref()));
    store.commit_checkpoint().expect("commit ok");
}

// ── 9. Staging delete + overwrite semantics ──────────────────────────────

#[test]
fn staging_delete_and_reput() {
    let dir = temp_dir("del-reput");
    {
        let mut store = DurableStore::open(&dir).expect("open");
        store.put(Bytes::from_static(b"k"), Bytes::from_static(b"old"));
        store.begin_checkpoint();
        store.delete(b"k");
        assert_eq!(store.get(b"k"), None);
        store.put(Bytes::from_static(b"k"), Bytes::from_static(b"new"));
        assert_eq!(store.get(b"k").as_deref(), Some(b"new".as_ref()));
        store.commit_checkpoint().expect("commit");
    }
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"k").as_deref(), Some(b"new".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 10. Simulate full EOS unit order without broker ──────────────────────

#[test]
fn simulate_eos_step_order_commit_and_abort() {
    let dir = temp_dir("eos-sim");
    let mut reduce = count_reduce_durable(&dir).expect("open");

    // --- successful EOS-like step ---
    reduce.begin_checkpoint();
    reduce.process(count_rec(b"z", "1")).expect("process");
    // (broker txn would commit here)
    reduce.commit_checkpoint().expect("commit ckpt");
    assert_eq!(reduce.get(b"z").as_deref(), Some(b"1".as_ref()));

    // --- failed / aborted EOS-like step ---
    reduce.begin_checkpoint();
    reduce.process(count_rec(b"z", "1")).expect("process");
    assert_eq!(reduce.get(b"z").as_deref(), Some(b"2".as_ref()));
    // (broker txn fails → abort txn + abort checkpoint)
    reduce.abort_checkpoint();
    assert_eq!(reduce.get(b"z").as_deref(), Some(b"1".as_ref()));

    drop(reduce);
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"z").as_deref(), Some(b"1".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}
