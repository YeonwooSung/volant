//! Phase 149 — durable stream state store (redb).

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use volant_core::{Offset, Record};
use volant_stream::{
    count_reduce, count_reduce_durable, count_reduce_with_store, DurableStore, KeyValueStore,
    MemoryStore, Operator, Pipeline, StreamBuilder,
};

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("volant-phase149-{label}-{nanos}"));
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

// ── 1. put / get / delete / len / iter ───────────────────────────────────

#[test]
fn durable_store_crud() {
    let dir = temp_dir("crud");
    let mut store = DurableStore::open(&dir).expect("open");
    assert!(store.is_empty());
    assert_eq!(store.path(), dir.as_path());

    store.put(Bytes::from_static(b"alpha"), Bytes::from_static(b"1"));
    store.put(Bytes::from_static(b"beta"), Bytes::from_static(b"2"));
    store.put(Bytes::from_static(b"gamma"), Bytes::from_static(b"3"));

    assert_eq!(store.len(), 3);
    assert_eq!(store.get(b"alpha").as_deref(), Some(b"1".as_ref()));
    assert_eq!(store.get(b"missing"), None);

    let keys: Vec<_> = store
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(&k).into_owned(),
                String::from_utf8_lossy(&v).into_owned(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            ("alpha".into(), "1".into()),
            ("beta".into(), "2".into()),
            ("gamma".into(), "3".into()),
        ]
    );

    store.delete(b"beta");
    assert_eq!(store.len(), 2);
    assert_eq!(store.get(b"beta"), None);
    store.flush().expect("flush");

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 2. restart: open, put, drop, reopen → keys present ───────────────────

#[test]
fn durable_store_survives_process_restart() {
    let dir = temp_dir("restart");
    {
        let mut store = DurableStore::open(&dir).expect("open");
        store.put(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        store.put(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        store.flush().expect("flush");
        // drop store / close db
    }
    {
        let store = DurableStore::open(&dir).expect("reopen");
        assert_eq!(store.get(b"k1").as_deref(), Some(b"v1".as_ref()));
        assert_eq!(store.get(b"k2").as_deref(), Some(b"v2".as_ref()));
        assert_eq!(store.len(), 2);
        let n = store.iter().count();
        assert_eq!(n, 2);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 3. count_reduce with DurableStore + restart aggregates ───────────────

#[test]
fn count_reduce_durable_survives_restart() {
    let dir = temp_dir("reduce");

    // First process: count some records.
    {
        let mut reduce = count_reduce_durable(&dir).expect("open reduce");
        for r in [
            count_rec(b"foo", "1"),
            count_rec(b"bar", "1"),
            count_rec(b"foo", "1"),
        ] {
            reduce.process(r).expect("process");
        }
        assert_eq!(reduce.get(b"foo").as_deref(), Some(b"2".as_ref()));
        assert_eq!(reduce.get(b"bar").as_deref(), Some(b"1".as_ref()));
        reduce.store().flush().expect("flush");
    }

    // Second process: aggregates still present; continue counting.
    {
        let mut reduce = count_reduce_durable(&dir).expect("reopen reduce");
        assert_eq!(reduce.get(b"foo").as_deref(), Some(b"2".as_ref()));
        assert_eq!(reduce.get(b"bar").as_deref(), Some(b"1".as_ref()));

        reduce
            .process(count_rec(b"foo", "1"))
            .expect("process more");
        assert_eq!(reduce.get(b"foo").as_deref(), Some(b"3".as_ref()));
    }

    // count_reduce_with_store path
    {
        let store = DurableStore::open(&dir).expect("open store");
        let reduce = count_reduce_with_store(store);
        assert_eq!(reduce.get(b"foo").as_deref(), Some(b"3".as_ref()));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pipeline_with_durable_count_reduce() {
    let dir = temp_dir("pipeline");
    let reduce = count_reduce_durable(&dir).expect("open");
    let mut pipeline = Pipeline::new().then(reduce);
    let out = pipeline
        .process(vec![
            count_rec(b"a", "1"),
            count_rec(b"a", "1"),
            count_rec(b"b", "2"),
        ])
        .expect("process");
    assert_eq!(out.len(), 3);
    // final emissions: a=1, a=2, b=2
    assert_eq!(out[1].value.as_ref(), b"2");
    assert_eq!(out[2].value.as_ref(), b"2");

    drop(pipeline);
    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"a").as_deref(), Some(b"2".as_ref()));
    assert_eq!(store.get(b"b").as_deref(), Some(b"2".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}

// ── 4. MemoryStore still works ───────────────────────────────────────────

#[test]
fn memory_store_count_reduce_still_works() {
    let mut pipeline = Pipeline::new().then(count_reduce());
    let out = pipeline
        .process(vec![
            count_rec(b"x", "1"),
            count_rec(b"x", "1"),
            count_rec(b"y", "1"),
        ])
        .expect("process");
    assert_eq!(out.len(), 3);
    assert_eq!(out[1].value.as_ref(), b"2");
    assert_eq!(out[2].value.as_ref(), b"1");

    let mut mem = MemoryStore::new();
    mem.put(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
    assert_eq!(mem.get(b"k").as_deref(), Some(b"v".as_ref()));
    assert_eq!(mem.len(), 1);
}

#[test]
fn stream_builder_state_dir_and_reduce_count_durable() {
    let dir = temp_dir("builder");
    let pipeline = StreamBuilder::new("t")
        .state_dir(&dir)
        .reduce_count_durable()
        .expect("reduce durable")
        .build_pipeline();
    // Topology path: offline only here; state_dir propagates on full build when
    // source/sink set — smoke that builder accepts state_dir.
    drop(pipeline);

    {
        let mut p = StreamBuilder::new("t2")
            .state_dir(&dir)
            .reduce_count_durable()
            .expect("ok")
            .build_pipeline();
        let out = p
            .process(vec![count_rec(b"z", "1"), count_rec(b"z", "1")])
            .expect("process");
        assert_eq!(out.last().unwrap().value.as_ref(), b"2");
        // Drop pipeline (and its DurableStore) before reopening the same path.
    }

    let store = DurableStore::open(&dir).expect("reopen");
    assert_eq!(store.get(b"z").as_deref(), Some(b"2".as_ref()));
    let _ = std::fs::remove_dir_all(&dir);
}
