//! Offline word-count pipeline test (no broker).

use std::collections::HashMap;

use bytes::Bytes;
use volant_core::{Offset, Record};
use volant_stream::{count_reduce, flat_map, record_from_value, Pipeline};

fn line_record(text: &str) -> Record {
    record_from_value(Bytes::from(text.to_owned()), 0)
}

fn split_words(record: Record) -> volant_core::Result<Vec<Record>> {
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

#[test]
fn word_count_pipeline_counts_words() {
    let mut pipe = Pipeline::new()
        .then(flat_map(split_words))
        .then(count_reduce());
    let out = pipe
        .process(vec![
            line_record("the quick brown fox"),
            line_record("the fox"),
        ])
        .expect("pipeline");
    let counts = final_counts(&out);
    assert_eq!(counts.get("the"), Some(&2));
    assert_eq!(counts.get("fox"), Some(&2));
    assert_eq!(counts.get("quick"), Some(&1));
    assert_eq!(counts.get("brown"), Some(&1));
}
