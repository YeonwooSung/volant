//! Word-count stream topology example.
//!
//! Prerequisites: a running broker with topics `lines` and `counts`.
//!
//! ```bash
//! # terminal 1
//! cargo run -p volant-server -- --data-dir /tmp/vdata --listen 127.0.0.1:9092
//!
//! # terminal 2 — create topics
//! cargo run -p volant-cli -- topic create lines --partitions 1 --broker 127.0.0.1:9092
//! cargo run -p volant-cli -- topic create counts --partitions 1 --broker 127.0.0.1:9092
//!
//! # terminal 3 — run word-count
//! cargo run -p volant-stream --example word_count -- --broker 127.0.0.1:9092
//!
//! # terminal 4 — produce lines and observe counts
//! cargo run -p volant-cli -- produce lines --value "the quick brown fox" --broker 127.0.0.1:9092
//! cargo run -p volant-cli -- consume counts --partition 0 --from 0 --max 50 --broker 127.0.0.1:9092
//! ```

use std::env;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use volant_client::{Client, ClientConfig};
use volant_core::{Offset, Record, Result};
use volant_stream::{SourceConfig, StreamApp, StreamBuilder};

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

fn parse_broker() -> String {
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--broker" {
            if let Some(v) = args.next() {
                return v;
            }
        } else if let Some(v) = a.strip_prefix("--broker=") {
            return v.to_owned();
        }
    }
    "127.0.0.1:9092".to_owned()
}

#[tokio::main]
async fn main() -> Result<()> {
    let broker = parse_broker();
    eprintln!("word-count connecting to {broker}");

    let client = Arc::new(
        Client::connect(ClientConfig {
            brokers: vec![broker],
            ..ClientConfig::default()
        })
        .await?,
    );

    let topology = StreamBuilder::new("word-count")
        .source_topic("lines", SourceConfig::new("wc-app"))
        .flat_map(split_words)
        .reduce_count()
        .sink_topic("counts")
        .build()?;

    let mut app = StreamApp::start(client, topology).await?;
    eprintln!("word-count running (ctrl-c to stop); source=lines sink=counts");

    // Run until process is killed. Small sleep between empty polls.
    loop {
        app.step().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
