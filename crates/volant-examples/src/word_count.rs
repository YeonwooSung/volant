//! Word-count stream example.
//!
//! Topology: source → flat_map(tokenize) → count_reduce → sink.
//!
//! At-least-once: consumer offsets are committed **after** successful sink produce.
//!
//! # Usage
//!
//! ```text
//! cargo run -p volant-server -- --data-dir /tmp/v --listen 127.0.0.1:9092
//! cargo run -p volant-cli -- topic create lines --partitions 1 --broker 127.0.0.1:9092
//! cargo run -p volant-cli -- topic create counts --partitions 1 --broker 127.0.0.1:9092
//! cargo run -p volant-examples --bin word-count -- --broker 127.0.0.1:9092
//! cargo run -p volant-cli -- produce lines --value "hello world" --broker 127.0.0.1:9092
//! cargo run -p volant-cli -- consume counts --partition 0 --from 0 --broker 127.0.0.1:9092
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use tracing::info;
use volant_client::{Client, ClientConfig};
use volant_core::{Offset, Record};
use volant_stream::{SourceConfig, StreamApp, StreamBuilder};

/// Word-count stream processing example for Volant Phase 4.
#[derive(Debug, Parser)]
#[command(
    name = "word-count",
    version,
    about = "Count words from a source topic and produce running counts to a sink topic"
)]
struct Args {
    /// Broker address (`host:port`).
    #[arg(long, default_value = "127.0.0.1:9092")]
    broker: String,

    /// Consumer group id for the source topic.
    #[arg(long, default_value = "word-count")]
    group: String,

    /// Source topic (UTF-8 lines).
    #[arg(long, default_value = "lines")]
    source: String,

    /// Sink topic (word → count records).
    #[arg(long, default_value = "counts")]
    sink: String,

    /// Session timeout for the consumer group (ms).
    #[arg(long, default_value_t = 10_000)]
    session_timeout_ms: u32,

    /// Idle poll sleep when no records (ms).
    #[arg(long, default_value_t = 200)]
    poll_idle_ms: u64,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    info!(
        broker = %args.broker,
        group = %args.group,
        source = %args.source,
        sink = %args.sink,
        "starting word-count"
    );

    let client = Arc::new(
        Client::connect(ClientConfig {
            brokers: vec![args.broker.clone()],
            client_id: "word-count".into(),
            ..ClientConfig::default()
        })
        .await
        .with_context(|| format!("connect to {}", args.broker))?,
    );

    let topology = StreamBuilder::new("word-count")
        .source_topic(
            args.source.clone(),
            SourceConfig {
                group_id: args.group.clone(),
                session_timeout_ms: args.session_timeout_ms,
            },
        )
        .flat_map(split_words)
        .reduce_count()
        .sink_topic(args.sink.clone())
        .build()
        .context("build topology")?;

    let mut app = StreamApp::start(client, topology)
        .await
        .context("start stream app")?;
    info!("word-count running (ctrl-c to stop)");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down");
                break;
            }
            result = app.step() => {
                result.context("stream step")?;
                tokio::time::sleep(Duration::from_millis(args.poll_idle_ms)).await;
            }
        }
    }

    app.shutdown().await.context("shutdown")?;
    Ok(())
}
