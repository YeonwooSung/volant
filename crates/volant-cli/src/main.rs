//! Volant command-line interface.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset};

/// Volant CLI — manage topics and produce/consume messages.
#[derive(Debug, Parser)]
#[command(name = "volant", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print version and project status.
    Version,
    /// Topic administration.
    Topic {
        #[command(subcommand)]
        action: TopicCmd,
    },
    /// Produce a message to a topic.
    Produce {
        /// Topic name.
        topic: String,
        /// Message value (UTF-8).
        #[arg(long)]
        value: String,
        /// Optional message key (UTF-8).
        #[arg(long)]
        key: Option<String>,
        /// Explicit partition (omit for broker assignment).
        #[arg(long)]
        partition: Option<u32>,
        /// Broker address (`host:port`).
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Consume (fetch) messages from a topic partition.
    Consume {
        /// Topic name.
        topic: String,
        /// Partition to read.
        #[arg(long)]
        partition: u32,
        /// Start offset.
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Maximum messages to fetch.
        #[arg(long, default_value_t = 100)]
        max: u32,
        /// Broker address (`host:port`).
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[derive(Debug, Subcommand)]
enum TopicCmd {
    /// List topics on the cluster.
    List {
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Create a topic.
    Create {
        /// Topic name.
        name: String,
        /// Partition count.
        #[arg(long, default_value_t = 1)]
        partitions: u32,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Delete a topic.
    Delete {
        /// Topic name.
        name: String,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Version => {
            println!("volant {}", env!("CARGO_PKG_VERSION"));
            println!("status: Phase 2 — networked produce/consume");
        }
        Commands::Topic { action } => match action {
            TopicCmd::List { broker } => {
                let client = connect(&broker).await?;
                let meta = client.metadata().await.context("metadata")?;
                if meta.topics.is_empty() {
                    println!("(no topics)");
                } else {
                    for t in meta.topics {
                        println!(
                            "{}\tid={}\tpartitions={}",
                            t.name,
                            t.topic_id,
                            t.partitions.len()
                        );
                    }
                }
            }
            TopicCmd::Create {
                name,
                partitions,
                broker,
            } => {
                let client = connect(&broker).await?;
                let id = client
                    .create_topic(&name, partitions)
                    .await
                    .with_context(|| format!("create topic '{name}'"))?;
                println!(
                    "created topic '{name}' id={} partitions={partitions}",
                    id.0
                );
            }
            TopicCmd::Delete { name, broker } => {
                let client = connect(&broker).await?;
                client
                    .delete_topic(&name)
                    .await
                    .with_context(|| format!("delete topic '{name}'"))?;
                println!("deleted topic '{name}'");
            }
        },
        Commands::Produce {
            topic,
            value,
            key,
            partition,
            broker,
        } => {
            let client = connect(&broker).await?;
            let mut msg = Message::from_value(Bytes::from(value.clone()));
            if let Some(k) = key {
                msg.key = Some(Bytes::from(k));
            }
            let result = client
                .produce(&topic, partition, vec![msg])
                .await
                .with_context(|| format!("produce to '{topic}'"))?;
            println!(
                "produced topic={} partition={} base_offset={} count={}",
                result.topic, result.partition, result.base_offset, result.count
            );
        }
        Commands::Consume {
            topic,
            partition,
            from,
            max,
            broker,
        } => {
            let client = connect(&broker).await?;
            let result = client
                .fetch(&topic, partition, Offset::new(from), max, 0)
                .await
                .with_context(|| format!("consume from '{topic}' p{partition}"))?;
            if result.records.is_empty() {
                println!(
                    "(no records) topic={} partition={} hwm={}",
                    result.topic, result.partition, result.high_watermark
                );
            } else {
                for r in &result.records {
                    let key = r
                        .key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).into_owned())
                        .unwrap_or_else(|| "-".into());
                    let val = String::from_utf8_lossy(&r.value);
                    println!(
                        "offset={} key={} value={} ts={}",
                        r.offset, key, val, r.timestamp_ms
                    );
                }
                println!(
                    "fetched {} record(s) hwm={}",
                    result.records.len(),
                    result.high_watermark
                );
            }
        }
    }
    Ok(())
}

async fn connect(broker: &str) -> Result<Client> {
    if broker.is_empty() {
        bail!("broker address must not be empty");
    }
    Client::connect(ClientConfig {
        brokers: vec![broker.to_owned()],
        client_id: "volant-cli".into(),
    })
    .await
    .with_context(|| format!("connect to broker {broker}"))
}
