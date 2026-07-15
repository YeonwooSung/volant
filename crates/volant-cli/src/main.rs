//! Volant command-line interface.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use volant_client::{Client, ClientConfig, GroupConsumer, TransactionalProducer};
use volant_core::{Message, Offset};
use volant_protocol::{OffsetCommitEntry, OffsetEntry};

/// Volant CLI — manage topics and produce/consume messages.
#[derive(Debug, Parser)]
#[command(name = "volant", version, about)]
struct Cli {
    /// Shared auth token (matches server `--auth-token` / `VOLANT_AUTH_TOKEN`).
    #[arg(long, global = true, env = "VOLANT_AUTH_TOKEN")]
    auth_token: Option<String>,

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
    /// Consumer group administration.
    Group {
        #[command(subcommand)]
        action: GroupCmd,
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
    /// Transactional produce (Phase 18): begin → produce → commit.
    Txn {
        #[command(subcommand)]
        action: TxnCmd,
    },
    /// Consume (fetch) messages from a topic partition, or via a consumer group.
    Consume {
        /// Topic name.
        topic: String,
        /// Partition to read (required unless `--group` is set).
        #[arg(long)]
        partition: Option<u32>,
        /// Start offset (standalone mode only).
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Maximum messages to fetch.
        #[arg(long, default_value_t = 100)]
        max: u32,
        /// Consumer group id. When set, joins the group, polls, commits, and leaves.
        #[arg(long)]
        group: Option<String>,
        /// Session timeout ms for group mode.
        #[arg(long, default_value_t = 10_000)]
        session_timeout_ms: u32,
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
        /// Retention window in milliseconds (Phase 13).
        #[arg(long)]
        retention_ms: Option<u64>,
        /// Retention size limit in bytes (Phase 13).
        #[arg(long)]
        retention_bytes: Option<u64>,
        /// Segment roll size in bytes (Phase 13).
        #[arg(long)]
        segment_bytes: Option<u64>,
        /// Cleanup policy: `delete` (default) or `compact` (Phase 16).
        #[arg(long)]
        cleanup_policy: Option<String>,
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
    /// Describe a topic (metadata + configs, Phase 13).
    Describe {
        /// Topic name.
        name: String,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Topic configuration (Phase 13).
    Config {
        #[command(subcommand)]
        action: TopicConfigCmd,
    },
    /// Delete records before an offset (Phase 14).
    DeleteRecords {
        /// Topic name.
        name: String,
        /// Partition id.
        #[arg(long)]
        partition: u32,
        /// Drop sealed segments entirely before this offset.
        #[arg(long)]
        before_offset: u64,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Increase partition count (Phase 15).
    AddPartitions {
        /// Topic name.
        name: String,
        /// Desired total partition count (must exceed current).
        #[arg(long)]
        total: u32,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Show earliest/latest offsets (Phase 15).
    Offsets {
        /// Topic name.
        name: String,
        /// Optional partition filter.
        #[arg(long)]
        partition: Option<u32>,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[derive(Debug, Subcommand)]
enum TopicConfigCmd {
    /// Show topic configs.
    Get {
        /// Topic name.
        name: String,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Set a topic config key. Empty `--value` clears the key.
    Set {
        /// Topic name.
        name: String,
        /// Config key (`retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`).
        #[arg(long)]
        key: String,
        /// Config value (empty string clears).
        #[arg(long, default_value = "")]
        value: String,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[derive(Debug, Subcommand)]
enum GroupCmd {
    /// Fetch committed offsets for a consumer group.
    FetchOffsets {
        /// Consumer group id.
        #[arg(long)]
        group: String,
        /// Optional topic filter.
        #[arg(long)]
        topic: Option<String>,
        /// Optional partition filter (requires `--topic`).
        #[arg(long)]
        partition: Option<u32>,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Commit an offset for a consumer group (admin; generation=0).
    Commit {
        /// Consumer group id.
        #[arg(long)]
        group: String,
        /// Topic name.
        #[arg(long)]
        topic: String,
        /// Partition id.
        #[arg(long)]
        partition: u32,
        /// Offset to commit (next offset to read).
        #[arg(long)]
        offset: u64,
        /// Optional metadata string.
        #[arg(long, default_value = "")]
        metadata: String,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Show consumer group lag (hwm − committed) per partition.
    Lag {
        /// Consumer group id.
        #[arg(long)]
        group: String,
        /// Optional topic filter.
        #[arg(long)]
        topic: Option<String>,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Describe live group membership and assignments (Phase 11).
    Describe {
        /// Consumer group id.
        #[arg(long)]
        group: String,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// List known consumer groups (Phase 12).
    List {
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Delete committed offsets (Phase 12). Empty filters delete all for the group.
    DeleteOffsets {
        /// Consumer group id.
        #[arg(long)]
        group: String,
        /// Optional topic filter.
        #[arg(long)]
        topic: Option<String>,
        /// Optional partition filter (requires `--topic`).
        #[arg(long)]
        partition: Option<u32>,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[derive(Debug, Subcommand)]
enum TxnCmd {
    /// Produce one or more messages in a single transaction then commit.
    Produce {
        /// Transactional id (fences prior owners).
        #[arg(long)]
        transactional_id: String,
        /// Topic name.
        #[arg(long)]
        topic: String,
        /// Partition id.
        #[arg(long)]
        partition: u32,
        /// Message value (UTF-8). Repeat for multiple messages on the same partition.
        #[arg(long = "value", required = true)]
        values: Vec<String>,
        /// Optional second topic/partition/value triple for multi-partition demo.
        #[arg(long)]
        topic2: Option<String>,
        #[arg(long)]
        partition2: Option<u32>,
        #[arg(long)]
        value2: Option<String>,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let auth = cli.auth_token.clone();
    match cli.command {
        Commands::Version => {
            println!("volant {}", env!("CARGO_PKG_VERSION"));
            println!("status: Phase 16 — log compaction (cleanup.policy)");
        }
        Commands::Topic { action } => match action {
            TopicCmd::List { broker } => {
                let client = connect(&broker, auth).await?;
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
                retention_ms,
                retention_bytes,
                segment_bytes,
                cleanup_policy,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                let mut configs = Vec::new();
                if let Some(ms) = retention_ms {
                    configs.push(("retention.ms".into(), ms.to_string()));
                }
                if let Some(b) = retention_bytes {
                    configs.push(("retention.bytes".into(), b.to_string()));
                }
                if let Some(s) = segment_bytes {
                    configs.push(("segment.bytes".into(), s.to_string()));
                }
                if let Some(p) = cleanup_policy {
                    configs.push(("cleanup.policy".into(), p));
                }
                let id = client
                    .create_topic_with_configs(&name, partitions, configs)
                    .await
                    .with_context(|| format!("create topic '{name}'"))?;
                println!(
                    "created topic '{name}' id={} partitions={partitions}",
                    id.0
                );
            }
            TopicCmd::Delete { name, broker } => {
                let client = connect(&broker, auth).await?;
                client
                    .delete_topic(&name)
                    .await
                    .with_context(|| format!("delete topic '{name}'"))?;
                println!("deleted topic '{name}'");
            }
            TopicCmd::Describe { name, broker } => {
                let client = connect(&broker, auth).await?;
                let desc = client
                    .describe_configs(&name)
                    .await
                    .with_context(|| format!("describe topic '{name}'"))?;
                println!(
                    "topic={}\tid={}\tpartitions={}",
                    desc.topic, desc.topic_id, desc.partition_count
                );
                for (k, v) in &desc.configs {
                    let disp = if v.is_empty() { "(unset)" } else { v.as_str() };
                    println!("  {k}={disp}");
                }
            }
            TopicCmd::Config { action } => match action {
                TopicConfigCmd::Get { name, broker } => {
                    let client = connect(&broker, auth).await?;
                    let desc = client
                        .describe_configs(&name)
                        .await
                        .with_context(|| format!("config get '{name}'"))?;
                    for (k, v) in &desc.configs {
                        let disp = if v.is_empty() { "(unset)" } else { v.as_str() };
                        println!("{k}={disp}");
                    }
                }
                TopicConfigCmd::Set {
                    name,
                    key,
                    value,
                    broker,
                } => {
                    let client = connect(&broker, auth).await?;
                    client
                        .alter_configs(&name, vec![(key.clone(), value.clone())])
                        .await
                        .with_context(|| format!("config set '{name}'"))?;
                    if value.is_empty() {
                        println!("cleared {key} on topic '{name}'");
                    } else {
                        println!("set {key}={value} on topic '{name}'");
                    }
                }
            },
            TopicCmd::DeleteRecords {
                name,
                partition,
                before_offset,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                let res = client
                    .delete_records(&name, partition, before_offset)
                    .await
                    .with_context(|| format!("delete-records '{name}' p{partition}"))?;
                println!(
                    "deleted records before {before_offset} on {name}/{partition}; low_watermark={}",
                    res.low_watermark
                );
            }
            TopicCmd::AddPartitions {
                name,
                total,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                let n = client
                    .create_partitions(&name, total)
                    .await
                    .with_context(|| format!("add-partitions '{name}' total={total}"))?;
                println!("topic '{name}' now has {n} partitions");
            }
            TopicCmd::Offsets {
                name,
                partition,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                let parts = partition.map(|p| vec![p]).unwrap_or_default();
                let res = client
                    .list_offsets(&name, parts)
                    .await
                    .with_context(|| format!("offsets '{name}'"))?;
                for e in res.entries {
                    println!(
                        "{}\tp{}\tearliest={}\tlatest={}",
                        res.topic, e.partition, e.earliest, e.latest
                    );
                }
            }
        },
        Commands::Group { action } => match action {
            GroupCmd::FetchOffsets {
                group,
                topic,
                partition,
                broker,
            } => {
                if partition.is_some() && topic.is_none() {
                    bail!("--partition requires --topic");
                }
                let client = connect(&broker, auth).await?;
                let entries = match (topic.as_deref(), partition) {
                    (Some(t), Some(p)) => vec![OffsetEntry {
                        topic: t.to_owned(),
                        partition: p,
                    }],
                    (Some(t), None) => {
                        // Fetch all then filter by topic.
                        let all = client
                            .fetch_offsets(&group, vec![])
                            .await
                            .context("fetch_offsets")?;
                        let filtered: Vec<_> = all
                            .into_iter()
                            .filter(|e| e.topic == t)
                            .collect();
                        print_offsets(&group, &filtered);
                        return Ok(());
                    }
                    (None, None) => vec![],
                    (None, Some(_)) => unreachable!(),
                };
                let fetched = client
                    .fetch_offsets(&group, entries)
                    .await
                    .context("fetch_offsets")?;
                print_offsets(&group, &fetched);
            }
            GroupCmd::Commit {
                group,
                topic,
                partition,
                offset,
                metadata,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                client
                    .commit_offsets(
                        &group,
                        "",
                        0, // admin commit: skip generation check
                        vec![OffsetCommitEntry {
                            topic: topic.clone(),
                            partition,
                            offset,
                            metadata,
                        }],
                    )
                    .await
                    .context("commit_offsets")?;
                println!(
                    "committed group={group} topic={topic} partition={partition} offset={offset}"
                );
            }
            GroupCmd::Lag {
                group,
                topic,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                let meta = client.metadata().await.context("metadata")?;
                let offsets = client
                    .fetch_offsets(&group, vec![])
                    .await
                    .context("fetch_offsets")?;
                let mut rows = Vec::new();
                for e in &offsets {
                    if let Some(ref t) = topic {
                        if e.topic != *t {
                            continue;
                        }
                    }
                    let hwm = meta
                        .topics
                        .iter()
                        .find(|t| t.name == e.topic)
                        .and_then(|t| t.partitions.iter().find(|p| p.partition_id == e.partition))
                        .map(|p| p.hwm)
                        .unwrap_or(0);
                    let committed = e.offset;
                    let lag = if committed == u64::MAX {
                        hwm
                    } else {
                        hwm.saturating_sub(committed)
                    };
                    rows.push((e.topic.clone(), e.partition, committed, hwm, lag));
                }
                // Also show partitions with no commit yet when topic filter set.
                if let Some(ref t) = topic {
                    if let Some(ti) = meta.topics.iter().find(|x| x.name == *t) {
                        for p in &ti.partitions {
                            if !rows.iter().any(|r| r.0 == *t && r.1 == p.partition_id) {
                                rows.push((t.clone(), p.partition_id, u64::MAX, p.hwm, p.hwm));
                            }
                        }
                    }
                }
                rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                if rows.is_empty() {
                    println!("(no lag data for group={group})");
                } else {
                    println!("group\ttopic\tpartition\tcommitted\thwm\tlag");
                    for (t, p, c, h, l) in rows {
                        let c_disp = if c == u64::MAX {
                            "-".to_string()
                        } else {
                            c.to_string()
                        };
                        println!("{group}\t{t}\t{p}\t{c_disp}\t{h}\t{l}");
                    }
                }
            }
            GroupCmd::Describe { group, broker } => {
                let client = connect(&broker, auth).await?;
                let desc = client
                    .describe_group(&group)
                    .await
                    .context("describe_group")?;
                println!(
                    "group={}\tgeneration={}\tmembers={}",
                    desc.group_id,
                    desc.generation,
                    desc.members.len()
                );
                for m in &desc.members {
                    let topics = m.topics.join(",");
                    let assigns: Vec<String> = m
                        .assignment
                        .iter()
                        .map(|a| format!("{}:{}", a.topic, a.partition))
                        .collect();
                    println!(
                        "  member={}\ttopics=[{}]\tassignment=[{}]",
                        m.member_id,
                        topics,
                        assigns.join(",")
                    );
                }
            }
            GroupCmd::List { broker } => {
                let client = connect(&broker, auth).await?;
                let groups = client.list_groups().await.context("list_groups")?;
                if groups.is_empty() {
                    println!("(no groups)");
                } else {
                    println!("group\tstate\tmembers\tgeneration");
                    for g in groups {
                        let state = match g.state {
                            volant_protocol::GroupState::Stable => "Stable",
                            volant_protocol::GroupState::Empty => "Empty",
                        };
                        println!(
                            "{}\t{}\t{}\t{}",
                            g.group_id, state, g.member_count, g.generation
                        );
                    }
                }
            }
            GroupCmd::DeleteOffsets {
                group,
                topic,
                partition,
                broker,
            } => {
                let client = connect(&broker, auth).await?;
                let entries = match (topic.as_deref(), partition) {
                    (Some(t), Some(p)) => vec![OffsetEntry {
                        topic: t.to_owned(),
                        partition: p,
                    }],
                    (Some(_), None) => {
                        bail!("--partition is required when --topic is set for delete-offsets")
                    }
                    (None, Some(_)) => {
                        bail!("--topic is required when --partition is set for delete-offsets")
                    }
                    (None, None) => vec![],
                };
                let result = client
                    .delete_offsets(&group, entries)
                    .await
                    .context("delete_offsets")?;
                println!(
                    "deleted_offsets group={group} count={}",
                    result.deleted_count
                );
            }
        },
        Commands::Txn { action } => match action {
            TxnCmd::Produce {
                transactional_id,
                topic,
                partition,
                values,
                topic2,
                partition2,
                value2,
                broker,
            } => {
                let mut tp = TransactionalProducer::connect(vec![broker], transactional_id)
                    .await
                    .context("connect transactional producer")?;
                tp.begin().await.context("begin transaction")?;
                let msgs: Vec<Message> = values
                    .into_iter()
                    .map(|v| Message::from_value(Bytes::from(v)))
                    .collect();
                let r1 = tp
                    .produce(&topic, Some(partition), msgs)
                    .await
                    .context("txn produce")?;
                println!(
                    "buffered topic={} partition={} count={} (offset pending commit)",
                    r1.topic, r1.partition, r1.count
                );
                if let (Some(t2), Some(p2), Some(v2)) = (topic2, partition2, value2) {
                    let r2 = tp
                        .produce(&t2, Some(p2), vec![Message::from_value(Bytes::from(v2))])
                        .await
                        .context("txn produce 2")?;
                    println!(
                        "buffered topic={} partition={} count={}",
                        r2.topic, r2.partition, r2.count
                    );
                }
                let results = tp.commit().await.context("commit transaction")?;
                for r in results {
                    println!(
                        "committed topic={} partition={} base_offset={} count={}",
                        r.topic, r.partition, r.base_offset, r.count
                    );
                }
            }
        },
        Commands::Produce {
            topic,
            value,
            key,
            partition,
            broker,
        } => {
            let client = connect(&broker, auth).await?;
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
            group,
            session_timeout_ms,
            broker,
        } => {
            if let Some(group_id) = group {
                // Group path: join → poll (until max msgs or empty) → commit → leave.
                let client = Arc::new(connect(&broker, auth.clone()).await?);
                let mut consumer = GroupConsumer::join(
                    client,
                    group_id.clone(),
                    vec![topic.clone()],
                    session_timeout_ms,
                )
                .await
                .with_context(|| format!("join group '{group_id}'"))?;

                println!(
                    "joined group={group_id} member={} generation={} assignment={:?} revoked={:?}",
                    consumer.member_id(),
                    consumer.generation(),
                    consumer.assignment(),
                    consumer.last_revoked()
                );

                let mut total = 0u32;
                loop {
                    let records = consumer.poll().await.context("group poll")?;
                    if records.is_empty() {
                        break;
                    }
                    for r in &records {
                        let key = r
                            .record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k).into_owned())
                            .unwrap_or_else(|| "-".into());
                        let val = String::from_utf8_lossy(&r.record.value);
                        println!(
                            "topic={} partition={} offset={} key={} value={} ts={}",
                            r.topic, r.partition, r.record.offset, key, val, r.record.timestamp_ms
                        );
                        total += 1;
                        if total >= max {
                            break;
                        }
                    }
                    if total >= max {
                        break;
                    }
                }
                consumer.commit().await.context("group commit")?;
                println!("committed offsets for group={group_id} ({total} record(s))");
                consumer.leave().await.context("group leave")?;
            } else {
                let partition = partition.ok_or_else(|| {
                    anyhow::anyhow!("--partition is required unless --group is set")
                })?;
                let client = connect(&broker, auth).await?;
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
    }
    Ok(())
}

fn print_offsets(group: &str, entries: &[volant_protocol::OffsetFetchEntry]) {
    if entries.is_empty() {
        println!("(no committed offsets) group={group}");
        return;
    }
    for e in entries {
        let off = if e.offset == u64::MAX {
            "unknown".to_string()
        } else {
            e.offset.to_string()
        };
        let meta = if e.metadata.is_empty() {
            "-".to_string()
        } else {
            e.metadata.clone()
        };
        println!(
            "group={} topic={} partition={} offset={} metadata={}",
            group, e.topic, e.partition, off, meta
        );
    }
}

async fn connect(broker: &str, auth_token: Option<String>) -> Result<Client> {
    if broker.is_empty() {
        bail!("broker address must not be empty");
    }
    Client::connect(ClientConfig {
        brokers: vec![broker.to_owned()],
        client_id: "volant-cli".into(),
        auth_token,
        ..ClientConfig::default()
    })
    .await
    .with_context(|| format!("connect to broker {broker}"))
}
