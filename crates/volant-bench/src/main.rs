//! Multi-mode micro-benchmark harness for Volant storage and broker paths.
//!
//! ```text
//! cargo run -p volant-bench --release -- append
//! cargo run -p volant-bench --release -- fetch
//! cargo run -p volant-bench --release -- produce-batch
//! ```
//!
//! Prefer `--release` for published numbers. Default paths use only standard
//! storage (mmap reads, std writes) so macOS builds work without optional
//! `io-uring` / `direct-io` features.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use volant_broker::Broker;
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
use volant_storage::{PartitionLog, StorageConfig};

/// Default message count for meaningful throughput samples.
const DEFAULT_COUNT: u64 = 100_000;
/// Default payload size in bytes.
const DEFAULT_VALUE_SIZE: usize = 100;
/// Default batch size for produce-batch mode.
const DEFAULT_BATCH_SIZE: usize = 100;
/// Fetch chunk size (messages per `read` call).
const FETCH_CHUNK: usize = 1024;

/// Micro-benchmarks for Volant partition log and in-process broker paths.
#[derive(Debug, Parser)]
#[command(
    name = "volant-bench",
    about = "Volant micro-benchmark harness (append / fetch / produce-batch)",
    long_about = "Measures msgs/s and MB/s for storage append, sequential fetch, \
and in-process broker batch produce. Uses temporary directories that are cleaned \
up after each run. Run with --release for meaningful numbers."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Measure single-partition `PartitionLog::append` throughput.
    ///
    /// Opens a temp log, appends N fixed-size messages, prints msgs/s and MB/s.
    Append {
        /// Number of messages to append.
        #[arg(long, default_value_t = DEFAULT_COUNT)]
        count: u64,

        /// Value payload size in bytes.
        #[arg(long, default_value_t = DEFAULT_VALUE_SIZE)]
        value_size: usize,

        /// Flush (fsync) every N messages; 0 = no intermediate flush.
        #[arg(long, default_value_t = 0)]
        flush_every: u64,
    },

    /// Measure sequential `PartitionLog::read` throughput after a pre-fill.
    ///
    /// Setup (append) is not timed; only the sequential fetch phase is.
    Fetch {
        /// Number of messages to write then read.
        #[arg(long, default_value_t = DEFAULT_COUNT)]
        count: u64,

        /// Value payload size in bytes (used for pre-fill and MB/s).
        #[arg(long, default_value_t = DEFAULT_VALUE_SIZE)]
        value_size: usize,
    },

    /// Measure in-process `Broker::produce` with multi-message batches.
    ProduceBatch {
        /// Total number of messages to produce.
        #[arg(long, default_value_t = DEFAULT_COUNT)]
        count: u64,

        /// Messages per `produce` call.
        #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
        batch_size: usize,

        /// Value payload size in bytes.
        #[arg(long, default_value_t = DEFAULT_VALUE_SIZE)]
        value_size: usize,
    },
}

/// RAII guard that removes a temp directory on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn create(prefix: &str) -> Result<Self> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}-{nanos}"));
        fs::create_dir_all(&path)
            .with_context(|| format!("create temp dir {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Append {
            count,
            value_size,
            flush_every,
        } => run_append(count, value_size, flush_every),
        Commands::Fetch { count, value_size } => run_fetch(count, value_size),
        Commands::ProduceBatch {
            count,
            batch_size,
            value_size,
        } => run_produce_batch(count, batch_size, value_size),
    }
}

fn bench_storage_config(data_dir: PathBuf, flush_every_n: u64) -> StorageConfig {
    StorageConfig {
        data_dir,
        // Large enough that segment roll does not dominate micro-benches.
        segment_size: 256 * 1024 * 1024,
        use_mmap: true,
        flush_every_n,
        ..StorageConfig::default()
    }
}

fn make_payload(value_size: usize, tag: u64) -> Vec<u8> {
    let mut payload = vec![b'x'; value_size.max(4)];
    // Vary the first bytes so the compiler cannot elide the loop body.
    let bytes = (tag as u32).to_le_bytes();
    payload[..4].copy_from_slice(&bytes);
    // Truncate if caller asked for smaller than 4 (still valid).
    if value_size < 4 {
        payload.truncate(value_size);
    }
    payload
}

fn run_append(count: u64, value_size: usize, flush_every: u64) -> Result<()> {
    let tmp = TempDir::create("volant-bench-append")?;
    let config = bench_storage_config(tmp.path().to_path_buf(), flush_every);
    let mut log = PartitionLog::open(config).context("open PartitionLog for append")?;

    let start = Instant::now();
    for i in 0..count {
        let payload = make_payload(value_size, i);
        log.append(Message::from_value(payload))
            .with_context(|| format!("append message {i}"))?;
    }
    // Ensure durability accounting is consistent for flush_every == 0 runs.
    log.flush().context("final flush after append")?;
    let elapsed = start.elapsed();

    let payload_bytes = count.saturating_mul(value_size as u64);
    print_report(
        "append",
        count,
        value_size,
        payload_bytes,
        elapsed,
        &[
            ("flush_every", flush_every.to_string()),
            ("high_watermark", log.high_watermark().to_string()),
        ],
    );
    Ok(())
}

fn run_fetch(count: u64, value_size: usize) -> Result<()> {
    let tmp = TempDir::create("volant-bench-fetch")?;
    let config = bench_storage_config(tmp.path().to_path_buf(), 0);
    let mut log = PartitionLog::open(config).context("open PartitionLog for fetch setup")?;

    // Pre-fill (not timed).
    for i in 0..count {
        let payload = make_payload(value_size, i);
        log.append(Message::from_value(payload))
            .with_context(|| format!("setup append {i}"))?;
    }
    log.flush().context("flush before fetch")?;

    // Sequential read phase (timed).
    let start = Instant::now();
    let mut from = Offset::ZERO;
    let mut read_total = 0u64;
    loop {
        let records = log
            .read(from, FETCH_CHUNK)
            .context("PartitionLog::read")?;
        if records.is_empty() {
            break;
        }
        read_total += records.len() as u64;
        from = records
            .last()
            .map(|r| r.offset.next())
            .expect("non-empty records");
    }
    let elapsed = start.elapsed();

    if read_total != count {
        anyhow::bail!("fetch read {read_total} messages, expected {count}");
    }

    let payload_bytes = count.saturating_mul(value_size as u64);
    print_report(
        "fetch",
        count,
        value_size,
        payload_bytes,
        elapsed,
        &[("chunk", FETCH_CHUNK.to_string())],
    );
    Ok(())
}

fn run_produce_batch(count: u64, batch_size: usize, value_size: usize) -> Result<()> {
    if batch_size == 0 {
        anyhow::bail!("--batch-size must be >= 1");
    }

    let tmp = TempDir::create("volant-bench-produce-batch")?;
    let config = bench_storage_config(tmp.path().to_path_buf(), 0);
    let broker = Broker::new(config);
    let topic = TopicName::new("bench");
    broker
        .create_topic(topic.clone(), 1)
        .context("create topic")?;
    let partition = PartitionId(0);

    let mut remaining = count;
    let mut batches = 0u64;
    let start = Instant::now();
    while remaining > 0 {
        let n = (remaining as usize).min(batch_size);
        let mut batch = MessageBatch {
            messages: Vec::with_capacity(n),
        };
        let base = count - remaining;
        for i in 0..n {
            let payload = make_payload(value_size, base + i as u64);
            batch.messages.push(Message::from_value(payload));
        }
        broker
            .produce(&topic, partition, batch)
            .with_context(|| format!("produce batch of {n}"))?;
        remaining -= n as u64;
        batches += 1;
    }
    broker
        .flush(&topic, partition)
        .context("final broker flush")?;
    let elapsed = start.elapsed();

    let payload_bytes = count.saturating_mul(value_size as u64);
    let secs = elapsed_secs(elapsed);
    let batches_per_sec = if secs > 0.0 {
        batches as f64 / secs
    } else {
        f64::INFINITY
    };

    print_report(
        "produce-batch",
        count,
        value_size,
        payload_bytes,
        elapsed,
        &[
            ("batch_size", batch_size.to_string()),
            ("batches", batches.to_string()),
            ("batches/s", format!("{batches_per_sec:.0}")),
        ],
    );
    Ok(())
}

fn elapsed_secs(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64()
}

fn print_report(
    mode: &str,
    count: u64,
    value_size: usize,
    payload_bytes: u64,
    elapsed: Duration,
    extra: &[(&str, String)],
) {
    let secs = elapsed_secs(elapsed);
    let msgs_per_sec = if secs > 0.0 {
        count as f64 / secs
    } else {
        f64::INFINITY
    };
    let mb_per_sec = if secs > 0.0 {
        (payload_bytes as f64 / secs) / (1024.0 * 1024.0)
    } else {
        f64::INFINITY
    };

    println!("volant-bench — {mode}");
    println!("  messages   : {count}");
    println!("  value      : {value_size} bytes");
    println!("  payload    : {payload_bytes} bytes");
    println!("  elapsed    : {elapsed:.3?}");
    println!("  throughput : {msgs_per_sec:.0} msgs/s");
    println!("  bandwidth  : {mb_per_sec:.2} MB/s");
    for (k, v) in extra {
        println!("  {k:<10} : {v}");
    }
}
