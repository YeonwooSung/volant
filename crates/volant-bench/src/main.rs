//! Phase 1 micro-benchmark: single-partition append throughput.
//!
//! Measures how fast `PartitionLog::append` can accept ~100-byte messages.
//! Run with: `cargo run -p volant-bench --release`

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use volant_core::Message;
use volant_storage::{PartitionLog, StorageConfig};

/// Number of messages to append (must be ≥ 100_000 for a meaningful sample).
const MESSAGE_COUNT: u64 = 100_000;

/// Approximate value payload size in bytes.
const VALUE_SIZE: usize = 100;

fn main() -> anyhow::Result<()> {
    let data_dir = unique_temp_dir("volant-bench")?;
    fs::create_dir_all(&data_dir)?;

    let config = StorageConfig {
        data_dir: data_dir.clone(),
        // Large enough that segment roll does not dominate this micro-bench.
        segment_size: 256 * 1024 * 1024,
        use_mmap: true,
        // Explicit flush at the end; do not fsync every append on the hot path.
        flush_every_n: 0,
        ..StorageConfig::default()
    };

    let mut log = PartitionLog::open(config)?;

    // Fixed payload (~100 bytes) to keep allocation cost predictable.
    let value = vec![b'x'; VALUE_SIZE];

    let start = Instant::now();
    for i in 0..MESSAGE_COUNT {
        // Vary the first few bytes so the compiler cannot elide the loop body.
        let mut payload = value.clone();
        let tag = (i as u32).to_le_bytes();
        payload[..4].copy_from_slice(&tag);
        log.append(Message::from_value(payload))?;
    }
    let elapsed = start.elapsed();

    let total = MESSAGE_COUNT;
    let secs = elapsed.as_secs_f64();
    let msgs_per_sec = if secs > 0.0 {
        total as f64 / secs
    } else {
        f64::INFINITY
    };

    println!("volant-bench — partition append");
    println!("  messages : {total}");
    println!("  value    : {VALUE_SIZE} bytes");
    println!("  elapsed  : {elapsed:.3?}");
    println!("  throughput: {msgs_per_sec:.0} msgs/s");
    println!("  high_watermark: {}", log.high_watermark());
    println!("  data_dir : {}", data_dir.display());

    // Best-effort cleanup so repeated runs do not fill /tmp.
    let _ = fs::remove_dir_all(&data_dir);

    Ok(())
}

fn unique_temp_dir(prefix: &str) -> anyhow::Result<PathBuf> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{nanos}"));
    Ok(dir)
}
