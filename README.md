# Volant

**Lightweight, high-performance streaming message broker in Rust.**

Volant is a resource-efficient alternative to Apache Kafka, built for:

- **Fast messaging** — append-only logs, batch-oriented protocol, zero-copy reads
- **DMA-friendly I/O** — memory-mapped segments, with `io_uring` / O_DIRECT on the roadmap
- **Streaming processing** — first-class operators (`map`, `filter`, windows) without a heavy runtime
- **Small footprint** — native binary, predictable memory, simple operations

> Status: **Phase 1 complete** — durable segment log with recovery, retention, and broker produce/fetch.  
> APIs and on-disk formats may still change. See [ROADMAP.md](./ROADMAP.md) and the binding  
> [Phase 1 durable log spec](./docs/PHASE1_SPEC.md).

---

## Workspace layout

```
volant/
├── crates/
│   ├── volant-core       # Shared types & errors
│   ├── volant-protocol   # Wire protocol codec
│   ├── volant-storage    # Partition log / segments (DMA path)
│   ├── volant-broker     # Topics, produce/fetch logic
│   ├── volant-client     # Async producer/consumer SDK
│   ├── volant-stream     # Stream processing operators
│   ├── volant-server     # Broker binary
│   ├── volant-cli        # Admin CLI (`volant`)
│   └── volant-bench      # Storage micro-benchmarks
├── docs/
│   └── PHASE1_SPEC.md    # Binding durable-log format & API
├── ROADMAP.md
└── Cargo.toml            # Workspace root
```

---

## Quick start

**Requirements:** Rust 1.75+ (edition 2021)

```bash
# Clone and build
cargo build --workspace

# Run broker (in-process scaffold; network listener is Phase 2)
cargo run -p volant-server -- --data-dir ./data --listen 0.0.0.0:9092

# CLI
cargo run -p volant-cli -- version
cargo run -p volant-cli -- topic list

# Phase 1 append throughput micro-bench (≥100k × ~100-byte messages)
cargo run -p volant-bench --release
```

### In-process produce (library)

```rust
use volant_broker::Broker;
use volant_core::{Message, MessageBatch, PartitionId, TopicName};
use volant_storage::StorageConfig;

let broker = Broker::new(StorageConfig::default());
let topic = TopicName::new("events");
broker.create_topic(topic.clone(), 1).unwrap();

let record = broker
    .produce_one(&topic, PartitionId(0), Message::from_value("hello"))
    .unwrap();
println!("offset={}", record.offset);
```

---

## Phase 1 — Durable log

Phase 1 is **complete**. The broker produce/fetch path uses a durable append-only segment log:

- Segment `.log` / sparse `.index` files under `{data_dir}`
- Buffered sequential append, configurable fsync, segment roll
- `mmap` reads, crash recovery (torn-tail truncate), time/size retention

On-disk layout, record encoding, and the `PartitionLog` / `Segment` APIs are specified in  
**[docs/PHASE1_SPEC.md](./docs/PHASE1_SPEC.md)** (binding for implementers).

Throughput target (single partition, laptop): **≥ 200k small msgs/s** (measured ~570k on a laptop). Re-run with:

```bash
cargo run -p volant-bench --release
```

---

## Roadmap (summary)

| Phase | Focus | Status |
|------:|-------|--------|
| 0 | Workspace scaffold | **Done** |
| 1 | Durable segment log + recovery | **Done** |
| 2 | TCP protocol, multi-partition, client SDK | Planned |
| 3 | Consumer groups & offsets | Planned |
| 4 | Stream processing operators | Planned |
| 5 | io_uring / DMA-oriented I/O | Planned |
| 6 | Clustering & replication | Planned |
| 7 | Metrics, TLS, packaging, optional Kafka shim | Planned |

Details, exit criteria, and open design questions: **[ROADMAP.md](./ROADMAP.md)**.

---

## Design sketch

```
Producer / Consumer / Stream app
            │
            ▼
    volant-protocol (binary frames)
            │
            ▼
    volant-broker (topics · partitions · groups)
            │
            ▼
    volant-storage (mmap segments · future io_uring)
```

---

## License

Apache-2.0
