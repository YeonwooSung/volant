# Volant

**Lightweight, high-performance streaming message broker in Rust.**

Volant is a resource-efficient alternative to Apache Kafka, built for:

- **Fast messaging** — append-only logs, batch-oriented protocol, zero-copy reads
- **DMA-friendly I/O** — memory-mapped segments, with `io_uring` / O_DIRECT on the roadmap
- **Streaming processing** — first-class operators (`map`, `filter`, windows) without a heavy runtime
- **Small footprint** — native binary, predictable memory, simple operations

> Status: **Phase 4 complete** — in-process stream operators, word-count example, offline + live e2e.  
> APIs and on-disk formats may still change. See [ROADMAP.md](./ROADMAP.md),  
> [Phase 1](./docs/PHASE1_SPEC.md), [Phase 2](./docs/PHASE2_SPEC.md),  
> [Phase 3](./docs/PHASE3_SPEC.md), and [Phase 4](./docs/PHASE4_SPEC.md) specs.

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
│   ├── PHASE1_SPEC.md    # Binding durable-log format & API
│   ├── PHASE2_SPEC.md    # Binding TCP protocol & client/server API
│   ├── PHASE3_SPEC.md    # Consumer groups & offsets
│   └── PHASE4_SPEC.md    # Stream operators & topology API
├── ROADMAP.md
└── Cargo.toml            # Workspace root
```

---

## Quick start

**Requirements:** Rust 1.75+ (edition 2021)

```bash
# Clone and build
cargo build --workspace

# Terminal 1 — start the broker
cargo run -p volant-server -- --data-dir /tmp/vdata --listen 127.0.0.1:9092

# Terminal 2 — admin + produce/consume
cargo run -p volant-cli -- topic create events --partitions 3 --broker 127.0.0.1:9092
cargo run -p volant-cli -- produce events --value hello --broker 127.0.0.1:9092
cargo run -p volant-cli -- consume events --partition 0 --from 0 --max 10 --broker 127.0.0.1:9092
cargo run -p volant-cli -- topic list --broker 127.0.0.1:9092

# Consumer groups (Phase 3)
cargo run -p volant-cli -- group commit --group my-cg --topic events --partition 0 --offset 10 --broker 127.0.0.1:9092
cargo run -p volant-cli -- group fetch-offsets --group my-cg --broker 127.0.0.1:9092
cargo run -p volant-cli -- consume events --group my-cg --max 10 --broker 127.0.0.1:9092

# Phase 1 append throughput micro-bench
cargo run -p volant-bench --release
```

### Networked client (library)

```rust
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset};

// inside an async context:
let client = Client::connect(ClientConfig {
    brokers: vec!["127.0.0.1:9092".into()],
    ..ClientConfig::default()
})
.await?;

client.create_topic("events", 3).await?;
client
    .produce("events", None, vec![Message::from_value("hello")])
    .await?;
let batch = client
    .fetch("events", 0, Offset::ZERO, 10, 0)
    .await?;
```

### In-process produce (library)

```rust
use volant_broker::Broker;
use volant_core::{Message, PartitionId, TopicName};
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

## Phase 2 — Network protocol

Phase 2 is **complete** for the core path:

- Framed TCP server (`volant-server` / `volant_broker::net`)
- Wire payloads for Produce / Fetch / CreateTopic / DeleteTopic / Metadata
- Multi-partition key routing (murmur2) and null-key round-robin
- Async `volant-client` SDK + `volant` CLI
- Localhost e2e tests (`crates/volant-client/tests/e2e_tcp.rs`)

Binding details: **[docs/PHASE2_SPEC.md](./docs/PHASE2_SPEC.md)**.

Still deferred: auth, TLS, idempotent producer PID, multi-partition latency bench.

---

## Phase 3 — Consumer groups & offsets

Phase 3 is **complete** for the core path:

- Server-side group coordinator (JoinGroup / Heartbeat / LeaveGroup)
- File-backed durable offsets under `{data_dir}/__consumer_offsets/`
- Range partition assignor (eager rebalance on join/leave/session timeout)
- `GroupConsumer` in `volant-client` (join → poll → commit → leave)
- CLI: `volant group fetch-offsets`, `volant group commit`, `volant consume --group`
- Multi-consumer e2e (`crates/volant-client/tests/e2e_group.rs`)

Binding details: **[docs/PHASE3_SPEC.md](./docs/PHASE3_SPEC.md)**.

### GroupConsumer (library)

```rust
use std::sync::Arc;
use volant_client::{Client, ClientConfig, GroupConsumer};

let client = Arc::new(Client::connect(ClientConfig {
    brokers: vec!["127.0.0.1:9092".into()],
    ..ClientConfig::default()
}).await?);

let mut consumer = GroupConsumer::join(
    client,
    "my-cg",
    vec!["events".into()],
    10_000,
).await?;

let records = consumer.poll().await?;
consumer.commit().await?;
consumer.leave().await?;
```

Still deferred: sticky/cooperative assignor (Phase 3.1), lag metrics.

---

## Phase 4 — Stream processing

Phase 4 is **complete** for the lightweight in-process path:

- Stateless operators: `map`, `filter`, `flat_map`, `foreach`
- Stateful: keyed `reduce` / `count_reduce`, tumbling windows
- In-memory `MemoryStore` (no RocksDB)
- `StreamBuilder` → `StreamApp` runtime with topic source/sink
- **At-least-once:** commit consumer offsets after successful sink produce
- Offline word-count + live broker e2e (`crates/volant-stream/tests/e2e_word_count.rs`)
- Example: `cargo run -p volant-stream --example word_count`

Binding details: **[docs/PHASE4_SPEC.md](./docs/PHASE4_SPEC.md)**.

### Programming model

```rust
use std::sync::Arc;
use volant_client::Client;
use volant_stream::{SourceConfig, StreamApp, StreamBuilder};

// Build a topology: lines → split words → count → counts topic
let topology = StreamBuilder::new("word-count")
    .source_topic("lines", SourceConfig::new("wc-app"))
    .flat_map(|record| { /* emit one record per word */ Ok(vec![/* ... */]) })
    .reduce_count()
    .sink_topic("counts")
    .build()?;

let client = Arc::new(Client::connect_addr("127.0.0.1:9092").await?);
let mut app = StreamApp::start(client, topology).await?;
app.run(None).await?; // until error; or app.step() in a loop
```

Offline (no broker) for tests:

```rust
use volant_stream::{flat_map, count_reduce, process_pipeline, Pipeline};

let mut pipeline = Pipeline::new()
    .then(flat_map(split_words))
    .then(count_reduce());
let out = process_pipeline(&mut pipeline, input_records, None)?;
```

### Word-count example

```bash
# terminal 1 — broker
cargo run -p volant-server -- --data-dir /tmp/vdata --listen 127.0.0.1:9092

# terminal 2 — topics
cargo run -p volant-cli -- topic create lines --partitions 1 --broker 127.0.0.1:9092
cargo run -p volant-cli -- topic create counts --partitions 1 --broker 127.0.0.1:9092

# terminal 3 — stream app
cargo run -p volant-stream --example word_count -- --broker 127.0.0.1:9092

# terminal 4 — produce lines, inspect counts
cargo run -p volant-cli -- produce lines --value "the quick brown fox" --broker 127.0.0.1:9092
cargo run -p volant-cli -- consume counts --partition 0 --from 0 --max 50 --broker 127.0.0.1:9092
```

Still deferred: exactly-once / transactions, RocksDB state, WASM operators, hopping windows.

**Next:** Phase 5 — DMA & high-performance I/O (`io_uring`, O_DIRECT).

---

## Roadmap (summary)

| Phase | Focus | Status |
|------:|-------|--------|
| 0 | Workspace scaffold | **Done** |
| 1 | Durable segment log + recovery | **Done** |
| 2 | TCP protocol, multi-partition, client SDK | **Done** |
| 3 | Consumer groups & offsets | **Done** |
| 4 | Stream processing operators | **Done** |
| 5 | io_uring / DMA-oriented I/O | **Next** |
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
