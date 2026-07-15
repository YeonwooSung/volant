# Volant

**Lightweight, high-performance streaming message broker in Rust.**

Volant is a resource-efficient alternative to Apache Kafka, built for:

- **Fast messaging** — append-only logs, batch-oriented protocol, zero-copy reads
- **DMA-friendly I/O** — memory-mapped segments; optional `io_uring` / O_DIRECT (Phase 5, feature-gated)
- **Streaming processing** — first-class operators (`map`, `filter`, windows) without a heavy runtime
- **Small footprint** — native binary, predictable memory, simple operations

> Status: **Phase 8 complete** — client leader redirect, optional client TLS,
> CLI auth token, Helm chart, rolling-restart tests; builds on Phase 6–7
> clustering and production readiness. Single-node mode (no `--cluster-config`)
> preserves Phase 1–5 behavior. See [ROADMAP.md](./ROADMAP.md),
> [ops runbook](./docs/ops.md), [deploy/](./deploy/),
> [consistency model](./docs/consistency.md), and Phase 1–8 specs under `docs/`.

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
│   ├── PHASE4_SPEC.md    # Stream operators & topology API
│   ├── PHASE5_SPEC.md    # DMA / high-performance I/O
│   ├── PHASE6_SPEC.md    # Clustering & ISR replication
│   ├── PHASE7_SPEC.md    # Metrics, auth, TLS, packaging
│   ├── PHASE8_SPEC.md    # Client redirect, client TLS, Helm
│   ├── ops.md            # Operator runbook (metrics / auth / TLS)
│   ├── consistency.md    # What “committed” means (HWM / acks)
│   └── tuning.md         # Ops tuning guide (ulimit, I/O, affinity)
├── deploy/               # Dockerfile, compose, systemd, Helm chart
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

# Storage append throughput micro-bench (always use --release)
cargo run -p volant-bench --release
```

### Multi-node cluster (Phase 6)

```bash
# terminals 1–3 (see examples/cluster.toml)
cargo run -p volant-server -- \
  --node-id 1 --cluster-config ./examples/cluster.toml \
  --data-dir ./data1 --listen 127.0.0.1:9092

cargo run -p volant-server -- \
  --node-id 2 --cluster-config ./examples/cluster.toml \
  --data-dir ./data2 --listen 127.0.0.1:9093

cargo run -p volant-server -- \
  --node-id 3 --cluster-config ./examples/cluster.toml \
  --data-dir ./data3 --listen 127.0.0.1:9094

# Create topics on the controller (lowest live broker id, usually node 1)
cargo run -p volant-cli -- topic create events --partitions 3 --broker 127.0.0.1:9092
```

Omit `--cluster-config` for single-node mode (Phase 1–5 behavior).  
Consistency guarantees: [docs/consistency.md](./docs/consistency.md).

### Observability, auth, TLS (Phase 7)

```bash
# Metrics + JSON logs
cargo run -p volant-server -- \
  --data-dir /tmp/vdata \
  --listen 127.0.0.1:9092 \
  --metrics-addr 127.0.0.1:9102 \
  --log-format json

curl -s http://127.0.0.1:9102/metrics | grep volant_

# Shared-token auth (server + CLI)
export VOLANT_AUTH_TOKEN=s3cret
cargo run -p volant-server -- --data-dir /tmp/vdata --listen 127.0.0.1:9092
cargo run -p volant-cli -- --auth-token s3cret topic list --broker 127.0.0.1:9092
# Rust client: ClientConfig { auth_token: Some("s3cret".into()), .. }

# Optional TLS (feature-gated; default build is plaintext)
cargo run -p volant-server --features tls -- \
  --data-dir /tmp/vdata --listen 127.0.0.1:9092 \
  --tls-cert ./server.crt --tls-key ./server.key
# Client TLS (lab / self-signed): ClientConfig { tls: true, tls_insecure: true, .. }
# Production: tls=true, tls_insecure=false, optional tls_ca (webpki-roots for public CAs)
# requires `volant-client` built with `--features tls`
# Inter-broker TLS is on by default when server TLS is enabled (Phase 9)
```

Packaging: [deploy/README.md](./deploy/README.md) (Docker, systemd, **Helm** multi-node).  
Ops details: [docs/ops.md](./docs/ops.md).

**Phase 8 client behavior:** on `NotLeaderForPartition`, the Rust client refreshes
metadata and **reconnects to the partition leader** (see `max_redirects`).

**Phase 9:** multi-node Helm (`cluster.enabled`), inter-broker TLS, client CA roots,
optional [fuzz/](./fuzz/) harness.

**Phase 10:** idempotent produce (`enable_idempotence`), produce retries, consumer
lag metrics + `volant group lag`.

**Phase 11:** sticky partition assignor, durable producer PID state under
`data_dir`, `volant group describe`.

**Phase 12:** `volant group list` / `delete-offsets`, static membership via
`group_instance_id`.

**Phase 13:** per-topic configs (`retention.ms` / `retention.bytes` /
`segment.bytes`), `volant topic describe` / `config`, background retention.

**Phase 14:** single-node topic catalog survives restart; `volant topic
delete-records` (truncate sealed segments before an offset).

**Phase 15:** `volant topic add-partitions` / `topic offsets` (CreatePartitions +
ListOffsets).

**Phase 16:** `cleanup.policy=compact` — key compaction on sealed segments
(tombstone = empty value).

**Phase 17:** cooperative rebalance — JoinGroup `revoked` list; `GroupConsumer`
keeps fetch positions on sticky-retained partitions.

**Phase 18:** transactions MVP — `BeginTxn`/`EndTxn`, transactional id fencing,
multi-partition atomic commit, deferred offsets; `volant txn produce`.

**Phase 19:** mTLS identity — `--tls-client-ca` / `--tls-client-allow`; client
`tls_cert`/`tls_key`; verified CN authenticates without shared token.

**Phase 20:** principal ACLs — allow/deny on topic/group/cluster; `--acl-enable` /
`--acl-file` / `--acl-super-users`; `volant acl create|list|delete`.

**Phase 21:** durable ACLs (`data_dir/__acls/acls.json`) + metrics Bearer auth
(`--metrics-token` / `VOLANT_METRICS_TOKEN`).

**Phase 22:** SCRAM-SHA-256 — `--scram-user user:pass`, durable
`data_dir/__scram/users.json`, client `scram_username`/`scram_password`,
`volant user create|list|delete`. Coexists with shared-token Auth and mTLS.

**Still deferred:** Kafka wire shim, multi-language clients, full SASL /
SCRAM-SHA-512, chaos-mesh / cargo-fuzz corpus CI.

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

---

## Phase 5 — DMA & high-performance I/O ✅

Phase 5 pushes the storage and runtime path toward hardware-friendly limits.
Binding design: **[docs/PHASE5_SPEC.md](./docs/PHASE5_SPEC.md)**.  
Ops guide: **[docs/tuning.md](./docs/tuning.md)**.

**Landed**

- `BufferPool` + `PooledBuf` (return-on-drop) for encode scratch buffers
- Pluggable `IoBackend` (`StdIoBackend`; Linux `UringIoBackend` behind `io-uring`)
- Optional `direct-io` (aligned buffers + `O_DIRECT` open hooks on Linux)
- Broker batch produce via `PartitionLog::append_batch` (single flush policy)
- Tuning guide (ulimit, `vm.dirty_*`, disk, O_DIRECT / io_uring when-to, huge pages, affinity)
- Optional server **CPU affinity / thread-per-core** (`thread-per-core` + `VOLANT_CPU_LIST`)
- Multi-mode `volant-bench` (`append` / `fetch` / `produce-batch`)

### Feature flags

| Crate | Feature | Default | Platforms | Effect |
|-------|---------|---------|-----------|--------|
| `volant-storage` | *(mmap path)* | on | all | Default buffered append + mmap reads |
| `volant-storage` | `io-uring` | off | **Linux only** | `io_uring` append/fsync backend (`compile_error!` elsewhere) |
| `volant-storage` | `direct-io` | off | Linux/Unix | `O_DIRECT` active-segment writes |
| `volant-server` | `thread-per-core` | off | all (best-effort) | Pin Tokio workers via `VOLANT_CPU_LIST` |

```bash
# Default (macOS / Linux) — no optional features
cargo build -p volant-server

# Optional CPU pinning (warns and continues if pin unsupported)
cargo build -p volant-server --features thread-per-core
VOLANT_CPU_LIST=0,1,2 cargo run -p volant-server --features thread-per-core -- \
  --data-dir /tmp/vdata --listen 127.0.0.1:9092

# Linux storage experiments
cargo build -p volant-storage --features io-uring
cargo build -p volant-storage --features direct-io
cargo build -p volant-storage --features "io-uring,direct-io"
```

### Benchmarks

Always measure with a **release** build on a quiet machine:

```bash
cargo run -p volant-bench --release -- append --count 100000 --value-size 100
cargo run -p volant-bench --release -- fetch --count 100000 --value-size 100
cargo run -p volant-bench --release -- produce-batch --count 100000 --value-size 100 --batch-size 100
```

**Sample laptop numbers** (Apple M3 Pro, macOS, default std/mmap path, 100-byte values,
no intermediate flush, temp dir on local SSD — not a competitive claim, just a
regression baseline):

| Mode | Throughput | Bandwidth |
|------|------------|-----------|
| `append` | ~562k msgs/s | ~54 MB/s |
| `fetch` | ~640k msgs/s | ~61 MB/s |
| `produce-batch` (batch=100) | ~616k msgs/s | ~59 MB/s |

Re-measure on your hardware before tuning. Feature-flag comparisons (`direct-io`,
`io-uring`) and ops guidance: **[docs/tuning.md](./docs/tuning.md)**.

### CPU affinity

```bash
cargo run -p volant-server --release --features thread-per-core -- \
  --data-dir /tmp/vdata --listen 127.0.0.1:9092
# with pinning:
VOLANT_CPU_LIST=2,3,4,5 cargo run -p volant-server --release --features thread-per-core -- \
  --data-dir /tmp/vdata --listen 127.0.0.1:9092
```

Unset/empty `VOLANT_CPU_LIST` → feature is a no-op (unpinned). Pin failures log a
warning and do **not** abort (important for macOS dev builds).

---

## Roadmap (summary)

| Phase | Focus | Status |
|------:|-------|--------|
| 0 | Workspace scaffold | **Done** |
| 1 | Durable segment log + recovery | **Done** |
| 2 | TCP protocol, multi-partition, client SDK | **Done** |
| 3 | Consumer groups & offsets | **Done** |
| 4 | Stream processing operators | **Done** |
| 5 | io_uring / DMA-oriented I/O + tuning | **Done** |
| 6 | Clustering & ISR replication | **Done** |
| 7 | Metrics, auth, TLS, packaging | **Done** (MVP) |
| 8 | Client redirect, client TLS, Helm | **Done** |

Details and deferred items: **[ROADMAP.md](./ROADMAP.md)**.  
Ops: **[docs/ops.md](./docs/ops.md)** · Tuning: **[docs/tuning.md](./docs/tuning.md)**.

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
    volant-storage (mmap · optional io_uring / O_DIRECT)
```

---

## License

Apache-2.0
