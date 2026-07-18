# Volant

**Lightweight, high-performance streaming message broker in Rust.**

Volant is a resource-efficient alternative to Apache Kafka, built for:

- **Fast messaging** — append-only logs, batch-oriented protocol, zero-copy reads
- **DMA-friendly I/O** — memory-mapped segments; optional `io_uring` / O_DIRECT (Phase 5, feature-gated)
- **Streaming processing** — first-class operators (`map`, `filter`, windows) without a heavy runtime
- **Small footprint** — native binary, predictable memory, simple operations

> Status: **Phases 0–102 landed** — durable log, clustering, security, stream
> operators, and a broad optional Kafka wire shim (classic + flexible;
> ApiVersions 0–5; Fetch 0–18; ACL admin 0–3; TRANSACTION_ABORTABLE subset;
> fetch session TTL/max; broker Describe/AlterConfigs for txn/session/sweep
> knobs with **sparse** durable restart restore; sweeper always-spawn 0→>0 without restart).
> Single-node mode (no `--cluster-config`) preserves the simple path.
> Start with the [whitepaper](./docs/WHITEPAPER.md) and
> [docs index](./docs/INDEX.md); also [ROADMAP.md](./ROADMAP.md),
> [ops](./docs/ops.md), [deploy/](./deploy/), [consistency](./docs/consistency.md).

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
│   ├── INDEX.md          # Documentation map
│   ├── WHITEPAPER.md      # Technical whitepaper
│   ├── KAFKA_COMPAT.md   # Kafka shim API matrix + honesty
│   ├── features.md       # Native features (post-core)
│   ├── ops.md            # Operator runbook
│   ├── consistency.md    # HWM / ISR / acks
│   ├── tuning.md         # Performance / I/O guide
│   ├── PHASE1–6_SPEC.md  # Binding core specs
│   ├── PHASE7–94_SPEC.md # Ship records (see history/)
│   └── history/          # Phase index + archived plans/reviews
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

### What shipped after core (compact)

Phases **0–6** are the binding core (log, native protocol, groups, streams, DMA I/O,
static ISR). Later work is summarized by band — full chronicle in
[ROADMAP.md](./ROADMAP.md) and [docs/history/PHASE_HISTORY.md](./docs/history/PHASE_HISTORY.md).

| Band | Phases | Theme |
|------|--------|-------|
| Ops / packaging | 7–9 | Metrics, token auth, TLS, Helm multi-node, client leader redirect |
| Native reliability | 10–17 | Idempotence, sticky/cooperative groups, topic configs, compaction |
| Txns & security | 18–22, **86** | Write-through txns + soft READ_COMMITTED (LSO/aborted); mTLS, ACLs, SCRAM |
| Kafka wire shim | 23–102 | Optional `--kafka-listen`; classic + flexible; **~38** keys; prepared 2PC (**90**) + timeouts (**92**/open **93**/max **96**); omit-unchanged sessions (**91**); session TTL/max (**95**); bg sweeper + metrics (**97**/always-spawn **101**); crash≡abort control batches (**98**); broker Describe/AlterConfigs knobs (**99**) + sparse durable restart (**100**/ **102**) |

**Kafka ceilings (code SoT):** ApiVersions **0–5**, Fetch **0–18**, Produce/Metadata
**0–13**, ACL admin **0–3** (User resource v3, store-only); Fetch isolation
READ_COMMITTED MVP (Phase 86); durable OffsetForLeaderEpoch history (Phase 87); Fetch DivergingEpoch + real fetch sessions MVP (Phase 88); Kafka control batches on EndTxn (Phase 89) and crash≡abort open promote (Phase 98); prepared 2PC MVP (Phase 90); omit-unchanged incremental sessions (Phase 91); prepared timeout auto-abort (Phase 92); open-txn timeout (Phase 93); fetch session idle TTL + max/LRU (Phase 95); transaction max timeout clamp (Phase 96); background txn/session sweeper + metrics (Phase 97; always-spawn / 0→>0 live Phase 101); BROKER Describe/AlterConfigs for txn/session/sweep knobs (Phase 99) with **sparse** durable restart restore (Phase 100/102).
Matrix + honesty: [docs/KAFKA_COMPAT.md](./docs/KAFKA_COMPAT.md).

**Still deferred:** multi-language clients, chaos-mesh / cargo-fuzz corpus CI,
multi-broker 2PC parity, multi-broker session affinity, empty-AddPartitions
control markers, marker GC, BROKER name=`node_id`, graceful sweeper join on stop.

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

## Core platform (Phases 1–6)

Binding specs and short summaries live under [docs/](./docs/) — not repeated here:

| Phase | Topic | Spec |
|------:|-------|------|
| 1 | Durable partition log | [PHASE1_SPEC](./docs/PHASE1_SPEC.md) |
| 2 | Native TCP protocol + client/CLI | [PHASE2_SPEC](./docs/PHASE2_SPEC.md) |
| 3 | Consumer groups & offsets | [PHASE3_SPEC](./docs/PHASE3_SPEC.md) |
| 4 | In-process stream operators | [PHASE4_SPEC](./docs/PHASE4_SPEC.md) |
| 5 | mmap / optional `io_uring` + `O_DIRECT` | [PHASE5_SPEC](./docs/PHASE5_SPEC.md) |
| 6 | Static ISR clustering | [PHASE6_SPEC](./docs/PHASE6_SPEC.md) |

Whitepaper overview: [docs/WHITEPAPER.md](./docs/WHITEPAPER.md). Append baseline:
`cargo run -p volant-bench --release`.

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
