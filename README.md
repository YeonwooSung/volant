# Volant

**Lightweight, high-performance streaming message broker in Rust.**

Volant is a resource-efficient alternative to Apache Kafka, built for:

- **Fast messaging** — append-only logs, batch-oriented protocol, zero-copy reads
- **DMA-friendly I/O** — memory-mapped segments; optional `io_uring` / O_DIRECT (Phase 5, feature-gated)
- **Streaming processing** — first-class operators (`map`, `filter`, windows) without a heavy runtime
- **Small footprint** — native binary, predictable memory, simple operations

> Status: **v0.2 shipped** (crate **0.2.0**, Phases **0–154** + residuals
> **v0.3–v0.60**). Durable log,
> Phase 6 ISR clustering, security MVP, in-process streams (149/151/153 +
> `TumblingWindow::durable` window buckets), optional Kafka shim (38 keys).
> Metadata serves the **live** assignment by default (Phase 152 committed-only
> is **opt-in**). Homemade metadata Raft stays behind flags. Single-node mode
> (no `--cluster-config`) preserves the simple path.
> Start with the [whitepaper](./docs/WHITEPAPER.md) and
> [docs index](./docs/INDEX.md); also [ROADMAP.md](./ROADMAP.md),
> [ops](./docs/ops.md), [deploy/](./deploy/), [consistency](./docs/consistency.md).
> v0.2 scope: [docs/V02_FREEZE.md](./docs/V02_FREEZE.md).

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
│   ├── PHASE7–130_SPEC.md # Ship records (see history/)
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

Phases **0–154** are the binding core (log, native protocol, groups, streams, DMA I/O,
Phases **0–154** are the binding core (log, native protocol, groups, streams, DMA I/O,
static ISR). Later work is summarized by band — full chronicle in
[ROADMAP.md](./ROADMAP.md) and [docs/history/PHASE_HISTORY.md](./docs/history/PHASE_HISTORY.md).

| Band | Phases | Theme |
|------|--------|-------|
| Ops / packaging | 7–9 | Metrics, token auth, TLS, Helm multi-node, client leader redirect |
| Native reliability | 10–17 | Idempotence, sticky/cooperative groups, topic configs, compaction |
| Txns & security | 18–22, **86** | Write-through txns + soft READ_COMMITTED (LSO/aborted); mTLS, ACLs, SCRAM |
| Kafka wire shim | 23–109 | Optional `--kafka-listen`; classic + flexible; **~38** keys; prepared 2PC (**90**) + timeouts (**92**/open **93**/max **96**); omit-unchanged sessions (**91**); session TTL/max (**95**); bg sweeper + metrics (**97**/always-spawn **101**/shutdown join **106**/accept drain + single-flight **109**); crash≡abort control batches (**98**); empty-AddPartitions control (**105**); broker Describe/AlterConfigs knobs (**99**) + sparse durable restart (**100**/ **102**) + name vs `node_id` (**103**); aborted soft-marker GC/clip (**104**/ **111**); phase103 parallel test isolation (**107**); follower-death ISR/HWM (**108**) |

**Kafka ceilings (code SoT):** ApiVersions **0–5**, Fetch **0–18**, Produce/Metadata
**0–13**, ACL admin **0–3** (User resource v3, store-only); Fetch isolation
READ_COMMITTED MVP (Phase 86) + soft-marker GC/clip on DeleteRecords/retention/load (Phase 104/111); durable OffsetForLeaderEpoch history (Phase 87); Fetch DivergingEpoch + real fetch sessions MVP (Phase 88); Kafka control batches on EndTxn (Phase 89), crash≡abort open promote (Phase 98), and empty AddPartitions membership (Phase 105); prepared 2PC MVP (Phase 90); omit-unchanged incremental sessions (Phase 91); prepared timeout auto-abort (Phase 92); open-txn timeout (Phase 93); fetch session idle TTL + max/LRU (Phase 95); transaction max timeout clamp (Phase 96); background txn/session sweeper + metrics (Phase 97; always-spawn / 0→>0 live Phase 101; graceful shutdown/join Phase 106); BROKER Describe/AlterConfigs for txn/session/sweep knobs (Phase 99) with **sparse** durable restart restore (Phase 100/102) and resource name empty-or-`node_id` (Phase 103; parallel test isolation Phase 107); follower-death ISR shrink + HWM recompute so rolling-restart `acks=all` does not time out (Phase 108); accept-loop drain + single-flight `start_background_tasks` (Phase 109); non-controller alive-set auto-death (Phase 110); straddle soft-marker clip (Phase 111); cluster admin fan-out — DeleteRecords best-effort replica truncate, controller-only BROKER config + ACL snapshot push (Phase 113); multi-broker Enable2Pc prepare/complete (Phase 114); durable local fetch sessions under `__fetch_sessions` (Phase 115); multi-broker fetch session handoff via owner-encoded id + transparent inter-broker forward (Phase 119); best-effort shared fetch session mirror + promote-on-owner-miss (Phase 138; not Raft) + coalesce/debounce + optional durable peer mirrors + `mirror_gen` fence (Phase 139); transparent EndTxn forward to Init-owner coordinator (Phase 120); sticky FindCoordinator via murmur2 static ring + Init-owner override (Phase 121); transparent AddOffsetsToTxn / TxnOffsetCommit forward (Phase 122); durable DeleteRecords outbox retry for offline peers (Phase 116) + new-leader outbox reconcile on leadership change (Phase 123); ACL/BROKER admin catch-up on rejoin/controller restart (Phase 117; durable gens + heartbeat re-push — not Raft); ISR rejoin when ReplicaFetch LEO ≥ HWM + lag-based shrink of slow-but-alive members (Phase 118);
time-based ISR lag shrink via `replica_lag_max_ms` (Phase 125); PreferredReadReplica max LEO lag + RC suppress metric (Phase 140); journal majority health gauges (Phase 141); Metadata leader ISR overlay + leader→controller IsrUpdate (Phase 142); fetch session promote claim fence lowest-id (Phase 143); preferred × established-session suppress (Phase 144); rack-aware create assignment (Phase 145).
time-based ISR lag shrink via `replica_lag_max_ms` (Phase 125); PreferredReadReplica max LEO lag + RC suppress metric (Phase 140); journal majority health gauges (Phase 141); Metadata leader ISR overlay + leader→controller IsrUpdate (Phase 142); fetch session promote claim fence lowest-id (Phase 143); preferred × established-session suppress (Phase 144); serve-from-mirror without promote on owner miss (Phase 147).
Matrix + honesty: [docs/KAFKA_COMPAT.md](./docs/KAFKA_COMPAT.md).

**Still deferred:** multi-language clients, chaos-mesh / long fuzz campaigns
(corpus smoke CI MVP → **Phase 112**), full Kafka preferred quota
(beyond 126/133/140/144 + v0.7 opt-in throttle/probe; PreferredReadReplica MVP → **Phase 126**;
rack-aware create assignment → **Phase 145**;
shared session mirror MVP → **Phase 138/139/143** — residual Raft registry /
serve-without-promote / incremental put), full KIP-890/939. Cluster admin fan-out →
selector beyond 126/133/140/144 (PreferredReadReplica MVP → **Phase 126**;
shared session mirror MVP → **Phase 138/139/143/147** — residual Raft registry /
dual-epoch converge / incremental put), full KIP-890/939. Cluster admin fan-out →
**Phase 113**; multi-broker Enable2Pc MVP → **Phase 114**; durable local sessions →
**Phase 115**; DeleteRecords offline outbox → **Phase 116**; ACL/BROKER catch-up →
**Phase 117**; ISR rejoin/lag shrink → **Phase 118**; multi-broker session handoff →
**Phase 119**; transparent EndTxn forward → **Phase 120**. Sticky FindCoordinator →
**Phase 121**. AddOffsets / TxnOffsetCommit forward → **Phase 122**. DeleteRecords
outbox leadership handoff → **Phase 123**. Durable txn coordinator registry →
**Phase 124**. Time-based ISR lag → **Phase 125**. Shared session mirror + promote →
**Phase 138**; mirror polish → **Phase 139**; promote claim fence → **Phase 143**.
Preferred lag/suppress → **Phase 140**. N=2 majority health gauges → **Phase 141**.
Metadata ISR freshness → **Phase 142**. Preferred × session thrash suppress → **Phase 144**.
Rack-aware create assignment → **Phase 145**.
**Phase 138**; mirror polish → **Phase 139**; promote claim fence → **Phase 143**;
serve-from-mirror without promote → **Phase 147**. Preferred lag/suppress → **Phase 140**.
N=2 majority health gauges → **Phase 141**. Metadata ISR freshness → **Phase 142**.
Preferred × session thrash suppress → **Phase 144**.

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
