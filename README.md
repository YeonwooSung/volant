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

**Phase 23:** Kafka wire shim MVP — `--kafka-listen host:port` for
ApiVersions / Metadata / Produce / Fetch (MessageSet magic 0/1). Native
Volant protocol stays on `--listen`.

**Phase 24:** Kafka RecordBatch (magic 2) on the shim — auto-detect produce
format; Fetch v4 returns RecordBatch; Produce 0–3 / Fetch 0–4 advertised.

**Phase 25:** Kafka admin on the shim — CreateTopics / DeleteTopics / ListOffsets
(earliest & latest) so clients need less Volant-native setup.

**Phase 26:** Kafka consumer groups on the shim — FindCoordinator, Join/Sync/
Heartbeat/Leave, OffsetCommit/Fetch mapped to Volant's group coordinator.

**Phase 27:** Kafka ops surface — List/Describe/DeleteGroups, CreatePartitions,
DescribeConfigs / AlterConfigs (topic keys).

**Phase 28:** Kafka RecordBatch compression on Produce — gzip, snappy (Xerial),
lz4 frame, zstd. Fetch remains uncompressed.

**Phase 29:** Kafka InitProducerId + idempotent Produce (PID/epoch/sequence
de-dupe on RecordBatch). Maps onto Volant Phase 10/11 producer state.

**Phase 30:** Kafka SASL on the shim — SaslHandshake / SaslAuthenticate with
PLAIN and SCRAM-SHA-256 against the Volant SCRAM store; principal feeds ACLs.

**Phase 31:** Kafka transactions on the shim — AddPartitionsToTxn / EndTxn /
TxnOffsetCommit mapped to Volant Phase 18 buffer-until-commit; FindCoordinator
v1 for transaction coordinators.

**Phase 32:** Kafka compressed Fetch — Fetch v4 RecordBatches compressed by
default (lz4); override with `VOLANT_KAFKA_FETCH_COMPRESSION`.

**Phase 33:** Kafka MessageSet compression — compressed Produce wrappers and
Fetch v0–3 MessageSets (gzip/snappy/lz4; zstd maps to lz4).

**Phase 34:** SCRAM-SHA-512 on the Kafka shim — dual SHA-256/512 credentials
per user; SaslHandshake advertises SCRAM-SHA-512.

**Phase 35:** Kafka DeleteRecords + ACL admin on the shim — keys 21 / 29 / 30 /
31 mapped to Phase 14 truncate and Phase 20/21 ACL store.

**Phase 36:** Kafka OffsetDelete + Fetch isolation honesty — key 47 maps to
Phase 12 offset delete; `READ_COMMITTED` LSO equals HWM (buffer-until-commit).

**Phase 37:** Kafka IncrementalAlterConfigs — key 44 SET/DELETE on topic
configs (Phase 13); APPEND/SUBTRACT rejected.

**Phase 38:** Kafka Metadata classic v0–8 — cluster_id, throttle, rack,
offline_replicas, leader_epoch=-1, authorized-ops bitfields; flexible v9+ still
out of scope.

**Phase 39:** Kafka OffsetForLeaderEpoch (key 23, classic v0–3) — end offset by
leader epoch for consumer truncation checks; no durable epoch history (eligible
epochs map to HWM).

**Phase 40:** Kafka ListOffsets classic v0–5 — isolation_level, throttle,
current_leader_epoch fencing, response leader_epoch; flexible v6+ out of scope.

**Phase 41:** Kafka OffsetFetch classic v0–5 — null topics = all, top-level
error, throttle, committed_leader_epoch=-1; flexible v6+ / multi-group out of
scope.

**Phase 42:** Kafka group classic static membership — JoinGroup 0–5,
Heartbeat/Sync/Leave 0–3 with `group.instance.id` → `static:{id}`.

**Phase 43:** Kafka group admin classic versions — DescribeGroups 0–4,
ListGroups 0–2, DeleteGroups 0–1 (throttle + authorized_ops + instance id).

**Phase 44:** Kafka OffsetCommit classic 0–7 + FindCoordinator 0–2 — throttle,
leader epoch field, `group.instance.id` on commit.

**Phase 45:** Kafka topic admin classic — CreateTopics 0–4, DeleteTopics 0–3,
CreatePartitions 0–1 (throttle framing + validate_only).

**Phase 46:** Kafka DescribeConfigs 0–3 + AlterConfigs 0–1 — throttle,
config_source/synonyms, config_type/documentation.

**Phase 47:** Kafka transaction APIs classic 0–2 — AddPartitionsToTxn,
AddOffsetsToTxn, EndTxn, TxnOffsetCommit (v2 leader_epoch ignored).

**Phase 48:** Kafka Produce classic 0–8 — log_start_offset (v5+), empty
record_errors + error_message (v8+); flexible v9+ deferred.

**Phase 49:** Kafka Fetch classic 0–11 — log_start_offset, session header,
leader-epoch fence, preferred_read_replica=-1; flexible v12+ deferred.

**Phase 50:** Kafka ApiVersions classic 0–2 — trailing throttle on v1–2.

**Phase 51:** Flexible wire foundation (KIP-482) + ApiVersions v3 compact
encoding; first flexible API on the shim.

**Phase 52:** Flexible Metadata v9 + FindCoordinator v3–4 (batch keys);
response header v1 for those APIs.

**Phase 53:** Flexible Produce v9 — compact records/topics + response header v1.

**Phase 54:** Flexible Fetch v12 — compact topics/records + response header v1.

**Phase 55:** Flexible group consumer APIs — JoinGroup v6, SyncGroup /
Heartbeat / LeaveGroup v4 (compact + response header v1).

**Phase 56:** Group flex field completeness — JoinGroup v7–9 (ProtocolType,
Reason, SkipAssignment), SyncGroup v5, LeaveGroup v5.

**Phase 57:** Flexible OffsetCommit v8 + OffsetFetch v6–7 (RequireStable
ignored; multi-group v8+ deferred).

**Phase 58:** OffsetFetch multi-group flexible v8 — Groups[] request/response
with per-group ACL errors.

**Phase 59:** Flexible group admin — DescribeGroups v5, ListGroups v3,
DeleteGroups v2 (compact + response header v1).

**Phase 60:** Flexible topic admin — CreateTopics v5, DeleteTopics v4,
CreatePartitions v2.

**Phase 61:** Flexible configs — DescribeConfigs v4, AlterConfigs v2,
IncrementalAlterConfigs v1.

**Phase 62:** Flexible transaction APIs — InitProducerId v2, AddPartitionsToTxn /
AddOffsetsToTxn / EndTxn / TxnOffsetCommit v3; classic paths unchanged.

**Phase 63:** Flexible ListOffsets v6 + OffsetForLeaderEpoch v4.

**Phase 64:** Flexible DeleteRecords v2 + Describe/Create/DeleteAcls v2.

**Phase 65:** SaslAuthenticate v2 flexible + DescribeCluster v0 + ListTransactions
v0 (always-flexible modern admin APIs).

**Phase 66:** DescribeTransactions v0 + DescribeProducers v0; DescribeCluster
0–1 (EndpointType); ListTransactions 0–1 (DurationFilter ignored).

**Phase 67:** Metadata TopicId v10–12 — deterministic UUID mapping, v11 cluster
ops removal, v12 lookup by TopicId.

**Phase 68:** Fetch TopicId v13 — request/response UUID topics (KIP-516);
unknown id → UnknownTopicId; v12 name path unchanged.

**Phase 69:** Admin TopicId — CreateTopics v7 TopicId response; DeleteTopics
v5 ErrorMessage + v6 delete-by-TopicId.

**Phase 70:** DescribeCluster v2 (IsFenced always false) + ListTransactions v2
(TransactionalIdPattern simple `*` glob).

**Phase 71:** Produce TopicId v13 — UUID topics (v10–12 name path unchanged);
unknown id → UnknownTopicId; KIP-951 tags empty.

**Phase 72:** OffsetCommit/OffsetFetch v9–10 — OffsetCommit v9≈v8 name path,
v10 TopicId; OffsetFetch v9 MemberId+MemberEpoch (ignored), v10 TopicId;
unknown id → UnknownTopicId.

**Phase 73:** Metadata v13 — top-level ErrorCode (always 0); request wire same
as v12 TopicId path.

**Phase 74:** ListOffsets v7–11 — MAX_TIMESTAMP (-3) scan; EARLIEST_LOCAL (-4);
tiered specials empty; TimeoutMs (v10) ignored.

**Phase 75:** KIP-890-era txn max versions — InitProducerId 0–5 (resume fields
ignored), AddPartitionsToTxn 0–5 (v4–5 batch), EndTxn 0–5 (v5 pid/epoch echo),
TxnOffsetCommit 0–5 (name path); AddOffsetsToTxn stays 0–3; no 2PC.

**Phase 76:** TxnOffsetCommit v6 TopicId — UUID topics (v3–5 name path unchanged);
unknown id → UnknownTopicId; buffers until EndTxn.

**Phase 77:** InitProducerId v6 — Enable2Pc / KeepPreparedTxn parsed+ignored;
OngoingTxnProducerId/Epoch always -1 (no prepared/2PC); max 0–6.

**Phase 78:** KIP-951 CurrentLeader / NodeEndpoints — Produce v10+ and Fetch
v12+ emit leader tags on NotLeader/FencedLeaderEpoch; success keeps empty tags.

**Phase 79:** Group admin version bumps — ListGroups 0–5 (StatesFilter/GroupState,
TypesFilter/GroupType=`classic`), DescribeGroups 0–6 + DeleteGroups 0–3
ErrorMessage fields.

**Still deferred:** multi-language clients, chaos-mesh / cargo-fuzz corpus CI,
true control-marker READ_COMMITTED, real 2PC.

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
