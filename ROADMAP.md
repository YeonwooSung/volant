# Volant Roadmap

**Volant** is a lightweight, high-performance streaming message broker written in Rust.
It aims to be a resource-efficient Kafka alternative with:

- **Streaming-first design** — produce/consume + first-class stream operators
- **DMA-oriented I/O** — mmap segments today, `io_uring` / O_DIRECT paths next
- **Small operational footprint** — single binary, low memory, few moving parts
- **Horizontal scalability** — partition-based sharding, then multi-node replication

---

## Design principles

| Principle | What it means in practice |
|-----------|---------------------------|
| Zero-copy where it counts | Batch frames, mmap reads, length-prefixed binary protocol |
| Sequential I/O wins | Append-only segment logs; avoid random writes on the hot path |
| Explicit complexity | Start single-node correct; add consensus only when needed |
| Resource efficiency | Bounded buffers, no JVM/GC tax, predictable latency |
| Operability | One server binary, one CLI, structured logs, clear metrics |

---

## Architecture (target)

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Producers  │────▶│ volant-     │────▶│ volant-storage   │
│  Consumers  │◀────│  server     │◀────│ (mmap / DMA log) │
└─────────────┘     │  + broker   │     └──────────────────┘
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ volant-     │
                    │  stream     │  (map / filter / window)
                    └─────────────┘
```

### Crate map

| Crate | Role |
|-------|------|
| `volant-core` | Shared types: `Message`, `Record`, `Offset`, errors |
| `volant-protocol` | Binary wire codec (frames, opcodes, CRC) |
| `volant-storage` | Partition log, segments, DMA/mmap I/O |
| `volant-broker` | Topics, partitions, produce/fetch, group coord |
| `volant-client` | Async producer/consumer SDK |
| `volant-stream` | Lightweight stream processing operators |
| `volant-server` | Broker process entrypoint |
| `volant-cli` | Admin CLI (`volant topic create`, …) |
| `volant-bench` | Storage / broker micro-benchmarks (Phase 5 multi-mode) |

---

## Phases

### Phase 0 — Scaffold ✅

**Goal:** Compilable workspace with clear module boundaries.

- [x] Cargo workspace + crate layout
- [x] Core types (`Message`, `Record`, `Offset`, topic IDs)
- [x] Protocol frame header + encode/decode skeleton
- [x] In-memory broker produce path (no durable log yet)
- [x] Server / CLI binaries with clap
- [x] Project roadmap

**Exit criteria:** `cargo build --workspace` and `cargo test --workspace` pass. **Met.**

---

### Phase 1 — Durable single-partition log ✅

**Goal:** Correct append-only storage with crash recovery.

Binding format and API: **[docs/PHASE1_SPEC.md](./docs/PHASE1_SPEC.md)**.

**Milestones**

1. **Segment file format**
   - [x] Fixed header: magic, version, base offset (`0x564C4E54` / v1)
   - [x] Record layout: length | crc | offset | timestamp | key | value | headers
   - [x] Sparse index (offset → file position) every N KB

2. **Append path**
   - [x] Buffered sequential write
   - [x] Configurable fsync policy (`flush_every_n` / explicit `flush`)
   - [x] Segment roll at `segment_size`

3. **Read path**
   - [x] `mmap` active + closed segments (`memmap2`)
   - [x] Fetch by offset with max bytes / max records
   - [x] Copy mapped bytes into `Bytes` (safe Phase 1 path)

4. **Recovery**
   - [x] Scan segments on open; rebuild next offset + index
   - [x] Truncate torn tails (partial last record / CRC failure)

5. **Retention**
   - [x] Time- and size-based segment deletion
   - [x] Manual `delete_records` API

6. **Tooling**
   - [x] Micro-bench harness (`volant-bench`) for append throughput
   - [x] Broker produce/fetch wired to durable `PartitionLog`

**Exit criteria**

- Survive kill -9 mid-append without corruption
- Sustained append ≥ 200k small msgs/s on laptop (single partition) — measure with:
  ```bash
  cargo run -p volant-bench --release
  ```
- **Baseline measured:** ~570k msgs/s (100-byte values, release, laptop) — exceeds 200k target
- Torn-tail recovery covered by unit/integration tests
- Fetch matches Kafka semantics for earliest / latest / by-offset

**Non-goals for Phase 1:** replication, multi-broker, consumer groups.

---

### Phase 2 — Network protocol & multi-partition broker ✅

**Goal:** Real clients talk to a running broker over TCP.

Binding wire format and APIs: **[docs/PHASE2_SPEC.md](./docs/PHASE2_SPEC.md)**.

**Milestones**

1. **Framed TCP server** (Tokio)
   - [x] Length-prefixed Volant frames (`volant-protocol` + CRC verify)
   - [x] Correlation IDs (sequential request/response per connection)
   - [x] Sequential per-connection I/O (explicit backpressure deferred)
   - [x] `volant_broker::net::{run_server, serve_listener}` accept loop

2. **APIs**
   - [x] `CreateTopic` / `DeleteTopic` / `Metadata`
   - [x] `Produce` (acks=1 flush on single node; response always returned)
   - [x] `Fetch` (long-poll / max wait)
   - [ ] Basic auth hook (optional token; pluggable later) — deferred

3. **Partitioning**
   - [x] Key hash → partition (Kafka-compatible murmur2)
   - [x] Round-robin for null keys
   - [x] Multiple partitions per topic on one node

4. **Client SDK**
   - [x] Async `Client` / `Producer` / `Consumer` over Tokio
   - [ ] Retry + idempotent produce (PID + sequence) — stretch, deferred
   - [x] E2E tests boot server on `127.0.0.1:0` via public net APIs

5. **CLI**
   - [x] `volant topic create|list|delete`
   - [x] `volant produce` / `volant consume` for debugging

**Exit criteria**

- [x] External process produces and consumes end-to-end (`volant-cli` + `volant-server`)
- [ ] Bench: multi-partition produce with bounded p99 latency (stretch / Phase 2 polish)
- [x] Integration tests over localhost TCP (`crates/volant-client/tests/e2e_tcp.rs`)

**Non-goals for Phase 2:** auth, TLS, consumer groups, Kafka wire compatibility.

---

### Phase 3 — Consumer groups & offsets ✅

**Goal:** Coordinated multi-consumer reading with committed offsets.

Binding wire format and APIs: **[docs/PHASE3_SPEC.md](./docs/PHASE3_SPEC.md)**.

**Milestones**

1. File-backed offset store (`{data_dir}/__consumer_offsets/...`)
   - [x] Durable `OffsetCommit` / `OffsetFetch` with fsync
2. Group membership + heartbeat + rebalance (eager rebalance)
   - [x] Server-side `GroupCoordinator` (Join / Heartbeat / Leave)
   - [x] Session expiry background task
3. Protocol opcodes 6–10 + error codes 9–12
   - [x] LE payload encode/decode roundtrips
4. Assignor
   - [x] Range assignor (uneven partitions covered by unit tests)
   - [ ] Sticky / cooperative assignor — deferred to Phase 3.1
5. Client + CLI
   - [x] `GroupConsumer` (join / poll / commit / leave)
   - [x] `volant group fetch-offsets` / `volant group commit`
   - [x] `volant consume --group G`
6. Lag metrics per group / partition
   - [ ] Stretch / Phase 3 polish

**Exit criteria**

- [x] Two consumers in one group split partitions (`e2e_group.rs`)
- [x] Restart resumes from committed offsets
- [x] Rebalance completes without stuck partitions (leave → remaining gets all)

**Non-goals for Phase 3:** cooperative sticky assignor, static membership, cross-node coordinator, transactional offsets.

---

### Phase 4 — Stream processing (lightweight) ✅

**Goal:** Kafka Streams–like operators without a heavy runtime.

Binding API: **[docs/PHASE4_SPEC.md](./docs/PHASE4_SPEC.md)**.

**Milestones**

1. Operator trait + pipeline
   - [x] `Operator` with `process` + `punctuate`
   - [x] Composable `Pipeline`
2. Stateless operators
   - [x] `map`, `filter`, `flat_map`, `foreach`
3. Stateful operators
   - [x] Keyed `reduce` / `count_reduce` with in-memory `MemoryStore`
   - [x] Tumbling windows (event-time)
   - [ ] Hopping windows — deferred
   - [ ] RocksDB / durable state — deferred (in-memory only)
4. Source / sink adapters
   - [x] `TopicSource` (`GroupConsumer`) + `TopicSink` (produce)
   - [x] `StreamBuilder` / `Topology` / `StreamApp` runtime
5. Processing guarantees
   - [x] At-least-once (commit offsets after successful sink produce)
   - [ ] Exactly-once / transactional produce — stretch, deferred
6. Optional WASM or plugin operators later — deferred

**Exit criteria**

- [x] Offline word-count pipeline test (`crates/volant-stream/tests/e2e_word_count.rs`)
- [x] Live broker e2e: source → operators → sink → fetch counts
- [x] Documented programming model + `word_count` example
  (`cargo run -p volant-stream --example word_count`)

**Non-goals for Phase 4:** exactly-once, WASM plugins, RocksDB, distributed stream workers.

---

### Phase 5 — DMA & high-performance I/O ✅

**Goal:** Push the storage/network path to hardware-friendly limits.

Binding design: **[docs/PHASE5_SPEC.md](./docs/PHASE5_SPEC.md)**.  
Ops guide: **[docs/tuning.md](./docs/tuning.md)**.

**Milestones**

1. **Linux `io_uring`** for append (feature-gated)
   - [x] `io-uring` feature + `IoBackend` / `UringIoBackend` (Linux; `compile_error!` elsewhere)
   - [x] Sync submit+wait path (acceptable Phase 5); sealed segments keep mmap reads
2. **O_DIRECT** optional path for predictable latency (aligned buffers)
   - [x] `direct-io` feature + aligned `BufferPool` path + open-flag hooks
3. **Batch produce coalescing** in the broker
   - [x] `PartitionLog::append_batch` + single-flush policy after multi-message produce
   - [x] `batches_coalesced` metric; single-lock multi-message `Broker::produce`
   - [ ] Optional write-behind queue (stretch — deferred)
4. **Kernel bypass experiments** (DPDK / AF_XDP) — research only
   - [x] Documented as research-only in `docs/tuning.md` (no production code)
5. **CPU affinity / thread-per-core** optional runtime mode
   - [x] `volant-server` feature `thread-per-core` + env `VOLANT_CPU_LIST`
   - [x] Unsupported pin → warn, do not abort (macOS best-effort)
6. Memory pool + slab for encode scratch buffers
   - [x] `BufferPool` / `PooledBuf` in `volant-storage` (return-on-drop)
7. **Docs & benches**
   - [x] Tuning guide (`docs/tuning.md`: ulimit, dirty ratios, O_DIRECT, io_uring, huge pages, affinity, DPDK/AF_XDP)
   - [x] README feature flags + bench how-to + tuning link
   - [x] Expanded `volant-bench` multi-mode CLI (`append` / `fetch` / `produce-batch`)
   - [x] Published release bench numbers in README

**Exit criteria**

- [x] Documented tuning guide (ulimit, disk, NIC, huge pages, affinity)
- [x] Optional `thread-per-core` feature; default macOS build green without it
- [x] Feature flags wired: storage `io-uring` + `direct-io`; server `thread-per-core`
- [x] Benchmark suite multi-mode with published numbers
- [x] Default `cargo build --workspace` remains green on macOS (no forced Linux features)

**Note:** DMA here means minimizing user↔kernel copies and enabling device-level
transfers where the OS allows — not a custom hardware driver.

**Status:** Phase 5 complete. Stretch items (write-behind queue, full async uring,
sendfile-style fetch) deferred. **Phase 6 clustering prototype complete.**

---

### Phase 6 — Clustering & replication ✅ *(prototype)*

**Goal:** Scale beyond one node with durable multi-replica partitions.

Binding design: **[docs/PHASE6_SPEC.md](./docs/PHASE6_SPEC.md)**.  
Consistency model: **[docs/consistency.md](./docs/consistency.md)**.

**Design (locked):** Kafka-style static membership + controller (lowest live broker id)
+ leader/follower ISR replication. Not Raft-per-partition.

**Milestones**

1. Cluster membership
   - [x] Static `cluster.toml` membership (`--cluster-config`, `--node-id`)
   - [x] Controller = lowest live broker id (HeartbeatBroker)
   - [ ] Dynamic membership / gossip (deferred)
2. Partition leader + followers
   - [x] Replica assignment on CreateTopic (RF, round-robin)
   - [x] Leaders accept produce; followers reject `NotLeaderForPartition`
   - [x] Followers `ReplicaFetch` + `append_with_offset`
   - [x] HWM = min LEO of ISR; client fetch capped at HWM
3. Controller / metadata
   - [x] Single controller (lowest live id); `NotController` on CreateTopic
   - [x] `ClusterState` snapshot + `assignment.json` persistence
   - [ ] Raft metadata quorum (deferred)
4. Producer acks
   - [x] `acks=1` (default) and `acks=all` (wire 255)
   - [x] `min_insync_replicas` → `NotEnoughReplicas`
5. Automatic leader election
   - [x] On broker death, elect new leader from ISR
   - [x] Integration test: 3-node acks=all, kill leader, fetch committed data
6. Rack-aware replica placement
   - [ ] Stub `rack` field only (placement ignored in Phase 6)

**Exit criteria**

- [x] 3-node cluster survives leader kill with no acknowledged `acks=all` data loss
  (`volant-broker` test `cluster_failover`)
- [ ] Rolling restart without full cluster downtime (manual / partial — automated deferred)
- [x] Clear consistency model doc ([docs/consistency.md](./docs/consistency.md))
- [x] Single-node mode (no cluster config) keeps Phase 1–5 tests green

**Non-goals remaining:** dynamic membership, rack-aware placement, exactly-once, Kafka wire compat.

---

### Phase 7 — Ecosystem & production readiness ✅ (MVP)

**Goal:** Something operators can run with confidence.

Binding: **[docs/PHASE7_SPEC.md](./docs/PHASE7_SPEC.md)**. Ops runbook: **[docs/ops.md](./docs/ops.md)**. Packaging: **[deploy/](./deploy/)**.

- [x] Prometheus metrics (`GET /metrics` on `--metrics-addr`) + tracing spans on produce/fetch RPC
- [x] Structured JSON logging (`--log-format text|json`)
- [x] Shared-token auth (opcodes 30/31, error codes 17/18; `--auth-token` / `VOLANT_AUTH_TOKEN`)
- [x] Optional TLS via feature `tls` (`--tls-cert` / `--tls-key`, rustls) — default build stays plaintext
- [x] Docker image + docker-compose + systemd unit (`deploy/`)
- [x] Protocol chaos tests (random/truncated decode must not panic)
- [x] Auth required / wrong token / metrics smoke integration tests
- [ ] Multi-language clients (Rust first; Go / Python FFI or REST gateway) — **deferred**
- [ ] Kafka protocol compatibility shim — **deferred**
- [ ] Full chaos mesh (partition loss, disk full, slow disk) — **deferred** (protocol chaos only)
- [ ] SCRAM / full SASL / mTLS identity mapping — **deferred**
- [ ] Security audit with `cargo fuzz` corpus CI — **deferred** (deterministic chaos tests ship now)

**Honest limitations (Phase 7):** Metrics endpoint has no auth (bind localhost). Inter-broker TLS was deferred to Phase 9 (now available when server TLS is enabled).

---

### Phase 8 — Client polish & ops packaging ✅

**Goal:** Close post-Phase-7 gaps for multi-node clients and deploys.

Binding: **[docs/PHASE8_SPEC.md](./docs/PHASE8_SPEC.md)**.

- [x] Client **leader redirect** — reconnect to partition leader on `NotLeaderForPartition`
- [x] Optional **client TLS** (`volant-client` feature `tls`, `tls_insecure` for lab certs)
- [x] CLI global `--auth-token` / `VOLANT_AUTH_TOKEN`
- [x] Minimal **Helm chart** (`deploy/helm/volant`)
- [x] Rolling restart integration test (follower down → produce continues)
- [x] Leader-redirect integration test

**Still deferred (Phase 8):** Kafka shim, multi-lang clients, inter-broker TLS, SCRAM, cargo-fuzz CI.

---

### Phase 9 — TLS hardening, multi-node deploy, fuzz ✅

**Goal:** Production-leaning TLS verification, encrypted inter-broker traffic, multi-node Helm, and a fuzz scaffold.

Binding: **[docs/PHASE9_SPEC.md](./docs/PHASE9_SPEC.md)**.

- [x] Client TLS via **webpki-roots** + optional `ClientConfig.tls_ca` PEM
- [x] **Inter-broker TLS** when server TLS is on (`--tls-peer-insecure` default true; `--tls-ca`; `--no-tls-inter-broker` escape hatch)
- [x] Multi-node **Helm StatefulSet** (`cluster.enabled`, headless Service, ConfigMap `cluster.toml`)
- [x] `fuzz/` cargo-fuzz targets (`decode_frame`, `decode_request`) + expanded deterministic chaos tests
- [x] Docs: ops.md, deploy README, ROADMAP honesty

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS identity, full cargo-fuzz corpus CI.

---

## Performance targets (aspirational)

| Metric | Single node target | Notes |
|--------|--------------------|-------|
| Produce throughput | ≥ 1M msgs/s | 100-byte payloads, batching on |
| Produce p99 latency | < 5 ms | Local NVMe, acks=1 |
| Fetch throughput | ≥ disk sequential BW | mmap / io_uring path |
| Memory (idle broker) | < 50 MB RSS | No topics under load |
| Binary size | < 15 MB stripped | `volant-server` release |

**Phase 1 baseline:** single-partition append ≥ 200k msgs/s (≈100-byte values), measured by
`cargo run -p volant-bench --release`. Broader targets will be revised after Phase 1–2 baselines.

---

## Comparison intent (vs Kafka)

| Area | Kafka | Volant direction |
|------|-------|------------------|
| Runtime | JVM | Native Rust |
| Storage | Page cache + OS | Explicit mmap + optional io_uring/O_DIRECT |
| Stream processing | Kafka Streams / ksqlDB | In-process `volant-stream` operators |
| Ops model | ZooKeeper/KRaft + heavy footprint | Single binary → small Raft quorum |
| Protocol | Kafka wire protocol | Native binary first; optional Kafka shim later |
| Goal | Full ecosystem | Subset that is fast, small, and correct |

Volant is **not** a drop-in Kafka replacement on day one. It prioritizes a clean
core over protocol compatibility; a compatibility layer is Phase 7 optional work.

---

## Suggested implementation order (PRs)

1. Phase 1 segment format + unit tests  
2. Phase 1 recovery + retention  
3. Phase 1 append bench baseline (`volant-bench`)  
4. Phase 2 protocol produce/fetch + TCP server  
5. Phase 2 client SDK + CLI  
6. Phase 3 consumer groups  
7. Phase 4 stream operators + example  
8. Phase 5 io_uring feature flag + benches  
9. Phase 6 replication prototype (2–3 nodes) ✅  
10. Phase 7 metrics, TLS, packaging ✅ (MVP; deferred items listed above)  

---

## Open design decisions

Track these before locking APIs:

1. **Replication:** ~~Raft-per-partition vs leader/follower + controller (Kafka-like)?~~ → **Kafka-style ISR (Phase 6)**
2. **Kafka wire compatibility:** first-class or optional adapter?
3. **State store for streams:** embed RocksDB, redb, or custom mmap store?
4. **Default durability:** fsync every batch vs group commit window?
5. **Multi-tenancy:** namespaces / quotas in v1 or later?

---

## Getting started (now)

```bash
# Build everything
cargo build --workspace

# Run the placeholder server
cargo run -p volant-server -- --data-dir ./data

# CLI version
cargo run -p volant-cli -- version

# Phase 1 append micro-bench (release recommended)
cargo run -p volant-bench --release

# Tests
cargo test --workspace
```

Phase 5 complete (`BufferPool`, `IoBackend`, `io-uring` / `direct-io` features,
batch produce coalescing, multi-mode `volant-bench`, `docs/tuning.md`,
`thread-per-core`). **Phase 7 MVP complete** (metrics, auth, optional TLS, packaging); deferred: Kafka shim, multi-lang, Helm, SCRAM.
