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
| `volant-bench` | Storage / broker micro-benchmarks |

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

### Phase 3 — Consumer groups & offsets *(next)*

**Goal:** Coordinated multi-consumer reading with committed offsets.

**Milestones**

1. Internal `__consumer_offsets` topic (or dedicated offset store)
2. Group membership + heartbeat + rebalance (eager rebalance first)
3. `OffsetCommit` / `OffsetFetch`
4. Assignor: range + sticky (cooperative later)
5. Lag metrics per group / partition

**Exit criteria**

- Two consumers in one group split partitions
- Restart resumes from committed offsets
- Rebalance completes without stuck partitions

---

### Phase 4 — Stream processing (lightweight)

**Goal:** Kafka Streams–like operators without a heavy runtime.

**Milestones**

1. Operator trait + pipeline (scaffold exists)
2. Stateless: `map`, `filter`, `flat_map`, `foreach`
3. Stateful: `reduce`, tumbling / hopping windows (RocksDB or custom store)
4. Source / sink adapters to Volant topics
5. At-least-once processing; exactly-once via transactional produce (stretch)
6. Optional WASM or plugin operators later

**Exit criteria**

- Word-count style topology on live topics
- Documented programming model + example crate

---

### Phase 5 — DMA & high-performance I/O

**Goal:** Push the storage/network path to hardware-friendly limits.

**Milestones**

1. **Linux `io_uring`** for append + sendfile-style fetch (feature-gated)
2. **O_DIRECT** optional path for predictable latency (aligned buffers)
3. **Batch produce coalescing** in the broker
4. **Kernel bypass experiments** (DPDK / AF_XDP) — research only
5. **CPU affinity / thread-per-core** optional runtime mode
6. Memory pool + slab for record headers to cut allocator pressure

**Exit criteria**

- Benchmark suite (`volant-bench`) with published numbers
- Feature flags: `io-uring`, `direct-io`, `thread-per-core`
- Documented tuning guide (ulimit, disk, NIC, huge pages)

**Note:** DMA here means minimizing user↔kernel copies and enabling device-level
transfers where the OS allows — not a custom hardware driver.

---

### Phase 6 — Clustering & replication

**Goal:** Scale beyond one node with durable multi-replica partitions.

**Milestones**

1. Cluster membership (static config → gossip / Raft later)
2. Partition leader + followers (ISR-style or Raft log replication)
3. Controller / metadata quorum (dedicated Raft group)
4. Producer `acks=all`, min ISR
5. Automatic leader election on failure
6. Rack-aware replica placement

**Exit criteria**

- 3-node cluster survives leader kill with no acknowledged data loss
- Rolling restart without full cluster downtime
- Clear consistency model doc (what “committed” means)

---

### Phase 7 — Ecosystem & production readiness

**Goal:** Something operators can run with confidence.

- [ ] Prometheus metrics + tracing spans on hot paths
- [ ] Structured JSON logging
- [ ] TLS + SASL-style auth
- [ ] Multi-language clients (Rust first; Go / Python FFI or REST gateway)
- [ ] Kafka protocol compatibility shim (optional — migrate without rewrite)
- [ ] Helm chart / systemd unit / Docker image
- [ ] Chaos tests (partition loss, disk full, slow disk)
- [ ] Security audit of protocol parser (fuzzing with `cargo fuzz`)

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
9. Phase 6 replication prototype (2–3 nodes)  
10. Phase 7 metrics, TLS, packaging  

---

## Open design decisions

Track these before locking APIs:

1. **Replication:** Raft-per-partition vs leader/follower + controller (Kafka-like)?
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

Phase 2 complete. Next: **Phase 3 — consumer groups and committed offsets**.
