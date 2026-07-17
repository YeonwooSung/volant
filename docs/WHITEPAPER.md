# Volant Whitepaper

**A lightweight, high-performance streaming message broker in Rust**

| | |
|---|---|
| Version | 0.1.0 (Apache-2.0) |
| Language | Rust 1.75+ |
| Status | Phases 0–79 landed |
| Date | 2026-07-18 |

---

## Abstract

Volant is a resource-efficient streaming message broker written in Rust. It
combines an append-only, DMA-friendly partition log with a small operational
footprint: one server binary, one CLI, Prometheus metrics, and optional
multi-node ISR replication. A **native binary protocol** is the primary client
path; an optional **Kafka wire-protocol shim** reuses the same storage, groups,
and security model for interop experiments.

Volant is **not** a drop-in Apache Kafka replacement. It prioritizes sequential
I/O, explicit complexity, and honest non-parity (especially around
transactions, isolation, and cluster control plane) over full ecosystem
coverage.

---

## 1. Motivation

Apache Kafka dominates event streaming but carries a heavy operational and
runtime tax: JVM memory, multi-component control planes, and a vast protocol
surface. Many deployments need:

- Durable, ordered partition logs with high sequential throughput
- Consumer groups and basic multi-replica durability
- A small binary and predictable latency
- Optional Kafka client interop without running a full Kafka stack

Volant targets that subset: **fast messaging**, **DMA-oriented storage**,
**in-process stream operators**, and a **single-binary ops model**.

### Design principles

| Principle | Practice |
|-----------|----------|
| Zero-copy where it counts | Batch frames, mmap reads, length-prefixed binary protocol |
| Sequential I/O wins | Append-only segment logs; avoid random writes on the hot path |
| Explicit complexity | Single-node correct first; static membership + ISR (not Raft-per-partition) |
| Resource efficiency | Native binary, no GC tax, bounded buffers |
| Operability | One server, one CLI, structured logs, clear metrics |
| Streaming-first | Produce/consume **plus** first-class in-process operators |

---

## 2. Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Producers  │────▶│ volant-     │────▶│ volant-storage   │
│  Consumers  │◀────│  server     │◀────│ (mmap / DMA log) │
│  Stream apps│     │  + broker   │     └──────────────────┘
│  Kafka clis │     └──────┬──────┘
└─────────────┘            │
                    ┌──────▼──────┐
                    │ volant-     │
                    │  stream     │  (map / filter / window)
                    └─────────────┘
```

### Dual protocol surface

| Port | Protocol | Clients |
|------|----------|---------|
| `--listen` (default `:9092`) | Native Volant binary | `volant-client`, `volant` CLI |
| `--kafka-listen` (optional) | Kafka wire (classic + flexible) | Kafka-compatible clients |

Both paths share `volant-broker` topics, partitions, groups, transactions, ACLs,
and `volant-storage` logs.

### Crate map

| Crate | Role |
|-------|------|
| `volant-core` | Shared types: `Message`, `Record`, `Offset`, errors |
| `volant-protocol` | Native wire codec (frames, opcodes, CRC) |
| `volant-storage` | Partition log, segments, mmap / optional `io_uring` + `O_DIRECT` |
| `volant-broker` | Topics, produce/fetch, groups, cluster, ACL/SCRAM, Kafka shim |
| `volant-client` | Async producer/consumer SDK |
| `volant-stream` | In-process stream operators |
| `volant-server` | Process entrypoint |
| `volant-cli` | Admin CLI (`volant`) |
| `volant-bench` | Storage micro-benchmarks |

---

## 3. Storage model

**Unit of durability:** `PartitionLog` → ordered `Segment`s under `{data_dir}`.

| Concern | Behavior |
|---------|----------|
| Segment files | `.log` + sparse `.index`; magic `0x564C4E54` (“VLNT”) |
| Append | Buffered sequential write; batch produce + single flush policy |
| Read | `mmap` sealed/active segments; copy into `Bytes` |
| Recovery | Scan segments; rebuild next offset + index; torn-tail truncate |
| Time retention | `retention.ms` drops old sealed segments |
| Size retention | `retention.bytes` drops oldest until under budget |
| DeleteRecords | Truncate whole sealed segments before offset |
| Compaction | `cleanup.policy=compact` on sealed segments; empty value = tombstone |

**Feature-gated I/O (Phase 5):** default buffered append + mmap; optional Linux
`io_uring` append/fsync and `O_DIRECT` on the active segment; optional
thread-per-core pinning via `VOLANT_CPU_LIST`.

**Compaction honesty:** no dirty-ratio gating; active segment compacted only
after roll; tombstones dropped at compact time; replicas compact independently.

---

## 4. Consistency and clustering

### Single-node

Omit `--cluster-config`: RF = 1, ISR = `[self]`, HWM = LEO always. `acks=all`
behaves like `acks=1`.

### Multi-node (Phase 6)

Static membership from `cluster.toml`. Controller = lowest live broker id.
Leaders accept produce; followers replicate via inter-broker `ReplicaFetch`.

| Term | Definition |
|------|------------|
| **LEO** | Next offset the local replica will write |
| **HWM** | `min(LEO of every broker in the ISR)` |
| **Committed** | `offset < HWM` (Fetch never returns uncommitted data) |

| `acks` | When produce responds |
|--------|------------------------|
| 0 / 1 | After local leader append |
| all (255) | After HWM covers the batch; requires `|ISR| ≥ min_insync_replicas` |

**Survives leader crash:** only `acks=all` with response received (data on all
ISR members). See [consistency.md](./consistency.md).

**Not Raft-per-partition**, no dynamic membership, no Raft metadata quorum.
Metadata may be briefly stale during controller failover.

---

## 5. Consumer groups and transactions

### Groups

Join / Heartbeat / Leave / Sync with durable offsets under
`{data_dir}/__consumer_offsets/`. Sticky assignor (default), cooperative revoke
list (Phase 17), static membership via `group_instance_id` → `static:{id}`.
Admin: list / describe / delete groups, delete offsets, lag metrics.

### Transactions (buffer-until-commit)

| Capability | Status |
|------------|--------|
| transactional_id fencing | Yes |
| Multi-partition atomic produce | Buffered **off-log** until commit |
| Deferred offset commits | Applied only on commit |
| Crash of open txn | ≡ **abort** (in-flight is memory-only) |
| Control markers | **No** |
| True `READ_COMMITTED` / LSO filtering | **No** — LSO always equals HWM |
| Real 2PC / prepared transactions | **No** (Kafka wire fields ignored) |

This is intentional honesty: Volant achieves multi-partition atomicity without
writing aborted data to the log, at the cost of Kafka-style isolation semantics.

---

## 6. Security

| Mechanism | Notes |
|-----------|-------|
| Shared-token Auth | Native protocol; `VOLANT_AUTH_TOKEN` |
| SCRAM-SHA-256 / 512 | Durable `__scram/users.json`; Kafka SASL + native |
| SASL PLAIN | Kafka shim only |
| mTLS identity | Feature `tls`; CN/SAN principal |
| TLS transport | Server, client, inter-broker |
| ACLs | Principal / resource / op; durable `__acls/acls.json` |
| Metrics Bearer | `--metrics-token` |

Auth is required when token **or** SCRAM users **or** mTLS is configured.
Inter-broker uses shared-token Auth, not SCRAM. No GSSAPI / OAUTHBEARER.

---

## 7. Kafka compatibility shim

Enable with `--kafka-listen host:port`. Phases **23–80** built classic then
flexible (KIP-482) coverage for the APIs modern clients negotiate most often.

**Authoritative API versions, per-key notes, and open limitations:**
[KAFKA_COMPAT.md](./KAFKA_COMPAT.md). The summary matrix below may lag; trust
KAFKA_COMPAT when they disagree.

### Advertised version matrix (summary)

| API | Versions | API | Versions |
|-----|----------|-----|----------|
| Produce | 0–13 | Fetch | 0–13 |
| Metadata | 0–13 | ListOffsets | 0–11 |
| OffsetCommit / Fetch | 0–10 | FindCoordinator | 0–4 |
| JoinGroup | 0–9 | Heartbeat | 0–4 |
| LeaveGroup | 0–5 | SyncGroup | 0–5 |
| DescribeGroups | 0–6 | ListGroups | 0–5 |
| DeleteGroups | 0–3 | ApiVersions | 0–3 |
| CreateTopics | 0–7 | DeleteTopics | 0–6 |
| CreatePartitions | 0–3 | InitProducerId | 0–6 |
| AddPartitionsToTxn | 0–5 | EndTxn | 0–5 |
| TxnOffsetCommit | 0–6 | AddOffsetsToTxn | 0–3 |
| DescribeConfigs | 0–4 | AlterConfigs | 0–2 |
| IncrementalAlterConfigs | 0–1 | DeleteRecords | 0–2 |
| ACL admin | 0–2 | OffsetForLeaderEpoch | 0–4 |
| SaslHandshake | 0–1 | SaslAuthenticate | 0–2 |
| DescribeCluster | 0–2 | ListTransactions | 0–2 |
| DescribeProducers / Transactions | 0 | OffsetDelete | 0 |

**Highlights:** TopicId (deterministic UUID), KIP-951 CurrentLeader tags on
leader errors, KIP-890-era txn max versions (2PC fields parsed and ignored),
RecordBatch + MessageSet compression (gzip/snappy/lz4/zstd).

---

## 8. Stream processing

`volant-stream` provides in-process operators (`map`, `filter`, `flat_map`,
`reduce`, windows, foreach) without a heavy runtime. State is **in-memory**;
delivery is at-least-once. No RocksDB, no distributed stream workers, no WASM
plugins (deferred).

---

## 9. Performance intent

| Metric | Target / baseline | Notes |
|--------|-------------------|-------|
| Single-partition append | ≥ 200k msgs/s exit; ~570k measured | ~100-byte values, laptop |
| Produce p99 (aspirational) | < 5 ms | Local NVMe, acks=1 |
| Idle RSS (aspirational) | < 50 MB | No topics under load |
| Binary size (aspirational) | < 15 MB stripped | `volant-server` release |

Baselines via `cargo run -p volant-bench --release`. Tuning:
[tuning.md](./tuning.md).

---

## 10. Honest non-parity (summary)

Volant deliberately does **not** claim production Kafka parity. Open gaps:

1. Multi-language clients (Rust only)
2. Dynamic membership / Raft metadata quorum
3. True control-marker `READ_COMMITTED` and real 2PC
4. Full Kafka API surface (many keys absent; no real fetch sessions)
5. Durable leader-epoch history (eligible epochs map to HWM)
6. Kafka cooperative-sticky assignor protocol parity
7. Stream state durability and distributed stream topology
8. Full chaos-mesh / cargo-fuzz corpus CI
9. ACL consensus across cluster nodes
10. Version **0.1.0** — MVP-oriented production readiness

### What is solid today

- Durable append log with crash recovery  
- Multi-partition produce/fetch with HWM semantics  
- Static multi-node ISR replication and `acks=all`  
- Consumer groups with durable offsets  
- Native security (token / SCRAM / mTLS / ACL / TLS)  
- Large Kafka wire surface for interop experiments  
- Lightweight in-process stream operators  
- Ops packaging (metrics, CLI, Docker, Helm)

---

## 11. Positioning vs Kafka

| Area | Kafka | Volant direction |
|------|-------|------------------|
| Runtime | JVM | Native Rust |
| Storage | Page cache + OS | Explicit mmap + optional io_uring / O_DIRECT |
| Stream processing | Kafka Streams / ksqlDB | In-process `volant-stream` |
| Ops model | ZooKeeper/KRaft + heavy footprint | Single binary → small static quorum |
| Protocol | Kafka wire | Native first; optional Kafka shim |
| Goal | Full ecosystem | Subset that is fast, small, and correct |

---

## 12. Getting started

```bash
cargo build --workspace
cargo run -p volant-server -- --data-dir /tmp/vdata --listen 127.0.0.1:9092
cargo run -p volant-cli -- topic create events --partitions 3 --broker 127.0.0.1:9092
cargo run -p volant-cli -- produce events --value hello --broker 127.0.0.1:9092
```

Optional Kafka port:

```bash
cargo run -p volant-server -- \
  --data-dir /tmp/vdata \
  --listen 127.0.0.1:9092 \
  --kafka-listen 127.0.0.1:9093
```

---

## 13. Document map

| Doc | Role |
|-----|------|
| [INDEX.md](./INDEX.md) | Documentation index |
| [ops.md](./ops.md) | Operator runbook |
| [consistency.md](./consistency.md) | HWM / ISR / acks |
| [tuning.md](./tuning.md) | Performance tuning |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | Current Kafka API matrix + honesty |
| [features.md](./features.md) | Native features (post-core) |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | Phase 0–79 one-line index |
| [PHASE1_SPEC.md](./PHASE1_SPEC.md)–[PHASE6_SPEC.md](./PHASE6_SPEC.md) | Binding core specs |
| [../ROADMAP.md](../ROADMAP.md) | Full roadmap + deferred work |
| [../README.md](../README.md) | Quick start |

---

## Closing

Volant is a single-binary Rust streaming broker optimized for sequential logs
and operational simplicity. Use the native protocol for the best path; enable
the Kafka shim when client interop matters; and treat transaction/isolation and
cluster control-plane limitations as first-class product facts—not temporary
bugs. That honesty is part of the design.
