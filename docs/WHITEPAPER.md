# Volant Whitepaper

**A lightweight, high-performance streaming message broker in Rust**

| | |
|---|---|
| Version | 0.1.0 (Apache-2.0) |
| Language | Rust 1.75+ |
| Status | Phases 0–102 landed (product / git HEAD) |
| Date | 2026-07-18 |

---

## Abstract

Volant is a resource-efficient streaming message broker written in Rust. It
combines an append-only partition log (mmap segments; optional Linux
`io_uring` / `O_DIRECT`) with a small operational footprint: one server binary,
one CLI, Prometheus metrics, and optional multi-node ISR replication. A
**native binary protocol** is the primary client path; an optional **Kafka
wire-protocol shim** reuses the same storage, groups, and security model for
interop. The shim advertises **ApiVersions 0–5** and **Fetch 0–18** at Apache
Kafka wire max for those keys, with empty feature tags and
write-through transactions with soft READ_COMMITTED markers (Phase 86) and
Kafka-style COMMIT/ABORT control batches on EndTxn (Phase 89) and crash≡abort
open promote (Phase 98), prepared 2PC MVP (Phase 90) with prepared timeout
auto-abort (Phase 92), open-txn timeout (Phase 93), broker max timeout clamp
(Phase 96), background txn/session sweeper + expiry metrics (Phase 97; always-spawn
/ 0→>0 live Phase 101; graceful shutdown/join Phase 106), BROKER Describe/AlterConfigs with sparse durable restart
and name vs local `node_id` (Phase 99–103), durable OffsetForLeaderEpoch history (Phase 87 MVP),
Fetch DivergingEpoch + process-local fetch sessions (Phase 88 MVP),
omit-unchanged incremental session responses (Phase 91 MVP), and session idle
TTL / max concurrent sessions with lazy LRU eviction (Phase 95 MVP).

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

Volant targets that subset: **fast messaging**, **mmap / optional high-perf I/O**,
**in-process stream operators**, and a **single-binary ops model**.

### Design principles

| Principle | Practice |
|-----------|----------|
| Zero-copy where it counts | Batch frames, mmap reads (copied into `Bytes` for clients), length-prefixed binary protocol |
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
│  Consumers  │◀────│  server     │◀────│ (mmap / segment log) │
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
| `volant-bench` | Bench harness (`append` / `fetch` / `produce-batch`) |
| `volant-examples` | Example apps (e.g. stream word-count) |

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

### Transactions (write-through + soft markers, Phase 86)

| Capability | Status |
|------------|--------|
| transactional_id fencing | Yes |
| Multi-partition atomic produce | **Write-through** to log; LSO holds until EndTxn |
| Deferred offset commits | Applied only on commit |
| Crash of open txn | ≡ **abort** (open ranges → soft markers via `__txn_markers` + ABORT control batches, Phase 98) |
| Control markers | Soft markers (isolation SoT) + Kafka control batches on EndTxn (Phase 89) and crash promote (Phase 98) |
| Soft-marker GC | **Yes (MVP)** (Phase 104/111) — drop `end_offset <= log_start`; clip straddlers to `first_offset = log_start` |
| `READ_COMMITTED` / LSO | **Yes (MVP)** — LSO may be `<` HWM; aborted filtered; aborted list non-empty |
| `READ_UNCOMMITTED` | Sees unstable + aborted-on-log data |
| Real 2PC / prepared transactions | **MVP** (Phase 90; single-node prepare/complete; prepared timeout Phase 92; not full KIP-890/939) |
| Open txn timeout | **Yes (MVP)** (Phase 93; InitProducerId `transaction_timeout_ms` or broker default; lazy + background auto-abort) |
| Transaction max timeout | **Yes (MVP)** (Phase 96; default 15m; Init over-max → **50**; effective open/prepared clamp) |
| Background sweeper | **Yes (MVP)** (Phase 97/101/106; default 1s; open/prepared + idle sessions; `0` = pause bg; always-spawn so 0→>0 without restart; graceful shutdown/join) |

Native Volant fetch remains **committed-only**. Kafka Fetch isolation is real for
the MVP. EndTxn COMMIT/ABORT control batches are on the partition log (Phase 89);
crash≡abort of open write-through also appends ABORT control batches (Phase 98).
Empty AddPartitions-only partitions also get control markers (Phase 105;
control-only — no fake soft data ranges).

### Leader epochs (Phase 87)

| Capability | Status |
|------------|--------|
| Durable epoch → start-offset history | **Yes (MVP)** — `{data_dir}/__leader_epochs` |
| OffsetForLeaderEpoch prior epochs | Transition end offset (not always HWM) |
| Metadata `leader_epoch` | Live partition epoch |
| Full KRaft epoch state machine | **No** |
| Fetch DivergingEpoch | **Yes (MVP)** — tag 0 + OFFSET_OUT_OF_RANGE on truncation |
| Real fetch sessions | **Yes (MVP)** — process-local; omit-unchanged on empty-topics incremental (Phase 91); idle TTL + max/LRU (Phase 95); idle background-swept (Phase 97); no multi-broker stickiness |

---

## 6. Security

| Mechanism | Notes |
|-----------|-------|
| Shared-token Auth | Native protocol only; `VOLANT_AUTH_TOKEN` |
| SCRAM-SHA-256 | Durable `__scram/users.json`; **native** + Kafka SASL |
| SCRAM-SHA-512 | Dual-hash store; **Kafka SASL only** (native client is SHA-256) |
| SASL PLAIN | Kafka shim only |
| mTLS identity | Feature `tls`; CN/SAN principal |
| TLS transport | Server, client, inter-broker |
| ACLs | Topic / group / cluster (+ Kafka **User** resource store-only); durable `__acls/acls.json` |
| Metrics Bearer | Optional `--metrics-token` (Phase 21); open if unset |

Auth is required when token **or** SCRAM users **or** mTLS is configured.
Inter-broker uses shared-token Auth, not SCRAM. No GSSAPI / OAUTHBEARER.

---

## 7. Kafka compatibility shim

Enable with `--kafka-listen host:port`. Phases **23–87** shipped classic then
flexible (KIP-482) coverage for the APIs modern clients negotiate most often
(~38 keys in `SUPPORTED_APIS`).

**Source of truth for every key, version range, and open limitation:**
[KAFKA_COMPAT.md](./KAFKA_COMPAT.md). Do not treat this section as a matrix.

### Coverage classes (living summary)

| Class | Examples | Ceiling notes |
|-------|----------|---------------|
| Produce / Fetch / Metadata | TopicId, flex framing | Produce/Metadata **0–13**; Fetch **0–18** (Kafka max) |
| Groups / offsets | Join–Leave, commit/fetch | Coordinator-driven; GroupType always `classic` |
| Txn wire | Init / Add* / End / TxnOffsetCommit | Write-through + soft markers; EndTxn + crash-promote control batches (Phase 89/98) including empty AddPartitions (Phase 105); prepared 2PC MVP (Phase 90) + prepared/open timeout (Phase 92/93) + TRANSACTION_ABORTABLE subset (Phase 94) + max timeout clamp (Phase 96) + background sweeper (Phase 97/101/106) + soft-marker GC/clip (Phase 104/111) |
| Admin / configs / ACLs | CreateTopics, CreatePartitions, ACLs | CreatePartitions max **3**; ACL admin **0–3** (User resource v3); LITERAL only |
| Meta / auth | ApiVersions, FindCoordinator, SASL | ApiVersions **0–5** (Kafka max); SASL PLAIN/SCRAM |

**Auth on Kafka port:** SASL or principal `kafka-anonymous` (+ ACLs). Shared-token
Auth applies only on the native `--listen` port.

**Highlights (post–Phase 90–98):** deterministic TopicId UUIDs; KIP-951
CurrentLeader on leader errors + Produce NodeEndpoints v10+ / Fetch
NodeEndpoints v16+; KIP-890 txn max versions + prepared 2PC MVP (Phase 90) +
prepared/open timeout auto-abort (Phase 92/93) + broker max timeout clamp
(Phase 96; default 15m; Init **50**) + background sweeper + expiry metrics
(Phase 97/101) + crash≡abort ABORT control batches (Phase 98) + honest
`TRANSACTION_ABORTABLE` (123) after timeout on Produce/EndTxn/Add*/TxnOffsetCommit
(Phase 94; FindCoordinator never); BROKER Describe/AlterConfigs sparse durable
(Phase 99–102); ApiVersions 0–5 with empty feature tags and
ignored v5 ClusterId/NodeId (never `REBOOTSTRAP_REQUIRED`); Fetch **0–18**
(Kafka max) with DivergingEpoch + omit-unchanged sessions + idle TTL/max
(Phase 88/91/95); ACL admin **0–3** (User resource storage only); write-through
txn + soft READ_COMMITTED (LSO/aborted); durable OffsetForLeaderEpoch history
MVP + Metadata live leader_epoch; compression codecs gzip/snappy/lz4/zstd on
the wire.

---

## 8. Stream processing

`volant-stream` provides in-process operators (`map`, `filter`, `flat_map`,
`reduce`, windows, foreach) without a heavy runtime. State is **in-memory**;
delivery is at-least-once. No RocksDB, no distributed stream workers, no WASM
plugins (deferred).

---

## 9. Performance intent

| Metric | Status | Notes |
|--------|--------|-------|
| Single-partition append | **Measured baseline** (Phase 1) | Exit criterion ≥ 200k msgs/s; ~570k once measured on a laptop (~100-byte values). Re-run: `cargo run -p volant-bench --release -- append` |
| Produce p99 | **Aspirational** | < 5 ms local NVMe, acks=1 — not a CI SLA |
| Idle RSS | **Aspirational** | < 50 MB with no topics under load |
| Binary size | **Aspirational** | < 15 MB stripped `volant-server` |

Do not treat aspirational rows as continuous guarantees. Tuning:
[tuning.md](./tuning.md).

---

## 10. Honest non-parity (summary)

Volant deliberately does **not** claim production Kafka parity. Open gaps:

1. Multi-language clients (Rust only)
2. Dynamic membership / Raft metadata quorum
3. Multi-broker 2PC (empty AddPartitions control batches closed by Phase 105)
4. Full Kafka API surface beyond advertised keys; multi-broker session affinity / durable sessions
5. Full KRaft epoch state machine (durable history is MVP)
6. Kafka cooperative-sticky assignor **protocol** parity (native JoinGroup revoke list exists)
7. Stream state durability and distributed stream topology (`MemoryStore` only)
8. Full chaos-mesh / long fuzz campaigns (corpus smoke CI MVP → Phase 112; `fuzz/` seeds + deterministic replay)
9. ACL consensus across cluster nodes; DeleteRecords does not fan out to followers
10. Helm chart has no `--kafka-listen` surface; version **0.1.0** MVP readiness

### What is solid today

- Durable append log with crash recovery  
- Multi-partition produce/fetch with HWM semantics  
- Static multi-node ISR replication and `acks=all`  
- Consumer groups with durable offsets  
- Native security (token / SCRAM-256 / mTLS / ACL / TLS)  
- Optional Kafka shim (**38** keys in `SUPPORTED_APIS`): ApiVersions **0–5**,
  Fetch **0–18**, Produce/Metadata **0–13**, ACL admin **0–3** (see
  [KAFKA_COMPAT.md](./KAFKA_COMPAT.md))  
- Lightweight in-process stream operators  
- Ops packaging (metrics + optional Bearer, CLI, Docker, Helm)

---

## 11. Positioning vs Kafka

Native Rust single binary vs JVM + ZooKeeper/KRaft; explicit mmap / optional
`io_uring` vs page-cache-first storage; in-process `volant-stream` vs Kafka
Streams; native protocol first with optional Kafka shim vs wire-only ecosystem.
Detail: [ROADMAP.md](../ROADMAP.md) comparison table.

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
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | Phase 0–92 one-line index |
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
