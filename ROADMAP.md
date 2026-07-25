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
   - [x] Retry + idempotent produce (PID + sequence) — **Phase 10**
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
   - [x] Prometheus + CLI lag — **Phase 10**

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
- [ ] SCRAM / full SASL — **deferred** (mTLS identity mapping: **Phase 19**)
- [ ] Security audit with `cargo fuzz` corpus CI — **deferred** as full audit/long campaigns; deterministic chaos + **corpus smoke CI MVP closed by Phase 112**

**Honest limitations (Phase 7):** Metrics endpoint auth deferred then closed by **Phase 21** (`--metrics-token`). Inter-broker TLS was deferred to Phase 9 (now available when server TLS is enabled).

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

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS identity;
full cargo-fuzz corpus CI → **closed (MVP) by Phase 112** (seed smoke + CI;
long campaigns still deferred).

---

### Phase 10 — Idempotent produce, retries, consumer lag ✅

**Goal:** Safe produce retries and group lag visibility.

Binding: **[docs/PHASE10_SPEC.md](./docs/PHASE10_SPEC.md)**.

- [x] `InitProducerId` (opcode 32/33) + produce PID/epoch/sequence trailer
- [x] Broker in-memory de-dupe (duplicate batch → same offsets; OOO/epoch/unknown PID errors 19–21)
- [x] Client `enable_idempotence`, `max_retries`, `retry_backoff_ms`
- [x] Prometheus `volant_consumer_group_lag{group,topic,partition}`
- [x] CLI `volant group lag --group G`
- [x] Integration tests (`phase10_idempotent_lag`)

**Honest limitations (Phase 10):** producer state was in-memory only — closed by **Phase 11**.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 11 — Sticky assignor, durable producer state, group describe ✅

**Goal:** Lower rebalance churn, survive broker restart for idempotent PIDs, ops visibility into groups.

Binding: **[docs/PHASE11_SPEC.md](./docs/PHASE11_SPEC.md)**.

- [x] Sticky partition assignor as default group rebalance (`sticky_assign` / `sticky_assign_multi`)
- [x] Durable producer state under `{data_dir}/__producer_state/state.json`
- [x] `DescribeGroup` (opcode 34/35) + `Client::describe_group` + `volant group describe`
- [x] Integration tests (`phase11_sticky_durable`)

**Honest limitations (at ship):** eager rebalance only — closed by **Phase 17** for incremental handoff; sticky is Volant-local (not Kafka sticky wire protocol); no multi-partition transactions.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 12 — Group admin: ListGroups, DeleteOffsets, static membership ✅

**Goal:** Ops visibility and offset management for consumer groups; stable member ids for redeploys.

Binding: **[docs/PHASE12_SPEC.md](./docs/PHASE12_SPEC.md)**.

- [x] `ListGroups` (opcode 36/37) + `Client::list_groups` + `volant group list`
- [x] `DeleteOffsets` (opcode 38/39) + `Client::delete_offsets` + `volant group delete-offsets`
- [x] Static membership via JoinGroup `group_instance_id` → `static:{id}` member ids
- [x] Integration tests (`phase12_group_admin`)

**Honest limitations (at ship):** eager rebalance only — closed by **Phase 17**; static membership is Volant-local (not Kafka `group.instance.id` wire parity).

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 13 — Topic configs & retention ops ✅

**Goal:** Per-topic retention/segment settings with durable store, protocol, CLI, and background apply.

Binding: **[docs/PHASE13_SPEC.md](./docs/PHASE13_SPEC.md)**.

- [x] Config keys: `retention.ms`, `retention.bytes`, `segment.bytes`
- [x] Durable store `{data_dir}/__topic_configs/`
- [x] CreateTopic config trailer; `DescribeConfigs` (40/41); `AlterConfigs` (42/43)
- [x] Client + CLI (`topic create` flags, `topic describe`, `topic config get|set`)
- [x] Background retention task (5s)
- [x] Integration tests (`phase13_topic_configs`)

**Honest limitations (at ship):** configs durable; topic partition auto-reload on restart deferred to **Phase 14**. No compact cleanup policy.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 14 — Durable topic catalog & DeleteRecords ✅

**Goal:** Single-node topics and data survive broker restart; admin truncate-by-offset.

Binding: **[docs/PHASE14_SPEC.md](./docs/PHASE14_SPEC.md)**.

- [x] Durable catalog `{data_dir}/__topics/catalog.json` (id + partition count)
- [x] `Broker::new` reloads topics and opens existing partition logs (+ Phase 13 configs)
- [x] Persist catalog on create/delete (single-node); cluster still uses `assignment.json`
- [x] `DeleteRecords` (opcode 44/45) + `Client::delete_records` + `volant topic delete-records`
- [x] Integration tests (`phase14_topic_catalog`)

**Honest limitations:** DeleteRecords does not fan out to cluster followers; no compact policy; dynamic partition count increase deferred to **Phase 15**.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 15 — CreatePartitions & ListOffsets ✅

**Goal:** Grow topic partition counts and inspect log offset ranges for ops.

Binding: **[docs/PHASE15_SPEC.md](./docs/PHASE15_SPEC.md)**.

- [x] `CreatePartitions` (opcode 46/47) — increase total partition count
- [x] Single-node catalog update; multi-node controller + assignment.json
- [x] `ListOffsets` (opcode 48/49) — earliest (log start) + latest (LEO) per partition
- [x] Client + CLI (`topic add-partitions`, `topic offsets`)
- [x] Integration tests (`phase15_partitions_offsets`)

**Honest limitations:** cannot shrink partitions; new partitions start empty; cluster CreatePartitions does not wait for all brokers; ListOffsets latest is LEO not client HWM; compact policy deferred to **Phase 16**.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 16 — Log compaction (`cleanup.policy`) ✅

**Goal:** Key-based compaction of sealed segments for changelog-style topics.

Binding: **[docs/PHASE16_SPEC.md](./docs/PHASE16_SPEC.md)**.

- [x] Config `cleanup.policy` = `delete` | `compact` (default delete)
- [x] `PartitionLog::compact_sealed` — latest value per key; empty value = tombstone; null keys kept
- [x] Sparse offsets in sealed segments (recovery + rewrite preserve original offsets)
- [x] Applied on background retention loop when policy is compact
- [x] CLI `--cleanup-policy` / `config set cleanup.policy`
- [x] Integration tests (`phase16_compaction`)

**Honest limitations:** no dirty-ratio gating; active segment not compacted until roll; tombstones dropped at compact time (no separate tombstone retention); cluster replicas compact independently.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 17 — Cooperative rebalance ✅

**Goal:** Incremental partition handoff on rebalance — keep fetch positions for sticky-retained partitions; surface revoked lists.

Binding: **[docs/PHASE17_SPEC.md](./docs/PHASE17_SPEC.md)**.

- [x] JoinGroup response trailing `revoked` list (backward compatible)
- [x] Coordinator tracks per-member `delivered` assignment for accurate revoke
- [x] `GroupConsumer` cooperative position handoff (retain / add / drop)
- [x] CLI join line prints `revoked=...`
- [x] Integration tests (`phase17_cooperative`)

**Honest limitations:** not Kafka cooperative-sticky (no two-phase revoke barrier / assignor epochs); revoke applied at re-join not mid-batch; sticky assignor still Volant-local.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS.

---

### Phase 18 — Transactions MVP ✅

**Goal:** Multi-partition atomic produce (and deferred offsets) with transactional id fencing.

Binding: **[docs/PHASE18_SPEC.md](./docs/PHASE18_SPEC.md)**.

- [x] `InitProducerId` optional `transactional_id` + epoch fencing
- [x] `BeginTxn` / `EndTxn` (opcodes 50–53); error `InvalidTxnState` (22)
- [x] Broker-side off-log buffer; commit flushes all batches; abort drops
- [x] Deferred offset commits on EndTxn trailer
- [x] `TransactionalProducer` client helper + `volant txn produce` CLI
- [x] Integration tests (`phase18_transactions`)

**Honest limitations:** in-flight txn is memory-only (crash ≡ abort); no Kafka control markers / `READ_COMMITTED` fetch filter; produce-in-txn responses do not carry final log offsets (see EndTxn results); single-node coordinator only.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM / full SASL.

---

### Phase 19 — mTLS identity mapping ✅

**Goal:** Mutual TLS with client cert verification; map verified CN → connection principal; cert auth without shared token.

Binding: **[docs/PHASE19_SPEC.md](./docs/PHASE19_SPEC.md)**.

- [x] `--tls-client-ca` enables mTLS (`WebPkiClientVerifier`)
- [x] `--tls-client-allow` optional CN allowlist
- [x] Principal from leaf CN (fallback DNS SAN); authenticates without Auth token
- [x] Client `tls_cert` / `tls_key` presentation (`ClientConfig`, feature `tls`)
- [x] Inter-broker presents server cert as client identity when mTLS is on
- [x] Integration tests (`phase19_mtls`) + principal unit test

**Honest limitations:** CN/SAN principal only (no SPIFFE); per-topic ACLs closed by **Phase 20**; metrics auth closed by **Phase 21**; inter-broker reuses server cert (no separate peer identity file).

**Still deferred:** Kafka shim, multi-lang clients, SCRAM / full SASL.

---

### Phase 20 — Principal-based ACLs ✅

**Goal:** Authorize requests against the connection principal (mTLS CN or token principal).

Binding: **[docs/PHASE20_SPEC.md](./docs/PHASE20_SPEC.md)**.

- [x] ACL model: principal / resource_type / resource / operation / allow|deny; `*` wildcards
- [x] Default deny when enabled; deny overrides allow; super-users bypass
- [x] Enforce on produce/fetch/admin/group ops; skip inter-broker opcodes
- [x] `CreateAcls` / `DeleteAcls` / `ListAcls` (opcodes 54–59); error `AuthorizationFailed` (23)
- [x] Server: `--acl-enable`, `--acl-file`, `--acl-super-users`, `--auth-principal`
- [x] Client + `volant acl create|list|delete` CLI
- [x] Integration tests (`phase20_acls`)

**Honest limitations:** durable store closed by **Phase 21**; no cluster ACL consensus; no prefix patterns beyond `*`; Metadata-all uses Cluster Describe only; inter-broker not ACL-gated.

**Still deferred:** Kafka shim, multi-lang clients, SCRAM / full SASL.

---

### Phase 21 — Durable ACLs + metrics auth ✅

**Goal:** Persist ACLs under the broker data dir; protect Prometheus `/metrics` with a shared Bearer token.

Binding: **[docs/PHASE21_SPEC.md](./docs/PHASE21_SPEC.md)**.

- [x] `{data_dir}/__acls/acls.json` snapshot (`enabled` + `entries`); atomic write
- [x] Load on `Broker::new`; CreateAcls / DeleteAcls / flag import auto-save
- [x] `--metrics-token` / `VOLANT_METRICS_TOKEN` → `Authorization: Bearer` on `/metrics`
- [x] Integration tests (`phase21_durable_acls_metrics`) + durable unit test

**Honest limitations:** single-node file only (no ACL raft); super-users still runtime flags; metrics auth is shared-token only (no mTLS on metrics port).

**Still deferred:** Kafka shim, multi-lang clients, SCRAM / full SASL.

---

### Phase 22 — SCRAM-SHA-256 authentication ✅

**Goal:** User/password auth via SCRAM-SHA-256 (RFC 5802 crypto, Volant binary wire); durable credentials; principal = username for ACLs.

Binding: **[docs/PHASE22_SPEC.md](./docs/PHASE22_SPEC.md)**.

- [x] Opcodes 60–69: ScramFirst/Final, Create/Delete/ListScramUser(s)
- [x] Durable `{data_dir}/__scram/users.json` (salt + StoredKey + ServerKey)
- [x] `auth_required` when token **or** SCRAM users **or** mTLS
- [x] Bootstrap `CreateScramUser` when store empty; `--scram-user user:pass`
- [x] Client `scram_username`/`scram_password`; CLI `--scram-user`/`--scram-password` + `user create|list|delete`
- [x] Integration tests (`phase22_scram`) + unit crypto roundtrip

**Honest limitations:** no channel binding; no SCRAM-SHA-512; not Kafka SASL handshake bytes; CreateScramUser sends password in clear (use TLS); challenge state is per-TCP-connection only; multi-node inter-broker still uses shared-token Auth (not SCRAM).

**Still deferred:** Kafka shim, multi-lang clients, full SASL (PLAIN/GSSAPI), SCRAM-SHA-512.

---

### Phase 23 — Kafka wire protocol shim (MVP) ✅

**Goal:** Optional second listen port speaking classic Kafka framing so simple
clients can discover metadata and produce/fetch MessageSets against the same
broker storage.

Binding: **[docs/PHASE23_SPEC.md](./docs/PHASE23_SPEC.md)**.

- [x] `--kafka-listen host:port` (default disabled; native protocol on `--listen`)
- [x] ApiVersions (0), Metadata (0–1; raised to 0–8 in Phase 38), Produce (0), Fetch (0)
- [x] Legacy MessageSet magic 0/1 encode/decode
- [x] ACL checks as principal `kafka-anonymous` when ACLs enabled
- [x] Integration tests (`phase23_kafka_shim`) + MessageSet unit tests

**Honest limitations (at ship):** no magic=2 RecordBatch — closed by **Phase 24**;
no Kafka consumer groups / SASL / CreateTopics on the shim port; no flexible
versions; sequential req/resp only.

**Still deferred (at ship):** multi-lang clients, RecordBatch, full Kafka API,
Kafka SASL, full SASL/SCRAM-SHA-512, cargo-fuzz corpus CI.

---

### Phase 24 — Kafka RecordBatch (magic 2) ✅

**Goal:** Accept and emit Kafka RecordBatch (magic=2) on the shim port so modern
clients that no longer speak legacy MessageSet can produce/fetch.

Binding: **[docs/PHASE24_SPEC.md](./docs/PHASE24_SPEC.md)**.

- [x] Auto-detect produce payload by magic at byte 16 (0/1 MessageSet, 2 RecordBatch)
- [x] RecordBatch encode/decode with CRC-32C + zig-zag varint records + headers
- [x] Produce advertised 0–3 (v3 transactional_id field ignored); Fetch 0–4
- [x] Fetch v0–3 → MessageSet; Fetch v4 → RecordBatch (+ throttle, LSO)
- [x] Reject compressed batches; integration tests (`phase24_record_batch`)

**Honest limitations:** no compression (gzip/snappy/lz4/zstd); no transactional /
idempotent producer semantics; no control batches; Fetch v4 has empty aborted
txns only; no flexible versions / tagged fields; no Kafka consumer groups or SASL.

**Still deferred (at ship):** multi-lang clients, Kafka admin APIs (CreateTopics /
ListOffsets), consumer groups, SASL, SCRAM-SHA-512, cargo-fuzz corpus CI.

---

### Phase 25 — Kafka admin APIs (Create/DeleteTopics, ListOffsets) ✅

**Goal:** Enough Kafka admin surface on `--kafka-listen` that clients can create
topics and discover offsets without speaking the native Volant protocol.

Binding: **[docs/PHASE25_SPEC.md](./docs/PHASE25_SPEC.md)**.

- [x] CreateTopics 0–1 → `Broker::create_topic` (RF/assignment ignored)
- [x] DeleteTopics 0–1 → `Broker::delete_topic`
- [x] ListOffsets 0–1 → earliest (-2) / latest (-1) via `Broker::list_offsets`
- [x] ApiVersions advertises keys 2 / 19 / 20; ACL checks for create/delete/describe
- [x] Integration tests (`phase25_kafka_admin`)

**Honest limitations:** no CreatePartitions / DescribeConfigs / AlterConfigs on
Kafka wire; no timestamp-indexed ListOffsets; replica assignment from CreateTopics
ignored; no Kafka consumer groups or SASL.

**Still deferred (at ship):** multi-lang clients, Kafka consumer groups / offset
commit, Kafka SASL, SCRAM-SHA-512, cargo-fuzz corpus CI.

---

### Phase 26 — Kafka consumer groups on the shim ✅

**Goal:** Map Kafka FindCoordinator / JoinGroup / SyncGroup / Heartbeat /
LeaveGroup / OffsetCommit / OffsetFetch onto Volant's existing group coordinator
so simple Kafka consumers can subscribe and commit offsets on `--kafka-listen`.

Binding: **[docs/PHASE26_SPEC.md](./docs/PHASE26_SPEC.md)**.

- [x] FindCoordinator (0) → advertised broker host/port
- [x] JoinGroup (0–1) + SyncGroup (0) with consumer protocol subscription/assignment
- [x] Heartbeat / LeaveGroup (0)
- [x] OffsetCommit (0–2) / OffsetFetch (0–1) via durable `__consumer_offsets`
- [x] Coordinator-driven assignment (leader SyncGroup payload ignored)
- [x] Integration tests (`phase26_kafka_groups`)

**Honest limitations:** not a full Kafka assignor (eager coordinator assignment);
no Describe/List/DeleteGroups on Kafka wire; FindCoordinator may return native
`--listen` port; no Kafka SASL / static `group.instance.id` wire fields.

**Still deferred (at ship):** multi-lang clients, Kafka ops admin APIs,
Kafka SASL, SCRAM-SHA-512, cargo-fuzz corpus CI.

---

### Phase 27 — Kafka ops surface (groups + configs + partitions) ✅

**Goal:** Admin/ops APIs on `--kafka-listen` for group visibility and topic
lifecycle beyond create/delete: list/describe/delete groups, grow partitions,
read/write topic configs.

Binding: **[docs/PHASE27_SPEC.md](./docs/PHASE27_SPEC.md)**.

- [x] ListGroups (16) / DescribeGroups (15) / DeleteGroups (42)
- [x] CreatePartitions (37) → `Broker::create_partitions` (total count)
- [x] DescribeConfigs (32) / AlterConfigs (33) for TOPIC resources
- [x] Volant keys: `retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`
- [x] Integration tests (`phase27_kafka_ops`)

**Honest limitations:** TOPIC configs only (no broker resources); no config
synonyms/docs; DescribeGroups member metadata is best-effort; CreatePartitions
ignores replica assignment; no IncrementalAlterConfigs; no Kafka SASL.

---

### Phase 28 — Kafka RecordBatch compression ✅

**Goal:** Accept compressed RecordBatch payloads from real Kafka producers
(gzip / snappy / lz4 / zstd) on the shim Produce path.

Binding: **[docs/PHASE28_SPEC.md](./docs/PHASE28_SPEC.md)**.

- [x] Decompress attributes bits 0–2 on Produce (RecordBatch magic 2)
- [x] Codecs: none, gzip (`flate2`), snappy (Xerial + raw), lz4 frame, zstd
- [x] `encode_record_batch_compressed` for tests / tooling
- [x] Fetch still returns uncompressed RecordBatch / MessageSet
- [x] Integration tests (`phase28_compression`)

**Honest limitations:** MessageSet compression still unsupported; Fetch never
compresses; snappy/lz4 framing is best-effort Kafka-compatible; no compression
level knobs; no client-negotiated preferred codec.

---

### Phase 29 — Kafka InitProducerId & idempotent Produce ✅

**Goal:** Modern Kafka producers with `enable.idempotence=true` can allocate a
PID via InitProducerId and de-dupe RecordBatch produces on the shim.

Binding: **[docs/PHASE29_SPEC.md](./docs/PHASE29_SPEC.md)**.

- [x] InitProducerId (22) v0–1 → `Broker::init_producer_id_with_txn`
- [x] RecordBatch producerId / epoch / baseSequence on Produce
- [x] De-dupe via Phase 10/11 `check_idempotent_produce` + durable state
- [x] Kafka error mapping (45 / 47 / 48 / 59)
- [x] Integration tests (`phase29_idempotent`)

**Honest limitations:** No Kafka transactions (Begin/End/AddPartitions) on the
shim; MessageSet cannot carry sequences; `transaction_timeout_ms` ignored;
Volant reserves PID `0` as non-idempotent.

---

### Phase 30 — Kafka SASL (PLAIN + SCRAM-SHA-256) ✅

**Goal:** Real Kafka clients can authenticate on `--kafka-listen` with PLAIN or
SCRAM-SHA-256 using the same durable users as Volant Phase 22.

Binding: **[docs/PHASE30_SPEC.md](./docs/PHASE30_SPEC.md)**.

- [x] SaslHandshake (17) v0–1 — mechanisms `PLAIN`, `SCRAM-SHA-256`
- [x] SaslAuthenticate (36) v0–1 — PLAIN + SCRAM multi-step
- [x] `ScramStore::verify_password` for PLAIN
- [x] Connection principal after success; ACL checks use it
- [x] When SCRAM users exist, gate non-auth APIs
- [x] Integration tests (`phase30_kafka_sasl`)

**Honest limitations:** No SCRAM-SHA-512 / GSSAPI / OAUTHBEARER; no channel
binding; no pre-1.0 raw SASL frames; shared-token does not apply to Kafka port.

---

### Phase 31 — Kafka transactions on the shim ✅

**Goal:** Kafka transactional producers can commit/abort multi-partition
produces (and deferred offsets) on `--kafka-listen` via Phase 18 buffer semantics.

Binding: **[docs/PHASE31_SPEC.md](./docs/PHASE31_SPEC.md)**.

- [x] AddPartitionsToTxn (24) → `ensure_txn_open`
- [x] AddOffsetsToTxn (25) / TxnOffsetCommit (28) → deferred offsets
- [x] EndTxn (26) → commit/abort flush
- [x] Transactional Produce buffers off-log until EndTxn
- [x] FindCoordinator v1 (`key_type` group|transaction)
- [x] Integration tests (`phase31_transactions`)

**Honest limitations:** No control markers / `READ_COMMITTED` fetch filtering;
crash ≡ abort; `transaction_timeout_ms` ignored; no flexible versions;
partition membership not strictly enforced after AddPartitions.

---

### Phase 32 — Kafka compressed Fetch (RecordBatch) ✅

**Goal:** Fetch v4 responses on `--kafka-listen` can return compressed
RecordBatches (same codecs as Produce), cutting wire size for large reads.

Binding: **[docs/PHASE32_SPEC.md](./docs/PHASE32_SPEC.md)**.

- [x] Fetch v4 uses `encode_record_batch_compressed` (default **lz4**)
- [x] `VOLANT_KAFKA_FETCH_COMPRESSION` = none|gzip|snappy|lz4|zstd
- [x] Fetch v0–3 MessageSet remains uncompressed
- [x] Integration tests (`phase32_fetch_compression`)

**Honest limitations (at ship):** MessageSet Fetch was still uncompressed — closed
by **Phase 33**. Codec is process-global env (not per-topic); log storage still
plain (re-encode on Fetch); no level knobs.

---

### Phase 33 — Kafka MessageSet compression ✅

**Goal:** Legacy MessageSet (magic 0/1) Produce accepts compressed wrappers;
Fetch v0–3 can return compressed MessageSets using the same fetch codec env.

Binding: **[docs/PHASE33_SPEC.md](./docs/PHASE33_SPEC.md)**.

- [x] `decode_message_set` decompresses wrapper messages (attributes bits 0–2)
- [x] `encode_message_set_compressed` for Produce tests + Fetch v0–3
- [x] Codecs: gzip / snappy / lz4 (zstd → lz4 on MessageSet encode)
- [x] Integration tests (`phase33_message_set_compression`)

**Honest limitations:** No native zstd MessageSet; wrapper encode is magic 1;
process-global codec only; log stays plain.

---

### Phase 34 — SCRAM-SHA-512 ✅

**Goal:** Kafka clients can authenticate with SCRAM-SHA-512; new users get both
SHA-256 and SHA-512 credentials from one password upsert.

Binding: **[docs/PHASE34_SPEC.md](./docs/PHASE34_SPEC.md)**.

- [x] Dual-credential store (legacy flat SHA-256 still loads)
- [x] `begin_with_hash` / `client_proof_and_server_sig_for`
- [x] Kafka SaslHandshake lists `SCRAM-SHA-512`; full auth round-trip
- [x] PLAIN + SCRAM-SHA-256 unchanged
- [x] Integration tests (`phase34_scram_sha512`)

**Honest limitations:** Legacy users need re-upsert for SHA-512; Volant-native
SCRAM wire remains SHA-256 only; no channel binding / GSSAPI / OAUTHBEARER.

**Still deferred (at ship):** multi-lang clients, cargo-fuzz corpus CI, Kafka
DeleteRecords / ACL admin on the shim, control batches / `READ_COMMITTED`.

---

### Phase 35 — Kafka DeleteRecords + ACL admin ✅

**Goal:** Kafka admin clients can truncate logs (`DeleteRecords`) and manage
ACLs (`DescribeAcls` / `CreateAcls` / `DeleteAcls`) on `--kafka-listen`.

Binding: **[docs/PHASE35_SPEC.md](./docs/PHASE35_SPEC.md)**.

- [x] DeleteRecords (21) v0–1 → Phase 14 segment truncate + low watermark
- [x] DescribeAcls (29) / CreateAcls (30) / DeleteAcls (31) v0–1
- [x] Kafka↔Volant resource type / operation / permission mapping
- [x] `User:` principal strip/prefix; `kafka-cluster` ⇄ `volant`
- [x] Integration tests (`phase35_delete_records_acls`)

**Honest limitations:** Segment-granularity delete only; no host/prefix ACLs;
DescribeConfigs/AlterConfigs/IdempotentWrite ops collapse to Describe/Alter/Write;
no flexible Kafka versions.

**Still deferred (at ship):** multi-lang clients, cargo-fuzz corpus CI,
OffsetDelete on Kafka wire, honest Fetch isolation docs / control markers.

---

### Phase 36 — Kafka OffsetDelete + Fetch isolation honesty ✅

**Goal:** Kafka clients can delete consumer offsets (`OffsetDelete`) and use
`isolation.level=read_committed` with correct LSO semantics under Volant's
buffer-until-commit model.

Binding: **[docs/PHASE36_SPEC.md](./docs/PHASE36_SPEC.md)**.

- [x] OffsetDelete (47) v0 → Phase 12 `delete_offsets`
- [x] Group Delete ACL on OffsetDelete
- [x] Fetch v4 isolation 0/1 accepted; LSO = HWM; empty aborted list
- [x] Txn abort + READ_COMMITTED fetch remains empty
- [x] Integration tests (`phase36_offset_delete_isolation`)

**Honest limitations:** No control markers / aborted-txn lists (nothing unstable
on the log); empty OffsetDelete topic list is a no-op (not delete-all).

**Still deferred (at ship):** multi-lang clients, cargo-fuzz corpus CI,
IncrementalAlterConfigs on the Kafka wire.

---

### Phase 37 — Kafka IncrementalAlterConfigs ✅

**Goal:** Modern Kafka AdminClient config updates via IncrementalAlterConfigs
(SET/DELETE) on topic resources, mapped to Phase 13 topic configs.

Binding: **[docs/PHASE37_SPEC.md](./docs/PHASE37_SPEC.md)**.

- [x] IncrementalAlterConfigs (44) v0 classic
- [x] SET → alter_configs; DELETE → clear (empty value)
- [x] APPEND/SUBTRACT → InvalidConfig; TOPIC-only
- [x] `validate_only` does not persist
- [x] Integration tests (`phase37_incremental_alter_configs`)

**Honest limitations:** TOPIC only; no list-typed APPEND/SUBTRACT; no flexible v1.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI.

### Phase 38 — Kafka Metadata classic v0–8 ✅

**Goal:** Raise Metadata (API key 3) from classic v0–1 to classic **v0–8** so
modern clients negotiate a fuller catalog response without flexible encoding.

Binding: **[docs/PHASE38_SPEC.md](./docs/PHASE38_SPEC.md)**.

- [x] Metadata max version 8 in ApiVersions
- [x] v1 broker rack (null); fix null-topics = all / empty = none
- [x] v2 `cluster_id = "volant"`; v3+ throttle 0
- [x] v5 empty offline_replicas; v7 leader_epoch = -1
- [x] v8 authorized-ops bitfields (or `INT32_MIN` when not requested)
- [x] Integration tests (`phase38_metadata_classic`); phase23 updated

**Honest limitations:** no flexible Metadata v9+; no real leader epochs / offline
replicas; no Metadata auto-create; authorized-ops best-effort.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 39 — Kafka OffsetForLeaderEpoch ✅

**Goal:** OffsetForLeaderEpoch (API key 23) classic **v0–3** so consumers can
query end offsets by leader epoch (truncation checks) without flexible framing.

Binding: **[docs/PHASE39_SPEC.md](./docs/PHASE39_SPEC.md)**.

- [x] OffsetForLeaderEpoch 0–3 advertised
- [x] Happy path → HWM + current leader epoch
- [x] Fencing via `current_leader_epoch`; unknown epoch errors
- [x] Unknown topic + Topic Describe ACL
- [x] Integration tests (`phase39_offset_for_leader_epoch`)

**Honest limitations:** no durable epoch→offset history (eligible epochs map to
current HWM); no flexible v4+; Metadata still advertises leader_epoch=-1.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 40 — Kafka ListOffsets classic v0–5 ✅

**Goal:** Raise ListOffsets (API key 2) from classic v0–1 to classic **v0–5**
so modern consumers negotiate isolation_level, throttle, and leader-epoch fencing.

Binding: **[docs/PHASE40_SPEC.md](./docs/PHASE40_SPEC.md)**.

- [x] ListOffsets max version 5 in ApiVersions
- [x] v2+ isolation_level (accepted; LSO ≡ HWM) + throttle 0
- [x] v4+ current_leader_epoch fencing + response leader_epoch
- [x] earliest/latest only; invalid timestamps rejected
- [x] Integration tests (`phase40_list_offsets`); phase25 v1 still works

**Honest limitations:** no flexible v6+; no timestamp/max-timestamp/tiered
lookups; isolation does not filter under buffer-until-commit.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 41 — Kafka OffsetFetch classic v0–5 ✅

**Goal:** Raise OffsetFetch (API key 9) from classic v0–1 to classic **v0–5**
for modern consumer offset reads (null topics, throttle, top-level error,
committed leader epoch field).

Binding: **[docs/PHASE41_SPEC.md](./docs/PHASE41_SPEC.md)**.

- [x] OffsetFetch max version 5 in ApiVersions
- [x] v2+ null topics = all / empty = none; top-level error
- [x] v3+ throttle 0; v5+ committed_leader_epoch = -1
- [x] Group Read ACL → GroupAuthorizationFailed (v2+)
- [x] Integration tests (`phase41_offset_fetch`); phase26/36 still work

**Honest limitations:** no flexible v6+ / multi-group v8+; no durable committed
leader epoch; no require_stable.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 42 — Kafka group classic static membership ✅

**Goal:** Raise JoinGroup / Heartbeat / SyncGroup / LeaveGroup classic versions
and wire `group.instance.id` to Volant Phase 12 static membership (`static:{id}`).

Binding: **[docs/PHASE42_SPEC.md](./docs/PHASE42_SPEC.md)**.

- [x] JoinGroup 0–5 (throttle v2+, group_instance_id v5+)
- [x] Heartbeat / SyncGroup / LeaveGroup 0–3 (throttle v1+, instance v3+)
- [x] LeaveGroup v3 batch members + per-member errors
- [x] Static join → `static:{instance}`; rejoin stable
- [x] Integration tests (`phase42_group_static`); phase26 still works

**Honest limitations:** no flexible group versions; no MEMBER_ID_REQUIRED
double-join; instance id not stored separately (prefix-derived).

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 43 — Kafka group admin classic versions ✅

**Goal:** Raise DescribeGroups / ListGroups / DeleteGroups classic versions;
fix DeleteGroups throttle framing; surface static instance ids on Describe v4.

Binding: **[docs/PHASE43_SPEC.md](./docs/PHASE43_SPEC.md)**.

- [x] DescribeGroups 0–4 (throttle v1+, authorized_ops v3+, group_instance_id v4+)
- [x] ListGroups 0–2 (throttle v1+)
- [x] DeleteGroups 0–1 (throttle on all versions; was missing on v0)
- [x] Integration tests (`phase43_group_admin`); phase27 updated for throttle

**Honest limitations:** no flexible group-admin versions; no StatesFilter;
instance id only for `static:` members; coarse authorized-ops bitfield.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 44 — Kafka OffsetCommit classic + FindCoordinator v2 ✅

**Goal:** Raise OffsetCommit to classic 0–7 (throttle, leader epoch, static
instance id) and FindCoordinator to 0–2.

Binding: **[docs/PHASE44_SPEC.md](./docs/PHASE44_SPEC.md)**.

- [x] OffsetCommit 0–7 (throttle v3+, no retention v5+, epoch v6+, instance v7+)
- [x] FindCoordinator 0–2 (v2 wire-identical to v1)
- [x] Static instance on commit maps to `static:{id}` when member_id empty
- [x] Integration tests (`phase44_offset_commit`); phase26/31 updated

**Honest limitations:** no flexible versions; leader epoch not stored; retention
ignored; no multi-key FindCoordinator batch.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 45 — Kafka topic admin classic versions ✅

**Goal:** Raise CreateTopics / DeleteTopics / CreatePartitions classic versions;
fix throttle and error_message framing to match Kafka.

Binding: **[docs/PHASE45_SPEC.md](./docs/PHASE45_SPEC.md)**.

- [x] CreateTopics 0–4 (error_message v1+, throttle v2+, default partitions v4)
- [x] DeleteTopics 0–3 (leading throttle v1+)
- [x] CreatePartitions 0–1 (throttle all versions + validate_only)
- [x] Integration tests (`phase45_topic_admin`); phase25/27 framing fixes

**Honest limitations:** no flexible topic-admin; no topic UUID; RF/assignments
ignored; default partitions = 1.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 46 — Kafka Describe/AlterConfigs classic versions ✅

**Goal:** Raise DescribeConfigs / AlterConfigs classic versions; fix leading
throttle and DescribeConfigs field order to match Kafka.

Binding: **[docs/PHASE46_SPEC.md](./docs/PHASE46_SPEC.md)**.

- [x] DescribeConfigs 0–3 (throttle; config_source/synonyms v1+; type/docs v3+)
- [x] AlterConfigs 0–1 (leading throttle all versions)
- [x] Kafka field order: error → error_message → type → name
- [x] Integration tests (`phase46_configs`); phase27 framing fixes

**Honest limitations:** TOPIC only; empty synonyms; IncrementalAlterConfigs
stays classic v0 (flexible 1+).

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions).

### Phase 47 — Kafka transaction APIs classic versions ✅

**Goal:** Raise AddPartitionsToTxn / AddOffsetsToTxn / EndTxn / TxnOffsetCommit
to classic max v0–2 (last non-flexible); parse TxnOffsetCommit v2 leader_epoch.

Binding: **[docs/PHASE47_SPEC.md](./docs/PHASE47_SPEC.md)**.

- [x] AddPartitionsToTxn 0–2 (v1–2 wire-identical to v0)
- [x] AddOffsetsToTxn 0–2
- [x] EndTxn 0–2
- [x] TxnOffsetCommit 0–2 (`committed_leader_epoch` parsed, ignored)
- [x] InitProducerId stays 0–1 (already classic max; flexible 2+)
- [x] Integration tests (`phase47_transactions`); phase31 ApiVersions update

**Honest limitations:** no flexible txn APIs (v3+); no control markers /
READ_COMMITTED LSO filtering; crash ≡ abort; leader epoch not stored.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin APIs (DescribeCluster / ListTransactions), true control-marker
READ_COMMITTED.

### Phase 48 — Kafka Produce classic versions ✅

**Goal:** Raise Produce from classic 0–3 to classic max **0–8** (flexible 9+);
emit log_start_offset and v8 record_errors framing.

Binding: **[docs/PHASE48_SPEC.md](./docs/PHASE48_SPEC.md)**.

- [x] Produce 0–8 dispatch + ApiVersions max 8
- [x] Response log_append_time (v2+), log_start_offset (v5+)
- [x] Response empty record_errors[] + null error_message (v8+)
- [x] Trailing throttle_time_ms (v1+)
- [x] Integration tests (`phase48_produce`); phase24 ApiVersions update

**Honest limitations:** no flexible Produce v9+; record_errors always empty;
log_append_time always -1.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin, true control-marker READ_COMMITTED.

### Phase 49 — Kafka Fetch classic versions ✅

**Goal:** Raise Fetch from classic 0–4 to classic max **0–11** (flexible 12+);
parse session/rack/epoch fields; emit log_start_offset and preferred_read_replica.

Binding: **[docs/PHASE49_SPEC.md](./docs/PHASE49_SPEC.md)**.

- [x] Fetch 0–11 dispatch + ApiVersions max 11
- [x] Request: log_start_offset (v5+), session (v7+), leader epoch (v9+), rack (v11+)
- [x] Response: log_start_offset (v5+), top-level error+session (v7+), preferred_read_replica=-1 (v11+)
- [x] Leader-epoch fencing (v9+); LSO≡HWM honesty unchanged
- [x] Integration tests (`phase49_fetch`); phase24/48 ApiVersions updates

**Honest limitations:** no flexible Fetch v12+; no real incremental sessions;
preferred_read_replica always -1; aborted_transactions always empty.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin, true control-marker READ_COMMITTED.

### Phase 50 — Kafka ApiVersions classic versions ✅

**Goal:** Raise ApiVersions from classic v0 to classic max **0–2** (flexible 3+);
emit trailing throttle_time_ms on v1–2.

Binding: **[docs/PHASE50_SPEC.md](./docs/PHASE50_SPEC.md)**.

- [x] ApiVersions 0–2 dispatch + self-advertise max 2
- [x] Response trailing throttle_time_ms (v1+)
- [x] v2 wire-identical to v1
- [x] Integration tests (`phase50_api_versions`)

**Honest limitations (at ship):** no flexible ApiVersions — closed by **Phase 51**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible modern
admin, true control-marker READ_COMMITTED.

### Phase 51 — Flexible wire foundation + ApiVersions v3 ✅

**Goal:** Introduce KIP-482 compact/tag-buffer codec primitives and ship the
first flexible API (**ApiVersions v3**).

Binding: **[docs/PHASE51_SPEC.md](./docs/PHASE51_SPEC.md)**.

- [x] Unsigned varint, compact string/array, tag buffer encode/decode
- [x] Flexible request helper (`encode_request_flexible`) + header TAG_BUFFER
- [x] ApiVersions 0–3 (v3 compact response; header stays v0)
- [x] Parse ClientSoftwareName/Version (ignored); empty feature tags
- [x] Integration tests (`phase51_flexible_api_versions`); codec unit tests

**Honest limitations:** only ApiVersions is flexible; no SupportedFeatures;
other APIs still classic-only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
Metadata/Produce/Fetch/admin, DescribeCluster / ListTransactions, true
control-marker READ_COMMITTED.

### Phase 52 — Flexible Metadata v9 + FindCoordinator v3–4 ✅

**Goal:** Extend KIP-482 flexible framing to **Metadata v9** and
**FindCoordinator v3–4** (including batch keys), with response header **v1**.

Binding: **[docs/PHASE52_SPEC.md](./docs/PHASE52_SPEC.md)**.

- [x] Response header v1 helper (`put_response_header_v1`)
- [x] Metadata 0–9 (v9 compact topics/brokers/tags; classic 0–8 unchanged)
- [x] FindCoordinator 0–4 (v3 compact single-key; v4 CoordinatorKeys batch)
- [x] Integration tests (`phase52_flexible_metadata_find_coordinator`)

**Honest limitations:** no Metadata TopicId (v10+); no flexible Produce/Fetch/
group/txn/admin; empty tag buffers only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
Produce/Fetch/admin, DescribeCluster / ListTransactions, true control-marker
READ_COMMITTED.

### Phase 53 — Flexible Produce v9 ✅

**Goal:** Extend KIP-482 flexible framing to **Produce v9** (compact
transactional_id / topics / records + response header v1).

Binding: **[docs/PHASE53_SPEC.md](./docs/PHASE53_SPEC.md)**.

- [x] Compact bytes/records codec (`get_compact_bytes` / `put_compact_bytes`)
- [x] Produce 0–9 (v9 flexible; classic 0–8 unchanged)
- [x] Response header v1 for Produce v9+
- [x] Integration tests (`phase53_flexible_produce`)

**Honest limitations:** no Produce v10 CurrentLeader/NodeEndpoints; empty
record_errors; no flexible Fetch v12+ / group / txn / admin.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
Fetch/admin, Metadata TopicId, DescribeCluster / ListTransactions, true
control-marker READ_COMMITTED.

### Phase 54 — Flexible Fetch v12 ✅

**Goal:** Extend KIP-482 flexible framing to **Fetch v12** (compact topics /
records + response header v1), pairing with Produce v9 for modern clients.

Binding: **[docs/PHASE54_SPEC.md](./docs/PHASE54_SPEC.md)**.

- [x] Fetch 0–12 (v12 flexible; classic 0–11 unchanged)
- [x] Parse LastFetchedEpoch / forgotten / rack / ClusterId tags (ignored)
- [x] Compact records + empty partition/top-level tag buffers
- [x] Integration tests (`phase54_flexible_fetch`)

**Honest limitations:** no Fetch TopicId (v13+); no diverging-epoch /
CurrentLeader tags; no real incremental sessions; empty aborted list.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
group/txn/admin, Metadata TopicId, DescribeCluster / ListTransactions, true
control-marker READ_COMMITTED.

### Phase 55 — Flexible group consumer APIs ✅

**Goal:** Extend KIP-482 flexible framing to consumer group lifecycle APIs —
**JoinGroup v6**, **SyncGroup v4**, **Heartbeat v4**, **LeaveGroup v4** —
so modern clients can complete join/sync/heartbeat/leave without falling back
to classic versions.

Binding: **[docs/PHASE55_SPEC.md](./docs/PHASE55_SPEC.md)**.

- [x] JoinGroup 0–6 (v6 flexible; classic 0–5 unchanged)
- [x] SyncGroup / Heartbeat / LeaveGroup 0–4 (v4 flexible; classic 0–3 unchanged)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase55_flexible_group`)

**Honest limitations:** no JoinGroup ProtocolType/Reason/SkipAssignment (v7+);
no SyncGroup ProtocolType/Name (v5+); no LeaveGroup Reason (v5+) — closed by
**Phase 56**. Empty tags only; coordinator semantics unchanged.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
OffsetCommit/OffsetFetch/admin/txn, Metadata TopicId, DescribeCluster /
ListTransactions, true control-marker READ_COMMITTED.

### Phase 56 — Flexible group field completeness ✅

**Goal:** Raise flexible group APIs to Kafka-current field sets —
**JoinGroup v7–9** (ProtocolType, Reason, SkipAssignment), **SyncGroup v5**
(ProtocolType/Name), **LeaveGroup v5** (Reason).

Binding: **[docs/PHASE56_SPEC.md](./docs/PHASE56_SPEC.md)**.

- [x] JoinGroup 0–9 (v7 ProtocolType; v8 Reason ignored; v9 SkipAssignment=false)
- [x] SyncGroup 0–5 (v5 ProtocolType/Name echo)
- [x] LeaveGroup 0–5 (v5 member Reason ignored)
- [x] Heartbeat remains 0–4
- [x] Integration tests (`phase56_flexible_group_fields`)

**Honest limitations:** SkipAssignment always false; Sync ProtocolType/Name
not validated against join; Reason fields discarded; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
OffsetCommit/OffsetFetch/admin/txn — OffsetCommit/Fetch closed by **Phase 57**.
Metadata TopicId, DescribeCluster / ListTransactions, true control-marker
READ_COMMITTED.

### Phase 57 — Flexible OffsetCommit + OffsetFetch ✅

**Goal:** Extend KIP-482 flexible framing to consumer offset APIs —
**OffsetCommit v8** and **OffsetFetch v6–7** (RequireStable flag).

Binding: **[docs/PHASE57_SPEC.md](./docs/PHASE57_SPEC.md)**.

- [x] OffsetCommit 0–8 (v8 flexible; classic 0–7 unchanged)
- [x] OffsetFetch 0–7 (v6 flexible; v7 RequireStable ignored)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase57_flexible_offsets`)

**Honest limitations:** no multi-group OffsetFetch v8+ — closed by **Phase 58**.
Leader epoch not stored; RequireStable ignored; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
admin/txn, Metadata TopicId, DescribeCluster / ListTransactions, true
control-marker READ_COMMITTED.

### Phase 58 — OffsetFetch multi-group v8 ✅

**Goal:** Support **OffsetFetch v8** multi-group `Groups[]` framing so modern
clients can fetch offsets for multiple groups in one request.

Binding: **[docs/PHASE58_SPEC.md](./docs/PHASE58_SPEC.md)**.

- [x] OffsetFetch 0–8 (v8 multi-group; v6–7 single-group flexible unchanged)
- [x] Per-group ACL error without failing sibling groups
- [x] Null topics = all; empty topics = none
- [x] Integration tests (`phase58_flexible_offset_fetch_multi`)

**Honest limitations:** no MemberId/MemberEpoch (v9+); RequireStable ignored;
leader epoch always -1; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible
admin/txn — group-admin flex closed by **Phase 59**. Metadata TopicId,
DescribeCluster / ListTransactions, true control-marker READ_COMMITTED.

### Phase 59 — Flexible group admin ✅

**Goal:** First flexible versions of DescribeGroups / ListGroups / DeleteGroups
so modern admin clients can use compact framing and response header v1.

Binding: **[docs/PHASE59_SPEC.md](./docs/PHASE59_SPEC.md)**.

- [x] DescribeGroups 0–5 (v5 flexible; classic 0–4 unchanged)
- [x] ListGroups 0–3 (v3 flexible; classic 0–2 unchanged)
- [x] DeleteGroups 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase59_flexible_group_admin`)

**Honest limitations:** no Describe/Delete ErrorMessage (v6/v3); no List
StatesFilter/TypesFilter or GroupState/GroupType (v4+/v5+); empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible txn /
configs, Metadata TopicId, DescribeCluster / ListTransactions, true
control-marker READ_COMMITTED. Topic-admin flex closed by **Phase 60**.

### Phase 60 — Flexible topic admin ✅

**Goal:** First flexible versions of CreateTopics / DeleteTopics /
CreatePartitions for modern admin clients.

Binding: **[docs/PHASE60_SPEC.md](./docs/PHASE60_SPEC.md)**.

- [x] CreateTopics 0–5 (v5 flexible; classic 0–4 unchanged)
- [x] DeleteTopics 0–4 (v4 flexible; classic 0–3 unchanged)
- [x] CreatePartitions 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase60_flexible_topic_admin`)

**Honest limitations:** null CreateTopics configs; no TopicId; no Delete
ErrorMessage; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible txn,
Metadata TopicId, DescribeCluster / ListTransactions, true control-marker
READ_COMMITTED. Configs flex closed by **Phase 61**.

### Phase 61 — Flexible configs ✅

**Goal:** First flexible versions of DescribeConfigs / AlterConfigs /
IncrementalAlterConfigs for modern admin clients.

Binding: **[docs/PHASE61_SPEC.md](./docs/PHASE61_SPEC.md)**.

- [x] DescribeConfigs 0–4 (v4 flexible; classic 0–3 unchanged)
- [x] AlterConfigs 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] IncrementalAlterConfigs 0–1 (v1 flexible; classic v0 unchanged)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase61_flexible_configs`)

**Honest limitations:** TOPIC only; empty synonyms; no APPEND/SUBTRACT; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, flexible txn,
Metadata TopicId, DescribeCluster / ListTransactions, true control-marker
READ_COMMITTED.

---

### Phase 62 — Flexible transaction APIs ✅

**Goal:** First flexible versions of InitProducerId and the classic txn APIs
for modern transactional producers (KIP-482).

Binding: **[docs/PHASE62_SPEC.md](./docs/PHASE62_SPEC.md)**.

- [x] InitProducerId 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] AddPartitionsToTxn 0–3 (v3 flexible; classic 0–2 unchanged)
- [x] AddOffsetsToTxn 0–3 (v3 flexible)
- [x] EndTxn 0–3 (v3 flexible)
- [x] TxnOffsetCommit 0–3 (v3 flexible; member/generation ignored)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase62_flexible_transactions`)

**Honest limitations:** no Init v3+ resume/2PC; no AddPartitions v4+ broker-batch;
no EndTxn v5 pid/epoch; no TxnOffsetCommit TopicId; empty tags only; same
buffer-until-commit honesty as classic.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Metadata TopicId,
DescribeCluster / ListTransactions, true control-marker READ_COMMITTED,
higher KIP-890 txn versions.

---

### Phase 63 — Flexible ListOffsets + OffsetForLeaderEpoch ✅

**Goal:** First flexible versions of ListOffsets and OffsetForLeaderEpoch for
modern consumer offset/epoch queries (KIP-482).

Binding: **[docs/PHASE63_SPEC.md](./docs/PHASE63_SPEC.md)**.

- [x] ListOffsets 0–6 (v6 flexible; classic 0–5 unchanged)
- [x] OffsetForLeaderEpoch 0–4 (v4 flexible; classic 0–3 unchanged)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase63_flexible_list_offsets_ofle`)

**Honest limitations:** no ListOffsets v7+ max-timestamp/tiered/remote; no epoch
history; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Metadata TopicId,
DescribeCluster / ListTransactions, true control-marker READ_COMMITTED,
DeleteRecords/ACL flex, higher KIP-890 txn versions.

---

### Phase 64 — Flexible DeleteRecords + ACL admin ✅

**Goal:** First flexible versions of DeleteRecords and Describe/Create/DeleteAcls
for modern admin clients (KIP-482).

Binding: **[docs/PHASE64_SPEC.md](./docs/PHASE64_SPEC.md)**.

- [x] DeleteRecords 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] DescribeAcls 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] CreateAcls 0–2 (v2 flexible)
- [x] DeleteAcls 0–2 (v2 flexible)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase64_flexible_delete_records_acls`)

**Honest limitations:** no USER resource (v3); LITERAL patterns only; host filter
ignored; sealed-segment DeleteRecords only; empty tags only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Metadata TopicId,
DescribeCluster / ListTransactions, true control-marker READ_COMMITTED,
higher KIP-890 txn versions, SASL flex.

---

### Phase 65 — SaslAuthenticate flexible + DescribeCluster + ListTransactions ✅

**Goal:** Finish remaining flexible SASL and always-flexible modern admin APIs
(KIP-482 / KIP-700).

Binding: **[docs/PHASE65_SPEC.md](./docs/PHASE65_SPEC.md)**.

- [x] SaslAuthenticate 0–2 (v2 flexible; classic 0–1 unchanged)
- [x] DescribeCluster 0 (always flexible; cluster_id + brokers)
- [x] ListTransactions 0 (always flexible; open txns as Ongoing)
- [x] Response header v1 for those flexible versions
- [x] Integration tests (`phase65_flexible_sasl_describe_cluster`)

**Honest limitations:** session_lifetime=0; no DescribeCluster EndpointType /
fenced brokers; ListTransactions only Ongoing open memory txns; no
DescribeTransactions / duration or pattern filters.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Metadata TopicId,
true control-marker READ_COMMITTED, higher KIP-890 txn versions, DescribeCluster
v1–2, ListTransactions v1–2, DescribeTransactions.

---

### Phase 66 — DescribeTransactions + DescribeProducers + admin bumps ✅

**Goal:** Complete modern txn/admin describe APIs and small version bumps for
DescribeCluster / ListTransactions (KIP-700 / KIP-664).

Binding: **[docs/PHASE66_SPEC.md](./docs/PHASE66_SPEC.md)**.

- [x] DescribeTransactions 0 (always flexible; Empty/Ongoing)
- [x] DescribeProducers 0 (always flexible; active producers)
- [x] DescribeCluster 0–1 (EndpointType brokers-only)
- [x] ListTransactions 0–1 (DurationFilter ignored)
- [x] Response header v1 for those APIs
- [x] Integration tests (`phase66_describe_txn_producers`)

**Honest limitations:** timeout/start always 0; duration filter ignored; no
fenced brokers / controller endpoint; DescribeProducers timestamp/coord/txn-start
placeholders; no ListTransactions pattern filter.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Metadata TopicId,
true control-marker READ_COMMITTED, higher KIP-890 txn versions, DescribeCluster
v2, ListTransactions v2.

---

### Phase 67 — Metadata TopicId (v10–12) ✅

**Goal:** Modern clients receive and can query by Kafka TopicId UUIDs on
Metadata (KIP-516 / KIP-482).

Binding: **[docs/PHASE67_SPEC.md](./docs/PHASE67_SPEC.md)**.

- [x] Metadata 0–12 (v10+ TopicId in response; classic/v9 unchanged)
- [x] Deterministic UUID from Volant numeric topic id
- [x] v11: no ClusterAuthorizedOperations request/response field
- [x] v12: resolve by TopicId; unknown → UnknownTopicId
- [x] Integration tests (`phase67_metadata_topic_id`)

**Honest limitations:** deterministic UUID (not random KRaft ids); unknown
name still omitted (no error row); no Metadata v13 top-level error; Fetch/Produce
TopicId versions still deferred.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, DescribeCluster v2, ListTransactions
v2, Fetch/admin TopicId. Fetch TopicId closed by **Phase 68**.

---

### Phase 68 — Fetch TopicId (v13) ✅

**Goal:** Modern clients that negotiate TopicId can Fetch by UUID (KIP-516)
using the same deterministic mapping as Metadata.

Binding: **[docs/PHASE68_SPEC.md](./docs/PHASE68_SPEC.md)**.

- [x] Fetch 0–13 (v13 TopicId request/response; v12 name path unchanged)
- [x] ForgottenTopicsData TopicId on v13
- [x] Unknown / non-Volant UUID → UnknownTopicId (100)
- [x] Integration tests (`phase68_fetch_topic_id`)

**Honest limitations:** no Fetch v14+ CurrentLeader/NodeEndpoints; no real
sessions; LSO ≡ HWM; deterministic UUID only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, DescribeCluster v2, ListTransactions
v2, admin TopicId (Create/DeleteTopics), Produce TopicId. Admin TopicId closed
by **Phase 69**.

---

### Phase 69 — Admin TopicId (CreateTopics v7 / DeleteTopics v5–6) ✅

**Goal:** Topic admin APIs expose and accept Kafka TopicId UUIDs for modern
clients (KIP-516), matching Metadata/Fetch deterministic mapping.

Binding: **[docs/PHASE69_SPEC.md](./docs/PHASE69_SPEC.md)**.

- [x] CreateTopics 0–7 (v7 response TopicId; v5–6 flexible unchanged)
- [x] DeleteTopics 0–6 (v5 ErrorMessage; v6 delete by TopicId)
- [x] Unknown TopicId → UnknownTopicId (100)
- [x] Integration tests (`phase69_admin_topic_id`)

**Honest limitations:** CreateTopics Configs always null; validate_only returns
zero TopicId; no quota throttle errors; deterministic UUID only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, DescribeCluster v2, ListTransactions
v2, Produce TopicId. DescribeCluster/ListTransactions v2 closed by **Phase 70**.

---

### Phase 70 — DescribeCluster v2 + ListTransactions v2 ✅

**Goal:** Finish remaining always-flexible admin version bumps for fenced-broker
visibility (KIP-1073) and transactional-id pattern filter (KIP-1152).

Binding: **[docs/PHASE70_SPEC.md](./docs/PHASE70_SPEC.md)**.

- [x] DescribeCluster 0–2 (IncludeFencedBrokers; IsFenced always false)
- [x] ListTransactions 0–2 (TransactionalIdPattern simple `*` glob)
- [x] Integration tests (`phase70_describe_cluster_list_txn_v2`)

**Honest limitations:** no real fenced membership; pattern is glob not RE2J;
DurationFilter still ignored; only Ongoing open memory txns.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, Produce TopicId. Produce TopicId
closed by **Phase 71**.

---

### Phase 71 — Produce TopicId (v10–13) ✅

**Goal:** Modern clients can Produce by Kafka TopicId UUID (KIP-516), matching
Metadata/Fetch/admin deterministic mapping; intermediate flexible versions
v10–12 advertised for negotiation.

Binding: **[docs/PHASE71_SPEC.md](./docs/PHASE71_SPEC.md)**.

- [x] Produce 0–13 (v13 TopicId request/response; v9–12 name path unchanged)
- [x] Unknown / non-Volant UUID → UnknownTopicId (100)
- [x] KIP-951 CurrentLeader/NodeEndpoints tags empty (honest)
- [x] Integration tests (`phase71_produce_topic_id`)

**Honest limitations:** no CurrentLeader redirect hints; deterministic UUID only;
record_errors empty; no v14+.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, OffsetCommit/Fetch TopicId.
OffsetCommit/Fetch TopicId closed by **Phase 72**.

---

### Phase 72 — OffsetCommit/OffsetFetch TopicId (v9–10) ✅

**Goal:** Modern clients can commit and fetch consumer offsets by Kafka TopicId
UUID (KIP-516), and negotiate OffsetFetch v9 MemberId fields without rejection.

Binding: **[docs/PHASE72_SPEC.md](./docs/PHASE72_SPEC.md)**.

- [x] OffsetCommit 0–10 (v9 wire≈v8 name path; v10 TopicId request/response)
- [x] OffsetFetch 0–10 (v9 MemberId+MemberEpoch ignored; v10 TopicId)
- [x] Unknown / non-Volant UUID → UnknownTopicId (100) per partition
- [x] Integration tests (`phase72_offset_topic_id`)

**Honest limitations:** MemberId/Epoch ignored (no KIP-848); RequireStable
ignored; leader_epoch always -1 on fetch; deterministic UUID only; no v11+.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, Metadata v13+. Metadata v13 closed
by **Phase 73**. (ListOffsets has no TopicId in Apache Kafka protocol.)

---

### Phase 73 — Metadata v13 (top-level ErrorCode) ✅

**Goal:** Advertise Metadata through v13 so modern clients negotiate the latest
stable Metadata version; emit top-level response ErrorCode.

Binding: **[docs/PHASE73_SPEC.md](./docs/PHASE73_SPEC.md)**.

- [x] Metadata 0–13 (v13 request = v12 TopicId path; response + ErrorCode)
- [x] Success path ErrorCode always 0
- [x] Integration tests (`phase73_metadata_v13`)

**Honest limitations:** top-level ErrorCode always 0 (no cluster-level failure
path); no v14+.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions, ListOffsets v7+ max-timestamp.
ListOffsets v7–11 specials closed by **Phase 74**.

---

### Phase 74 — ListOffsets v7–11 (special timestamps) ✅

**Goal:** Modern clients can negotiate ListOffsets through v11 and use
MAX_TIMESTAMP / local-log / tiered specials without UnsupportedVersion.

Binding: **[docs/PHASE74_SPEC.md](./docs/PHASE74_SPEC.md)**.

- [x] ListOffsets 0–11 (flexible v6–11; TimeoutMs v10 ignored)
- [x] MAX_TIMESTAMP (-3) log scan → `(offset, max_ts)`
- [x] EARLIEST_LOCAL (-4) ≡ earliest; tiered specials (-5/-6) → -1/-1
- [x] Integration tests (`phase74_list_offsets_specials`)

**Honest limitations:** full scan for max timestamp (no time index); no
tiered/remote storage; TimeoutMs ignored; positive timestamps still
InvalidTimestamp; no v12+.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, higher KIP-890 txn versions. KIP-890-era txn maxes closed by
**Phase 75**.

---

### Phase 75 — KIP-890-era transaction API max versions ✅

**Goal:** Modern clients can negotiate InitProducerId / AddPartitionsToTxn /
EndTxn / TxnOffsetCommit through v5 with honest shallow semantics (no 2PC,
no TopicId, no real TRANSACTION_ABORTABLE).

Binding: **[docs/PHASE75_SPEC.md](./docs/PHASE75_SPEC.md)**.

- [x] InitProducerId 0–5 (v3–5 resume fields parsed+ignored; v6 unsupported)
- [x] AddPartitionsToTxn 0–5 (v4–5 batch Transactions[]; VerifyOnly ignored)
- [x] EndTxn 0–5 (v5 response ProducerId/Epoch echo)
- [x] TxnOffsetCommit 0–5 (name path = v3 wire; TopicId deferred)
- [x] AddOffsetsToTxn stays 0–3
- [x] ProducerFenced / TransactionAbortable error codes defined
- [x] Integration tests (`phase75_kip890_txn_versions`)

**Honest limitations:** resume fields ignored; VerifyOnly always add;
no TRANSACTION_ABORTABLE emission; no 2PC; no TxnOffsetCommit TopicId;
no READ_COMMITTED; buffer-until-commit unchanged.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, InitProducerId v6 OngoingTxn wire (Phase 77), TxnOffsetCommit
TopicId (Phase 76).

---

### Phase 76 — TxnOffsetCommit TopicId (v6) ✅

**Goal:** Modern transactional consumers can commit offsets by Kafka TopicId
UUID (KIP-1319), matching Metadata/OffsetCommit deterministic mapping.

Binding: **[docs/PHASE76_SPEC.md](./docs/PHASE76_SPEC.md)**.

- [x] TxnOffsetCommit 0–6 (v3–5 name flexible; **v6 TopicId** request/response)
- [x] Unknown / non-Volant UUID → UnknownTopicId (100) per partition (no buffer)
- [x] v0–5 name path unchanged; v7 UnsupportedVersion header v1
- [x] Integration tests (`phase76_txn_offset_topic_id`)

**Honest limitations:** v4–5 wire≡v3; member/generation ignored; leader_epoch
ignored; buffer-until-EndTxn unchanged; deterministic UUID only; no v7+.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, InitProducerId v6 OngoingTxn wire closed by **Phase 77**.

---

### Phase 77 — InitProducerId v6 (OngoingTxn / 2PC wire) ✅

**Goal:** Modern clients can negotiate InitProducerId through v6 and parse
Enable2Pc / KeepPreparedTxn / OngoingTxn* without UnsupportedVersion, with
honest shallow semantics (no real prepared/2PC transactions).

Binding: **[docs/PHASE77_SPEC.md](./docs/PHASE77_SPEC.md)**.

- [x] InitProducerId 0–6 (v6 Enable2Pc + KeepPreparedTxn parsed+ignored)
- [x] v6 response OngoingTxnProducerId/Epoch always **-1** (no prepared txns)
- [x] v0–5 response shape unchanged; v7 UnsupportedVersion header v1
- [x] Integration tests (`phase77_init_producer_id_v6`)

**Honest limitations:** no real 2PC / prepared transactions; OngoingTxn* never
surfaces open buffer-until-commit txns; resume pid/epoch still ignored; no
TRANSACTION_ABORTABLE emission; AddPartitions/EndTxn maxes unchanged.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, real 2PC / prepared transaction state. KIP-951 CurrentLeader /
NodeEndpoints closed by **Phase 78**.

---

### Phase 78 — KIP-951 CurrentLeader / NodeEndpoints ✅

**Goal:** On leader-related Produce/Fetch errors, modern clients get
CurrentLeader (and Produce NodeEndpoints) tagged fields instead of empty
tag buffers, so they can refresh leader without a full Metadata round-trip.

Binding: **[docs/PHASE78_SPEC.md](./docs/PHASE78_SPEC.md)**.

- [x] Produce v10+: partition CurrentLeader (tag 0) on NotLeader / FencedLeaderEpoch
- [x] Produce v10+: top-level NodeEndpoints (tag 0) when any CurrentLeader emitted
- [x] Fetch v12+: partition CurrentLeader (tag 1) on FencedLeaderEpoch
- [x] Success / other errors keep empty tags; no ApiVersions max bumps
- [x] Integration tests (`phase78_kip951_current_leader`)

**Honest limitations:** no Fetch NodeEndpoints (Kafka v16+); no DivergingEpoch;
single-node CurrentLeader is almost always self; empty tags on success.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, real 2PC / prepared transaction state. Group-admin version bumps
closed by **Phase 79**.

---

### Phase 79 — Group admin version bumps ✅

**Goal:** Raise List/Describe/DeleteGroups to current Kafka wire maxes so modern
admin clients get StatesFilter / TypesFilter / ErrorMessage without
UnsupportedVersion.

Binding: **[docs/PHASE79_SPEC.md](./docs/PHASE79_SPEC.md)**.

- [x] ListGroups 0–5: StatesFilter + GroupState (v4); TypesFilter + GroupType (v5)
- [x] DescribeGroups 0–6: ErrorMessage per group (null on success)
- [x] DeleteGroups 0–3: ErrorMessage per result
- [x] GroupType always `"classic"`; states Stable/Empty only
- [x] Integration tests (`phase79_group_admin_versions`); phase59 maxes updated

**Honest limitations:** no PreparingRebalance/CompletingRebalance in ListGroups;
no KIP-848 / share GroupType; ErrorMessage is short static English.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, real 2PC / prepared transaction state. CreatePartitions v3
closed by **Phase 80**.

---

### Phase 80 — CreatePartitions v3 ✅

**Goal:** Raise CreatePartitions to Kafka wire max **0–3** so modern admin
clients negotiate v3 without UnsupportedVersion. v3 is wire-identical to
flexible v2 (KIP-599 quota error only — not implemented).

Binding: **[docs/PHASE80_SPEC.md](./docs/PHASE80_SPEC.md)**.

- [x] CreatePartitions 0–3 (v2–3 flexible; classic 0–1 unchanged)
- [x] v3 same compact framing + ErrorMessage as v2
- [x] Never emit THROTTLING_QUOTA_EXCEEDED; ThrottleTimeMs always 0
- [x] v4 → UnsupportedVersion + response header v1
- [x] Integration tests (`phase80_create_partitions_v3`); phase60/45 maxes updated

**Honest limitations:** no quota system / KIP-599; replica assignments ignored;
no TopicId on CreatePartitions.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, real 2PC / prepared transaction state. FindCoordinator v5–6
closed by **Phase 81**.

---

### Phase 81 — FindCoordinator v5–6 ✅

**Goal:** Raise FindCoordinator to Kafka wire max **0–6** so modern clients
negotiate v5–6 without UnsupportedVersion. v5–6 are wire-identical to flexible
v4 batch (KIP-890 TRANSACTION_ABORTABLE never emitted; KIP-932 share key_type
rejected).

Binding: **[docs/PHASE81_SPEC.md](./docs/PHASE81_SPEC.md)**.

- [x] FindCoordinator 0–6 (v3 flex single-key; v4–6 batch; classic 0–2 unchanged)
- [x] v5–6 same compact Coordinators framing as v4
- [x] Never emit TRANSACTION_ABORTABLE; share key_type 2 → InvalidRequest
- [x] v7 → UnsupportedVersion + response header v1
- [x] Integration tests (`phase81_find_coordinator_v5_v6`); phase52/31/44 maxes updated

**Honest limitations:** no share groups (KIP-932); no TRANSACTION_ABORTABLE;
always resolves to local broker; ThrottleTimeMs always 0.

### Phase 82 — AddOffsetsToTxn v4 ✅

**Goal:** Raise AddOffsetsToTxn to Kafka wire max **0–4** so modern clients
negotiate v4 without UnsupportedVersion. v4 is wire-identical to flexible v3
(KIP-890 TRANSACTION_ABORTABLE never emitted).

Binding: **[docs/PHASE82_SPEC.md](./docs/PHASE82_SPEC.md)**.

- [x] AddOffsetsToTxn 0–4 (classic 0–2; flex v3–4 same wire)
- [x] v4 same compact framing as v3; never emit TRANSACTION_ABORTABLE
- [x] v5 → UnsupportedVersion + response header v1
- [x] Integration tests (`phase82_add_offsets_to_txn_v4`); phase31/47/62/75 maxes updated

**Honest limitations:** no TRANSACTION_ABORTABLE; buffer-until-commit only;
ThrottleTimeMs always 0.

### Phase 83 — ApiVersions v4–5 ✅

**Goal:** Raise ApiVersions to Kafka wire max **0–5** so modern clients
negotiate v4–5 without UnsupportedVersion. Empty feature tags; v5 ClusterId /
NodeId parsed and ignored (no REBOOTSTRAP_REQUIRED).

Binding: **[docs/PHASE83_SPEC.md](./docs/PHASE83_SPEC.md)**.

- [x] ApiVersions 0–5 (classic 0–2; flex v3–5; response header always v0)
- [x] v4 wire-identical to v3 body (no SupportedFeatures registry)
- [x] v5 request ClusterId/NodeId parsed + ignored; response same as v3–4
- [x] v6 → UnsupportedVersion + response header v0
- [x] Integration tests (`phase83_api_versions_v4_v5`); phase50/51 maxes updated

**Honest limitations:** no SupportedFeatures / FinalizedFeatures /
ZkMigrationReady; no REBOOTSTRAP_REQUIRED / cluster identity checks;
ThrottleTimeMs always 0.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, real 2PC / prepared transaction state. Fetch v14+ closed by
**Phase 84**.

---

### Phase 84 — Fetch v14–18 (Kafka max) ✅

**Goal:** Raise Fetch to Kafka wire max **0–18** so modern clients negotiate
v14–18 without UnsupportedVersion. Honest parse-ignore for ReplicaState /
directory / HWM tags; NodeEndpoints on v16+ leader errors.

Binding: **[docs/PHASE84_SPEC.md](./docs/PHASE84_SPEC.md)**.

- [x] Fetch 0–18 (classic 0–11; flex v12–18; TopicId v13+)
- [x] v14 wire-identical to v13 (no OffsetMovedToTieredStorage)
- [x] v15: drop top-level ReplicaId; ReplicaState tag parse-ignore
- [x] v16+: NodeEndpoints (tag 0) when CurrentLeader emitted; empty on success
- [x] v17–18: partition tags (ReplicaDirectoryId / HighWatermark) parse-ignore
- [x] v19 → UnsupportedVersion + response header v1
- [x] Integration tests (`phase84_fetch_v14_plus`); phase54/49/68/51/50 maxes updated

**Honest limitations:** no tiered storage error; ReplicaState ignored; no real
fetch sessions / DivergingEpoch; LSO ≡ HWM; preferred_read_replica always -1.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, true control-marker
READ_COMMITTED, real 2PC / prepared transaction state. ACL admin v3 closed by
**Phase 85**.

---

### Phase 85 — ACL admin v3 (User resource type; Kafka max) ✅

**Goal:** Raise Describe/Create/DeleteAcls to Kafka wire max **0–3** and accept
the **User** resource type on flexible v3 (wire-identical framing to v2).

Binding: **[docs/PHASE85_SPEC.md](./docs/PHASE85_SPEC.md)**.

- [x] DescribeAcls / CreateAcls / DeleteAcls 0–3 (flex v2–3; classic 0–1)
- [x] v3 wire-identical to v2; `ResourceType = 7` (User) accepted on v3 only
- [x] Persist User ACLs as `ResourceType::User` in Phase 20/21 store
- [x] v2 + User type → InvalidRequest; v4 → UnsupportedVersion + header v1
- [x] Integration tests (`phase85_acl_v3`); phase64 max assertions updated

**Honest limitations:** User ACLs are storage/admin round-trip only (no SCRAM
credential API gating); no TransactionalId/DelegationToken; host always `*`;
LITERAL only; no cluster ACL consensus.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Kafka control
batches on the data log (soft markers only), real 2PC / prepared transaction
state. Soft-marker READ_COMMITTED closed by **Phase 86**.

---

### Phase 86 — True control-marker READ_COMMITTED (MVP) ✅

**Goal:** Differentiate Fetch `isolation_level` with write-through transactional
produces, true LSO, soft abort markers, and READ_COMMITTED filtering — without
claiming full Kafka control-batch wire parity.

Binding: **[docs/PHASE86_SPEC.md](./docs/PHASE86_SPEC.md)**.

- [x] Transactional produce write-through (real base offsets; HWM advances)
- [x] LSO = min open write-through first offset (may be `<` HWM)
- [x] EndTxn commit finalizes sequences; abort records soft markers
- [x] Fetch READ_COMMITTED: cap at LSO, filter aborted, non-empty aborted list
- [x] Fetch READ_UNCOMMITTED: all data up to HWM (incl. unstable/aborted)
- [x] ListOffsets READ_COMMITTED latest = LSO
- [x] Native fetch remains committed-only; `__txn_markers` crash ≡ abort
- [x] Integration tests (`phase86_read_committed`); prior txn tests updated

**Honest limitations:** soft markers only (no Kafka control batch bytes on the
data log); Fetch re-encode omits transactional attributes; aborted marker file
not compacted with DeleteRecords; single-node coordinator; no 2PC.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Kafka control
batches on the data log, real 2PC / prepared transaction state, real fetch
sessions / DivergingEpoch. Durable OFLE history closed by **Phase 87**;
Fetch sessions / DivergingEpoch closed by **Phase 88**.

---

### Phase 87 — Durable OffsetForLeaderEpoch history (MVP) ✅

**Goal:** Persist per-partition leader-epoch → start-offset history so
OffsetForLeaderEpoch returns correct end offsets for prior epochs (not always
HWM), and advertise live leader epochs on Metadata.

Binding: **[docs/PHASE87_SPEC.md](./docs/PHASE87_SPEC.md)**.

- [x] Durable `{data_dir}/__leader_epochs/state.json` history
- [x] Record history on epoch bump (`set_partition_leader_epoch`) + failover best-effort
- [x] OffsetForLeaderEpoch: prior epochs → transition end; current/`-1` → HWM
- [x] Metadata v7+/flex: live `leader_epoch` (not always `-1`)
- [x] History survives restart; seed epoch 0 on topic create
- [x] Integration tests (`phase87_leader_epoch_history`)

**Honest limitations:** JSON MVP (not KRaft epoch SM); multi-node start offset
best-effort from local LEO; no Fetch DivergingEpoch; no real fetch sessions
(closed by **Phase 88**).

**Still deferred (pre-88):** multi-lang clients, cargo-fuzz corpus CI, Kafka
control batches on the data log, real 2PC.

---

### Phase 88 — Fetch DivergingEpoch + real fetch sessions (MVP) ✅

**Goal:** Close the two related Fetch gaps left by Phase 87: emit
**DivergingEpoch** (tag 0) when the client's `last_fetched_epoch` /
`fetch_offset` indicates truncation vs durable history, and maintain
**process-local fetch sessions** (create, forgotten, invalid epoch/id,
empty-topics re-fetch).

Binding: **[docs/PHASE88_SPEC.md](./docs/PHASE88_SPEC.md)**.

- [x] Fetch v12+: DivergingEpoch tag 0 + OFFSET_OUT_OF_RANGE on truncation
- [x] Process-local sessions: create (id=0/epoch=0), merge, forgotten, close FINAL
- [x] Incremental empty-topics re-fetch of session partitions (full data always)
- [x] Session errors 70 / 71; FINAL → response session id 0
- [x] Integration tests (`phase88_fetch_sessions_diverging`)

**Honest limitations:** sessions process-local (not durable / multi-broker);
omit-unchanged closed by **Phase 91**; session TTL/max closed by **Phase 95**;
DivergingEpoch from local history only.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, Kafka control
batches on the data log (closed by **Phase 89**), real 2PC (closed by
**Phase 90**), omit-unchanged (closed by **Phase 91**), session TTL/max
(closed by **Phase 95**), multi-broker session affinity.

---

### Phase 89 — Kafka control batches on the data log (MVP) ✅

**Goal:** On EndTxn commit/abort (and fence), append Kafka-style magic-2
**control RecordBatch**(es) (COMMIT/ABORT) to each partition with write-through
ranges, dual-written with Phase 86 soft markers.

Binding: **[docs/PHASE89_SPEC.md](./docs/PHASE89_SPEC.md)**.

- [x] Control marker Volant records + Fetch v4+ control batch re-encode
- [x] EndTxn commit → COMMIT; abort/fence → ABORT per written partition
- [x] Soft markers remain SoT for LSO / aborted list / crash recovery
- [x] READ_COMMITTED includes control batches; filters aborted app data
- [x] MessageSet Fetch omits control; native fetch hides control markers
- [x] Integration tests (`phase89_control_batches`)

**Honest limitations:** no control batch for crash≡abort without EndTxn
(closed by **Phase 98**); no markers for AddPartitions-only partitions;
coordinator_epoch always 0; no multi-broker txn log / 2PC.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, real 2PC
(closed by **Phase 90** MVP), omit-unchanged fetch cache (closed by
**Phase 91**), multi-broker session affinity, crash≡abort control batches
(closed by **Phase 98**).

---

### Phase 90 — Real 2PC / prepared transactions (MVP) ✅

**Goal:** Honest prepared-transaction state for InitProducerId v6 Enable2Pc /
KeepPreparedTxn and a two-phase EndTxn path, with durable prepared recovery and
non-default OngoingTxn* when prepared.

Binding: **[docs/PHASE90_SPEC.md](./docs/PHASE90_SPEC.md)**.

- [x] Enable2Pc marks producer; first EndTxn → Prepared (PrepareCommit/Abort)
- [x] Second EndTxn with matching decision finalizes (soft + control markers)
- [x] KeepPreparedTxn=true returns OngoingTxn* = prepared pid/epoch (no fence)
- [x] KeepPreparedTxn=false force-aborts prepared then fences
- [x] Durable `{data_dir}/__txn_prepared/state.json` (prepared survives crash)
- [x] LSO / unstable includes prepared ranges; Describe/List show Prepare*
- [x] Non-2PC EndTxn remains one-shot finalize
- [x] Integration tests (`phase90_prepared_txns`)

**Honest limitations:** not full KIP-890/939 parity; no multi-broker txn log;
no TRANSACTION_ABORTABLE; KeepPreparedTxn does not keep ordinary open
(non-prepared) txns; resume pid/epoch fields still ignored for allocation.
Prepared timeout closed by **Phase 92**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker
session affinity, full KRaft epoch SM, multi-broker 2PC coordinator.
Omit-unchanged closed by **Phase 91**; prepared timeout by **Phase 92**.

---

### Phase 91 — Omit-unchanged incremental fetch sessions (MVP) ✅

**Goal:** Close the main honesty gap left by Phase 88 sessions: empty-topics
incremental Fetch **omits** partitions with no new data (HWM+LSO unchanged and
empty records), and **includes** when HWM/LSO advanced or records are available.

Binding: **[docs/PHASE91_SPEC.md](./docs/PHASE91_SPEC.md)**.

- [x] Per-session per-partition `last_hwm` / `last_lso` cache
- [x] Empty-topics incremental omit when unchanged
- [x] Include on new produce / HWM or LSO advance (empty records + updated offsets OK)
- [x] Create / forgotten / 70 / 71 / FINAL preserved (Phase 88)
- [x] Integration tests (`phase91_omit_unchanged_sessions`)

**Honest limitations:** process-local only; HWM+LSO+empty-records omit (not
byte-identical Kafka compressed response cache); partial-topic incremental
always returns those partitions; session TTL/max closed by **Phase 95**;
no multi-broker affinity.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker
session affinity / durable sessions, multi-broker 2PC. Session TTL / max
sessions closed by **Phase 95**. Prepared timeout closed by **Phase 92**.

---

### Phase 92 — Prepared transaction timeout / auto-abort (MVP) ✅

**Goal:** Prevent prepared (2PC phase-1) transactions from pinning LSO forever
by tracking `prepared_at_ms` and auto-aborting after a configurable timeout.

Binding: **[docs/PHASE92_SPEC.md](./docs/PHASE92_SPEC.md)**.

- [x] Durable `prepared_at_ms` on prepare + `__txn_prepared` snapshot
- [x] Default timeout **60s**; `VOLANT_PREPARED_TXN_TIMEOUT_MS` + setter; `0` disables
- [x] Lazy auto-abort (= force-abort: soft markers + ABORT control batches)
- [x] Sweep on InitProducerId / EndTxn / List/Describe / produce guards / LSO
- [x] Integration tests (`phase92_prepared_timeout`)

**Honest limitations (at ship):** open (non-prepared) txns still had no timeout
then (closed by Phase 93); lazy only (no background sweeper); single-node clock;
TRANSACTION_ABORTABLE closed by Phase 94.

**Still deferred at Phase 92 ship:** multi-lang clients, cargo-fuzz corpus CI,
multi-broker 2PC / session affinity, session TTL → Phase 95, open-txn timeout →
Phase 93, TRANSACTION_ABORTABLE → Phase 94.

---

### Phase 93 — Open transaction timeout (MVP) ✅

**Goal:** Honor InitProducerId `transaction_timeout_ms` (or broker default/env)
for **open** write-through transactions; lazy auto-abort on timeout so LSO does
not pin forever without a background thread.

Binding: **[docs/PHASE93_SPEC.md](./docs/PHASE93_SPEC.md)**.

- [x] `opened_at_ms` on open txn (begin / ensure-open)
- [x] Client `transaction_timeout_ms` stored per producer; broker default
      **60s** via `VOLANT_OPEN_TXN_TIMEOUT_MS` + setter; effective `0` disables
- [x] Lazy auto-abort (= EndTxn abort: soft markers + ABORT control batches;
      drop deferred offsets)
- [x] Sweep on InitProducerId / EndTxn / List/Describe / produce guards / LSO
- [x] Clean interaction with prepared path (Phase 90/92)
- [x] Integration tests (`phase93_open_txn_timeout`)

**Honest limitations:** lazy only (no background sweeper); single-node clock;
no TRANSACTION_ABORTABLE; no `transaction.max.timeout.ms` clamp; open
`opened_at_ms` is memory-only (crash already aborts open ranges).

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity, session TTL → **closed by Phase 95**, TRANSACTION_ABORTABLE →
**closed by Phase 94** (honest subset), background txn sweeper / metrics.

---

### Phase 94 — TRANSACTION_ABORTABLE emission (honest subset) ✅

**Goal:** Emit Kafka error **TRANSACTION_ABORTABLE (123)** after open/prepared
timeout auto-abort on the APIs Volant can honestly support, without claiming
full KIP-890 multi-broker parity.

Binding: **[docs/PHASE94_SPEC.md](./docs/PHASE94_SPEC.md)**.

- [x] Protocol `ErrorCode::TransactionAbortable = 24` → Kafka **123**
- [x] Abortable producer set marked on open/prepared timeout expiry
- [x] Produce / EndTxn / AddPartitions / AddOffsets / TxnOffsetCommit emit 123
- [x] EndTxn clears mark; never-opened stays InvalidTxnState (48)
- [x] FindCoordinator never emits 123
- [x] Integration tests (`phase94_transaction_abortable`)

**Honest limitations:** timeout-only mark (not mid-txn partition failures);
memory-only abortable set; no FindCoordinator 123; not full KIP-890 surface.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity, session TTL → **closed by Phase 95**, background txn sweeper /
metrics, `transaction.max.timeout.ms` clamp.

---

### Phase 95 — Fetch session TTL / max sessions (MVP) ✅

**Goal:** Close the session lifecycle gaps left by Phase 88/91: **idle TTL**
eviction and a **max concurrent sessions** cap with lazy LRU pressure, without
a background thread.

Binding: **[docs/PHASE95_SPEC.md](./docs/PHASE95_SPEC.md)**.

- [x] Idle TTL default **60s**; `VOLANT_FETCH_SESSION_IDLE_MS` + setter; `0` disables
- [x] Max sessions default **1000**; `VOLANT_FETCH_SESSION_MAX` + setter; `0` = unlimited
- [x] Lazy idle eviction on create / begin_incremental; touch `last_activity` on success
- [x] At cap: **LRU-evict** oldest idle session (create always succeeds)
- [x] Evicted session next incremental → **70**; omit-unchanged preserved
- [x] Cheap metrics: `volant_fetch_sessions_active` / `_evicted_total`
- [x] Integration tests (`phase95_fetch_session_limits`)

**Honest limitations:** process-local only; lazy only (no background sweeper);
LRU by `last_activity_ms` only; not multi-broker sticky.

**Still deferred at Phase 95 ship:** multi-lang clients, cargo-fuzz corpus CI,
multi-broker 2PC / session affinity / durable sessions, byte-identical response
cache, background txn/session sweeper; `transaction.max.timeout.ms` clamp →
**closed by Phase 96**.

---

### Phase 96 — Broker `transaction.max.timeout.ms` clamp (MVP) ✅

**Goal:** Cap client and broker open/prepared transaction timeouts with a
Kafka-ish broker maximum (default **15 minutes**), reject InitProducerId when
the client requests more than the max, and clamp effective open/prepared clocks.

Binding: **[docs/PHASE96_SPEC.md](./docs/PHASE96_SPEC.md)**.

- [x] Default max **900_000 ms** (15m); `VOLANT_TRANSACTION_MAX_TIMEOUT_MS` + setter
- [x] `0` = no max (disable clamp + Init reject)
- [x] InitProducerId client timeout **> max** → Kafka **50** (`INVALID_TRANSACTION_TIMEOUT`)
- [x] Effective open + prepared timeouts clamped when max > 0
- [x] Expire / Describe use clamped values; below-max paths unchanged
- [x] Integration tests (`phase96_transaction_max_timeout`)

**Honest limitations:** lazy only; single-node; Volant still accepts client
timeout ≤ 0 as broker-default (not full Kafka `> 0` validation); DescribeConfigs
surface for knobs → **closed by Phase 99**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions; background txn/session sweeper / richer
metrics → **closed by Phase 97**.

---

### Phase 97 — Background txn + session sweeper with metrics (MVP) ✅

**Goal:** Periodic background expiry of timed-out open/prepared transactions and
idle fetch sessions (same paths as lazy expire), plus richer Prometheus counters
and gauges. Lazy paths remain correct without the sweeper.

Binding: **[docs/PHASE97_SPEC.md](./docs/PHASE97_SPEC.md)**.

- [x] Default sweep interval **1000 ms**; `VOLANT_SWEEP_INTERVAL_MS` + setter; `0` disables
- [x] Background task in `start_background_tasks` (server entry)
- [x] `Broker::sweep_timeouts()` → open + prepared expire + idle session eviction
- [x] Counters: open/prepared expired; fetch sessions idle-evicted
- [x] Gauges: open_txns, prepared_txns, fetch_sessions_active
- [x] Lazy expire paths unchanged
- [x] Integration tests (`phase97_background_sweeper`)

**Honest limitations:** single-node wall clock; fire-and-forget tokio task (no
join on drop) → **closed by Phase 106**; idle session sweep only (LRU still
lazy-on-create); Admin config surface → **closed by Phase 99**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, graceful sweeper shutdown → **closed by
Phase 106**; crash≡abort control batches → **closed by Phase 98**.

---

### Phase 98 — Control batches for crash≡abort open txns (MVP) ✅

**Goal:** When crash recovery promotes stored open write-through ranges to
aborted soft markers, also append Kafka-style **ABORT control RecordBatch**(es)
(Phase 89 dual-write), using durable `producer_epoch` on open marker ranges.
Idempotent across restarts (only promote open→aborted once).

Binding: **[docs/PHASE98_SPEC.md](./docs/PHASE98_SPEC.md)**.

- [x] `__txn_markers` open ranges store optional `producer_epoch`
- [x] `OpenTxn.producer_epoch` set at begin; persisted on open markers
- [x] `load_txn_markers`: promote open→aborted **+** ABORT control per written partition
- [x] Epoch resolution: stored → producer_state best-effort → skip control
- [x] Idempotent: second reload with empty open does not re-append
- [x] Empty open (no write-through) invents no control batch (Phase 98; empty
      AddPartitions membership control → **closed by Phase 105**)
- [x] EndTxn path unchanged; isolation rules unchanged
- [x] Integration tests (`phase98_crash_abort_control`)

**Honest limitations:** empty AddPartitions-only control → **closed by Phase 105**;
pre-98 open files without epoch and without producer_state may still lack control;
rare partial mid-append re-append; coordinator_epoch always 0; no multi-broker
marker consensus; no historical reconstruction of pre-98 crash-aborts.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, marker GC → **closed by Phase 104**; Admin/DescribeConfigs for
timeout + sweep knobs → **closed by Phase 99**.

---

### Phase 99 — DescribeConfigs (broker) for txn/session/sweep knobs (MVP) ✅

**Goal:** Expose process-local open/prepared/max txn timeouts, fetch-session
idle/max, and sweep interval via Kafka **DescribeConfigs** on **BROKER**
resources; support **AlterConfigs** / **IncrementalAlterConfigs** SET/DELETE
through the same setters as env overrides.

Binding: **[docs/PHASE99_SPEC.md](./docs/PHASE99_SPEC.md)**.

- [x] BROKER resource type (4) on DescribeConfigs 0–4
- [x] Config keys: `transaction.max.timeout.ms` + five `volant.*` knobs
- [x] Values from live Broker getters; product defaults documented
- [x] AlterConfigs + IncrementalAlterConfigs SET/DELETE (process-local, non-durable)
- [x] Unknown key → InvalidConfig; other resource types still InvalidRequest
- [x] Cluster Describe/Alter ACL when enabled
- [x] TOPIC configs unchanged
- [x] Integration tests (`phase99_broker_configs`)

**Honest limitations:** single-node (resource name ignored → **closed by Phase 103**); non-durable; six
knobs only; empty synonyms; sweeper task spawn still tied to
`start_background_tasks` initial interval; no BROKER_LOGGER / full Kafka
broker catalog / KRaft DynamicBrokerConfig.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, durable dynamic broker config file →
**closed by Phase 100**, marker compaction/GC → **closed by Phase 104**, graceful sweeper enable on
0→>0 interval → **closed by Phase 101**, empty-AddPartitions control markers →
**closed by Phase 105**.

---

### Phase 100 — Durable dynamic broker config (MVP) ✅

**Goal:** Persist the six Phase 99 BROKER knobs under `data_dir` so
AlterConfigs / IncrementalAlterConfigs survive process restart; load after
env defaults on `Broker::new` / `with_cluster`.

Binding: **[docs/PHASE100_SPEC.md](./docs/PHASE100_SPEC.md)**.

- [x] Path `{data_dir}/__broker_config/state.json` (atomic write)
- [x] Precedence: product default → env → durable file → runtime alter
- [x] Full snapshot persist on successful non-validate_only Alter / Incremental
  (Phase 100; **Phase 102** switches to sparse overlay)
- [x] DELETE / empty Alter restores product default **and** rewrites durable file
- [x] Direct `set_*` setters remain process-local (no auto-persist)
- [x] TOPIC configs unchanged
- [x] Integration tests (`phase100_broker_config_durable`)

**Honest limitations:** six knobs only; full snapshot overrides env after any
Alter write → **closed by Phase 102**; sweeper task spawn at boot with interval 0
→ **closed by Phase 101**; no multi-broker fan-out / full Kafka catalog / KRaft.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, marker compaction/GC → **closed by Phase 104**,
graceful sweeper enable on 0→>0 interval → **closed by Phase 101**,
empty-AddPartitions control markers → **closed by Phase 105**, validate BROKER
resource name against `node_id` → **closed by Phase 103**, sparse durable file
(env re-apply after DELETE) → **closed by Phase 102**.

---

### Phase 101 — Graceful sweeper enable on 0→>0 interval (MVP) ✅

**Goal:** Always spawn the background open/prepared/session sweeper so a
process that starts with `volant.sweep.interval.ms = 0` can enable sweeping
later via setter or AlterConfigs without restart. Interval `0` is pause-only.

Binding: **[docs/PHASE101_SPEC.md](./docs/PHASE101_SPEC.md)**.

- [x] Always spawn sweeper task in `start_background_tasks` (remove outer `>0` guard)
- [x] Interval `0` pauses (200ms poll); `>0` sweeps; re-read Atomic each loop
- [x] `0 → >0` via setter and AlterConfigs enables without restart
- [x] `>0 → 0` still pauses; lazy expire remains
- [x] Metrics / `sweep_timeouts` semantics unchanged
- [x] Integration tests (`phase101_sweeper_restart`)

**Honest limitations:** fire-and-forget (no join on stop) → **closed by Phase 106**;
single-node clock; duplicate `start_background_tasks` still spawns duplicate tasks;
six BROKER knobs only; resource name still ignored → **closed by Phase 103**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, marker compaction/GC → **closed by Phase 104**,
empty-AddPartitions control markers → **closed by Phase 105**, validate BROKER
resource name against `node_id` → **closed by Phase 103**, sparse durable file
(env re-apply after DELETE) → **closed by Phase 102**, graceful sweeper
shutdown / join on stop → **closed by Phase 106**.

---

### Phase 102 — Sparse durable broker config (MVP) ✅

**Goal:** Persist only explicitly altered BROKER knobs (sparse overlay) so
untouched keys keep product→env on restart; DELETE removes the key from the
file so env can re-apply.

Binding: **[docs/PHASE102_SPEC.md](./docs/PHASE102_SPEC.md)**.

- [x] Sparse merge on Alter SET (only key K written)
- [x] DELETE / empty removes key from durable file; empty overlay clears file
- [x] Load: product → env → sparse file keys only
- [x] validate_only / direct `set_*` / TOPIC unchanged
- [x] Integration tests (`phase102_sparse_broker_config`)

**Honest limitations:** six knobs only; live DELETE still product default until
restart; legacy Phase 100 full snapshots pin keys until DELETE; no multi-broker
fan-out / full Kafka catalog; BROKER name still ignored → **closed by Phase 103**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, marker compaction/GC → **closed by Phase 104**,
empty-AddPartitions control markers → **closed by Phase 105**, validate BROKER
resource name against `node_id` → **closed by Phase 103**, graceful sweeper
shutdown / join on stop → **closed by Phase 106**.

---

### Phase 103 — Validate BROKER resource name vs `node_id` (MVP) ✅

**Goal:** Accept BROKER config resource names only when empty or equal to this
process's `node_id` decimal string; reject others with `INVALID_REQUEST` on
DescribeConfigs / AlterConfigs / IncrementalAlterConfigs.

Binding: **[docs/PHASE103_SPEC.md](./docs/PHASE103_SPEC.md)**.

- [x] Helper: empty **or** exact `node_id.to_string()` match
- [x] Describe / Alter / IncrementalAlter BROKER paths
- [x] Wrong name → error code 42; no mutation
- [x] TOPIC paths unchanged; `"0"` single-node default kept green
- [x] Integration tests (`phase103_broker_name`)

**Honest limitations:** local validation only (no multi-broker fan-out); empty
name still accepted for client convenience; six knobs / sparse durable unchanged.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, marker compaction/GC → **closed by Phase 104**,
empty-AddPartitions control markers → **closed by Phase 105**, multi-broker
BROKER config fan-out, graceful sweeper shutdown / join on stop → **closed by Phase 106**.

---

### Phase 104 — Aborted soft-marker GC with DeleteRecords (MVP) ✅

**Goal:** Drop aborted soft markers whose ranges are entirely below the new log
start after DeleteRecords (and retention), persist `__txn_markers`, and self-heal
on load — without rewriting control-batch log history.

Binding: **[docs/PHASE104_SPEC.md](./docs/PHASE104_SPEC.md)**.

- [x] GC rule: drop when `end_offset <= log_start`; retain partial overlaps
- [x] Hook: `delete_records` success path
- [x] Hook: `apply_retention_all` after segment drop
- [x] Hook: `load_txn_markers` self-heal
- [x] Persist `__txn_markers` after GC; metric `volant_aborted_markers_gc_total`
- [x] Integration tests (`phase104_marker_gc`)

**Honest limitations:** whole-segment truncate only; no partial marker trim; no
control-batch rewrite; single-node marker store.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, empty-AddPartitions control markers →
**closed by Phase 105**, multi-broker BROKER config fan-out, graceful sweeper
shutdown / join on stop → **closed by Phase 106**.

---

### Phase 105 — Control batches for empty AddPartitions (MVP) ✅

**Goal:** Track AddPartitionsToTxn membership even without produce; on EndTxn
commit/abort and crash≡abort open promote, append Kafka control batches for
empty partitions too (control-only — no fake soft data ranges).

Binding: **[docs/PHASE105_SPEC.md](./docs/PHASE105_SPEC.md)**.

- [x] `OpenTxn.added` membership + `record_txn_added_partitions`
- [x] Persist `open_added` under `__txn_markers`; prepared snapshot carries `added`
- [x] `append_txn_control_markers` = written ∪ added (dedup)
- [x] Soft abort remains written-only (empty → control only)
- [x] Crash promote extends Phase 98 for `open_added`
- [x] Integration tests (`phase105_empty_add_partitions_control`)

**Honest limitations:** pre-105 snapshots lack `open_added`; native produce
without AddPartitions still relies on written ranges; no multi-broker consensus.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, multi-broker BROKER config fan-out,
graceful sweeper shutdown / join on stop → **closed by Phase 106**.

---

### Phase 106 — Graceful background task shutdown / join (MVP) ✅

**Goal:** Stop and join group-expiry, retention, txn/session sweeper, and
cluster background loops on server stop so in-flight sweeps do not race drop
and tests/ops can drain cleanly. Phase 101 always-spawn + 0-pause preserved.

Binding: **[docs/PHASE106_SPEC.md](./docs/PHASE106_SPEC.md)**.

- [x] `start_background_tasks` → `BackgroundTasks` (`watch` stop + `JoinHandle`s)
- [x] All loops observe stop via `tokio::select!` and exit cleanly
- [x] `BackgroundTasks::shutdown` signals stop + joins (5s timeout, then abort)
- [x] `serve_listener` / `run_server` / TLS path drain bg on exit or signal
- [x] Phase 97/101 tests call explicit shutdown; Phase 101 0→>0 preserved
- [x] Integration tests (`phase106_background_shutdown`)

**Honest limitations:** native/Kafka/metrics accept loops not drained at ship
time → **closed by Phase 109**; duplicate `start_background_tasks` → **closed by
Phase 109**; timeout aborts stragglers.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, multi-broker BROKER config fan-out;
phase103 parallel flake → **closed by Phase 107**; accept-loop drain +
single-flight → **closed by Phase 109**.

---

### Phase 107 — Stabilize phase103 parallel test flake (MVP) ✅

**Goal:** Root-cause the intermittent `phase103_broker_name` failures under
default cargo test parallelism (shared temp `data_dir` → ENOENT / AlterConfigs
`-1`) without serializing the binary.

Binding: **[docs/PHASE107_SPEC.md](./docs/PHASE107_SPEC.md)**.

- [x] Diagnose: macOS coarse `SystemTime` nanos + shared prefix/label paths
- [x] Harden `tests/common/mod.rs::temp_dir` (atomic seq + thread id; no create wipe)
- [x] Distinct phase103 setup labels; defensive catalog/config `create_dir_all`
- [x] Multi-run green under default parallel threads (no `serial_test`)

**Honest limitations:** unit-test local `temp_dir` helpers outside integration
common are unchanged; product code paths unchanged aside from defensive parent
recreate on topic catalog/config save.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, multi-broker BROKER config fan-out;
accept-loop drain + single-flight → **closed by Phase 109**;
phase8 follower-down produce timeout → **closed by Phase 108**.

---

### Phase 108 — Fix rolling restart produce timeout when follower down (MVP) ✅

**Goal:** `acks=all` produce while a non-leader follower is dead must not
REQUEST_TIMED_OUT when remaining `|ISR| >= min.insync.replicas`. Shrink local
ISR on death observation, bump assignment generation on pure ISR shrink, and
recompute HWM so waiters unblock.

Binding: **[docs/PHASE108_SPEC.md](./docs/PHASE108_SPEC.md)**.

- [x] Root-cause: pure follower death did not apply ISR shrink / HWM recompute
- [x] Every death observer: local ISR drop + HWM recompute + notify
- [x] Controller: generation bump on ISR-only shrink; empty-ISR restore last known
- [x] `apply_local_assignment` recomputes HWM for local leaders
- [x] Multi-run green: `phase8_redirect_restart` + `cluster_failover` smoke

**Honest limitations:** lag-based ISR shrink threshold unchanged.
Non-controller alive-set auto-death → **closed by Phase 110**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI, multi-broker 2PC /
session affinity / durable sessions, multi-broker BROKER config fan-out;
accept-loop drain + single-flight → **closed by Phase 109**.

---

### Phase 109 — Accept-loop drain + single-flight background tasks (MVP) ✅

**Goal:** Stop double-spawning background loops when `start_background_tasks` is
called twice, and drain native / Kafka / metrics accept loops (plus connection
tasks) on shutdown so SIGTERM/ctrl_c does not leave fire-and-forget accepts.

Binding: **[docs/PHASE109_SPEC.md](./docs/PHASE109_SPEC.md)**.

- [x] Per-broker `AtomicBool` single-flight: first spawn wins; later no-op handle
- [x] Native `serve_listener_until` + connection abort drain (≤2s)
- [x] Kafka `serve_kafka_listener_until` + same drain pattern
- [x] Metrics `run_metrics_server_until` + same drain pattern
- [x] `volant-server` aborts side listeners after primary return; TLS uses full
      `shutdown_signal` + connection drain
- [x] Phase 101 always-spawn + Phase 106 join timeout preserved
- [x] Integration tests (`phase109_shutdown_drain`)

**Honest limitations:** connections aborted (not half-closed); no-op second
handle cannot stop first-flight tasks; multi-broker 2PC still deferred.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI → **closed (MVP) by
Phase 112**, multi-broker 2PC / session affinity / durable sessions,
multi-broker BROKER config fan-out, non-controller alive-set auto-death →
**closed by Phase 110**, straddle marker clip → **closed by Phase 111**.

---

### Phase 110 — Non-controller auto-death from heartbeat alive-set diffs (MVP) ✅

**Goal:** Non-controllers detect dead peers from controller `HeartbeatBroker`
`alive_brokers` gaps (and local membership expire) and call `on_broker_death`
immediately so local ISR shrink + HWM recompute do not wait on ClusterState.

Binding: **[docs/PHASE110_SPEC.md](./docs/PHASE110_SPEC.md)**.

- [x] `apply_controller_alive_set` diffs live set vs controller alive list
- [x] `heartbeat_to_controller` reconciles deaths before ClusterState pull
- [x] `tick_cluster` runs `on_broker_death` on every observer (not only controller)
- [x] `live_brokers` / `local_partition_isr` helpers
- [x] Integration tests (`phase110_alive_set_death`)

**Honest limitations:** controller remains membership SoT (no peer gossip);
assignment/Metadata ISR may lag until ClusterState; rejoin/ISR expand →
**closed by Phase 118**.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI → **closed (MVP) by
Phase 112**, multi-broker 2PC / session affinity / durable sessions,
multi-broker BROKER config fan-out, straddle marker clip → **closed by Phase 111**.

---

### Phase 111 — Clip straddling soft abort markers to log_start (MVP) ✅

**Goal:** When log start advances into a soft abort marker range, clip
`first_offset` to the live `log_start` (keep `end_offset`) so durable
`__txn_markers` / memory no longer hold an obsolete prefix. Fully-below
markers still drop (Phase 104); fully-live markers unchanged.

Binding: **[docs/PHASE111_SPEC.md](./docs/PHASE111_SPEC.md)**.

- [x] GC path clips straddlers (`first_offset < log_start < end_offset`)
- [x] Persist `__txn_markers` on clip or drop; drop counter unchanged on clip
- [x] DeleteRecords / retention / load self-heal share the same rule
- [x] Integration tests (`phase111_straddle_marker_clip`)
- [x] Phase 104 full-drop / retain regressions remain green

**Honest limitations:** whole-segment DeleteRecords only; control batches on
the log are not rewritten; no dedicated clip metric; single-node marker store.

**Still deferred:** multi-lang clients, cargo-fuzz corpus CI → **closed (MVP) by
Phase 112**, multi-broker 2PC / session affinity / durable sessions,
multi-broker BROKER config fan-out; long fuzz campaigns / chaos-mesh remain open.

---

### Phase 112 — cargo-fuzz corpus smoke + CI (MVP) ✅

**Goal:** Close the long-deferred “cargo-fuzz corpus CI” gap with a practical
MVP: checked-in seed corpus, deterministic unit-test seed replay (same decode
paths as `fuzz/` targets), GitHub Actions workflow, and a short optional local
`cargo fuzz` helper. No multi-hour CI campaigns.

Binding: **[docs/PHASE112_SPEC.md](./docs/PHASE112_SPEC.md)**.

- [x] Inventory `fuzz/` targets + protocol chaos unit tests
- [x] Seed corpus under `fuzz/corpus/{decode_frame,decode_request}/`
- [x] `corpus_smoke_decode_paths` in `volant-protocol` (built-in + on-disk seeds)
- [x] `.github/workflows/ci.yml` — workspace tests + explicit corpus smoke
- [x] `scripts/fuzz_corpus_smoke.sh` + `fuzz/README.md` (CI vs local paths)
- [x] Living docs / ROADMAP honesty

**Honest limitations:** seed replay only on CI (no libFuzzer mutation loop);
native protocol only; not a security audit; chaos-mesh / multi-hour campaigns
still deferred.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
multi-broker 2PC / session affinity / durable sessions, multi-broker BROKER
config fan-out → **closed by Phase 113**, Kafka wire fuzz targets.

---

### Phase 113 — Cluster admin fan-out (MVP) ✅

**Goal:** Make cluster-scoped admin ops reach every relevant replica instead of
silently applying only on the node that handled the client request.

Binding: **[docs/PHASE113_SPEC.md](./docs/PHASE113_SPEC.md)**.

- [x] Inter-broker opcodes 70–75 (`ReplicaDeleteRecords`, `ClusterBrokerConfig`,
      `ClusterAclSnapshot`) + encode/decode + dispatch
- [x] **DeleteRecords** best-effort fan-out from partition leader to other replicas
      (Phase 104/111 GC/clip on peers; client success independent of fan-out errors)
- [x] **BROKER** Describe/Alter: controller-only Alter in cluster mode; generationed
      push of the six Phase 99 knobs; sparse durable on each peer
- [x] **ACL** Create/Delete: controller-only in cluster mode; generationed full
      snapshot push; peers install + persist `__acls`
- [x] Metrics: fan-out error counters + config/acl generation gauges
- [x] Tests: `phase113_delete_records_fanout`, `phase113_broker_config_fanout`,
      `phase113_acl_fanout`
- [x] Living docs honesty

**Honest limitations (at Phase 113 ship):** DeleteRecords fan-out is best-effort
(no durable pending queue → **closed by Phase 116**); BROKER knobs are
homogeneous (not per-broker overrides); ACL/config rely on controller liveness
(brief lag on failover); inter-broker admin RPCs are not ACL-gated (shared-token /
TLS only); multi-broker 2PC → **closed by Phase 114**.

**Still deferred (at Phase 113 ship):** multi-lang clients, chaos-mesh / long fuzz
campaigns, multi-broker 2PC / session affinity / durable sessions, Kafka wire
fuzz targets, dynamic membership / Raft metadata, full Kafka broker catalog.

---

### Phase 114 — Multi-broker 2PC / KIP-890-ish MVP ✅

**Goal:** Coordinate Enable2Pc prepare/commit across partition leaders on
different brokers; durable controller prepared index; fence cluster-wide.

Binding: **[docs/PHASE114_SPEC.md](./docs/PHASE114_SPEC.md)**.

- [x] Inter-broker opcodes 76–81 (`TxnParticipantOpen` / `Prepare` / `Complete`)
- [x] Open fan-out after BeginTxn / AddPartitions (best-effort)
- [x] Strict prepare + complete fan-out for Enable2Pc EndTxn (native + Kafka)
- [x] Local `__txn_prepared` + controller `__txn_prepared/cluster.json` index
- [x] Fence: complete with `commit=false` force-aborts peer PrepareCommit
- [x] Metrics: `volant_txn_2pc_fanout_errors_total`, `volant_cluster_prepared_txns`
- [x] Tests: `phase114_multi_broker_2pc` (happy path + fence + single-node)
- [x] Living docs honesty

**Honest limitations:** not full KIP-890/939; no Kafka `__transaction_state`
topic; open fan-out best-effort for down peers; prepare strict for **live** peers
only; clients should pin Init/Begin/EndTxn to the coordinator broker (no
transparent forward); controller failover may drop cluster index until next
prepare; inter-broker 2PC RPCs not ACL-gated.

**Still deferred (at Phase 114 ship):** multi-lang clients, chaos-mesh / long fuzz
campaigns, multi-broker session affinity / durable sessions, full KIP-890/939 /
`__transaction_state`, Kafka wire fuzz targets, dynamic membership / Raft
metadata, full Kafka broker catalog, transparent EndTxn forward.

---

### Phase 115 — Durable fetch sessions (MVP) ✅

**Goal:** Persist Fetch session_id → partition state (epoch, omit HWM/LSO cache)
under `{data_dir}/__fetch_sessions` so a broker restart on the same data_dir
restores incremental sessions within idle TTL. Multi-broker handoff deferred
(sticky-by-convention).

Binding: **[docs/PHASE115_SPEC.md](./docs/PHASE115_SPEC.md)**.

- [x] Durable snapshot `{data_dir}/__fetch_sessions/state.json` (atomic write)
- [x] Load on `Broker::new` / `with_cluster` with idle-TTL filter
- [x] Persist on create / incremental / merge / forget / note_returned / close / evict
- [x] Metrics: `volant_fetch_sessions_restored`, `volant_fetch_sessions_persist_errors_total`
- [x] Tests: `phase115_durable_fetch_sessions` + unit roundtrip / idle load filter
- [x] Living docs honesty (not multi-broker sticky)

**Honest limitations:** per-broker local only (not replicated / no handoff);
wrong broker ⇒ **70**; full snapshot + fsync on mutation (debounce deferred);
not a Kafka shared consumer-session store; pin Fetch to session-owner broker.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
multi-broker session handoff / affinity routing, full KIP-890/939 /
`__transaction_state`, Kafka wire fuzz targets, dynamic membership / Raft
metadata, full Kafka broker catalog, transparent EndTxn forward, byte-identical
response cache, debounced session persist,
durable pending DeleteRecords for offline replicas → **closed by Phase 116**.

---

### Phase 116 — Durable DeleteRecords outbox for offline replicas (MVP) ✅

**Goal:** When DeleteRecords fan-out fails (offline / flaky peer), remember the
pending truncate on the leader under `{data_dir}/__delete_records_outbox` and
retry via `ReplicaDeleteRecords` so peer log starts catch up after the peer
returns. Client success remains independent of fan-out (Phase 113).

Binding: **[docs/PHASE116_SPEC.md](./docs/PHASE116_SPEC.md)**.

- [x] Durable outbox snapshot `{data_dir}/__delete_records_outbox/state.json`
- [x] Enqueue on Phase 113 fan-out failure (merge max `before_offset` per peer/tp)
- [x] Background drain for live peers (~500ms) + explicit `drain_delete_records_outbox`
- [x] Metrics: depth, enqueued, retry success/errors, capacity drops
- [x] Tests: `phase116_delete_records_outbox` (offline catch-up, enqueue, drain, unit)
- [x] Living docs honesty

**Honest limitations:** leader-local outbox only (not consensus / not controller
SoT); leadership change handoff → **Phase 123** (new leader reconcile); bounded
10k keys; whole-segment truncate only; not multi-DC.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
multi-broker session handoff, full KIP-890/939 / `__transaction_state`, Kafka
wire fuzz targets, dynamic membership / Raft, full Kafka broker catalog,
transparent EndTxn forward, outbox handoff on leadership change, per-broker
BROKER config overrides,
controller failover ACL/config permanent drift → **closed by Phase 117**.

---

### Phase 117 — Controller failover catch-up for ACL + BROKER config (MVP) ✅

**Goal:** After peer rejoin, controller restart, or brief controller change,
brokers converge on generationed ACL snapshot + BROKER knobs instead of silent
permanent drift when a Phase 113 push was missed.

Binding: **[docs/PHASE117_SPEC.md](./docs/PHASE117_SPEC.md)**.

- [x] Durable admin generations `{data_dir}/__cluster_admin/state.json`
- [x] `HeartbeatBroker` applied-config/acl generation piggyback (backward compatible)
- [x] Controller lag-driven re-push via opcodes 72–75 (full effective BROKER + ACL snapshot)
- [x] Metrics: `volant_cluster_admin_catchup_success_total` /
      `volant_cluster_admin_catchup_errors_total` (+ existing gen gauges)
- [x] Tests: `phase117_admin_catchup` (offline rejoin config+ACL, controller restart gens)
- [x] Living docs honesty

**Honest limitations:** still not Raft (brief lag until heartbeat + catch-up RPC);
catch-up BROKER body is full effective six knobs (may expand peer sparse overlay);
a new controller that never received prior pushes may re-push stale local state at
its durable gen; inter-broker admin RPCs still not ACL-gated.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
multi-broker session handoff, full KIP-890/939 / `__transaction_state`, Kafka
wire fuzz targets, dynamic membership / Raft, full Kafka broker catalog,
transparent EndTxn forward, outbox handoff on leadership change, per-broker
BROKER config overrides,
ISR rejoin / lag-based shrink → **closed by Phase 118**.

---

### Phase 118 — ISR rejoin + lag-based shrink (MVP) ✅

**Goal:** After Phase 108/110 death shrink, a recovering follower that
ReplicaFetches up to the leader (LEO ≥ HWM and lag ≤ `replica_lag_max_messages`)
re-expands the ISR; slow-but-alive followers exceeding the lag threshold are
dropped from ISR with metrics.

Binding: **[docs/PHASE118_SPEC.md](./docs/PHASE118_SPEC.md)**.

- [x] `reconcile_isr` / rejoin when LEO ≥ HWM and lag ≤ `replica_lag_max_messages`
- [x] Lag-based shrink of in-ISR members on ReplicaFetch (same Phase 6 knob)
- [x] ClusterState apply preserves leader-local caught-up rejoin members
- [x] Metrics: `volant_isr_expand_total` / `volant_isr_shrink_total`
- [x] Tests: `phase118_isr_rejoin` (death→rejoin, lag shrink, preserve, single-node)
- [x] Living docs honesty

**Honest limitations:** static membership; offset lag only (no time-based lag);
no preferred replica; controller durable assignment / Metadata ISR may lag when
the partition leader is not the controller (produce/HWM uses leader-local ISR).

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
multi-broker session handoff → **closed by Phase 119**, full KIP-890/939 /
`__transaction_state`, Kafka wire fuzz targets, dynamic membership / Raft,
full Kafka broker catalog, transparent EndTxn forward, outbox handoff on
leadership change, per-broker BROKER config overrides, time-based ISR lag /
preferred replica.

---

### Phase 119 — Multi-broker fetch session handoff / affinity (MVP) ✅

**Goal:** A Fetch session opened on broker A remains usable when the client
(or LB) hits broker B: cluster `session_id` embeds the owner; non-owner
transparent-forwards the Kafka Fetch body over inter-broker RPC so epoch and
omit-unchanged stay correct on the single owner SoT.

Binding: **[docs/PHASE119_SPEC.md](./docs/PHASE119_SPEC.md)**.

- [x] Owner-encoded session_id in cluster mode (`node_id << 19 | local`)
- [x] Native opcodes 82/83 `KafkaFetchForward` (request/response bodies)
- [x] Kafka shim: foreign-owner miss → forward; owner encode_fetch local only
- [x] Metrics: `volant_fetch_session_forward_total` / `_errors_total`
- [x] Tests: `phase119_fetch_session_handoff` (forward + omit, epoch 71, FINAL, single-node)
- [x] Living docs honesty

**Honest limitations:** not preferred-replica; not a controller-replicated
session store (owner death ⇒ 70); forward adds one RTT; single-node sequential
ids unchanged; large responses bounded by native `MAX_PAYLOAD`.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
full KIP-890/939 / `__transaction_state`, Kafka wire fuzz targets, dynamic
membership / Raft, full Kafka broker catalog,
transparent EndTxn forward → **closed by Phase 120**,
sticky FindCoordinator → **closed by Phase 121**,
outbox handoff on leadership change, per-broker BROKER config overrides,
time-based ISR lag / preferred replica / shared session registry.

---

### Phase 120 — Transparent EndTxn / txn RPC forward (MVP) ✅

**Goal:** When a client sends EndTxn to a non-coordinator broker, transparent
inter-broker forward to the Init-owner coordinator so Enable2Pc prepare/complete
and classic one-shot succeed without permanent broken state.

Binding: **[docs/PHASE120_SPEC.md](./docs/PHASE120_SPEC.md)**.

- [x] Native opcodes 84/85 `KafkaTxnForward` (Kafka API key + body proxy)
- [x] Txn coordinator registry (Init owner) + `TxnParticipantOpen` trailer
      (`coordinator_node_id`, `install_open`)
- [x] Init registration fan-out (`install_open=false`) + open fan-out carries owner
- [x] Kafka EndTxn path: non-coordinator → forward (no dual prepare)
- [x] Metrics: `volant_txn_forward_total` / `_errors_total`
- [x] Tests: `phase120_endtxn_forward` (2PC + classic + fence + single-node)
- [x] Living docs honesty

**Honest limitations:** not full KIP-890/939 / `__transaction_state`; coordinator
discovery is Init-owner registry (not Raft); full Init re-home still prefer
client pin when skipping FindCoordinator.
FindCoordinator sticky hash → **closed by Phase 121**.
AddOffsets / TxnOffsetCommit forward → **closed by Phase 122**.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
full KIP-890/939 / `__transaction_state`, Kafka wire fuzz targets, dynamic
membership / Raft, full Kafka broker catalog, outbox handoff on leadership change,
per-broker BROKER config overrides, time-based ISR lag / preferred replica /
shared session registry (AddOffsets / TxnOffsetCommit forward closed by Phase 122).

---

### Phase 121 — Sticky FindCoordinator assignment (MVP) ✅

**Goal:** Map group_id / transactional_id stably to a live broker via consistent
hash over static membership (not always first metadata broker); known
transactional_id returns Phase 120 Init owner.

Binding: **[docs/PHASE121_SPEC.md](./docs/PHASE121_SPEC.md)**.

- [x] Sticky murmur2 over sorted configured broker ids + next-live failover
- [x] Init-owner registry overrides hash for known transactional_id
- [x] Kafka FindCoordinator v0–6 per-key resolve (group + transaction)
- [x] Tests: `phase121_sticky_find_coordinator` (stability, spread, dead-node,
      registry override, Phase 120 EndTxn interaction, single-node)
- [x] Living docs honesty

**Honest limitations:** not full KIP-890/939 / `__transaction_state`; static
membership only; group state not migrated on death failover; Init on non-sticky
broker still allowed (registry then overrides FindCoordinator); native protocol
has no FindCoordinator API.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
full KIP-890/939 / `__transaction_state`, Kafka wire fuzz targets, dynamic
membership / Raft, full Kafka broker catalog, outbox handoff on leadership change,
per-broker BROKER config overrides, time-based ISR lag / preferred replica /
shared session registry; AddOffsets / TxnOffsetCommit forward → **closed by Phase 122**.

---

### Phase 122 — Transparent AddOffsetsToTxn / TxnOffsetCommit forward (MVP) ✅

**Goal:** When a client sends AddOffsetsToTxn or TxnOffsetCommit to a
non-coordinator broker, transparent inter-broker forward to the Init-owner
coordinator so deferred offsets buffer only on the coordinator SoT (no dual-commit).

Binding: **[docs/PHASE122_SPEC.md](./docs/PHASE122_SPEC.md)**.

- [x] Reuse native opcodes 84/85 `KafkaTxnForward` for Kafka API keys 25 + 28
- [x] Peek transactional_id / producer_id; resolve Init-owner registry
- [x] Non-coordinator client path always forwards when owner known (no local buffer)
- [x] Coordinator handler dispatches AddOffsets / TxnOffsetCommit / EndTxn
- [x] Metrics: `volant_txn_forward_*` cover 25/26/28
- [x] Tests: `phase122_txn_offset_forward` (multi-node forward + EndTxn apply + single-node)
- [x] Living docs honesty

**Honest limitations:** not full KIP-890/939 / `__transaction_state`; registry miss
still local-path (honest error); TxnOffsetCommit forward-failure body is empty
topics; native has no separate AddOffsets/TxnOffsetCommit RPCs; Init still best
on sticky coordinator / after FindCoordinator.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
full KIP-890/939 / `__transaction_state`, Kafka wire fuzz targets, dynamic
membership / Raft, full Kafka broker catalog,
per-broker BROKER config overrides, time-based ISR lag / preferred replica /
shared session registry. (Outbox leadership handoff → **Phase 123**.)

---

### Phase 123 — DeleteRecords outbox leadership handoff (MVP) ✅

**Goal:** When the partition leader that held a durable DeleteRecords outbox is
demoted or dies, pending truncates for offline peers are **not permanently lost**.
The new leader rebuilds pending targets from its local `log_start` and retries via
existing `ReplicaDeleteRecords` drain.

Binding: **[docs/PHASE123_SPEC.md](./docs/PHASE123_SPEC.md)**.

- [x] `Broker::reconcile_delete_records_outbox` from local log_start + current epoch
- [x] In-memory last_reconcile dedup per `(topic, partition) → (epoch, log_start)`
- [x] Background loop: reconcile then drain (~500ms cluster mode)
- [x] Drain stamps current epoch when still leader (avoid self-fence)
- [x] Metric: `volant_delete_records_outbox_reconcile_total`
- [x] Tests: `phase123_delete_records_outbox_handoff` (handoff + idempotent + single-node)
- [x] Living docs honesty

**Honest limitations:** not a consensus truncate log; new leader must already hold
the advanced log start (was online during DeleteRecords); demoted leader outbox is
not bulk-transferred; bounded outbox may still drop under extreme backlog.

**Still deferred:** multi-lang clients, chaos-mesh / long fuzz campaigns,
full KIP-890/939 / `__transaction_state`, Kafka wire fuzz targets, dynamic
membership / Raft, full Kafka broker catalog, consensus truncate journal /
controller SoT pending set, per-broker BROKER config overrides, time-based ISR
lag / preferred replica / shared session registry.

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
| Ops model | ZooKeeper/KRaft + heavy footprint | Single binary → small static ISR quorum |
| Protocol | Kafka wire protocol | Native binary first; optional Kafka shim (`--kafka-listen`, Phases 23–109; cluster Phases 6/108/110) |
| Goal | Full ecosystem | Subset that is fast, small, and correct |

Volant is **not** a drop-in Kafka replacement. It prioritizes a clean core; the
optional Kafka wire shim is **shipped** (Phases 23–109; cluster ISR death 108/110;
marker clip 111; fuzz corpus smoke CI 112; cluster admin fan-out 113) — see
[docs/KAFKA_COMPAT.md](./docs/KAFKA_COMPAT.md).

---

## Suggested implementation order (PRs)

Phases **0–121 are shipped**. Historical PR order for the core:

1. Phase 1 segment format + unit tests  
2. Phase 1 recovery + retention  
3. Phase 1 append bench baseline (`volant-bench`)  
4. Phase 2 protocol produce/fetch + TCP server  
5. Phase 2 client SDK + CLI  
6. Phase 3 consumer groups  
7. Phase 4 stream operators + example  
8. Phase 5 io_uring feature flag + benches  
9. Phase 6 replication prototype (2–3 nodes) ✅  
10. Phase 7 metrics, TLS, packaging ✅  
11. Phases 8–22 (redirect, groups, configs, txns, mTLS, ACLs, SCRAM) ✅  
12. Phases 23–111 (Kafka wire shim + marker GC/clip + empty-AddPartitions control + bg shutdown join + phase103 flake fix + follower-death ISR + accept drain / single-flight bg + non-controller alive-set death + straddle marker clip) ✅  
13. Phase 112 (cargo-fuzz corpus smoke + CI MVP) ✅  
14. Phase 113 (cluster admin fan-out: DeleteRecords + BROKER config + ACL snapshot) ✅  
15. Phase 114 (multi-broker Enable2Pc prepare/complete MVP) ✅  
16. Phase 115 (durable local fetch sessions MVP) ✅  
17. Phase 116 (durable DeleteRecords outbox for offline replicas MVP) ✅  
18. Phase 117 (controller failover catch-up for ACL + BROKER config MVP) ✅  
19. Phase 118 (ISR rejoin + lag-based shrink MVP) ✅  
20. Phase 119 (multi-broker fetch session handoff MVP) ✅  
21. Phase 120 (transparent EndTxn forward MVP) ✅  
22. Phase 121 (sticky FindCoordinator MVP) ✅  
23. Phase 122 (AddOffsets / TxnOffsetCommit forward MVP) ✅  
24. Phase 123 (DeleteRecords outbox leadership handoff MVP) ✅  

---

## Open design decisions

Track these before locking APIs:

1. **Replication:** ~~Raft-per-partition vs leader/follower + controller (Kafka-like)?~~ → **Kafka-style ISR (Phase 6)**
2. **Kafka wire compatibility:** ~~first-class or optional adapter?~~ → **optional adapter (`--kafka-listen`, Phases 23–85 shipped)**
3. **State store for streams:** embed RocksDB, redb, or custom mmap store?
4. **Default durability:** fsync every batch vs group commit window?
5. **Multi-tenancy:** namespaces / quotas in v1 or later?

---

## Getting started (now)

```bash
# Build everything
cargo build --workspace

# Run the server (native protocol)
cargo run -p volant-server -- --data-dir ./data --listen 127.0.0.1:9092

# Optional Kafka wire port
cargo run -p volant-server -- \
  --data-dir ./data \
  --listen 127.0.0.1:9092 \
  --kafka-listen 127.0.0.1:9093

# CLI
cargo run -p volant-cli -- version

# Phase 1 append micro-bench (release recommended)
cargo run -p volant-bench --release

# Tests
cargo test --workspace
```

**Status (post–Phase 123):** core broker, ops (metrics / TLS / auth / SCRAM /
ACLs / Helm), and the Kafka wire shim are **shipped** (Fetch 0–18 Kafka max;
ACL admin 0–3 with User resource; soft-marker `READ_COMMITTED` with **marker GC/clip**
on DeleteRecords/retention/load (Phase 104/111); durable OFLE history; Fetch DivergingEpoch +
sessions with omit-unchanged incremental + idle TTL/max + **durable local restore**
(Phase 115); Kafka control batches
on EndTxn **and** crash≡abort open promote **including empty AddPartitions**;
prepared 2PC MVP + **multi-broker Enable2Pc** (Phase 114); prepared + open txn
timeouts + broker max timeout clamp;
background txn/session sweeper + richer expiry metrics (always-spawn; 0→>0
without restart; **graceful shutdown/join** Phase 106; **accept-loop drain +
single-flight bg** Phase 109); BROKER Describe/AlterConfigs
with **sparse** durable restart restore and resource name empty-or-local-`node_id`
(Phase 103; **parallel test isolation** Phase 107); **follower-death ISR shrink +
HWM recompute** Phase 108; **non-controller alive-set auto-death** Phase 110;
**ISR rejoin + lag-based shrink** Phase 118; **straddle soft-marker clip** Phase 111;
**cargo-fuzz corpus smoke + CI MVP** Phase 112; **cluster admin fan-out** Phase 113
(DeleteRecords best-effort replica truncate; controller-only BROKER config + ACL
snapshot push); **multi-broker 2PC MVP** Phase 114 (inter-broker prepare/complete;
controller cluster prepared index — not full KIP-890 / `__transaction_state`);
**durable fetch sessions** Phase 115 (`__fetch_sessions`);
**multi-broker session handoff** Phase 119 (owner-encoded id + transparent forward);
**durable DeleteRecords outbox** Phase 116 (`__delete_records_outbox`; at-least-once
retry for offline peers) + **leadership handoff reconcile** Phase 123 (new leader
rebuilds from local `log_start` — still not a consensus truncate log);
**ACL/BROKER admin catch-up** Phase 117; sticky FindCoordinator Phase 121;
txn EndTxn/AddOffsets/TxnOffsetCommit forward Phases 120/122).
Still deferred: multi-language clients, full chaos-mesh suites / long fuzz
campaigns, preferred-replica / shared session store, full KIP-890/939.
Details: [docs/KAFKA_COMPAT.md](./docs/KAFKA_COMPAT.md), [docs/ops.md](./docs/ops.md).
