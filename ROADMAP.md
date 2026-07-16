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
- [ ] Security audit with `cargo fuzz` corpus CI — **deferred** (deterministic chaos tests ship now)

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

**Still deferred:** Kafka shim, multi-lang clients, SCRAM, mTLS identity, full cargo-fuzz corpus CI.

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
2. **Kafka wire compatibility:** ~~first-class or optional adapter?~~ → **optional adapter (`--kafka-listen`, Phase 23 MVP)**
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
