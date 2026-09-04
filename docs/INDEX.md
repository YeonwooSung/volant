# Volant documentation index

Start here. Prefer living docs over individual phase ship records.

## Essential

| Document | Purpose |
|----------|---------|
| **[WHITEPAPER.md](./WHITEPAPER.md)** | Technical whitepaper (architecture, positioning, honesty) |
| **[V02_FREEZE.md](./V02_FREEZE.md)** | v0.2 product freeze (scope lock) |
| **[ops.md](./ops.md)** | Operator runbook (flags, metrics, auth, TLS, Kafka listen) |
| **[consistency.md](./consistency.md)** | What “committed” means (HWM / ISR / acks) |
| **[tuning.md](./tuning.md)** | Performance / I/O / affinity guide |
| **[KAFKA_COMPAT.md](./KAFKA_COMPAT.md)** | Current Kafka API versions + open limitations |
| **[features.md](./features.md)** | Native features beyond core (idempotence → SCRAM) |
| [../README.md](../README.md) | Quick start |
| [../ROADMAP.md](../ROADMAP.md) | Full phase chronicle + deferred work |
| [../deploy/README.md](../deploy/README.md) | Docker / systemd / Helm |

## Binding core specs (formats & native APIs)

| Spec | Topic |
|------|-------|
| [PHASE1_SPEC.md](./PHASE1_SPEC.md) | Durable log format & storage API |
| [PHASE2_SPEC.md](./PHASE2_SPEC.md) | Native TCP protocol & client/server API |
| [PHASE3_SPEC.md](./PHASE3_SPEC.md) | Consumer groups & offsets |
| [PHASE4_SPEC.md](./PHASE4_SPEC.md) | Stream operators & topology |
| [PHASE5_SPEC.md](./PHASE5_SPEC.md) | DMA / high-performance I/O |
| [PHASE6_SPEC.md](./PHASE6_SPEC.md) | Clustering & ISR replication |

## Phase history

| Document | Purpose |
|----------|---------|
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index for phases 0–154 + **155 open** + residuals v0.3–v0.226 |
| [PHASE7_SPEC.md](./PHASE7_SPEC.md) … [PHASE155_SPEC.md](./PHASE155_SPEC.md) | Per-phase ship records (155 in progress) |
| [V06_SPEC.md](./V06_SPEC.md) … [V226_SPEC.md](./V226_SPEC.md) | Residual slices v0.6–v0.226 |
| [history/archive/](./history/archive/) | Implementation plans & reviews (archaeology) |

## Config samples

| File | Purpose |
|------|---------|
| [cluster.toml](./cluster.toml) | Example 3-node cluster config |
| [../examples/cluster.toml](../examples/cluster.toml) | Same, under examples/ |

## How to read by role

| Role | Read first |
|------|------------|
| New engineer | WHITEPAPER → README → PHASE1–2 |
| Operator | ops → consistency → tuning → deploy |
| Kafka interop | KAFKA_COMPAT → ops (Kafka listen) → PHASE23+ as needed |
| Protocol implementer | PHASE1–6 binding specs |
| Roadmap / deferred | ROADMAP end sections |

## Compaction note (2026-08-13, post–Phase 147 ship)

Living docs match **git HEAD product** (**v0.2 + residuals v0.3–v0.226**, crate **0.2.0**, **Phase 155 open**):

- **Status ceiling:** **v0.2 shipped** + residuals **v0.3–v0.226**. Phases **0–154** shipped, **155 open** ([PHASE155_SPEC.md](./PHASE155_SPEC.md)). Residual **v0.155** is DeleteRecords wait config, **not Phase 155**. Homemade 154 hatch **deleted** (v0.222). Kafka shim **39 keys** (key **45** v0 = AlterPartitionReassignments, v0.225). Kafka shim **23–109** (… **135 = optional DeleteRecords majority wait**; **136 = non-blocking admin catch-up**; **137 = native DeleteRecords wait trailer + journal topic GC** — Kafka still env-only for wait; **138 = best-effort shared fetch session mirror + promote**; **139 = mirror coalesce/debounce + optional durable + `mirror_gen` fence**; **140 = preferred max LEO lag + RC suppress metric**; **141 = N=2 majority health gauges** `volant_cluster_*`; **142 = Metadata leader ISR overlay + IsrUpdate 94/95**; **143 = promote claim fence lowest-id `promoted_by`**; **144 = preferred × established-session suppress**; **147 = serve-from-mirror without promote on owner miss**; **149 = durable stream state**; **150/152 = assignment majority consensus + Metadata live by default** (152 committed-only **opt-in**); **151/153 = stream EOS + durable checkpoint staging**; **154 = KRaft-style metadata Raft log MVP**, hatch removed **v0.222**)
- **Kafka SoT:** [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — matrix + semantic honesty
- **WHITEPAPER:** architecture + positioning; no full API matrix
- **Binding core:** PHASE1–6; **ship records:** PHASE7–154 via [PHASE_HISTORY](./history/PHASE_HISTORY.md)

- **README / ops:** compact bands + ops table (not per-phase diaries)
- **Txn honesty (shipped):** write-through + soft markers + EndTxn control batches (Phase 89) + crash-promote ABORT control (Phase 98) + **empty AddPartitions control** (Phase 105) + prepared 2PC MVP (Phase 90) + prepared/open timeout (Phase 92/93) + TRANSACTION_ABORTABLE honest subset after timeout (Phase 94) + transaction max timeout clamp (Phase 96; default 15m; Init **50** over-max) + background sweeper (Phase 97; always-spawn / 0→>0 live Phase 101; **graceful shutdown/join** Phase 106; **accept-loop drain + single-flight** Phase 109) + BROKER Describe/AlterConfigs knobs (Phase 99) + **sparse** durable restart restore (Phase 100/102) + BROKER name vs local `node_id` (Phase 103; **parallel test isolation** Phase 107) + **aborted soft-marker GC/clip** on DeleteRecords/retention/load (Phase 104/111) + **multi-broker Enable2Pc prepare/complete** (Phase 114; controller cluster prepared index; not full `__transaction_state`) + **transparent EndTxn forward** (Phase 120) + **sticky FindCoordinator** (Phase 121) + **AddOffsets / TxnOffsetCommit forward** (Phase 122)
- **Cluster ISR (Phase 108/110/118/125/142):** follower death shrinks local ISR + recomputes HWM on every observer; controller bumps generation on pure ISR shrink; **non-controllers** also apply controller `alive_brokers` diffs / local expire → `on_broker_death` (Phase 110); **Phase 118** re-expands ISR when a recovering follower ReplicaFetches to LEO ≥ HWM (lag ≤ `replica_lag_max_messages`) and lag-shrinks slow-but-alive members; **Phase 125** also time-shrinks members whose last caught-up stamp exceeds `replica_lag_max_ms` (default 30s; `0` off); **Phase 142** Metadata overlays leader-local ISR and leaders report ISR to controller (`IsrUpdate` 94/95, best-effort); metrics `volant_isr_expand_total` / `volant_isr_shrink_total` / `volant_isr_time_shrink_total`
- **Cluster admin fan-out (Phase 113 + 116 + 117 + 123):** DeleteRecords best-effort replica truncate + **durable leader outbox** retry for offline peers (Phase 116) + **new-leader reconcile from log_start** on leadership change (Phase 123); controller-only BROKER Alter + ACL Create/Delete with generationed push; **durable gens + heartbeat lag re-push** so rejoin/controller restart do not permanently drift (Phase 117; not Raft)
- **Multi-broker 2PC (Phase 114 + 120 + 121 + 122 + 124 + 127):** Enable2Pc EndTxn prepare/complete fans out to live peers; local `__txn_prepared` + controller `__txn_prepared/cluster.json`; fence complete-abort with `commit=false`; **EndTxn / AddOffsets / TxnOffsetCommit** to non-coordinator transparent-forwards to Init owner (Phase 120/122); **FindCoordinator** sticky murmur2 + Init-owner override (Phase 121); **Init-owner registry durable** under `__txn_coordinator` (Phase 124) with **TTL GC** (Phase 127; default 24h) + **BROKER config** (Phase 128); not full KIP-890/939
- **Epoch honesty (shipped):** durable OFLE history MVP; Metadata live leader_epoch; Fetch DivergingEpoch
- **Fetch sessions (shipped MVP):** create/forgotten/errors; omit-unchanged empty-topics incremental (Phase 91); idle TTL + max/LRU (Phase 95); background idle sweep (Phase 97/101/106); BROKER config surface (Phase 99–103 sparse durable + name validation; **cluster fan-out** Phase 113); **durable per-broker table** under `__fetch_sessions` (Phase 115); **multi-broker owner-encode + transparent forward** (Phase 119); **best-effort peer mirror + promote-on-owner-miss** (Phase 138; opcodes 90–93) + **coalesce/debounce Puts, optional durable `__fetch_session_mirrors`, `mirror_gen` fence** (Phase 139) + **promote claim fence** (Phase 143; lowest-id `promoted_by`; not Raft; brief dual primary until MirrorPut exchange)
- **PreferredReadReplica (Phase 126+133+140+144 + v0.7):** Fetch v11+ client rack; leader redirects to same-rack ISR peer with usable addr + LEO≥HWM (empty records); **rank highest LEO then lowest id** (133); optional `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` (140; unset = unlimited); **suppressed when isolation=READ_COMMITTED** with `volant_preferred_replica_suppressed_total` when a candidate existed (140); **suppressed when client has established fetch session** (`req_session_id != 0`) with `volant_preferred_replica_session_suppressed_total` (144); opt-in redirect `throttle_time_ms` + advertised-addr TCP probe (v0.7; both default off); Metadata rack from `cluster.toml`; not Kafka client-quota throttling

- **Fuzz / CI (Phase 112):** deterministic corpus smoke + `.github/workflows/ci.yml`; long campaigns / chaos-mesh still deferred
- **Still deferred (product):** multi-lang, chaos-mesh / long fuzz campaigns, full preferred selector (beyond 126/133/140/144), Raft session registry / serve-from-mirror-without-promote / incremental put residual, full KIP-890/939 / `__transaction_state` topic; full Kafka broker catalog
