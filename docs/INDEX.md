# Volant documentation index

Start here. Prefer living docs over individual phase ship records.

## Essential

| Document | Purpose |
|----------|---------|
| **[WHITEPAPER.md](./WHITEPAPER.md)** | Technical whitepaper (architecture, positioning, honesty) |
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
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index for phases 0–106 |
| [PHASE7_SPEC.md](./PHASE7_SPEC.md) … [PHASE106_SPEC.md](./PHASE106_SPEC.md) | Per-phase ship records (deep dive) |
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

## Compaction note (2026-07-18, post–Phase 106 ship)

Living docs match **git HEAD product** (`SUPPORTED_APIS`, last feature commit Phase **106**):

- **Status ceiling:** Phases **0–106**; Kafka shim **23–106**
- **Kafka SoT:** [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — matrix + semantic honesty
- **WHITEPAPER:** architecture + positioning; no full API matrix
- **Binding core:** PHASE1–6; **ship records:** PHASE7–106 via [PHASE_HISTORY](./history/PHASE_HISTORY.md)
- **README / ops:** compact bands + ops table (not per-phase diaries)
- **Txn honesty (shipped):** write-through + soft markers + EndTxn control batches (Phase 89) + crash-promote ABORT control (Phase 98) + **empty AddPartitions control** (Phase 105) + prepared 2PC MVP (Phase 90) + prepared/open timeout (Phase 92/93) + TRANSACTION_ABORTABLE honest subset after timeout (Phase 94) + transaction max timeout clamp (Phase 96; default 15m; Init **50** over-max) + background sweeper (Phase 97; always-spawn / 0→>0 live Phase 101; **graceful shutdown/join** Phase 106) + BROKER Describe/AlterConfigs knobs (Phase 99) + **sparse** durable restart restore (Phase 100/102) + BROKER name vs local `node_id` (Phase 103) + **aborted soft-marker GC** on DeleteRecords/retention/load (Phase 104)
- **Epoch honesty (shipped):** durable OFLE history MVP; Metadata live leader_epoch; Fetch DivergingEpoch
- **Fetch sessions (shipped MVP):** process-local create/forgotten/errors; omit-unchanged empty-topics incremental (Phase 91); idle TTL + max/LRU (Phase 95); background idle sweep (Phase 97/101/106); BROKER config surface (Phase 99–103 sparse durable + name validation)
- **Still deferred (product):** multi-lang, chaos/fuzz corpus CI, multi-broker 2PC /
  multi-broker session affinity / full KIP-890; multi-broker BROKER config fan-out;
  full Kafka broker catalog; accept-loop drain on shutdown
