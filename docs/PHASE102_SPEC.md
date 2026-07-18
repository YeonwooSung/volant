# Phase 102 — Sparse durable broker config (MVP)

## Goals

1. Persist only BROKER knobs that were **explicitly altered** under
   `{data_dir}/__broker_config/state.json` (sparse overlay, not full snapshot).
2. On load: product default → env at construction → **sparse file** (only keys
   present override).
3. On Alter SET of key K: update live value; merge only K into the durable map.
4. On DELETE / empty Alter of key K: restore **product** default live **and
   remove K from the durable file** so env can re-apply for K on next restart.
   Empty overlay → remove `state.json` (or equivalent empty map).
5. `validate_only` still does not touch the file; direct `set_*` remain
   process-local; TOPIC configs unchanged.
6. Tests (`phase102_*.rs`) + living docs honesty.

## Non-goals

- Full Kafka DynamicBrokerConfig / KRaft metadata quorum
- BROKER name = `node_id` validation → **closed by Phase 103**
- Marker GC / DeleteRecords → **closed by Phase 104** / empty-AddPartitions control markers → **closed by Phase 105**
- Graceful sweeper join on stop
- Multi-broker config broadcast / multi-broker 2PC / sessions
- Multi-lang clients / fuzz CI
- Auto-migrating pre–Phase 102 full-snapshot files down to “truly altered”
  keys only (legacy full files still load key-by-key; new writes are sparse)

## Problem (Phase 100 honesty gap)

Phase 100 wrote a **full snapshot** of all six live knobs on any successful
Alter. That froze env-only overrides for keys never touched by Alter. After
DELETE, product default was written back into the full file, so env still did
not re-apply on restart.

## Design

### Sparse overlay

```json
{
  "version": 1,
  "configs": {
    "transaction.max.timeout.ms": 222000
  }
}
```

Only keys present in `configs` override product→env. Missing keys are left to
product / env.

| Op | Live | Durable file |
|----|------|--------------|
| SET key K | apply value | insert/update K only |
| DELETE / empty K | product default | remove K; clear file if empty |
| validate_only | no change | no write |
| `Broker::set_*` | process-local | no write |

### Precedence (load order)

| Layer | When | Source |
|-------|------|--------|
| 1. Product default | Always baseline | PHASE99 table |
| 2. Env at construction | `Broker::new` / `with_cluster` | `VOLANT_*` |
| 3. Sparse durable file | After atomics / session manager | keys present in `state.json` only |
| 4. Runtime alter | After process start | Alter / IncrementalAlter / setters |

```
product_default → env_override → durable_file[K] if present → runtime_alter
```

### Path & format

Unchanged from Phase 100:

| Item | Value |
|------|-------|
| Directory | `{data_dir}/__broker_config/` |
| File | `state.json` |
| Write | `state.json.tmp` → fsync → rename |
| Empty overlay | remove `state.json` |

Schema version remains **1** (same JSON shape; semantics sparse).

### Legacy Phase 100 files

Existing full-snapshot files (all six keys) still load correctly: each present
key overrides. Subsequent sparse SET/DELETE only mutates keys in the alter
request. DELETE of a key unfreezes env for that key going forward.

## Exit criteria

1. Alter one key while env overrides another → restart → altered from file,
   env key still from env
2. DELETE altered key → file drops key → restart with env set → env applies
3. Multi-key SET restores those keys after restart; file holds only those keys
4. `validate_only` does not write the file
5. Phase 100-style single-key alter still survives restart
6. `phase102_*` + phase100 + phase99 green
7. Docs: PHASE102_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Six knobs only; single-node; resource name still ignored → **closed by Phase 103**
- DELETE live value is still **product** default (not env) until restart —
  same Phase 99/100 live semantics
- Direct setters do not persist
- No multi-broker fan-out / full Kafka catalog
- Pre–102 full snapshots may still pin keys until explicitly DELETE'd

## Test plan

`crates/volant-broker/tests/phase102_sparse_broker_config.rs`:

1. Alter one key + env on another → restart → sparse honesty
2. DELETE → file drops key → env re-applies on restart
3. Multi-key SET sparse + restart
4. validate_only no file write
5. Single-key alter survives restart (phase100 regression)
6. Direct setters do not auto-persist

## Phase 103 ideas

- Validate BROKER resource name against `node_id` → **closed by Phase 103**
- Graceful sweeper shutdown / join on server stop
- Marker compaction / GC with DeleteRecords
- Empty-AddPartitions control markers → **closed by Phase 105**
- Multi-broker config broadcast / multi-broker 2PC
- Multi-lang clients / cargo-fuzz corpus CI
