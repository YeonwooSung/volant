# Phase 100 — Durable dynamic broker config (MVP)

## Goals

1. Persist the six Phase 99 **BROKER** dynamic knobs under `data_dir` so
   AlterConfigs / IncrementalAlterConfigs survive process restart.
2. On `Broker::new` / `with_cluster`: after env-at-construction defaults, load
   the durable file and apply setters so live getters match last Alter.
3. On successful non-`validate_only` Alter / IncrementalAlter: write durable
   knobs under `state.json` (atomic temp + rename).
   **Phase 100 wrote a full snapshot of all six knobs; Phase 102 switches to a
   sparse overlay (only altered keys).**
4. DELETE / empty Alter value restores **product** default **and** updates the
   durable file (so restart does not re-apply a prior altered value).
   **Phase 102 additionally removes the key from the sparse file so env can
   re-apply on restart.**
5. Tests (`phase100_*.rs`) + living docs honesty.

## Non-goals

- Full Kafka `server.properties` / DynamicBrokerConfig catalog
- Multi-broker config broadcast / KRaft metadata quorum
- BROKER_LOGGER / CLIENT_METRICS / GROUP resources
- Multi-lang clients / fuzz CI
- Marker GC
- Spawning a sweeper task when `volant.sweep.interval.ms` transitions
  `0 → >0` without process restart → **closed by Phase 101**

## Path & format

| Item | Value |
|------|-------|
| Directory | `{data_dir}/__broker_config/` |
| File | `state.json` |
| Write | `state.json.tmp` → fsync → rename → `state.json` |
| Schema | versioned JSON snapshot of all six wire keys |

Example:

```json
{
  "version": 1,
  "configs": {
    "transaction.max.timeout.ms": 111000,
    "volant.open.transaction.timeout.ms": 60000,
    "volant.prepared.transaction.timeout.ms": 60000,
    "volant.fetch.session.idle.ms": 60000,
    "volant.fetch.session.max": 1000,
    "volant.sweep.interval.ms": 50
  }
}
```

Unknown keys in the file are ignored on load. Missing file = no durable overlay.
Corrupt / unreadable file → log and leave env/product values (or surface storage
error at construction — MVP: `expect`-style failure is acceptable if load returns
`Err`, matching other stores).

## Precedence (load order)

| Layer | When | Source |
|-------|------|--------|
| 1. Product default | Always baseline | Table in [PHASE99_SPEC.md](./PHASE99_SPEC.md) |
| 2. Env at construction | `Broker::new` / `with_cluster` | `VOLANT_*` env vars (same as Phase 92–97) |
| 3. Durable file | After atomics / fetch-session manager constructed | `{data_dir}/__broker_config/state.json` |
| 4. Runtime alter | After process start | AlterConfigs / IncrementalAlterConfigs / `Broker` setters |

**Effective restart value** for each key:

```
product_default → env_override → durable_file (if present) → runtime_alter
```

Notes:

- Env is only applied at construction (same as Phase 99). Changing env after
  start does not affect a running process.
- **Phase 100 (original):** once any successful Alter wrote the file, the file
  held a **full snapshot** of all six live values, overriding env for every key.
  **Closed by Phase 102:** sparse overlay — only keys present in `configs`
  (explicitly altered) override env; untouched keys keep product→env.
- DELETE restores the **product** default into the live knob (not env).
  **Phase 100** rewrote product default into the full snapshot; **Phase 102**
  removes the key from the sparse file so env can re-apply on restart.
- Direct `Broker::set_*` setters remain process-local only (no auto-persist).
  Kafka Alter paths call `alter_broker_configs`, which persists.

## Write path

| API | Persist? |
|-----|----------|
| AlterConfigs BROKER (non-validate_only, success) | Yes — Phase 100 full snapshot; **Phase 102 sparse merge** |
| IncrementalAlterConfigs BROKER SET/DELETE (success) | Yes (same) |
| validate_only | No |
| Unknown key / InvalidConfig | No mutation, no write |
| TOPIC Alter | Unchanged (topic store) |
| `Broker::set_*` | No (process-local; tests / internal) |

Persistence failure after in-memory apply: return storage error to the caller
(Admin API maps non-`InvalidArgument` to `UNKNOWN`). Prefer apply-then-persist
in one `alter_broker_configs` so validate already passed.

## Read path

Unchanged DescribeConfigs BROKER: values from live getters (which already
reflect durable load + alters).

## Authorization

Unchanged from Phase 99 (Cluster Describe / Alter).

## Exit criteria

1. Alter a BROKER knob → drop `Broker` → reopen same `data_dir` → DescribeConfigs
   shows the altered value
2. DELETE / empty Alter → product default live → restart → still product default
   (not a prior altered value)
3. TOPIC configs still work (regression)
4. `validate_only` does not write the durable file
5. `phase100_*` + prior config phases green
6. Docs: PHASE100_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Six knobs only (not full Kafka broker catalog)
- Single-node; resource name still ignored → **closed by Phase 103** (local `node_id` check)
- Full snapshot overrides env for all keys once any Alter has written the file
  → **closed by Phase 102** (sparse overlay)
- Direct setters do not persist (by design for this MVP)
- Sweeper task spawn at boot with interval `0` → **closed by Phase 101**
  (always spawn; `0` pauses; `0→>0` live)
- No multi-broker fan-out

## Test plan

`crates/volant-broker/tests/phase100_broker_config_durable.rs`:

1. AlterConfigs SET one knob → drop broker + server → new Broker same dir →
   Describe shows altered value
2. IncrementalAlter SET several knobs → restart → all restored
3. DELETE then restart → product default (not prior alter)
4. validate_only does not create / change durable file
5. TOPIC Describe/Alter still works after durable broker path

## Phase 101 ideas

- Graceful sweeper enable when interval transitions `0 → >0` without process restart → **closed by Phase 101**
- Validate BROKER resource name against `node_id` → **closed by Phase 103**
- Sparse durable file (only keys differing from product default) so env re-applies after DELETE → **closed by Phase 102**
- Marker compaction / GC with DeleteRecords
- Multi-broker config broadcast
- Multi-lang clients / cargo-fuzz corpus CI
