# Phase 99 — DescribeConfigs (broker) for txn / session / sweep knobs (MVP)

## Goals

1. Expose **BROKER** resource configs via Kafka **DescribeConfigs** for the
   process-local timeout and sweep knobs introduced in Phases 92–97.
2. Prefer Kafka standard names where they map cleanly; use `volant.*` for
   Volant-specific knobs with no Kafka equivalent.
3. Support **AlterConfigs** and **IncrementalAlterConfigs** SET/DELETE on the
   same keys via existing `Broker` setters (runtime, non-durable).
4. Keep **TOPIC** Describe/Alter/Incremental paths unchanged.
5. Tests (`phase99_*.rs`) + living docs honesty.

## Non-goals

- Full Kafka DynamicBrokerConfig / KRaft metadata quorum / durable
  `server.properties` rewrite
- BROKER_LOGGER / CLIENT_METRICS / GROUP config resources
- Multi-broker per-node config fan-out
- Synonym chains (broker defaults → topic overrides)
- Multi-lang clients / fuzz CI / multi-broker 2PC / marker GC

## Config name table

| Wire config name | Kafka standard? | Broker API | Product default | Env (startup) | Notes |
|------------------|-----------------|------------|-----------------|---------------|-------|
| `transaction.max.timeout.ms` | **Yes** (Kafka broker) | `transaction_max_timeout_ms` / `set_transaction_max_timeout_ms` | **900_000** | `VOLANT_TRANSACTION_MAX_TIMEOUT_MS` | Phase 96; `0` = no max |
| `volant.open.transaction.timeout.ms` | No | `open_txn_timeout_ms` / `set_open_txn_timeout_ms` | **60_000** | `VOLANT_OPEN_TXN_TIMEOUT_MS` | Phase 93 broker default when client timeout ≤ 0; `0` disables |
| `volant.prepared.transaction.timeout.ms` | No | `prepared_txn_timeout_ms` / `set_prepared_txn_timeout_ms` | **60_000** | `VOLANT_PREPARED_TXN_TIMEOUT_MS` | Phase 92; `0` disables |
| `volant.fetch.session.idle.ms` | No | `fetch_session_idle_ms` / `set_fetch_session_idle_ms` | **60_000** | `VOLANT_FETCH_SESSION_IDLE_MS` | Phase 95; `0` disables idle eviction |
| `volant.fetch.session.max` | No | `fetch_session_max` / `set_fetch_session_max` | **1000** | `VOLANT_FETCH_SESSION_MAX` | Phase 95; `0` = unlimited |
| `volant.sweep.interval.ms` | No | `sweep_interval_ms` / `set_sweep_interval_ms` | **1000** | `VOLANT_SWEEP_INTERVAL_MS` | Phase 97; `0` disables background sweeper |

**Why `volant.*` for most keys:** Apache Kafka does not expose open/prepared
timeouts, fetch-session idle/max, or Volant's sweeper interval as first-class
`DescribeConfigs` broker keys the same way. Only `transaction.max.timeout.ms`
has a clean Kafka broker name.

## Resource model

| Field | Value |
|-------|-------|
| Kafka `ConfigResource.Type` | **BROKER = 4** |
| Resource name | Any string accepted (single-node MVP; typically broker id as decimal). Name is **not** validated against `node_id`. |
| ConfigSource (v1+) | **DYNAMIC_BROKER_CONFIG = 2** (runtime-mutable process knobs) |
| ConfigType (v3+) | **LONG = 5** for all six keys |
| is_default (v0) | `1` when current value equals **product** default (table above); env-at-startup does not change product default for this flag |
| Synonyms | Empty (honest) |
| Documentation (v3+ when requested) | Short static strings per key |

Unsupported resource types (e.g. BROKER_LOGGER=8) still return
`INVALID_REQUEST` with message noting only TOPIC and BROKER are supported.

## Read path (DescribeConfigs)

1. Parse resources as today (classic 0–3 + flexible 4).
2. For each resource:
   - `type=TOPIC (2)` → existing topic path
   - `type=BROKER (4)` → Cluster **Describe** ACL (when enabled); emit the six
     keys (or filter to requested `configuration_keys`); values from live
     getters as decimal strings
   - other → `INVALID_REQUEST`
3. ACL deny on broker resource → `CLUSTER_AUTHORIZATION_FAILED` (31).

## Write path (AlterConfigs + IncrementalAlterConfigs)

Preferred MVP includes write (SET/DELETE already exist for topics).

| API | Behavior |
|-----|----------|
| **AlterConfigs** | For each `(name, value)` on a BROKER resource: SET if non-empty parseable integer; empty value = DELETE (restore product default). Unknown key → `INVALID_CONFIG`. |
| **IncrementalAlterConfigs** | **SET (0)** = set value; **DELETE (1)** = restore product default; **APPEND/SUBTRACT** → `INVALID_CONFIG` (no list-typed broker knobs). |
| `validate_only` | Parse/validate only; no mutation. |
| ACL | Cluster **Alter** when ACLs enabled; deny → `CLUSTER_AUTHORIZATION_FAILED`. |

**Persistence (Phase 99):** process-local only via setters. **Phase 100** adds
durable `{data_dir}/__broker_config/state.json` on successful Alter /
IncrementalAlter (see [PHASE100_SPEC.md](./PHASE100_SPEC.md)).

**Background sweeper note:** changing `volant.sweep.interval.ms` updates the
Atomic read by the running loop; setting `0` stops further sweeps on the next
iteration. If the process started with `0` and never spawned the task,
Describe reflects `0` but no task is created until server restart / re-entry
via `start_background_tasks` (existing Phase 97 honesty).

## Authorization

| Op | Resource | ACL |
|----|----------|-----|
| DescribeConfigs BROKER | Cluster / `volant` | Describe |
| Alter / IncrementalAlter BROKER | Cluster / `volant` | Alter |
| TOPIC paths | Topic / name | Describe / Alter (unchanged) |

## Exit criteria

1. DescribeConfigs BROKER returns all six keys at product (or env/setter) values
2. After `Broker` setter or Alter/Incremental SET, Describe reflects the change
3. Incremental DELETE restores product default; APPEND/SUBTRACT rejected
4. Unknown broker key → InvalidConfig; unsupported resource type → InvalidRequest
5. TOPIC configs still work (regression)
6. `phase99_*` + prior config phases green
7. Docs: PHASE99_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Single-node; resource name ignored
- Non-durable dynamic broker config → **closed by Phase 100** (six knobs only)
- Six knobs only (not full Kafka broker config catalog)
- Empty synonyms; no synonym layering
- Sweeper task spawn still tied to `start_background_tasks` initial interval
- No BROKER_LOGGER / CLIENT_METRICS

## Test plan

`crates/volant-broker/tests/phase99_broker_configs.rs`:

1. DescribeConfigs BROKER sees product defaults (or live getter values)
2. Setter then Describe reflects change for each major knob class
3. IncrementalAlter SET + Describe; DELETE restores product default
4. AlterConfigs SET works; unknown key InvalidConfig
5. TOPIC Describe/Alter still works in same process
6. Optional: unsupported resource type still InvalidRequest

## Phase 100 ideas

- Durable dynamic broker config file + restart restore → **closed by Phase 100**
- Validate broker resource name against `node_id`
- Graceful sweeper restart when interval transitions 0 → >0 without process restart
- Marker compaction / GC with DeleteRecords
- Multi-broker config broadcast
- Multi-lang clients / cargo-fuzz corpus CI
