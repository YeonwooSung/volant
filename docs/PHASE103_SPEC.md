# Phase 103 — Validate BROKER resource name against `node_id` (MVP)

## Goals

1. For Kafka **BROKER** config resources (type 4), accept the resource `name`
   only when it is:
   - the empty string, **or**
   - equal to this process's `Broker::node_id()` as a **decimal string**
     (single-node default: `"0"`).
2. Reject other non-empty names with **`INVALID_REQUEST` (42)** and a clear
   error message on **DescribeConfigs**, **AlterConfigs**, and
   **IncrementalAlterConfigs**.
3. Keep **TOPIC** Describe/Alter/Incremental paths unchanged.
4. Local validation only — **no** multi-broker fan-out or remote proxy.
5. Tests (`phase103_*.rs`) + living docs honesty.

## Non-goals

- Multi-broker config broadcast / proxy to other node ids
- Full Kafka DynamicBrokerConfig / KRaft catalog
- Marker GC / DeleteRecords
- Empty AddPartitions control markers
- Graceful sweeper join on stop
- Multi-lang clients / fuzz CI / multi-broker 2PC

## Problem (Phase 99–102 honesty gap)

Phases 99–102 accepted **any** string as the BROKER resource name. Kafka
clients typically send the broker id as the resource name; multi-broker
honesty requires rejecting wrong ids on this process.

## Design

### Acceptance rule

| Resource name | Result |
|---------------|--------|
| `""` (empty) | Accept (cluster-default style / clients that omit id) |
| `"0"` when `node_id == 0` | Accept (exact decimal match) |
| `"N"` when `node_id == N` | Accept |
| `"1"` when `node_id == 0` | **`INVALID_REQUEST`** |
| `"00"`, `" 0"`, `"broker-0"` | **`INVALID_REQUEST`** (not exact decimal) |

Match is **exact string equality** with `node_id.to_string()` — not integer
parse (so leading zeros / whitespace fail).

### Error

| Field | Value |
|-------|-------|
| Error code | **`INVALID_REQUEST` = 42** |
| Message | `BROKER resource name must be empty or "<node_id>" (this broker's node_id)` |
| Configs (Describe) | Empty array |

Name validation runs **before** ACL checks so a wrong id is not reported as
authorization failure.

### Surfaces

| API | Path |
|-----|------|
| DescribeConfigs | `encode_describe_configs` BROKER branch |
| AlterConfigs | `encode_alter_configs` `RES_BROKER` arm |
| IncrementalAlterConfigs | `encode_incremental_alter_configs` `RES_BROKER` arm |

Helper: `broker_resource_name_matches(node_id, name)` in
`crates/volant-broker/src/kafka/admin_api.rs`.

## Exit criteria

1. Describe/Alter/Incremental with name = `node_id` decimal → success
2. Describe/Alter/Incremental with empty name → success
3. Wrong non-empty name → `INVALID_REQUEST`; no mutation on Alter
4. TOPIC resources still work
5. Regression: Alter of a known Phase 99 knob with `"0"` still works
6. `phase103_*` + phase99 + phase100 + phase102 green
7. Docs: PHASE103_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Single process validation only (no fan-out to other brokers)
- Empty name accepted for client convenience (not Kafka multi-broker strictness)
- Six knobs only; sparse durable overlay unchanged (Phase 102)
- No graceful sweeper join / multi-broker 2PC
- Marker compaction/GC with DeleteRecords → **closed by Phase 104**

## Test plan

`crates/volant-broker/tests/phase103_broker_name.rs`:

1. Describe with matching `node_id` → success (6 keys)
2. Describe with empty name → success
3. Describe with wrong names → InvalidRequest
4. Alter matching + empty → success; wrong Alter/Incremental → InvalidRequest
5. TOPIC Describe/Alter regression
6. Smoke: Alter known knob `"0"` still applies

## Phase 104 ideas

- Marker compaction / GC with DeleteRecords → **shipped as Phase 104**
- Graceful sweeper shutdown / join on server stop
- Empty-AddPartitions control markers
- Multi-broker config broadcast / multi-broker 2PC
- Multi-lang clients / cargo-fuzz corpus CI
