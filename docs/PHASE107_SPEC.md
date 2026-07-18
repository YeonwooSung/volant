# Phase 107 — Stabilize phase103 parallel test flake (MVP)

## Goals

1. Stop intermittent `phase103_broker_name` failures under default
   `cargo test` parallelism (multi-thread, multi-test).
2. Prefer root-cause isolation over serializing the binary.
3. Keep product behavior of Phase 103 (BROKER name validation) unchanged.
4. Living docs honesty; multi-run evidence under parallel threads.

## Non-goals

- Drain native/Kafka/metrics accept loops on shutdown
- Single-flight / idempotent `start_background_tasks` (still deferred; cheap
  follow-up, not required for this flake)
- Straddle marker clip / multi-broker 2PC / sessions / multi-lang / fuzz CI
- Full rewrite of every unit-test `temp_dir` helper outside integration common

## Problem

Under default cargo test threads, `phase103_broker_name` flaked as:

| Symptom | Observed |
|---------|----------|
| AlterConfigs asserts `0`, gets **`-1`** (`Unknown`) | `regression_alter_known_knob`, `alter_name_matching_and_empty_succeed`, matching-name incremental |
| `create_topic` / catalog save | **ENOENT** on `catalog.json.tmp` open or topic config rename |

Serial `--test-threads=1` was green. Full workspace CI aborted early on the flake.

## Root cause

**Not** AlterConfigs framing/parse bugs. Integration helper
`tests/common/mod.rs::temp_dir` built paths as:

```text
volant-{prefix}-{label}-{pid}-{nanos}
```

All phase103 cases used the same prefix/label (`p103` / `name`). On macOS,
`SystemTime::now().as_nanos()` collides heavily for back-to-back calls (coarse
clock — thousands of collisions per 10k samples). Parallel tests then shared one
`data_dir`; one case's teardown `remove_dir_all` (or the next `temp_dir`'s
pre-create wipe) deleted a live peer's `__topics` / `__topic_configs` /
`__broker_config` mid-flight:

- Topic catalog/config open/rename → **ENOENT**
- `alter_broker_configs` persist failure → Kafka **`Unknown` (-1)**

## Fix

1. **`temp_dir` uniqueness (primary):** process-wide `AtomicU64` sequence +
   sanitized thread id in the path; **stop** `remove_dir_all` on create (unique
   paths must not clobber peers).
2. **phase103 labels:** each test case passes a distinct `setup(label)`.
3. **Defensive parent recreate:** `TopicConfigStore::save` and
   `TopicCatalogStore::save` call `create_dir_all` before tmp write (same
   pattern as `BrokerConfigStore::save`).

No `serial_test` / forced single-thread for this binary.

## Tests / evidence

- `cargo test -p volant-broker --test phase103_broker_name` (default threads)
  **12 consecutive green runs** after the fix (was failing within 1–2 runs
  before).
- Product Phase 103 assertions unchanged (name match / empty / wrong /
  TOPIC path / known knob alter).

## Files

| Path | Change |
|------|--------|
| `crates/volant-broker/tests/common/mod.rs` | Unique `temp_dir` (seq + tid); no create-time wipe |
| `crates/volant-broker/tests/phase103_broker_name.rs` | Per-case setup labels |
| `crates/volant-broker/src/topic_config.rs` | `create_dir_all` before save |
| `crates/volant-broker/src/topic_catalog.rs` | `create_dir_all` before save |

## Still deferred

- Accept-loop drain (native / Kafka / metrics)
- Duplicate `start_background_tasks` single-flight guard
- Straddle marker clip
- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Multi-broker BROKER config fan-out
