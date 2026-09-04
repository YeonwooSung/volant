# v0.243 — warn once on leftover homemade 154 dir

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** v0.222 deleted homemade 154. Leftover
`{data_dir}/__metadata_raft/` files are **unread**. Warn **once** at
`Broker::new` / `Broker::with_cluster` if that directory exists. Do
**not** read, migrate, or delete it.

This is residual **v0.243**. It is **not** a Kafka metadata Raft and
**not** an openraft grow. Do **not** add Kafka API keys. Do **not**
honor `VOLANT_METADATA_RAFT` (already warn-once ignore). Do **not**
touch ListOffsets, quotas, UnregisterBroker, UpdateFeatures, or
`group.rs`.

## Goals

1. If `{data_dir}/__metadata_raft` exists as a directory (or the known
   homemade 154 files `log.json` / `hard_state.json` are present), log
   a **warn once** (same `Once` style as `VOLANT_METADATA_RAFT`).
2. Message: leftover homemade 154 dir is unread; safe to delete;
   openraft uses `__openraft` if enabled.
3. Missing dir: silent. Do not create the dir.
4. Do **not** read file contents, migrate, or delete the leftover dir.

## Non-goals

| Deferred | Why |
|----------|-----|
| Read / migrate leftover 154 files | Homemade 154 stays gone |
| Delete leftover `__metadata_raft/` | Operator deletes when ready |
| Honor `VOLANT_METADATA_RAFT=1` | Already warn-once ignore (v0.222) |
| Kafka API keys | Frozen |
| ListOffsets / quotas / UnregisterBroker / UpdateFeatures / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
Broker::new / Broker::with_cluster
  │
  ├─ {data_dir}/__metadata_raft is a dir
  │     or log.json / hard_state.json exists
  │       → warn once; continue boot (unread)
  └─ missing
        → silent; do not create
```

Warn is process-lifetime (`std::sync::Once`). A second construct in
the same process does not warn again and still boots.

## Tests

```bash
cargo test -p volant-broker --lib -- --test-threads=1
cargo test -p volant-broker --test v243_metadata_raft_leftover -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Empty leftover dir present | `Broker::new` ok; dir unused (no new files); warn once |
| Leftover absent | `Broker::new` ok; dir not created |
| Second construct same process | ok; no second warn |
| Garbage `log.json` / `hard_state.json` | unread; bytes unchanged |

| File | What |
|------|------|
| `crates/volant-broker/src/broker/mod.rs` | warn-once at `new` / `with_cluster` |
| `crates/volant-broker/tests/v243_metadata_raft_leftover.rs` | present / absent / unread |
| `docs/V243_SPEC.md` | This spec |

## Honesty leftovers

- Leftover `{data_dir}/__metadata_raft/` is still on disk until an
  operator deletes it.
- `VOLANT_METADATA_RAFT=1` still only warns and is ignored (v0.222).
- Openraft is unchanged (`__openraft` only when that flag is on).
- Opcodes 98/99 still decode; inbound 98 still rejects.
- No Kafka keys.

## Merge notes

Keep this hunk local to the leftover-dir warn. Do **not** edit living
docs (`TODO.md`, `ROADMAP.md`, root `README.md`, `docs/INDEX.md`,
`docs/history/PHASE_HISTORY.md`, `docs/ops.md`, `docs/consistency.md`).

## Related

- [V222_SPEC.md](./V222_SPEC.md) — delete homemade 154 product
- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade metadata Raft (history)
- [V214_SPEC.md](./V214_SPEC.md) — inbound 98 gate (history)
- [V02_FREEZE.md](./V02_FREEZE.md) — 154 default off; stop extending
