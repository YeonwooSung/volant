# v0.222 — delete homemade metadata Raft hatch

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Delete homemade Phase 154 as a product hatch. Keep protocol
opcodes **98/99** encode/decode so a mixed-cluster old peer cannot
panic this broker.

This is residual **v0.222**. It is **not** a Kafka metadata Raft, **not**
an openraft grow, and **not** a crate 0.3.0. Do **not** add Kafka API
keys. Do **not** grow openraft.

## Goals

1. Delete `cluster/metadata_raft.rs` and all broker / fanout / metrics
   product for homemade 154.
2. Inbound opcode **98** always returns
   `Err(Error::Protocol("metadata raft not enabled"))` (the existing
   disabled arm). No apply. No `{data_dir}/__metadata_raft/` dir.
3. `maybe_fanout_assignment_consensus`: openraft → else Phase 150 notes.
   No homemade 154 branch.
4. `assignment_must_wait()`: drop the 154 wait-commit arm.
5. `VOLANT_METADATA_RAFT` / `VOLANT_METADATA_RAFT_WAIT_COMMIT`: if set
   **on**, warn once and ignore. Do not fail boot. Do not honor `=1`.
6. Protocol crate **unchanged** (`phase154_metadata_raft_append_roundtrip`
   stays).

## Non-goals

| Deferred | Why |
|----------|-----|
| Drop opcodes 98/99 from the protocol crate | Mixed-cluster decode must not panic |
| Grow openraft / RequestVote / InstallSnapshot | Frozen; other leftovers |
| Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Delete leftover `__metadata_raft/` files | Unread; do not migrate or wipe |
| Crate 0.3.0 | After 155 leftovers, not during |

## Semantics

```
CreateTopic / DeleteTopic / CreatePartitions
  │
  ├─ openraft on  → client_write SetAssignment (108/109)
  └─ else         → Phase 150 AssignmentConsensusNote (96/97)

Inbound MetadataRaftAppend (98)
  └─ always Error::Protocol("metadata raft not enabled")
```

`VOLANT_METADATA_RAFT=1` and `VOLANT_METADATA_RAFT_WAIT_COMMIT=1` log
once at `Broker::new` / `Broker::with_cluster` and do nothing else.

## Tests

```bash
cargo test -p volant-broker --lib -- --test-threads=1
cargo test -p volant-broker --test v02_create_topic_ungate -- --test-threads=1
cargo test -p volant-protocol -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default `Broker::new` / cluster | no `__metadata_raft` dir |
| Inbound 98 | protocol reject; SetAssignment **not** applied; no dir |
| `phase154_metadata_raft_append_roundtrip` | still encodes / decodes 98/99 |

| File | What |
|------|------|
| `cluster/metadata_raft.rs` | Deleted |
| `cluster/mod.rs`, `lib.rs`, `broker/*`, `net/*` | 154 product gone |
| `tests/phase154_metadata_raft.rs` | Deleted |
| `tests/v40_raft_wait_commit.rs` | Deleted |
| `tests/v213_isr_update_skips_154.rs` | Deleted |
| `tests/v02_create_topic_ungate.rs` | no-dir + inbound 98 |
| `docs/V222_SPEC.md` | This spec |
| `docs/ops.md`, `docs/consistency.md` | Honesty |

## Honesty leftovers

- Leftover `{data_dir}/__metadata_raft/` files from a prior enabled run
  are **unread**.
- Opcodes 98/99 still decode in the protocol crate. This broker always
  rejects inbound 98 with the disabled protocol error.
- Openraft is unchanged. No Kafka keys.

## Merge notes

Keep this hunk local to deleting homemade 154 product. Do **not** edit
historical `PHASE154_SPEC` / `V40` / `V214`. Do **not** edit
`TODO.md`, `ROADMAP.md`, root `README.md`, `docs/INDEX.md`,
`docs/history/PHASE_HISTORY.md`, `docs/PHASE155_SPEC.md`,
`docs/V02_FREEZE.md`.

## Related

- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade metadata Raft (history)
- [V40_SPEC.md](./V40_SPEC.md) — wait-commit on homemade 154 (history)
- [V214_SPEC.md](./V214_SPEC.md) — inbound 98 gate (history)
- [V02_FREEZE.md](./V02_FREEZE.md) — 154 default off; stop extending
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — openraft cluster SoT
