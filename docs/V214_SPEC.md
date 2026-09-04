# v0.214 — gate inbound homemade 154

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Tighten homemade 154 without deleting it. Default-off brokers
must not create `{data_dir}/__metadata_raft/`, and inbound
`MetadataRaftAppend` (opcode **98**) must not apply `SetAssignment`
unless `VOLANT_METADATA_RAFT` is on.

This is residual **v0.214**. It does **not** delete homemade 154, drop
opcodes 98/99, add Kafka API keys, or flip the openraft default.

## Goals

1. Do **not** `create_dir_all({data_dir}/__metadata_raft/)` unless
   homemade 154 is enabled (`VOLANT_METADATA_RAFT`).
2. Reject inbound opcode **98** unless 154 is enabled. Use the existing
   disabled-feature style (`Error::Protocol` → `Response::Error`, same
   family as openraft-not-started). Do **not** invent a new opcode.
3. Leave opcodes 98/99 in the protocol crate. Leave
   `metadata_raft.rs` and phase154 / v40 tests.

## Construction

| Path | Dir |
|------|-----|
| `MetadataRaftState::open` | Lazy: load files if present; **no** `create_dir_all` |
| `MetadataRaftState::open_enabled` | Creates `__metadata_raft/` (env on at boot) |
| Persist (`log.json` / `hard_state.json`) | Creates the parent dir on first write |

`Broker::new` / `Broker::with_cluster` call `open_enabled` only when
`default_metadata_raft_enabled` is true. Default remains **off**.

## Inbound 98

When `metadata_raft_enabled()` is false:

- `dispatch.rs` returns `Error::Protocol("metadata raft not enabled")`
  (maps to `Response::Error` / `ErrorCode::Protocol`).
- `handle_metadata_raft_append` returns `success=false` and does **not**
  persist or apply `SetAssignment`.

When the flag is on, today's AppendEntries + apply path is unchanged.
phase154 tests still `set_metadata_raft_enabled(true)` and set
`VOLANT_OPENRAFT_METADATA=0`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Delete homemade 154 / opcodes 98/99 | Tighten only; protocol stays |
| RequestVote / InstallSnapshot on 154 | Frozen (v0.2 / Phase 155) |
| Flip `VOLANT_OPENRAFT_METADATA` default | Stays on in cluster |
| Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Crate 0.3.0 | After 155 ships, not during |

## Tests

```bash
cargo test -p volant-broker --test phase154_metadata_raft --test v40_raft_wait_commit --test v02_create_topic_ungate -- --test-threads=1
cargo test -p volant-broker --lib metadata_raft -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Default-off `Broker::new` / cluster | no `__metadata_raft` dir |
| Inbound 98 while 154 off | protocol reject; SetAssignment **not** applied; no dir |
| `open` | does not create the dir |
| `open_enabled` | creates the dir |
| phase154 + v40 | still pass (`set_metadata_raft_enabled(true)` + OPENRAFT=0) |

## Honesty leftovers

- Homemade 154 still has no RequestVote / InstallSnapshot / election.
- Persist on first write still creates the dir when 154 is later enabled.
- Existing `{data_dir}/__metadata_raft/` from a prior enabled run is
  still loaded (lazy open does not delete).
- Openraft default stays on. No Kafka keys.

## Merge notes

v0.213 edits IsrUpdate outbound in `dispatch.rs`. Keep the inbound 98
gate as a **separate hunk**. Keep both.

## Related

- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade metadata Raft
- [V40_SPEC.md](./V40_SPEC.md) — wait-commit on homemade 154
- [V02_FREEZE.md](./V02_FREEZE.md) — 154 default off; stop extending
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — openraft cluster SoT
