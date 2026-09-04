# v0.248 — apply SyncGroup assignment when it decodes

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** SyncGroup currently **ignores** leader assignment bytes and
returns the coordinator Join assignment. If the bytes **decode** as a
known assignment, **apply** them.

This is residual **v0.248**. It is **not** a full Kafka leader-assignor
protocol (no PreparingRebalance join-set). Empty / unparseable bytes
keep today's peek. Do **not** fail SyncGroup because bytes did not
decode. Do **not** add Kafka keys. Do **not** touch DescribeQuorum,
AllocateProducerIds, ACLs, or AlterReplicaLogDirs.

## Goals

1. `GroupCoordinator::sync_group_with_assignments(...)` applies
   decoded `(member_id, assignment)` rows for members that exist, then
   confirms generation as today. Existing `sync_group` stays
   confirm-only (empty apply).
2. Native opcode **116/117**: if `assignment_bytes` decode as a native
   `Assignment` list (`u32 LE count` + `{topic, partition}`), set
   **this member’s** assignment.
3. Kafka key **14**: parse the assignments array (`member_id` → bytes).
   For each member that exists, if bytes decode via
   `decode_consumer_assignment` or the native list, apply that
   member's assignment. Then confirm generation as today.
4. Unparseable / empty bytes → keep today's peek (Join assignment).
   Error **0**. Do **not** fail SyncGroup.

## Non-goals

| Deferred | Why |
|----------|-----|
| PreparingRebalance join-set / wait for all joins | Not a full leader-assignor |
| Fail SyncGroup on garbage bytes | Honesty: peek Join assignment |
| New Kafka API keys | Frozen |
| DescribeQuorum / AllocateProducerIds / ACLs / AlterReplicaLogDirs | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
SyncGroup (native 116 / Kafka 14)
  │
  ├─ unknown member / wrong gen → 10 / 9 (unchanged)
  ├─ empty / unparseable assignment bytes
  │     → confirm generation; return Join assignment
  ├─ native bytes decode as Assignment list
  │     → set this member's assignment; confirm; return it
  └─ Kafka assignments[] member_id → bytes
        for each existing member whose bytes decode
          (consumer protocol or native list)
            → set that member's assignment
        then confirm this caller; return this member's assignment
```

Unknown member ids in the Kafka array are skipped. Heartbeat still
does not confirm.

## Tests

```bash
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-broker --test v248_sync_group_apply -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Sync with empty bytes | same as Join assignment; error **0** |
| Native Sync with explicit assignment bytes | member owns those partitions |
| Kafka SyncGroup leader payload (one member consumer assignment) | that member's assignment updates |
| Garbage bytes | error **0**; Join assignment unchanged |

| File | What |
|------|------|
| `crates/volant-broker/src/group.rs` | `sync_group_with_assignments` + native decode |
| `crates/volant-broker/src/net/dispatch.rs` | native 116 apply |
| `crates/volant-broker/src/kafka/group_api.rs` | key 14 apply |
| `crates/volant-broker/tests/v248_sync_group_apply.rs` | native + Kafka IT |
| `docs/V248_SPEC.md` | This spec |
| `docs/KAFKA_COMPAT.md` | SyncGroup row honesty |

## Honesty leftovers

- Not CompletingRebalance / PreparingRebalance join-set wait.
- Empty / garbage still peeks Join assignment.
- Clients that send empty assignment bytes are unchanged.
- GroupConsumer default path still uses JoinGroup assignment unless
  the caller supplies decodable SyncGroup bytes.

## Merge notes

Keep this hunk local to SyncGroup apply. Do **not** edit living docs
(`TODO.md`, `ROADMAP.md`, root `README.md`, `docs/INDEX.md`,
`docs/history/PHASE_HISTORY.md`, `docs/ops.md`, `docs/consistency.md`)
except the one-line `docs/KAFKA_COMPAT.md` SyncGroup row.

## Related

- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup 116/117 peek
- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation confirm fence
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka SyncGroup honesty
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — shim matrix
