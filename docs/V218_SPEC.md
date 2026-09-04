# v0.218 — CompletingRebalance group state while SyncGroup fence is open

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** When a group has live members but `!all_synced` (the
[v0.215](./V215_SPEC.md) SyncGroup fence), ListGroups / DescribeGroup
report **CompletingRebalance**, not Stable.

This is a **label**, not parked Join. Join still eager-assigns.
SyncGroup still confirms generation. No new opcodes. No Kafka API
keys. `SUPPORTED_APIS` stays 38.

## Goals

1. `GroupState`: `Empty = 0`, `Stable = 1`, `CompletingRebalance = 2`.
   `from_u8`: `2` → CompletingRebalance; unknown → Empty (keep).
2. Native ListGroups already sends a state byte. Emit **2** when
   live && `!all_synced`.
3. Kafka `encode_list_groups` / DescribeGroups state **string** is
   `"CompletingRebalance"` in that case (not `"Stable"`).
4. Language clients (Python / Go / Java) + Rust client decode 2 as
   CompletingRebalance / equivalent enum. Empty / Stable unchanged.

## Non-goals

| Deferred | Why |
|----------|-----|
| Parked Join / Join-retry-on-9 | Out of slice; Join still returns 9 immediately |
| PreparingRebalance | Not a coordinator rewrite |
| New opcode / Kafka API key | 36/37 and keys 15/16 already shipped |
| Flip openraft / grow homemade Raft | Other 155 PRs |
| Crate 0.3.0 | After 155 ships, not during |

## Semantics

```
listed_state(g) =
    Empty                  if no live members
    CompletingRebalance    if live && !all_synced
    Stable                 if live && all_synced
```

| Moment | ListGroups / DescribeGroup |
|--------|----------------------------|
| After first Join, before SyncGroup | CompletingRebalance (2) |
| After SyncGroup | Stable |
| Offset-only / no live members | Empty |

Join, SyncGroup, Heartbeat, Leave, and OffsetCommit behavior are
unchanged from v0.215.

## Wire

Native ListGroups entry: `u8` state after `group_id`.

```
0 Empty
1 Stable
2 CompletingRebalance
other → Empty
```

Kafka ListGroups v4+ `GroupState` and DescribeGroups `state` strings
use the same three names.

## Tests

```bash
cargo test -p volant-protocol -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
cd clients/go && go test ./...
cd clients/java && mvn -q test
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_groups_admin -q
```

| Case | Expect |
|------|--------|
| After first Join, before SyncGroup | CompletingRebalance (2) |
| After SyncGroup | Stable |
| Empty / offset-only group | Empty |
| `SUPPORTED_APIS.len()` | **38** |
| `from_u8(0/1/2/unknown)` | Empty / Stable / CompletingRebalance / Empty |

## Honesty leftovers

- Label only. Join is not parked; a fenced Join still returns 9.
- Dual-consume window until heartbeat **9** remains.
- Leader assignment bytes are still ignored.
- PreparingRebalance is not a state.

## Merge notes

v0.219 also edits `group.rs` `commit_offsets`. This hunk is
list/describe state. Keep both.

## Related

- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation fence
- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup peek
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
