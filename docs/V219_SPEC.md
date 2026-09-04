# v0.219 — OffsetCommit fenced until SyncGroup confirms generation

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** OffsetCommit with a non-empty `member_id` and matching
generation returns error **9** (`RebalanceInProgress`) until that
member has SyncGroup-confirmed the generation
(`synced_generation == generation`). Admin path is unchanged.

This shrinks the dual-consume hole: a member cannot commit offsets
for a generation they have not confirmed. This is **not** Kafka
CompletingRebalance. No new opcodes. No Kafka API keys.
`SUPPORTED_APIS` stays 38.

`Member.synced_generation` and the Join fence already exist
([V215_SPEC.md](./V215_SPEC.md)). This residual only changes the
member OffsetCommit path.

## Goals

1. After existing generation / member checks, if `member_id` is
   non-empty and `synced_generation != generation` → error **9**,
   offset not stored.
2. After SyncGroup, the same commit succeeds (error **0**).
3. `generation == 0` (admin / CLI) still skips membership checks,
   including the sync fence.
4. Empty `member_id` + nonzero generation: today's generation /
   member checks only. Do **not** add the sync fence.
5. Wrong generation is still **11**. Unknown member is still **10**.
   Those checks run before the fence.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CompletingRebalance / PreparingRebalance | Empty/Stable only |
| Fence empty-`member_id` / gen-0 admin | Admin / CLI path stays open |
| New opcode / Kafka API key | OffsetCommit already shipped |
| `GroupState` change | ListGroups still Empty/Stable (v0.218) |
| Flip openraft / grow homemade Raft | Other 155 PRs |

## Semantics

```
member OffsetCommit (member_id non-empty, generation != 0)
    │
    ├─ unknown group / unknown member  → 10
    ├─ generation != group.generation  → 11
    ├─ synced_generation != generation → 9  (no store)
    └─ else store; 0

admin (generation == 0)                → today's skip; no fence
empty member_id + nonzero generation   → today's 10 / 11 only
```

Heartbeat still does not confirm. Leave / public `expire_sessions`
still bump + reassign (survivors become unsynced and cannot member-
commit until they SyncGroup).

## Tests

```bash
cargo test -p volant-broker --lib group -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Join then member OffsetCommit before SyncGroup | error **9**; offset not stored |
| Same commit after SyncGroup | error **0**; offset stored |
| `generation == 0` during a fence | admin commit still works |
| Empty `member_id` + matching nonzero gen | today's checks only; no sync fence |
| Wrong gen / unknown member | **11** / **10** |

## Honesty leftovers

- Dual-consume window until heartbeat **9** remains for fetch: after
  a peer Join bumps generation, the incumbent keeps consuming its
  old assignment until it sees heartbeat 9 and re-Joins. This slice
  only blocks **member** OffsetCommit for an unconfirmed generation.
- Empty-`member_id` commits can still store during a fence.
- No parked OffsetCommit. A fenced commit returns 9 immediately.
- Leader assignment bytes are still ignored.

## Merge notes

v0.218 edits `list_groups` state in `group.rs`. Keep this hunk
inside `commit_offsets`. Keep both.

## Related

- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation confirm fence
- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup peek
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
