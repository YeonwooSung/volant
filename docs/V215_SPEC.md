# v0.215 — SyncGroup generation confirm fence

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** SyncGroup becomes a generation confirm fence. Join stays
eager-assign. This is **not** Kafka CompletingRebalance and **not**
parked Join. No new opcodes. No `GroupState` change. No Kafka API
keys. `SUPPORTED_APIS` stays 38.

Native SyncGroup opcodes **116 / 117** and Kafka key **14** already
exist ([V206_SPEC.md](./V206_SPEC.md)). This residual only changes
when a **new** member (or a topics-change Join) is allowed to bump.

## Goals

1. Add `Member.synced_generation: u32` (`0` = never).
2. `all_synced(g)` ⇔ no members OR every live member has
   `synced_generation == g.generation`.
3. New-member Join, or existing Join with a **topics change**: if
   `!all_synced` → error **9**, no insert, no bump, no reassign. On
   success: insert/update, `generation++`, `reassign()`. The joiner is
   **not** auto-synced.
4. Existing member, same topics: no barrier, no bump, do **not** mark
   synced.
5. `sync_group(group, member, gen)`: same 9/10 as heartbeat, then
   `synced_generation = gen`, return current assignment. Ignore
   assignment bytes.
6. Heartbeat does **not** confirm. Leave / public `expire_sessions`
   still bump + reassign (survivors become unsynced). OffsetCommit
   unchanged.
7. Kafka `encode_sync_group` must call `sync_group` too, or key **14**
   never lifts the fence.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CompletingRebalance / PreparingRebalance | Empty/Stable only |
| Parked Join / Join-retry-on-9 | Out of slice; clients still one-shot on 9 |
| Auto-sync the joiner | Confirm is SyncGroup only |
| New opcode / Kafka API key | 116/117 and key 14 already shipped |
| `GroupState` change | ListGroups still Empty/Stable |
| Flip openraft / grow homemade Raft | Other 155 PRs |

## Semantics

```
all_synced(g)  ⇔  no members OR every live member
                  has synced_generation == g.generation
```

| Call | Barrier | Effect |
|------|---------|--------|
| New-member Join, or existing Join with **topics change** | If `!all_synced` → **error 9**, no insert, no bump, no reassign | On success: insert/update, `generation++`, `reassign()`. Joiner is **not** auto-synced |
| Existing member, same topics | No | No bump. Do **not** mark synced |
| `sync_group(group, member, gen)` | — | Same 9/10 as heartbeat, then `synced_generation = gen`, return current assignment |
| Heartbeat | No | Unchanged; does **not** confirm |
| Leave / `expire_sessions` | No | Still bump + reassign. Survivors become unsynced |
| OffsetCommit | No | Unchanged |

Empty group: first Join always OK. Second Join needs the first
member's SyncGroup.

## Expire landmine

`join` / `heartbeat` / `sync_group` call `expire_sessions_inner`.
Inner expire is **drop-dead-only**: drop timed-out members, do not
bump generation, do not clear survivors' assignments. A subsequent
Join may bump+reassign to heal. Do not leave C Join stuck after A
expired and B assignment cleared.

Public `expire_sessions` (background, with partition counts) still
bumps + reassigns. Survivors become unsynced.

## Broker

- `GroupCoordinator::sync_group` is the confirm path.
- Native `Request::SyncGroup` and Kafka `encode_sync_group` both call
  it (heartbeat no longer stands in for SyncGroup).
- Join stays eager-assign on the success path.

## Tests

```bash
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-client --test v206_sync_group -- --test-threads=1
```

| Case | Expect |
|------|--------|
| First Join OK; second Join without Sync | error **9**; membership/gen unchanged |
| First `sync_group` then second Join | second Join OK; gen bumped |
| Two members; third Join | blocked until **both** SyncGroup (or leave) |
| Existing member re-Join during fence (same topics) | OK, no bump |
| Leave still bumps with no Sync | remaining must Sync before next new Join |
| `sync_group` unknown / wrong gen | **10** / **9** |
| ListGroups / `SUPPORTED_APIS` | Empty/Stable; len **38** |

Existing unit tests that `join` twice without SyncGroup insert
`sync_group` between bumps.

## Honesty leftovers

- No parked Join. A fenced Join returns 9 immediately; the caller
  is not queued.
- No CompletingRebalance on the wire. ListGroups is still
  Empty/Stable.
- Dual-consume window until heartbeat **9** remains: after a peer
  Join bumps generation, the incumbent keeps consuming its old
  assignment until it sees heartbeat 9 and re-Joins.
- Leader assignment bytes are still ignored.
- Join-retry-on-9 is not in this slice.

## Merge notes

v0.213/v0.214 also edit `dispatch.rs`. Keep the SyncGroup **inbound**
arm hunk local. Keep both.

## Related

- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup peek
- [V208_SPEC.md](./V208_SPEC.md) — GroupConsumer SyncGroup after join
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka SyncGroup honesty
