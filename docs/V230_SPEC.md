# v0.230 — PreparingRebalance while Join is parked

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** List/Describe report **PreparingRebalance** while a Join is
**parked** (v0.227 Condvar waiters exist). CompletingRebalance stays
the label for live && !all_synced with **no** waiters.

This is a **label**, not join-set wait, not a coordinator rewrite.
Do **not** change `join()` signature. Do **not** change park timeout.
Do **not** add Kafka keys. Do **not** touch `SUPPORTED_APIS`. Do
**not** touch Fetch or txn or SCRAM.

## Goals

1. `GroupState`: `Empty = 0`, `Stable = 1`, `CompletingRebalance = 2`,
   `PreparingRebalance = 3`. `from_u8(3)` → PreparingRebalance;
   unknown still Empty. `as_str` → `"PreparingRebalance"`.
2. Parked joiners are **not** members. Track per-group parked count
   on `GroupCoordinator` (`join_waiters: Mutex<HashMap<String, u32>>`).
3. Increment when `park_until_all_synced` is about to wait
   (`!all_synced` at entry). Decrement on every exit (synced,
   timeout, notify loop exit). Never go below 0. Remove key at 0.
4. Do this **inside** `park_until_all_synced` so `join()` stays
   unchanged (sibling v0.231 owns the timeout arg).
5. All ListGroups / DescribeGroup paths (native + Kafka
   `encode_list_groups` / DescribeGroups state string) use
   `listed_state`.
6. Language clients (Python / Go / Java) decode 3 as
   PreparingRebalance. CompletingRebalance decode stays.

## Non-goals

| Deferred | Why |
|----------|-----|
| Join-set wait / Kafka PreparingRebalance machine | Label only; Join stays eager-assign on success |
| `join()` signature / park timeout | Sibling v0.231 owns the timeout arg |
| New opcode / Kafka API key | Frozen; do not touch `SUPPORTED_APIS` |
| Fetch / txn / SCRAM | Orthogonal |
| Crate 0.3.0 | After 155 leftovers, not during |

## Semantics

```
listed_state(g, parked) =
    PreparingRebalance     if parked > 0
    Empty                  if no live members
    CompletingRebalance    if live && !all_synced
    Stable                 if live && all_synced
```

| Moment | ListGroups / DescribeGroup | member_count |
|--------|----------------------------|--------------|
| First Join, no second | CompletingRebalance (2) | 1 |
| A joined unsynced; B parked | PreparingRebalance (3) | 1 (B is not a member) |
| Sync A; B inserted, B not synced | CompletingRebalance (or PreparingRebalance only while B still parked) | 2 after insert |
| All synced | Stable (1) | live members |
| Offset-only / no live members | Empty (0) | 0 |

Parked joiners are not members. Join, SyncGroup, Heartbeat, Leave,
and OffsetCommit behavior are unchanged from v0.227.

## Wire

Native ListGroups entry: `u8` state after `group_id`.

```
0 Empty
1 Stable
2 CompletingRebalance
3 PreparingRebalance
other → Empty
```

Kafka ListGroups v4+ `GroupState` and DescribeGroups `state` strings
use the same four names.

## Tests

```bash
cargo test -p volant-protocol --lib -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
```

Plus a targeted Python / Go / Java unit if those enums are touched
(no full discover).

| Case | Expect |
|------|--------|
| First Join, no second | CompletingRebalance; no waiters |
| A joined unsynced; thread parks B | PreparingRebalance; `member_count` still 1 |
| Sync A; B unparks; B inserted, not synced | CompletingRebalance |
| All synced | Stable |
| `from_u8(0/1/2/3/unknown)` | Empty / Stable / CompletingRebalance / PreparingRebalance / Empty |

Sequential / parked Join tests use **150ms** session timeout so they
do not hang. `join()` parameters are unchanged.

## Honesty leftovers

- Label only. Not a PreparingRebalance state machine. Not join-set
  wait. Join stays eager-assign on the success path.
- Parked joiner is still not a member.
- Park timeout is still the session timeout. There is no separate
  `rebalance.timeout`.
- CompletingRebalance remains the live && !all_synced label when
  no Join is parked.
- Dual-consume window until heartbeat **9** remains.
- Leader assignment bytes are still ignored.

## Related

- [V227_SPEC.md](./V227_SPEC.md) — park Join until SyncGroup or timeout
- [V218_SPEC.md](./V218_SPEC.md) — CompletingRebalance label
- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation fence
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
