# v0.227 — Park Join until SyncGroup or session timeout

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Park a new-member / topics-change Join until the
[v0.215](./V215_SPEC.md) SyncGroup fence lifts, **without holding**
`GroupCoordinator.groups` (`parking_lot::Mutex`) during the wait.
Today that Join returned error **9** immediately when `!all_synced`.
That is still the timeout result. The rewrite waits (Condvar) so
another connection can Heartbeat / SyncGroup.

This is residual **v0.227**. It is **not** Kafka PreparingRebalance
(we do not wait for a join-set). Join stays eager-assign on the
success path. CompletingRebalance label ([v0.218](./V218_SPEC.md))
unchanged. Do **not** change clients. Do **not** add Kafka keys.
Do **not** touch `SUPPORTED_APIS`. Do **not** touch `kafka/mod.rs`.

## Goals

1. Add `join_park: parking_lot::Condvar` next to
   `groups: Mutex<HashMap<…>>`.
2. When new-member Join or existing Join with a **topics change**
   sees `!all_synced`:
   - `join_park.wait_for(&mut groups, …)` (releases the mutex).
   - Timeout is the same resolved session timeout already used for
     expiry (`0` → `10_000`).
   - On notify: loop and re-evaluate (spurious / still `!all_synced`
     is fine).
   - On timeout and still `!all_synced`: return `fenced_join`
     (error **9**), **no insert, no bump, no reassign**.
3. `notify_all` after any transition that might lift or change the
   fence:
   - `sync_group` success (after `synced_generation = generation`)
   - `leave` (removing an unsynced member can make `all_synced`)
   - `expire_sessions_inner` if any member was dropped
   - public `expire_sessions` after bump/reassign
4. Existing member, **same topics**: still no barrier, no bump, do
   **not** park.
5. Empty group first Join: still immediate OK (`all_synced`).
6. Heartbeat still does **not** confirm.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka PreparingRebalance / join-set wait | Join stays eager-assign |
| Separate `rebalance.timeout` | Park uses session timeout |
| CompletingRebalance rewrite | Label only (v0.218) |
| Client changes | Already retry 9 (v0.220–v0.224) |
| New opcodes / Kafka API keys | Frozen; do not touch `SUPPORTED_APIS` |
| Crate 0.3.0 | After 155 leftovers, not during |

## Why the naive park is forbidden

`groups` is a process-wide `parking_lot::Mutex`. If `join()` waits
while holding it, same-process Heartbeat / SyncGroup / Leave cannot
run → deadlock. Same-connection sequential I/O may still be blocked
(that is OK, Kafka parks Join on that socket). Other connections
**must** proceed.

## Semantics

```
new-member Join, or existing Join with topics change
  │
  ├─ all_synced            → insert/update, generation++, reassign
  │
  └─ !all_synced
          wait_for(join_park, session_timeout)  // releases groups
          │
          ├─ notify / spurious → re-evaluate
          ├─ all_synced        → insert/update, generation++, reassign
          └─ timeout + still !all_synced
                  → error 9, no insert, no bump, no reassign
```

| Call | Park? | Effect |
|------|-------|--------|
| New-member Join, or existing Join with **topics change** | Yes, until `all_synced` or session timeout | On success: insert/update, `generation++`, `reassign()`. Joiner is **not** auto-synced. Timeout: error **9** |
| Existing member, same topics | No | No bump. Do **not** mark synced |
| `sync_group` success | — | `synced_generation = gen`; `notify_all` |
| Heartbeat | No | Unchanged; does **not** confirm |
| Leave | — | Still bump + reassign; `notify_all` |
| `expire_sessions_inner` (drop) | — | `notify_all` if any member dropped |
| Public `expire_sessions` | — | Still bump + reassign; `notify_all` |

Empty group: first Join always OK. Second Join parks until the first
member's SyncGroup (or session timeout → 9).

`join` is **not** async. Native `net/dispatch.rs` and Kafka Join
already call `groups().join()` — one change covers both.

## Tests

```bash
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-broker --test phase26_kafka_groups -- --test-threads=1
```

Sequential tests that expect error **9** use **100–200ms** session
timeout (not `10_000`, which would park 10s).

| Case | Expect |
|------|--------|
| First Join OK; second Join without Sync (150ms) | error **9**; membership/gen unchanged |
| Parked second Join; peer SyncGroup | second Join OK; gen bumped; two members; not inserted before Sync |
| Heartbeat while second Join is parked | Heartbeat **0** (mutex not held); then Sync unparks Join |
| Second Join timeout 150ms, same thread | error **9**; gen unchanged; `member_count==1` |
| Existing member re-Join during fence (same topics) | OK, no bump, no park |
| Leave still bumps with no Sync | remaining must Sync (or timeout) before next new Join |

## Files

| File | What |
|------|------|
| `crates/volant-broker/src/group.rs` | `join_park` Condvar; park / notify; tests |
| `docs/V227_SPEC.md` | This spec |

## Honesty leftovers

- **Not** Kafka PreparingRebalance. We do not wait for a join-set.
  Join stays eager-assign on the success path.
- Park timeout **is** the session timeout. There is no separate
  `rebalance.timeout`.
- Same-connection sequential I/O is still blocked while Join parks
  (the socket waits). Other connections proceed because the mutex
  is released.
- CompletingRebalance is still a List/Describe **label** (v0.218),
  not a coordinator state machine.
- Heartbeat still does not confirm the generation.
- Dual-consume window until heartbeat **9** remains after a peer
  Join bumps generation.

## Related

- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation confirm fence
- [V218_SPEC.md](./V218_SPEC.md) — CompletingRebalance label
- [V221_SPEC.md](./V221_SPEC.md) — GroupConsumer retries Join on 9
- [V224_SPEC.md](./V224_SPEC.md) — Rust Client Join retries error 9
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka consumer groups
