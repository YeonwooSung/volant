# v0.231 — Join park uses rebalance timeout, not session

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** [v0.227](./V227_SPEC.md) reused `session_timeout_ms` as the
Condvar park budget. A 10s same-connection Join park could expire the
existing member. Split the knobs:

- `session_timeout_ms` = member expiry only
- `rebalance_timeout_ms` = how long Join parks
- `0` / omitted rebalance → **`DEFAULT_JOIN_PARK_MS = 1000`**
- **Never** use session timeout as the park budget

This is residual **v0.231**. It is **not** Kafka PreparingRebalance
(we do not wait for a join-set). Join stays eager-assign on the
success path. CompletingRebalance label ([v0.218](./V218_SPEC.md) /
sibling v0.230 `listed_state`) is unchanged. Do **not** add Kafka
keys. Do **not** touch Fetch / txn / SCRAM. Do **not** add a broker
`rebalance.timeout.ms` config.

## Goals

1. `GroupCoordinator::join` takes `rebalance_timeout_ms: u32` after
   `session_timeout_ms`.
2. Park budget = `if rebalance_timeout_ms == 0 { 1000 } else { rebalance_timeout_ms }`.
3. Session timeout (`0` → `10_000`) still only writes
   `Member.session_timeout_ms` for expiry.
4. Kafka `encode_join_group` already reads `rebalance_timeout` at
   v1+ and ignored it. Pass it through. v0 has no field → 0 → 1000.
5. Native optional trailer after `group_instance_id`:
   `u32_le rebalance_timeout_ms`. Missing / 0 → default 1000.
6. Rust `Client::join_group_once` / encode sends the trailer (`0` =
   broker default). Public Join signatures stay the same.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka PreparingRebalance / join-set wait | Join stays eager-assign |
| `listed_state` / GroupState rewrite | Sibling v0.230 |
| Broker `rebalance.timeout.ms` config | Per-Join field is enough |
| New Kafka API keys | Frozen; do not touch `SUPPORTED_APIS` |
| Fetch / txn / SCRAM | Orthogonal |
| Crate 0.3.0 | After 155 leftovers, not during |

## Semantics

```
new-member Join, or existing Join with topics change
  │
  ├─ all_synced            → insert/update, generation++, reassign
  │
  └─ !all_synced
          wait_for(join_park, rebalance_timeout)  // 0 → 1000ms
          │                                        // never session_timeout
          ├─ notify / spurious → re-evaluate
          ├─ all_synced        → insert/update, generation++, reassign
          └─ timeout + still !all_synced
                  → error 9, no insert, no bump, no reassign
```

| Call | Park? | Effect |
|------|-------|--------|
| New-member Join, or existing Join with **topics change** | Yes, until `all_synced` or rebalance timeout | On success: insert/update, `generation++`, `reassign()`. Joiner is **not** auto-synced. Timeout: error **9** |
| Existing member, same topics | No | No bump. Do **not** mark synced |
| `sync_group` success | — | `synced_generation = gen`; `notify_all` |
| Heartbeat | No | Unchanged; does **not** confirm |

Empty group: first Join always OK. Second Join parks until the first
member's SyncGroup (or rebalance timeout → 9).

## Wire

### Native JoinGroup

```
string group_id
string member_id
u32_le session_timeout_ms
u32_le topic_count + topics
string group_instance_id          // Phase 12; omitted on legacy
u32_le rebalance_timeout_ms       // v0.231; omitted on legacy → 0
```

| Trailer | Park |
|---------|------|
| Missing (legacy, no bytes after instance id) | 1000ms |
| Present `0` | 1000ms |
| Present `N` | N ms |

### Kafka JoinGroup

v0: no rebalance field → 0 → 1000ms.  
v1+: `rebalance_timeout` (i32, clamped ≥ 0) is the park budget.

## Tests

```bash
cargo test -p volant-protocol --lib -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-client --test v206_sync_group -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Timeout-9 path, session=10_000, rebalance=150 | error **9**; first member still alive |
| Park-success / heartbeat-during-park, rebalance=5_000, session=10_000 | Join OK after Sync; Heartbeat **0** while parked |
| Omitted / 0 rebalance | parks at most ~1s (not 10s), then **9** |
| Protocol trailer present | roundtrip `rebalance_timeout_ms = N` |
| Protocol legacy without trailer | `rebalance_timeout_ms = 0` |

## Files

| File | What |
|------|------|
| `crates/volant-broker/src/group.rs` | `DEFAULT_JOIN_PARK_MS`; park vs session; tests |
| `crates/volant-broker/src/net/dispatch.rs` | pass native trailer |
| `crates/volant-broker/src/kafka/group_api.rs` | pass Kafka rebalance_timeout |
| `crates/volant-protocol/src/request.rs` | `Request::JoinGroup.rebalance_timeout_ms` |
| `crates/volant-protocol/src/payload.rs` | encode/decode trailer |
| `crates/volant-client/src/client.rs` | send trailer `0` |
| `docs/V231_SPEC.md` | This spec |

## Honesty leftovers

- **Not** Kafka PreparingRebalance. We do not wait for a join-set.
  Join stays eager-assign on the success path.
- No broker `rebalance.timeout.ms` config. Park is per-Join.
- Same-connection sequential I/O is still blocked while Join parks
  (the socket waits). Other connections proceed because the mutex
  is released.
- CompletingRebalance is still a List/Describe **label** (v0.218),
  not a coordinator state machine. `listed_state` is sibling v0.230.
- Heartbeat still does not confirm the generation.
- Dual-consume window until heartbeat **9** remains after a peer
  Join bumps generation.

## Related

- [V227_SPEC.md](./V227_SPEC.md) — Park Join until SyncGroup
- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation confirm fence
- [V218_SPEC.md](./V218_SPEC.md) — CompletingRebalance label
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka consumer groups
