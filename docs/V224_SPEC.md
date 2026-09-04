# v0.224 — Rust Client Join retries error 9

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V221_SPEC.md](./V221_SPEC.md):
`GroupConsumer::do_join` already retries Join error **9**
(`RebalanceInProgress`) so overlapping joins can wait for a peer
SyncGroup. Thin [`Client::join_group_with_instance`] did not.
This residual adds the same `max_retries` / `retry_backoff_ms`
budget (default **0**) on the raw Client RPC.

This is residual **v0.224**. It is **not** Kafka CompletingRebalance
and **not** parked Join. Do **not** change Python / Go / Java. No
new opcodes. No Kafka API keys. Crate stays **0.2.0**.

## Goals

1. In `Client::join_group_with_instance`, when Join returns error
   **9**, retry up to [`ClientConfig::max_retries`] extra attempts.
2. Sleep [`ClientConfig::retry_backoff_ms`] between attempts (`0`
   allowed in tests).
3. Default extra attempts stay **0** so existing one-shot-on-9 tests
   remain valid.
4. Do **not** park Join on the broker. Each retry is a new Join RPC.
5. Do **not** retry 10 / 11. Transient 6 / 7 / 15 / 16 stay on the
   existing v0.205 loop (same budget).
6. Do **not** change Python / Go / Java thin Client.

## Non-goals

| Deferred | Why |
|----------|-----|
| Parked Join / CompletingRebalance | Coordinator rewrite; Empty/Stable only |
| Retry 10 / 11 on Join | Unknown member / illegal generation still fail |
| Python / Go / Java thin Client | Rust-only residual; GroupConsumer already retries (v0.220) |
| New opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Crate 0.3.0 | After 155 leftovers, not during |

## Semantics

```
join_group_with_instance
  │
  ├─ JoinGroup RPC
  │       │
  │       ├─ Ok            → return
  │       ├─ Err 9 + budget → sleep retry_backoff_ms; retry
  │       ├─ transient 6/7/15/16 + budget → same loop (v0.205)
  │       ├─ Err 14 + redirect budget → hunt controller
  │       └─ other Err     → return Err
  │
  └─ budget is 1 + max_retries Join RPCs (default max_retries=0)
```

- Only Join uses this 9-retry. Heartbeat / Leave / OffsetCommit /
  SyncGroup still surface 9 immediately.
- 10 / 11 stay not retried.
- Empty first Join still generates a UUID `member_id` (v0.210) so
  the retry guard sees a non-empty id.
- GroupConsumer still has its own 9 loop (v0.221) around this RPC.

## Tests

```bash
cargo test -p volant-client --test v224_join_retries_9 --test v205_join_group_retry --test v221_join_fence_retry -- --test-threads=1
```

| Case | Expect |
|------|--------|
| First Join 9, `max_retries=1`, second Join 0 | join Ok; 2 Join RPCs |
| Default `max_retries=0`, first Join 9 | Err 9; one Join RPC |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `join_group_with_instance` retry-on-9 |
| `crates/volant-client/src/lib.rs` | crate-doc sentence |
| `crates/volant-client/tests/v224_join_retries_9.rs` | fake TCP |
| `crates/volant-client/tests/v221_join_fence_retry.rs` | Client assertion now expects retry when `max_retries>0` |
| `docs/V224_SPEC.md` | This spec |

## Honesty leftovers

- No parked Join. A fenced Join still returns 9 immediately from the
  broker; the Client polls it on `retry_backoff_ms`.
- Default `max_retries=0`.
- Python / Go / Java thin `Client.join_group` still one-shot on 9.
- SyncGroup is still peek, not CompletingRebalance.
- Kafka stays 38 keys. Key 14 unchanged.

## Merge notes

Keep this hunk local to `join_group_with_instance` retry-on-9. Do
**not** add 9 to the global Heartbeat transient set.

Do **not** edit living docs (`TODO.md`, `ROADMAP.md`, root
`README.md`, `docs/INDEX.md`, `docs/history/PHASE_HISTORY.md`).

## Related

- [V221_SPEC.md](./V221_SPEC.md) — Rust GroupConsumer Join 9 retry
- [V220_SPEC.md](./V220_SPEC.md) — language GroupConsumer Join 9 retry
- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation confirm fence
- [V205_SPEC.md](./V205_SPEC.md) — Client Join transient retry
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
