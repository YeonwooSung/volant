# v0.221 — Rust GroupConsumer retries Join on error 9

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V215_SPEC.md](./V215_SPEC.md): a
new-member Join while a peer has not SyncGroup'd returns error **9**
immediately (no parked Join). `Client::join_group_with_instance` still
does **not** retry 9 (rebalance stays visible on the raw RPC).
`GroupConsumer::do_join` retries that fence so overlapping joins can
wait for the other member's SyncGroup.

This is residual **v0.221**. It is **not** Kafka CompletingRebalance
and **not** parked Join. Do **not** change the Client Join transient
set. Do **not** change Python / Go / Java. No new opcodes. No Kafka
API keys. Crate stays **0.2.0**.

## Goals

1. In `do_join`, when `join_group_with_instance` returns error **9**
   (`RebalanceInProgress`), retry up to
   [`ClientConfig::max_retries`] extra attempts.
2. Sleep [`ClientConfig::retry_backoff_ms`] between attempts (`0`
   allowed in tests).
3. Default extra attempts stay **0** so existing GroupConsumer tests
   remain one-shot on 9.
4. Do **not** change `Client::join_group_with_instance` (9 stays not
   retried there).
5. Do **not** increment `heartbeat_count` on these Joins.
6. After a successful Join, the existing SyncGroup peek (v0.208)
   still runs.

## Non-goals

| Deferred | Why |
|----------|-----|
| Retry 9 inside `join_group_with_instance` | Raw Client must still surface the fence |
| Parked Join / CompletingRebalance | Coordinator rewrite; Empty/Stable only |
| Retry 10 / 11 at GroupConsumer | Unknown member / illegal generation still fail |
| Python / Go / Java GroupConsumer | Rust-only residual |
| New opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Crate 0.3.0 | After 155 leftovers, not during |

## Semantics

```
do_join
  │
  ├─ join_group_with_instance
  │       │
  │       ├─ Ok            → continue
  │       ├─ Err 9 + budget → sleep retry_backoff_ms; retry
  │       └─ other Err     → return Err
  │
  ├─ sync_group peek (v0.208)
  │
  └─ range override / OffsetFetch / reset (unchanged)
```

- Budget is `1 + max_retries` Join RPCs. Default `max_retries=0`.
- Only error **9** is retried at this layer. Transient 6 / 7 / 15 /
  16 stay on the Client Join loop (v0.205).
- HeartbeatCount / `heartbeat_count` stays a Heartbeat RPC counter.
  Join retries are not Heartbeats.

## Tests

```bash
cargo test -p volant-client --test v221_join_fence_retry --test v208_group_sync -- --test-threads=1
```

| Case | Expect |
|------|--------|
| First Join 9, `max_retries=1`, second Join 0 | join Ok; 2 Join RPCs; 1 SyncGroup; `heartbeat_count=0` |
| Default `max_retries=0`, first Join 9 | Err 9; one Join; no SyncGroup |
| `Client::join_group` 9 with `max_retries=1` | Err 9; one Join (Client still does not retry 9) |

| File | What |
|------|------|
| `crates/volant-client/src/group.rs` | `do_join` retry-on-9 |
| `crates/volant-client/src/lib.rs` | crate-doc sentence |
| `crates/volant-client/tests/v221_join_fence_retry.rs` | fake TCP |
| `docs/V221_SPEC.md` | This spec |

## Honesty leftovers

- No parked Join. A fenced Join still returns 9 immediately from the
  broker; GroupConsumer polls it on `retry_backoff_ms`.
- `Client::join_group_with_instance` still does not retry 9.
- Default `max_retries=0`.
- SyncGroup is still peek, not CompletingRebalance.
- Python / Go / Java GroupConsumer still one-shot on 9.
- Kafka stays 38 keys. Key 14 unchanged.

## Merge notes

Keep this hunk local to `do_join` retry-on-9. v0.218 / v0.219 do not
touch `volant-client` `group.rs` except if 218 changes GroupState
types — keep both.

## Related

- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation confirm fence
- [V208_SPEC.md](./V208_SPEC.md) — GroupConsumer SyncGroup peek
- [V205_SPEC.md](./V205_SPEC.md) — Client Join retry (9 not in set)
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
