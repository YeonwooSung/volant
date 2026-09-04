# v0.223 — language Client.join_group retries error 9

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Language thin **Client.join_group** (Python / Go / Java)
retries error **9** (`RebalanceInProgress`) with the same
`max_retries` / `retry_backoff` as other Join retries (default still
**0**). Do **not** change Rust (sibling v0.224). Do **not** park Join
on the broker.

Today v0.205 retries 6 / 7 / 15 / 16 + TCP only, and **not** 9.
GroupConsumer already retries 9 (v0.220). Thin Client callers (and
concurrent joins) still see the first 9.

## Goals

1. In the shared join loop (after member_id fill-in from v0.209):
   error **9** is retried like a transient, up to `max_retries`,
   sleep `retry_backoff`.
2. Still do **not** retry 10 / 11 / 2 / 13 / 17 / 18 / 21 / 22 /
   protocol.
3. Error **14** still uses `max_redirects`.
4. Default `max_retries=0` → first 9 still surfaces.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust Client Join retry 9 | Sibling v0.224 |
| Parked Join / CompletingRebalance | Coordinator rewrite; Empty/Stable only |
| Add 9 to the shared transient set | Heartbeat / Leave / other RPCs stay 6/7/15/16 |
| New opcodes / Kafka API keys | Frozen |
| Crate 0.3.0 | After 155 leftovers, not during |

## Semantics

```
join_group
  │
  ├─ member_id fill-in (v0.209)
  │
  ├─ send Join
  │       │
  │       ├─ error 9 and retry_attempt < max_retries
  │       │       sleep retry_backoff; Join again
  │       ├─ transient 6/7/15/16 + TCP (v0.205)
  │       ├─ error 14 → max_redirects
  │       ├─ 10 / 11 / 2 / 13 / 17 / 18 / 21 / 22 / protocol → surface
  │       └─ ok → return
```

| Call | Retry 9? |
|------|----------|
| Thin `Client.join_group` (empty or not) | Yes, `max_retries` extra attempts |
| GroupConsumer first join / rejoin | Already yes (v0.220); inherits Client too |
| Heartbeat 9 / 10 / 11 | Existing rejoin policy (unchanged) |

Default `max_retries=0`: first 9 still surfaces. Concurrent joins
need `max_retries > 0`. Each retry is a new Join RPC (not parked).

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_join_member_id tests.test_group -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

| Case | Expect |
|------|--------|
| Join 9 then 0, `max_retries=1` | two Join RPCs; success |
| `max_retries=0`, first Join 9 | 9 surfaces; one Join RPC |

## Files

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | `join_group` retry 9 |
| `clients/go/client.go` | `joinGroup` retry 9 |
| `clients/java/.../Client.java` | shared `joinGroup` retry 9 |
| Language Client / group tests | Join 9 then 0 |
| Client READMEs | thin Client now retries 9; default 0 |
| `docs/V223_SPEC.md` | This spec |

## Honesty leftovers

- Default `max_retries=0`: first 9 still surfaces unless the caller
  raises the budget.
- Not parked Join: each retry is a new Join RPC.
- 10 / 11 still surface immediately.
- Rust Client Join is sibling v0.224.
- Empty/Stable only. No CompletingRebalance.

## Merge notes

Sibling v0.224 edits Rust `client.rs`. Keep this hunk local to
language Client join. Do **not** add 9 to the shared transient set.

Do **not** edit living docs (`TODO.md`, `ROADMAP.md`, root
`README.md`, `docs/INDEX.md`, `docs/history/PHASE_HISTORY.md`).

## Related

- [V220_SPEC.md](./V220_SPEC.md) — GroupConsumer retries Join on error 9
- [V209_SPEC.md](./V209_SPEC.md) — generate member_id on empty first Join
- [V205_SPEC.md](./V205_SPEC.md) — JoinGroup transient retry (was not 9)
- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation fence
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
