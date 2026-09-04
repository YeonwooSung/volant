# v0.220 — GroupConsumer retries Join on error 9

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Language **GroupConsumer** (Python / Go / Java) retries
JoinGroup when the broker returns error **9** (v0.215 generation
fence) so overlapping joins can wait for the other member's
SyncGroup. Budget is the existing `max_retries` /
`retry_backoff` (default still **0** extra attempts).

This is **not** parked Join and **not** Kafka CompletingRebalance.
Thin `Client.join_group` is unchanged: 9 is still not in the
transient set (v0.205). Do **not** change Rust (sibling v0.221).

## Goals

1. In `_do_join` / `doJoin` / `joinGroupWithFenceRetry`, on
   BrokerError / code **9**, if `retry_attempt < max_retries`:
   sleep `retry_backoff`, Join again (same stored `member_id` /
   instance).
2. Default `max_retries=0`: first 9 surfaces (behavior unchanged).
   Concurrent joins need `max_retries > 0`.
3. 10 / 11 still use the existing heartbeat / poll rejoin path.
   Do not invent new 10 / 11 handling on Join.
4. After a successful Join, existing SyncGroup peek (v0.207) still
   runs.
5. Do **not** increment `heartbeat_count` / `HeartbeatCount` on
   these Join attempts.
6. Do **not** retry 9 on thin `Client.join_group` beyond v0.205
   (9 is not transient).

## Non-goals

| Deferred | Why |
|----------|-----|
| Retry 9 on thin `Client.join_group` | v0.205 transient set; first-join empty path stays one-shot for 9 |
| Rust GroupConsumer retry | Sibling v0.221 |
| Parked Join / CompletingRebalance | Coordinator rewrite; Empty/Stable only |
| New 10 / 11 Join handling | Existing rejoin-on-heartbeat path |
| New opcodes / Kafka API keys | Frozen |
| Flip openraft / grow homemade Raft | Other 155 leftovers |

## Semantics

```
do_join
  │
  ├─ join_group (same member_id / instance)
  │       │
  │       ├─ error 9 and retry_attempt < max_retries
  │       │       sleep retry_backoff; Join again
  │       ├─ error 9 and max_retries exhausted → surface 9
  │       ├─ 10 / 11 / other → surface (existing)
  │       └─ ok → continue
  │
  └─ sync_group peek (v0.207)
```

| Call | Retry 9? |
|------|----------|
| GroupConsumer first join / rejoin | Yes, `max_retries` extra attempts |
| Thin `Client.join_group` (empty or not) | No (v0.205; 9 not transient) |
| Heartbeat 9 / 10 / 11 | Existing rejoin policy (unchanged) |

Empty group: first Join still OK. A second overlapping Join that
hits the v0.215 fence gets 9; with `max_retries > 0` it sleeps and
Joins again after the first member's SyncGroup.

## Tests

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_group -q
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

No full Python discover.

| Case | Expect |
|------|--------|
| Fake Join 9 then 0, `max_retries=1` | two Joins then SyncGroup; `heartbeat_count` stays 0 |
| `max_retries=0`, first Join 9 | 9 surfaces; one Join; no SyncGroup |

## Files

| File | What |
|------|------|
| `clients/python/src/volant/group.py` | `_do_join` fence-9 loop |
| `clients/go/group.go` | `doJoin` fence-9 loop |
| `clients/java/.../GroupConsumer.java` | `joinGroupWithFenceRetry` |
| Language GroupConsumer tests | fake Join 9 then 0 |
| Client READMEs | concurrent-join `max_retries > 0` |
| `docs/V220_SPEC.md` | This spec |

## Honesty leftovers

- Default `max_retries=0`: concurrent joins still see the first 9
  unless the caller raises the budget.
- Thin `Client.join_group` still surfaces 9 immediately.
- Not parked Join: each retry is a new Join RPC.
- SyncGroup peek stays best-effort (v0.207).
- Rust GroupConsumer is sibling v0.221.
- Empty/Stable only. No CompletingRebalance.

## Merge notes

Sibling v0.221 edits Rust `group.rs`. Keep this hunk local to
language GroupConsumer join. Do **not** add 9 to the Client
transient set.

Do **not** edit living docs (`TODO.md`, `ROADMAP.md`, root
`README.md`, `docs/INDEX.md`, `docs/history/PHASE_HISTORY.md`).

## Related

- [V215_SPEC.md](./V215_SPEC.md) — SyncGroup generation fence
- [V207_SPEC.md](./V207_SPEC.md) — GroupConsumer SyncGroup peek
- [V205_SPEC.md](./V205_SPEC.md) — JoinGroup transient retry (not 9)
- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
