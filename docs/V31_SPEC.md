# v0.31 — Python high-level GroupConsumer

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python has JoinGroup / Heartbeat / LeaveGroup
(v0.28) and offsets (v0.24) but no poll loop” by adding a high-level
`GroupConsumer` on the native Python client.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker.

## Goals

1. **Python** `GroupConsumer.join` / `poll` / `commit` / `close` matching
   `crates/volant-client/src/group.rs` as closely as practical on the
   existing sync `Client` RPCs.
2. Join keeps `member_id`, `generation`, and `assignment` as
   `(topic, partition)` pairs.
3. Positions start from `offset_fetch` when committed, else `0`
   (`u64::MAX` / unknown → 0).
4. `poll` heartbeats, fetches each assigned partition from the current
   position, and advances positions (`last+1`).
5. Broker error **9** (and 10 / 11, same as Rust `needs_rebalance`) →
   re-JoinGroup and retry the fetch **once**.
6. `commit` uses the joined `member_id` + `generation` (not the admin
   empty-member path).
7. Cooperative revoked list (Phase 17): drop positions and stop fetching
   revoked partitions; retain in-memory positions for sticky-kept ones.
8. Export `GroupConsumer` / `FetchedRecord` from `volant`.
9. Unit tests against a fake `Client`; optional `VOLANT_E2E=1` live test.

## Non-goals

| Deferred | Why |
|----------|-----|
| Go / Java GroupConsumer | This slice is Python only |
| Client-side assignor | Broker already assigns; client returns it |
| Background heartbeat thread | Heartbeat is on `poll`, same as Rust |
| Multi-entry OffsetCommit helper on `Client` | Existing one-partition `offset_commit` is enough |
| Kafka JoinGroup / consumer API keys | Native opcodes 6–10; no Kafka keys |
| TLS / SCRAM / shared-token Auth | Unchanged; consumer uses the existing `Client` |
| Broker / protocol / Rust client changes | Wire and Rust `GroupConsumer` already exist |
| Auto-commit / pause-resume two-phase revoke | Phase 17 leftover; revoke applies at re-join |

## API

```python
from volant import Client, GroupConsumer
c = Client("127.0.0.1:9092")
g = GroupConsumer.join(c, group="g", topics=["t"], session_timeout_ms=10_000)
recs = g.poll(max_wait_ms=500)   # fetch assigned partitions; heartbeat as needed
g.commit()                       # commit positions via offset_commit
g.close()                        # leave_group
```

`session_timeout_ms=0` defaults to 10000 (same as Rust). Optional
`group_instance_id=` is Phase 12 static membership (empty = dynamic).
`close()` does **not** close the `Client`. `leave()` is an alias of
`close()`. Context manager form calls `close()` on exit.

`poll` returns `list[FetchedRecord]` (`topic`, `partition`, `offset`,
`key`, `value`, `timestamp_ms`, `headers`).

Inspectable state: `g.member_id`, `g.generation`, `g.assignment`,
`g.last_revoked`, `g.positions`, `g.group_id`.

Two members need **two** `Client` connections (one TCP each).

## Semantics

Match Rust `GroupConsumer` (`crates/volant-client/src/group.rs`):

1. **Join.** `join_group` with the stored `member_id` (empty on first
   join). Store broker `member_id` / `generation` / `assignment`.
2. **Positions.** Cooperative handoff (Phase 17):
   - `retained = old ∩ new` — keep in-memory fetch positions
   - `added = new − old` — OffsetFetch (or 0 if unknown)
   - `revoked = (old − new) ∪ broker revoked` — drop positions; do not fetch
   First join OffsetFetches the full assignment. Missing / `OFFSET_UNKNOWN`
   (`2^64-1`) → `0`.
3. **Poll.** Heartbeat. On error 9 / 10 / 11, re-join. Fetch each
   assigned (non-revoked) partition from its position (`max_messages=100`,
   `max_wait_ms` from the caller, default 500). Advance that partition to
   `record.offset + 1`.
4. **Rejoin retry.** A rebalance / unknown-member / illegal-generation
   error on heartbeat or fetch re-joins and retries the fetch **once**.
   A second 9/10/11 propagates.
5. **Commit.** `offset_commit` per assigned position with the joined
   `member_id` and `generation`.
6. **Close.** `leave_group(group, member_id)`. Idempotent.

## Tests

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Fake `Client`: join positions, poll advance, commit member/generation, error 9/10 rejoin once, cooperative retain/drop, close → leave |
| `clients/python/tests/test_e2e.py` | Live one-consumer poll+commit resume; two consumers on 2 partitions (disjoint assignment). Skip unless `VOLANT_E2E=1` |

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
# live:
cargo build -p volant-server
VOLANT_E2E=1 PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
```

## Honesty leftovers

- No Go / Java `GroupConsumer` (Rust still has the reference; Python is
  the first thin-client port).
- Thin `Client.heartbeat` still **raises** `BrokerError` on nonzero
  `error_code` (including 9). `GroupConsumer` catches 9/10/11 and
  rejoins; raw RPC callers must do that themselves (v0.28 leftover).
- `commit` is one `offset_commit` RPC per assigned partition, not a
  single multi-entry OffsetCommit (Rust sends one request).
- `max_wait_ms` is passed through to **each** assigned-partition Fetch
  (a 2-partition empty poll can wait up to `2 * max_wait_ms`).
- No background heartbeat; a silent consumer still expires after
  `session_timeout_ms`.
- Not Kafka cooperative-sticky (no separate revoke/assign generations).
  Revoke applies at re-join, not mid-batch (Phase 17).
- Broker assignor is unchanged. No client-side sticky assignor.
- Still no Kafka-wire SDK, leader redirect, or auto-commit on this
  client. Sync only; one TCP connection per `Client`.
- Broker and Rust `volant-client` are unchanged.

See [clients/python/README.md](../clients/python/README.md) and
[V28_SPEC.md](./V28_SPEC.md).
