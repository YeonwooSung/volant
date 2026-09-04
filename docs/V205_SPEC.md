# v0.205 — JoinGroup retry when member or instance id is set

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Phase 155 PR3. Heartbeat already retries transient 6 / 7 / 15 /
16 + TCP/IO (`max_retries` default **0**) and hunts error **14** on
`max_redirects`. JoinGroup was one shot everywhere because a first join
with a new UUID is not idempotent: a lost success plus retry creates a
ghost member and bumps generation.

Retry JoinGroup **only when it is safe**: stored `member_id` (rejoin) or
non-empty `group_instance_id` (static membership). Empty first join
stays one shot. Copy the Heartbeat loop. No new opcodes, Kafka keys, or
openraft default changes.

## Goals

1. Extra JoinGroup attempts after the first on **transient** errors
   only, when `member_id` or `group_instance_id` is non-empty. Budget is
   independent of `max_redirects`.
2. Same transient set as Heartbeat / `is_transient_error_code`:
   - Broker: **6** Io, **7** Timeout, **15** NotEnoughReplicas, **16**
     BrokerNotAvailable
   - Transport: TCP / IO (same helpers as Heartbeat)
3. Error **14** (`NotController`) uses `max_redirects` (independent of
   retry) and hunts like Heartbeat / LeaveGroup. Broker may never emit
   14 on Join.
4. **Guard:** if `member_id` is empty **AND** `group_instance_id` is
   empty, do **not** enter the retry loop (single attempt).
5. **Not retried:** 9 / 10 / 11 / 2 / 13 / 17 / 18 / 21 / 22 / protocol.
6. Default `max_retries=0`. Sleep `retry_backoff` between attempts
   (`0` allowed in tests).
7. HeartbeatCount / `heartbeat_count` must **not** increment on Join.
   GroupConsumer keeps calling the Client RPC (no second loop).

## Transient errors

Match Heartbeat and `crates/volant-client` `is_transient_error_code`.

| Code | Name |
|------|------|
| 6 | Io |
| 7 | Timeout |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

**Transport:** `Error::Io` / `OSError` / `isTransientTransport`.

**Not retried:** 9 / 10 / 11 / 2 / 13 / 17 / 18 / 21 / 22 / protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Retry empty first Join | Ghost member + generation++ |
| New opcodes / Kafka API keys | Frozen; Phase 155 PR4 is SyncGroup |
| Openraft default flip | Phase 155 PR5 |
| Go CreateTopic return type | Phase 155 PR2 |

## API

Existing `join_group` / `join_group_with_instance` / `joinGroup`
signatures stay. Public wrappers share the guarded impl.

```python
c.join_group("g", topics=["t"])                          # empty; one shot
c.join_group("g", topics=["t"], member_id="m-1")         # retries
c.join_group("g", topics=["t"], group_instance_id="i-1") # retries
```

```go
c.JoinGroup("g", topics, 10000)                          // empty; one shot
c.JoinGroupMember("g", "m-1", topics, 10000)             // retries
c.JoinGroupWithInstance("g", topics, 10000, "i-1")       // retries
```

```java
c.joinGroup("g", List.of("t"), 10000);                   // empty; one shot
c.joinGroupMember("g", "m-1", List.of("t"), 10000);      // retries
c.joinGroupWithInstance("g", List.of("t"), 10000, "i-1"); // retries
```

```rust
client.join_group("g", "", 10_000, topics).await?; // one shot
client.join_group("g", "m-1", 10_000, topics).await?; // retries
client.join_group_with_instance("g", "", 10_000, topics, "i-1").await?;
```

## Semantics

- Both ids empty: one Join RPC even if `max_retries >= 1`.
- Stored `member_id` or static `group_instance_id`: Heartbeat-shaped
  loop. `max_retries=2`, backoff 0, first 7 then 0 → two Join RPCs.
- Error **14** on `max_redirects`; does not increment `retry_attempt`.
- 9 / 10 / 11 surface immediately.
- GroupConsumer inherits via the Client RPC.

## Tests

```bash
cargo test -p volant-client --test v205_join_group_retry -- --test-threads=1
cd clients/go && go test ./...
cd clients/java && mvn -q test
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_group -q
```

| Case | Expect |
|------|--------|
| empty member + empty instance + transient then ok | **1** Join RPC |
| non-empty member_id + transient then ok | **2** Join RPCs |
| empty member + non-empty instance + transient then ok | **2** Join RPCs |

HeartbeatCount stays 0 on these Join RPCs.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `join_group_with_instance` loop + empty-id helper |
| `crates/volant-client/src/lib.rs` | crate-doc sentence |
| `crates/volant-client/tests/v205_join_group_retry.rs` | fake TCP |
| `clients/python/src/volant/client.py` | `join_group` |
| `clients/go/client.go` | `joinGroup` |
| `clients/java/.../Client.java` | shared `joinGroup` |
| Language tests next to join/heartbeat retry | same 3 cases |
| Client READMEs | guard wording |
| `docs/V205_SPEC.md` | This spec |

## Honesty leftovers

- Empty first Join is still not retried.
- Broker may never emit 14 on Join; client-side wrap only.
- Default `max_retries=0`.
- Not Kafka `retries` / JoinGroup versions.
- No new opcodes / Kafka keys / openraft default change.

## Merge notes

Sibling PR2 edits Go `client.go` / README. Sibling PR4 edits protocol +
all clients. Keep Join retry hunk local to `joinGroup` /
`join_group_with_instance`. Keep both on conflicts.

## Related

- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — this is PR3
- [V74_SPEC.md](./V74_SPEC.md) — language Heartbeat retry
- [V80_SPEC.md](./V80_SPEC.md) — Rust Heartbeat retry
- [V135_SPEC.md](./V135_SPEC.md) — Heartbeat error 14
- [V137_SPEC.md](./V137_SPEC.md) — LeaveGroup error 14
