# v0.188 — Python GroupConsumer heartbeat_count

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V44_SPEC.md](./V44_SPEC.md) /
[V37_SPEC.md](./V37_SPEC.md): Rust already has
`GroupConsumer::heartbeat_count()` (poll + background Heartbeat
attempts; JoinGroup is not counted). Python has no public counter.

Expose `GroupConsumer.heartbeat_count`. Do **not** change join /
poll / heartbeat semantics except the counter.

This is residual **v0.188**. It is **not** Phase 155. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Rust, Go, or Java.

## Goals

1. **Python:** public `@property heartbeat_count -> int`. Return
   `self._heartbeat_count`.
2. Increment **once per Heartbeat attempt**, before the RPC,
   matching Rust. Increment only inside `_heartbeat()` (the method
   that calls `self._client.heartbeat(...)`).
3. `poll` and the background `_heartbeat_once` both call
   `_heartbeat()` under `_lock`. Do **not** double-count.
4. Do **not** increment on join (`_do_join`).
5. Do **not** change poll / heartbeat / join behavior beyond the
   counter.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change poll / heartbeat / join semantics | Frozen; counter only |
| Increment on JoinGroup | Frozen; JoinGroup is not a Heartbeat |
| SyncGroup / sticky / cooperative assignor | Frozen; still no SyncGroup |
| JoinGroup retry | Frozen |
| Go / Java / Rust counters | Rust already shipped; others out of slice |
| Kafka consumer heartbeat API | Native Heartbeat only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```python
@property
def heartbeat_count(self) -> int:
    """Heartbeat RPCs issued by poll + background (not JoinGroup)."""
    return self._heartbeat_count
```

```python
g = GroupConsumer.join(c, "g", ["t"], heartbeat=False)
_ = g.heartbeat_count  # 0 after join
g.poll()
_ = g.heartbeat_count  # 1 after one poll Heartbeat
```

Existing `join` / `poll` / `heartbeat=` signatures are unchanged.

## Semantics

- Counter starts at **0** in `__init__`.
- Increment once at the start of `_heartbeat()`, before
  `self._client.heartbeat(...)`. Failed Heartbeat attempts still
  count (same as Rust).
- `poll` calls `_heartbeat()` once at the start of the call.
- Background `_heartbeat_once` also calls `_heartbeat()` under the
  lock. One increment per attempt; no double-count.
- `_do_join` / JoinGroup does **not** increment.
- The getter reads the stored field. It does **not** send Heartbeat
  or JoinGroup.
- Not Kafka `heartbeat.interval.ms`.

## Tests

Existing group fakes (same `FakeClient` as v0.31 / v0.37):

| Case | Expect |
|------|--------|
| `GroupConsumer.join(..., heartbeat=False)` | `heartbeat_count == 0` before poll |
| After one `poll` | `heartbeat_count == 1` |

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_group -q
```

Do **not** change Rust / Go / Java / broker / protocol. Do **not**
run full Python discover. Do **not** run `tests.test_client`. Do
**not** run cargo workspace.

## Honesty leftovers

- **Still no SyncGroup.** Local `"range"` still uses DescribeGroup
  members.
- Counter only. Poll / heartbeat / join behavior is unchanged.
- Rust `heartbeat_count()` already exists (v0.44).
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Python `GroupConsumer` should keep
this hunk local to the counter:

- **Keep the increment inside `_heartbeat()` only.** Do not
  increment in `poll` or `_heartbeat_once` as well.
- Do not increment on join.
- Do not change poll / heartbeat / join semantics.
- Do not change Rust, Go, Java, broker, or protocol.

Expect conflicts on:

- Python `clients/python/src/volant/group.py` (`_heartbeat`,
  `__init__`, `assignor`)
- `clients/python/tests/test_group.py`
- `clients/python/README.md`

The hunk is local to the counter + existing group tests.

## Related

- [V37_SPEC.md](./V37_SPEC.md) — Python background heartbeat
- [V44_SPEC.md](./V44_SPEC.md) — Rust GroupConsumer heartbeat +
  `heartbeat_count()`
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer poll loop
- [V184_SPEC.md](./V184_SPEC.md) — leftover getter pattern
