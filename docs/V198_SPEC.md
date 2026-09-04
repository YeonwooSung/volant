# v0.198 — Python reassign_partitions_all

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V59_SPEC.md](./V59_SPEC.md) /
[V167_SPEC.md](./V167_SPEC.md) / [V168_SPEC.md](./V168_SPEC.md): Go
already has `ReassignAllPartitions(topic, replicas)` wrapping
`ReassignPartitions(topic, replicas, nil)`. Rust has
`reassign_partitions_all` (v0.168). Java has
`reassignPartitions(topic, replicas)` (null partition). Python
`reassign_partitions(topic, replicas, partition=None)` already
reassigns all when `partition` is `None` (wire `REASSIGN_ALL_PARTITIONS`
= `0xFFFFFFFF`), but there is no named `reassign_partitions_all`
helper matching Go/Rust.

Add `Client.reassign_partitions_all`. Reuse `reassign_partitions`
(do not reimplement the RPC). `reassign_partitions` stays unchanged.
This is **not** Kafka AlterPartitionReassignments.

This is residual **v0.198** (Python reassign_partitions_all). It is
**not** Phase 155. It does **not** open Phase 155, add Kafka API keys,
add native opcodes, or change the broker, protocol, Go, Java, or Rust.

## Goals

1. Add public `def reassign_partitions_all(self, topic: str,
   replicas: list[int]) -> int` that calls
   `reassign_partitions(topic, replicas)` (default `partition=None`;
   wire partition `REASSIGN_ALL_PARTITIONS` = `u32::MAX`).
2. Return `int` generation (same as `reassign_partitions`).
3. Inherit retry / error **14** from `reassign_partitions`
   (`_admin_round_trip`: v0.103 transient retry + v0.72 error 14).
   No new retry policy.
4. Do **not** change `reassign_partitions`.
5. Do **not** change broker / protocol / Go / Java / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `reassign_partitions` | Frozen; `None` already means all |
| Kafka AlterPartitionReassignments (API key 45) | Native opcode 114/115 only |
| Overlay / assignment wait-rollback | Broker-side (v0.18 / v0.39); overlay remains SoT |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Go `ReassignAllPartitions` | Already shipped **v0.167** |
| Rust `reassign_partitions_all` | Already shipped **v0.168** |
| Java `reassignPartitions(topic, replicas)` | Already has topic+replicas overload (v0.59) |
| Phase 155 / homemade Raft | Frozen |

## API

```python
def reassign_partitions_all(self, topic: str, replicas: list[int]) -> int:
    """Reassign every partition of ``topic``.

    Same as ``reassign_partitions(topic, replicas)`` /
    ``reassign_partitions(topic, replicas, None)``.
    Error 14 / transient retry inherit from ``reassign_partitions``.
    """
    return self.reassign_partitions(topic, replicas)
```

```python
gen = c.reassign_partitions_all("events", [1, 2])           # all partitions
gen = c.reassign_partitions("events", [1, 2])               # unchanged: same wire
gen = c.reassign_partitions("events", [1, 2], None)         # unchanged: same wire
gen = c.reassign_partitions("events", [1, 2], partition=0)
gen = c.reassign_partitions_all("events", [])               # auto-place
```

## Semantics

- `partition=None` / `REASSIGN_ALL_PARTITIONS` (`u32::MAX`) applies
  to every partition of the topic (same as today). The wrapper sends
  the same wire as `reassign_partitions(topic, replicas)`.
- `reassign_partitions_all` is a named wrapper; it does not re-encode.
- Empty `replicas` still means auto-place with current membership
  (same as CreateTopic). Unchanged.
- `reassign_partitions(topic, replicas, partition=None)` is unchanged
  (`None` still means all).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `reassign_partitions` / `_admin_round_trip` (v0.103; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.72).
- Overlay remains source of truth. Not Kafka
  AlterPartitionReassignments (no throttle, no per-partition error
  array, no TopicId).

## Tests

Fake TCP stub that records decoded ReassignPartitions topic /
partition / replicas (same `_ReassignPartitionsServer` as existing
`test_reassign_partitions.py`).

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_reassign_partitions -q
```

| Case | Expect |
|------|--------|
| `reassign_partitions_all("events", [1, 2])` | partition `REASSIGN_ALL_PARTITIONS` (`u32::MAX`), replicas `[1, 2]` |
| Existing `reassign_partitions` `None` / explicit / error cases | still pass |

Existing ReassignPartitions retry / 14 tests must still pass
(`reassign_partitions` unchanged).

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | `reassign_partitions_all` wraps `reassign_partitions` |
| `clients/python/tests/test_reassign_partitions.py` | fake TCP all-partition wire check |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V198_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** AlterPartitionReassignments (API key 45). Native
  opcode **114/115** only. No throttle, per-partition errors, or
  TopicId.
- Overlay still SoT.
- `None` / `u32::MAX` still reassigns **all** partitions of the topic.
- Empty `replicas` still auto-places.
- `reassign_partitions(...)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Go / Java / Rust / broker / protocol are unchanged.

## Merge notes

Sibling **v0.196** / **v0.197** also edit
`clients/python/src/volant/client.py` and the Python README. Keep this
hunk local to `reassign_partitions_all` after `reassign_partitions`:

- **Keep the named wrapper only.** Do not change `reassign_partitions`.
- Do not change the ReassignPartitions send loop (v0.103 retry +
  v0.72 14).
- Do not change Go, Java, Rust, broker, or protocol.

Expect conflicts on:

- `clients/python/src/volant/client.py` — hunk is local to
  `reassign_partitions_all` after `reassign_partitions`
- `clients/python/tests/test_reassign_partitions.py`
- `clients/python/README.md`

## Related

- [V59_SPEC.md](./V59_SPEC.md) — language ReassignPartitions
- [V72_SPEC.md](./V72_SPEC.md) — language admin error 14
- [V103_SPEC.md](./V103_SPEC.md) — language admin_round_trip transient retry
- [V167_SPEC.md](./V167_SPEC.md) — Go ReassignAllPartitions (same wrapper pattern)
- [V168_SPEC.md](./V168_SPEC.md) — Rust reassign_partitions_all (same wrapper pattern)
- [V18_SPEC.md](./V18_SPEC.md) — native ReassignPartitions 114/115
