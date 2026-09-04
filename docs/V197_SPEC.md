# v0.197 — Python list_offsets_all

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V50_SPEC.md](./V50_SPEC.md) /
[V163_SPEC.md](./V163_SPEC.md) / [V166_SPEC.md](./V166_SPEC.md): Go
already has `ListOffsetsAll(topic)` → `ListOffsets(topic, nil)`. Rust
has `list_offsets_all(topic)` → `list_offsets(topic, Vec::new())`.
Java has `listOffsets(topic)` (no partitions). Python
`list_offsets(topic, partitions=None)` already lists all when
partitions is `None` / empty, but there is no named
`list_offsets_all` helper matching Go / Rust.

Add `Client.list_offsets_all`. Reuse `list_offsets` (do not
reimplement the RPC). `list_offsets(topic, partitions)` stays
unchanged. This is **not** Kafka ListOffsets.

This is residual **v0.197** (Python list_offsets_all). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Go, Java, or Rust.

## Goals

1. Add public `def list_offsets_all(self, topic: str) ->
   list[OffsetListing]` that calls `self.list_offsets(topic)`.
2. Inherit retry / error **13** from `list_offsets` (v0.82 transient
   retry + v0.112 error 13). No new retry policy.
3. Do **not** change `list_offsets(topic, partitions)`.
4. Do **not** change broker / protocol / Go / Java / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `list_offsets(topic, partitions)` | Frozen; `None` / `[]` already means all |
| Kafka ListOffsets (API key 2) isolation / timestamp | Native opcode 48/49 only |
| Kafka specials (max-timestamp, earliest-local, tiered) | Kafka shim only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Go / Java / Rust | Already have topic-only helpers (v0.163 / v0.50 / v0.166) |
| Phase 155 / homemade Raft | Frozen |

## API

```python
def list_offsets_all(self, topic: str) -> list[OffsetListing]:
    """List earliest/latest for every partition of ``topic``.

    Same as ``list_offsets(topic)`` / ``list_offsets(topic, None)``.
    Retry / error 13 inherit from ``list_offsets``.
    """
    return self.list_offsets(topic)
```

```python
bounds = c.list_offsets_all("events")                 # all partitions
same = c.list_offsets("events")                       # unchanged: same wire
same = c.list_offsets("events", None)                 # unchanged: same wire
filtered = c.list_offsets("events", [0, 1])
```

## Semantics

- Empty wire partitions = all partitions of the topic (same as
  today). `None` / `[]` already means all (wire count 0). The
  wrapper sends the same.
- `list_offsets_all` is a named wrapper; it does not re-encode.
- `list_offsets(topic, partitions)` is unchanged (`None` / empty
  still mean all).
- Transient 6 / 7 / 15 / 16 and transport retry via `list_offsets`
  (v0.82; default `max_retries=0`).
- Error 13 follows `max_redirects` (v0.112).
- Not Kafka ListOffsets (no timestamp or isolation); both ends of
  each log are returned. `latest` is LEO.

## Tests

Fake TCP stub that records decoded ListOffsets partitions (same helper
as existing `test_list_offsets.py`).

```bash
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_list_offsets -q
```

Do **not** run full Python `discover`. Do **not** append codec tests.

| Case | Expect |
|------|--------|
| `list_offsets_all("events")` | wire partitions empty (count 0); same as `list_offsets(topic)` |
| Existing `list_offsets` empty / explicit / retry / 13 cases | still pass |

Existing ListOffsets retry / 13 tests must still pass
(`list_offsets` unchanged).

| File | What |
|------|------|
| `clients/python/src/volant/client.py` | `list_offsets_all` wraps `list_offsets(topic)` |
| `clients/python/tests/test_list_offsets.py` | empty-partitions wire check |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V197_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** ListOffsets (API key 2). Native opcode **48/49**
  only. No isolation, timestamp, max-timestamp, earliest-local, or
  tiered specials.
- Empty partitions still list **all** partitions of the topic.
- `list_offsets(topic, partitions)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Go / Java / Rust / broker / protocol are unchanged.

## Merge notes

Sibling v0.196 / v0.198 also edit `clients/python/src/volant/client.py`
and the Python README. Keep this hunk local to `list_offsets_all`
after `list_offsets`:

- **Keep the wrapper only.** Do not change `list_offsets`.
- Do not change the ListOffsets send loop (v0.82 retry + v0.112 13).
- Do not change Go, Java, Rust, broker, or protocol.

Expect conflicts on:

- `clients/python/src/volant/client.py` — hunk is local to
  `list_offsets_all` after `list_offsets`
- `clients/python/tests/test_list_offsets.py`
- `clients/python/README.md`

## Related

- [V50_SPEC.md](./V50_SPEC.md) — language ListOffsets
- [V82_SPEC.md](./V82_SPEC.md) — language ListOffsets transient retry
- [V112_SPEC.md](./V112_SPEC.md) — language ListOffsets error 13
- [V163_SPEC.md](./V163_SPEC.md) — Go ListOffsetsAll (same wrapper pattern)
- [V166_SPEC.md](./V166_SPEC.md) — Rust list_offsets_all (same wrapper pattern)
- [PHASE15_SPEC.md](./PHASE15_SPEC.md) — native 48/49
