# v0.69 — client-side multi-member range via DescribeGroup

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V41_SPEC.md](./V41_SPEC.md): local
range assignor is still **solo**. `range_assign_multi` already exists
and is correct; `_local_range_assignment` / `localRangeAssignment`
only passed `[self.member_id]`.

JoinGroup still returns assignment/revoked only — **no member list**.
DescribeGroup (opcodes **34/35**, shipped [v0.49](./V49_SPEC.md))
**does** return members + topic subscriptions + assignment. Use that.
No new wire.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes
(there is still no SyncGroup), or change the broker / protocol / Rust
client.

## Goals

1. When `assignor == "range"` (already an opt-in on GroupConsumer),
   after a successful JoinGroup (so `member_id` / generation are set),
   call existing `describe_group` / `DescribeGroup` / `describeGroup`.
2. Build `member_ids` + per-member topic lists from DescribeGroup
   members.
   - Include **self** with `self._topics` if the describe reply omitted
     us (join just happened; describe can race).
   - A member with empty `topics`: skip them for topics they did not
     subscribe to (`range_assign_multi` already filters by
     subscription).
   - Stable order: pass members in describe order (or sorted —
     `range_assign` sorts by id internally, so either is fine). **Do
     not** invent a different assignor.
3. Call existing `range_assign_multi(member_ids, member_topics,
   partition_counts)` and take **this** member’s assignment. Partition
   counts still come from `metadata()` as today.
4. **Fallback to today’s solo `[self]`** if:
   - DescribeGroup raises / returns a broker error
   - members list is empty after including self
   - you cannot find self’s index
   Do **not** fail the join just because describe failed.
5. Cooperative / broker-assignment path (`assignor != range`) is
   **unchanged**. Do not call DescribeGroup there.
6. No new public methods. Existing `assignor="range"` /
   `WithAssignor("range")` / `joinWithAssignor` stay the entry point.
7. Default assignor stays broker-assignment. Range stays opt-in.

## Non-goals

| Deferred | Why |
|----------|-----|
| SyncGroup / member list on JoinGroup | Frozen; do not invent an opcode |
| Sticky / cooperative client assignor | Broker already has sticky |
| `earliest` via ListOffsets | Different residual; keep v0.62 (`earliest` = position 0) |
| Rust `GroupConsumer` multi-member range | Frozen (broker / protocol / Rust client) |
| New Kafka API keys / native opcodes | Frozen |
| Phase 155 / homemade metadata Raft | Frozen |
| Changing `range_assign_multi` | Algorithm is already correct |

## Behavior

```
assignor == "range" after successful JoinGroup
    │
    ├─ metadata() → partition counts (as today)
    │
    ├─ describe_group(group)
    │       │
    │       ├─ error / empty after including self / self missing
    │       │     → solo range_assign_multi([self], [self.topics], counts)
    │       │
    │       └─ members (describe order; append self + self.topics if omitted)
    │             → range_assign_multi(ids, topics, counts)[self]
    │
assignor != "range"
    └─ honor JoinGroup assignment; no DescribeGroup
```

Range for `n=4` partitions and sorted members `m-a`, `m-b`: first
gets `0–1`, second `2–3`.

## API

No new public methods. Existing:

```python
GroupConsumer.join(..., assignor="range")
```

```go
JoinGroupConsumer(..., WithAssignor("range"))
```

```java
GroupConsumer.join(..., "range")
GroupConsumer.joinWithAssignor(backend, ..., "range")
```

Default assignor remains `"broker"`.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| range + describe 2 members + 4 parts | this member gets the range half for their sorted rank |
| range + describe error | assignment = all partitions (solo fallback) |
| assignor default / cooperative | no DescribeGroup RPC |
| describe omits self | self is still included; assignment computed |

## Honesty leftovers

- **Still no SyncGroup.** Native JoinGroup does not return the member
  list. This slice reuses DescribeGroup (34/35) only.
- DescribeGroup can race the just-completed join (omit self). Clients
  append self with the local subscription. A describe **error** still
  falls back to solo, so two live range members may briefly overlap
  on every partition.
- **Not Kafka cooperative-sticky.** Range only; sticky stays broker.
- **Not kafka-python / kafka-clients / kafka-go.** Native protocol
  only.
- Rust `GroupConsumer` is unchanged (still broker assignment).
- `earliest` reset is still position 0 (v0.62); this slice does not
  call ListOffsets for earliest.

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/group.py` | `_local_range_assignment` + DescribeGroup members |
| `clients/go/group.go` | `localRangeAssignment` + DescribeGroup members |
| `clients/java/src/main/java/io/volant/GroupConsumer.java` | same; Backend `describeGroup` |
| `clients/python/tests/test_assignor.py` | 2-member / error / omit-self / no-describe |
| `clients/go/assignor_test.go` | same (fake TCP DescribeGroup on `group_test.go`) |
| `clients/java/src/test/java/io/volant/RangeAssignorTest.java` | same |
| `docs/V69_SPEC.md` | This spec |
