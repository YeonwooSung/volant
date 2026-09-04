# v0.211 — JoinGroup members trailer for range assignor

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Optional JoinGroup response trailer of live member ids at this
generation, so `assignor="range"` does not need a racy DescribeGroup.

This is **not** Kafka SyncGroup CompletingRebalance. Not a new opcode.
Not a Kafka API key. It does **not** flip openraft defaults or grow
homemade Raft.

## Goals

1. After the existing Phase 17 revoked list, encode an optional
   `member_count` + member-id strings.
2. Broker fills **all live member ids** (including the joiner), stable
   sort. Empty group cannot happen on success.
3. Clients decode the trailer into `JoinGroupResult.members`. Encode
   always writes the trailer (tests / fakes).
4. Range assignor uses those ids (plus metadata for partition counts)
   when the list is non-empty. Empty / missing keeps today's
   DescribeGroup fallback.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CompletingRebalance / PreparingRebalance | Coordinator rewrite; Empty/Stable only |
| New opcode / Kafka API key | Trailer on existing JoinGroup response |
| Apply leader assignment bytes | Unrelated; SyncGroup peek is v0.206 |
| Flip openraft / grow homemade Raft | Other 155 PRs |

## Wire

After the Phase 17 revoked list:

- u32 LE `member_count`
- `member_count` times string (u16 LE length + UTF-8 `member_id`)

Legacy payloads without the trailer: `members = empty` (range falls
back to DescribeGroup).

## Broker

When building a JoinGroup response, include all live member ids in the
current group (including the joiner), stable sort.

## Clients

```rust
result.members // Vec<String>; empty on legacy payloads
```

```python
result.members  # list[str]
```

```go
result.Members // []string
```

```java
result.members // List<String>
```

Range:

```
assignor == "range" after successful JoinGroup
    │
    ├─ result.members non-empty
    │     → metadata() → partition counts
    │     → range_assign_multi(ids, [self.topics] * n, counts)[self]
    │     → no DescribeGroup
    │
    └─ empty / missing
          → today's DescribeGroup fallback
```

## Tests

```bash
cargo test -p volant-protocol -- --test-threads=1
cargo test -p volant-client --lib -- --test-threads=1
cd clients/go && go test ./...
cd clients/java && mvn -q test
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_group tests.test_assignor -q
```

| Case | Expect |
|------|--------|
| Encode/decode JoinGroup | Round-trip includes members |
| Decode without trailer | `members = []` |
| Range + members present | Uses those ids; no DescribeGroup |
| Range + empty members | DescribeGroup fallback still works |

## Honesty leftovers

- Still not Kafka CompletingRebalance.
- Per-member subscriptions are not on the trailer (range uses this
  consumer's topics for every id).
- Default assignor stays broker JoinGroup assignment.
- Kafka stays 38 keys.

## Merge notes

v0.207/v0.208 add SyncGroup after join in GroupConsumer — keep the
range hunk separate. v0.209/v0.210 change request `member_id`, not
response. Codec `JoinGroupResponse`: add `members` field with default
empty. Keep both on conflicts.

Java: extract by brace matching. Python dataclasses MUST have
`@dataclass`.

## Related

- [V69_SPEC.md](./V69_SPEC.md) — language range via DescribeGroup
- [V73_SPEC.md](./V73_SPEC.md) — Rust range via DescribeGroup
- [V206_SPEC.md](./V206_SPEC.md) — native SyncGroup peek
- [PHASE17_SPEC.md](./PHASE17_SPEC.md) — revoked trailer
