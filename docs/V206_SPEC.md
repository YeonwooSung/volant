# v0.206 — native SyncGroup opcodes 116/117

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Phase 155 PR4. Add native **SyncGroup** opcodes **116 / 117**
as a peek/confirm of the JoinGroup assignment. This is **not** Kafka
CompletingRebalance. Kafka API key **14** already exists in
`SUPPORTED_APIS` (38 keys) and is **unchanged**.

This **is** Phase 155 PR4. It does **not** flip openraft defaults, grow
homemade Raft, add a 39th Kafka key, or change GroupConsumer's default
fetch set (still JoinGroup assignment). Range assignor stays
DescribeGroup.

## Goals

1. Native request opcode **116** / response opcode **117**.
2. Broker: same membership/generation check as Heartbeat, then return
   this member's current `assignment()`. No new `GroupState`. Do not
   apply leader assignment bytes.
3. Thin public APIs on Rust / Python / Go / Java clients.
4. Inherit Heartbeat retry: transient 6/7/15/16 + TCP on `max_retries`
   (default 0). Error **9 / 10 / 11** not retried. Error **14** on
   `max_redirects`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka CompletingRebalance / PreparingRebalance | Coordinator rewrite; Empty/Stable only |
| Apply leader assignment bytes | Broker ignores them (same honesty as Kafka shim key 14) |
| New Kafka API key | Key 14 already in `SUPPORTED_APIS` (38) |
| Change GroupConsumer default path | Still uses JoinGroup assignment |
| Range assignor via SyncGroup | Still DescribeGroup |
| Flip openraft / grow homemade Raft | Other 155 PRs |

## Wire

Request opcode **116** `RequestOpcode::SyncGroup`:

- string `group_id` (u16 LE length + UTF-8)
- string `member_id`
- u32 LE `generation`
- u32 LE `assignment_bytes_len` + that many bytes (MVP: clients write 0 /
  empty; broker ignores the bytes)

Response opcode **117** `ResponseOpcode::SyncGroup`:

- u16 LE `error_code`
- then the same assignment list encoding as JoinGroup (`assignment[]` of
  `Assignment { topic, partition }`)

Magic/CRC frame is the existing 16-byte native frame.

## Broker

`Request::SyncGroup` calls the same `GroupCoordinator::heartbeat`
membership/generation check. Unknown member → error **10**. Generation
mismatch → error **9**. On success, return this member's current
`assignment()` (already computed on Join). Empty/Stable only. Leader
assignment bytes are not applied.

## Clients

```rust
client.sync_group(group_id, member_id, generation).await?; // Vec<Assignment>
```

```python
asgn = c.sync_group("g", member_id, generation)  # list[Assignment]
```

```go
asgn, err := c.SyncGroup("g", memberID, generation) // []Assignment
```

```java
List<Codec.Assignment> asgn = c.syncGroup("g", memberId, generation);
```

Error 9/10/11 surface to the caller (not retried). Error 14 follows
`max_redirects` like Heartbeat.

## Tests

```bash
cargo test -p volant-protocol -- --test-threads=1
cargo test -p volant-client --test v206_sync_group -- --test-threads=1
cd clients/go && go test ./...
cd clients/java && mvn -q test
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_sync_group -q
```

| Case | Expect |
|------|--------|
| Encode/decode 116/117 | Round-trip; empty assignment bytes |
| SyncGroup after Join | Same partitions as JoinGroup |
| Unknown member | error **10** |
| Generation mismatch | error **9** |
| `SUPPORTED_APIS.len()` | still **38**; key **14** still advertised |

## Honesty leftovers

- SyncGroup is peek, not CompletingRebalance.
- Leader assignment bytes are ignored.
- GroupConsumer default path still uses JoinGroup assignment.
- Range assignor is still DescribeGroup (no generation barrier).
- Kafka stays 38 keys. Key 14 unchanged.
- Empty/Stable only. No PreparingRebalance.

## Merge notes

PR2/PR3 also touch Go `client.go` / Java `Client.java` / Python
`client.py` / READMEs. Keep SyncGroup hunks local (new methods + codec
types + dispatch arm). Keep both on conflicts.

Java: extract by brace matching. Python dataclasses MUST have
`@dataclass`.

## Related

- [PHASE155_SPEC.md](./PHASE155_SPEC.md) — Phase 155
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka SyncGroup honesty
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — shim matrix (key 14 already listed)
- [V18_SPEC.md](./V18_SPEC.md) — native ReassignPartitions 114/115
