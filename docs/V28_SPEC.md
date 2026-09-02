# v0.28 — JoinGroup / Heartbeat / LeaveGroup on native clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python/Go clients have offsets (v0.24) but no
JoinGroup” by exposing native **JoinGroup** (opcode 8), **Heartbeat**
(opcode 9), and **LeaveGroup** (opcode 10) on the native clients.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker protocol.

## Goals

1. **Python** `Client.join_group` / `Client.heartbeat` / `Client.leave_group`
   matching `crates/volant-protocol/src/payload.rs`.
2. **Go** `Client.JoinGroup` / `Client.Heartbeat` / `Client.LeaveGroup`
   with the same little-endian payloads.
3. **Java** same opcodes (small; same wire as Python/Go).
4. Empty `member_id` on first join (broker assigns one). Field names
   match the Rust client (`member_id` / `MemberID`, `generation`,
   `assignment`, `revoked`).
5. **BrokerError** on nonzero `error_code` (same as produce/fetch/offsets).
6. **Codec unit tests** with exact-byte fixtures from `payload.rs`.
7. **E2E** gated by `VOLANT_E2E=1`: join → heartbeat → leave. Skip if no
   server.

## Non-goals

| Deferred | Why |
|----------|-----|
| High-level GroupConsumer / poll loop | Rust-only; this slice is the three RPCs |
| Cooperative assignor client logic | Broker already assigns; clients just return it |
| Kafka JoinGroup / Heartbeat / LeaveGroup API keys | Native opcodes 8/9/10; no Kafka keys |
| Offset commit with joined member (required) | v0.24 admin path still works; Python can pass member/generation |
| TLS / SCRAM / shared-token Auth | Unchanged plaintext MVP |
| Required CI language job | Existing optional smoke scripts only |
| Broker / protocol changes | Wire already exists |

## Wire

Unchanged from Phase 3 / 12 / 17 / `payload.rs`. Payloads are little-endian.
Strings are `u16_le` length + UTF-8.

### JoinGroup request (opcode 8)

```
group_id: string
member_id: string          # empty = new member
session_timeout_ms: u32
topic_count: u32
topics: repeated string
group_instance_id: string  # Phase 12 trailer; always written; empty = dynamic
```

Legacy payloads without the instance trailer still decode (`group_instance_id = ""`).

### JoinGroup response

```
error_code: u16
generation: u32
member_id: string
assignment_count: u32
assignment: repeated { topic: string, partition: u32 }
revoked_count: u32         # Phase 17 trailer; always written
revoked: repeated { topic: string, partition: u32 }
```

Legacy responses without the revoked trailer decode as empty `revoked`.

### Heartbeat request (opcode 9)

```
group_id: string
member_id: string
generation: u32
```

### Heartbeat response

```
error_code: u16            # 0 = ok; 9 = rebalance (client should re-JoinGroup)
```

### LeaveGroup request (opcode 10)

```
group_id: string
member_id: string
```

### LeaveGroup response

```
error_code: u16
```

## API

```python
member_id, generation, assignment = c.join_group("g", topics=["t"], session_timeout_ms=10000)
c.heartbeat("g", member_id, generation)
c.leave_group("g", member_id)
```

```go
j, err := c.JoinGroup("g", []string{"t"}, 10000)
err = c.Heartbeat("g", j.MemberID, j.Generation)
err = c.LeaveGroup("g", j.MemberID)
```

```java
JoinGroupResult j = c.joinGroup("g", List.of("t"), 10000);
c.heartbeat("g", j.memberId, j.generation);
c.leaveGroup("g", j.memberId);
```

Python also accepts optional `member_id=` / `group_instance_id=` on
`join_group` for rejoin / static membership. `session_timeout_ms=0`
defaults to 10000 (same as the Rust `GroupConsumer`).

`JoinGroupResult` fields: `member_id` / `MemberID`, `generation`,
`assignment`, `revoked`. Python unpacks as
`(member_id, generation, assignment)`.

## Tests

| File | What |
|------|------|
| `clients/python/tests/test_codec.py` | Exact-byte Join/Heartbeat/Leave fixtures |
| `clients/python/tests/test_e2e.py` | Live join → heartbeat → leave; skip unless `VOLANT_E2E=1` |
| `clients/go/codec/codec_test.go` | Same fixtures |
| `clients/go/e2e_test.go` | Live join → heartbeat → leave; skip unless `VOLANT_E2E=1` |
| `clients/java/src/test/java/io/volant/CodecTest.java` | Same fixtures |
| `clients/java/src/test/java/io/volant/E2ETest.java` | Live join → heartbeat → leave; skip unless `VOLANT_E2E=1` |

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
# if Java was touched:
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- No high-level GroupConsumer / poll / cooperative commit loop on
  Python/Go/Java (Rust `volant-client` still has that).
- Go/Java `JoinGroup` always send empty `member_id` / empty
  `group_instance_id` (first join). Python can pass them as kwargs.
- Heartbeat nonzero `error_code` is `BrokerError` (including 9 =
  rebalance). The Rust client returns `HeartbeatResult` instead of
  failing so a GroupConsumer can rejoin; these thin clients do not.
- Java still has no OffsetCommit / OffsetFetch (v0.24 leftover).
- Still no Kafka-wire SDK, TLS, or leader redirect on these clients.
- Broker and Rust `volant-client` are unchanged.

See [clients/python/README.md](../clients/python/README.md),
[clients/go/README.md](../clients/go/README.md), and
[clients/java/README.md](../clients/java/README.md).
