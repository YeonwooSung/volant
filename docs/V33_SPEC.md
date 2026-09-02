# v0.33 — Java OffsetCommit / OffsetFetch + GroupConsumer

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Java has JoinGroup (v0.28) but no
OffsetCommit/Fetch (v0.24 was Python/Go only), and no high-level
consumer” by exposing native **OffsetCommit** (opcode 6),
**OffsetFetch** (opcode 7), and a Rust-shaped **GroupConsumer**.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker protocol.

## Goals

1. **Java** `Client.offsetCommit` / `Client.offsetFetch` matching
   `crates/volant-protocol/src/payload.rs` (little-endian; same wire as
   Python/Go v0.24).
2. **Admin commit path:** empty `memberId`, `generation = 0` (skip
   generation check), same as the CLI / Rust `commit_offsets`.
3. **GroupConsumer** with the same semantics as
   `crates/volant-client/src/group.rs`: join, OffsetFetch assigned
   partitions, heartbeat on poll, commit with member+generation, rejoin
   on heartbeat error 9 (and 10/11, matching Rust `needs_rebalance`).
4. **BrokerException** on nonzero `error_code` (same as produce/fetch).
5. **Codec unit tests** with exact-byte fixtures from `payload.rs`.
6. **GroupConsumer unit tests** against a mock backend (no broker).
7. **E2E** gated by `VOLANT_E2E=1`: admin commit → fetch; GroupConsumer
   poll → commit → resume. Skip if no server.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python / Go GroupConsumer | This slice is Java only |
| Cooperative assignor client logic | Broker already assigns; client retains sticky positions |
| Static membership (`group_instance_id`) | Rust has `join_static`; Java join still sends empty |
| Kafka OffsetCommit / OffsetFetch API keys | Native opcodes 6/7; no Kafka keys |
| Multi-entry convenience API | One topic/partition per `offsetCommit` is enough |
| TLS / SCRAM / shared-token Auth | Unchanged plaintext MVP |
| Required CI language job | Existing optional smoke scripts only |
| Broker / protocol changes | Wire already exists |

## Wire

Unchanged from Phase 3 / `payload.rs`. Payloads are little-endian.
Strings are `u16_le` length + UTF-8.

### OffsetCommit request (opcode 6)

```
group_id: string
member_id: string          # empty for admin commits
generation: u32            # 0 = skip generation check
entry_count: u32
entries: repeated {
  topic: string
  partition: u32
  offset: u64              # next offset to read
  metadata: string         # may be empty
}
```

### OffsetCommit response

```
error_code: u16
```

### OffsetFetch request (opcode 7)

```
group_id: string
entry_count: u32           # 0 = all committed offsets for the group
entries: repeated {
  topic: string
  partition: u32
}
```

### OffsetFetch response

```
error_code: u16
entry_count: u32
entries: repeated {
  topic: string
  partition: u32
  offset: u64              # u64::MAX = unknown / not committed
  metadata: string
}
```

The convenience `offsetFetch(group, topic)` sends empty OffsetFetch
entries (all group offsets), then filters to that topic client-side
(same as the CLI / Python / Go).

## API

```java
c.offsetCommit("g", "t", 0, 5);
List<Offset> offs = c.offsetFetch("g", "t");

GroupConsumer g = GroupConsumer.join(c, "g", List.of("t"), 10_000);
List<Record> recs = g.poll(500);
g.commit();
g.close();
```

- `offsetCommit(group, topic, partition, offset)` is the admin path.
  An overload accepts `memberId` + `generation` for a joined member.
- `offsetFetch` returns `List<Offset>` (`partition`, `offset`).
- `GroupConsumer.join` sends empty `memberId` on first join (broker
  assigns one). `sessionTimeoutMs` 0 defaults to 10000.
- `poll(timeoutMs)` heartbeats, rejoins on error 9/10/11, then fetches
  assigned partitions. `timeoutMs` is Fetch `max_wait_ms` on the first
  assigned partition (0 = non-blocking).
- `commit` sends last+1 positions with the current member+generation.
- `close` leaves the group; it does not close the underlying `Client`.

Unknown OffsetFetch (`u64::MAX`) starts at 0. On rebalance, revoked
positions are dropped; newly assigned partitions are OffsetFetched;
sticky-kept partitions keep their local position.

## Tests

| File | What |
|------|------|
| `clients/java/src/test/java/io/volant/CodecTest.java` | Exact-byte OffsetCommit/OffsetFetch fixtures |
| `clients/java/src/test/java/io/volant/GroupConsumerTest.java` | Join / poll / commit / rejoin-on-9 against a mock |
| `clients/java/src/test/java/io/volant/E2ETest.java` | Live admin commit/fetch + GroupConsumer resume; skip unless `VOLANT_E2E=1` |

```bash
cd clients/java && mvn -q test
# live broker:
cargo build -p volant-server
VOLANT_E2E=1 mvn -q -f clients/java/pom.xml test
```

## Honesty leftovers

- No Python / Go GroupConsumer (those clients still have the thin RPCs
  only).
- Public `joinGroup` still sends empty `memberId` / empty
  `group_instance_id` (first join). `GroupConsumer` sends the assigned
  member on rejoin; static membership is not exposed.
- Convenience `offsetCommit` is admin-only (`generation=0`).
  `GroupConsumer.commit` is the joined path.
- OffsetFetch topic filter is client-side (empty wire entries).
- `poll` returns a flat `List<Record>` (no topic/partition on `Record`).
  `timeoutMs` is first-partition `max_wait_ms`, not a Kafka-style wait
  loop across all assigned partitions.
- Client `heartbeat` still throws `BrokerException` on error 9;
  `GroupConsumer` catches 9/10/11 and rejoins.
- Still no Kafka-wire SDK, SCRAM, or leader redirect on this client.
- Broker and Rust `volant-client` are unchanged.

See [clients/java/README.md](../clients/java/README.md).
