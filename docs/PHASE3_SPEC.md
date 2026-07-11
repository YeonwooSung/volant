# Phase 3 — Consumer Groups & Offsets (binding)

## Goals

- Multiple consumers in one group split topic partitions
- Commit/fetch offsets; restart resumes from committed positions
- Eager rebalance on join/leave/session timeout (server-side range assignor)
- No stuck partitions after rebalance

## Design: server-side coordinator

Single-node broker owns group membership and assignment (no client-side leader election).

### State (in-memory + durable offsets)

```
Group {
  group_id: String,
  generation: u32,            // increments on membership change
  members: Map<member_id, Member>,
  subscriptions: // union of member topics
}
Member {
  member_id: String,          // broker-assigned uuid if client sends empty
  session_timeout_ms: u32,
  last_heartbeat: Instant,
  topics: Vec<String>,        // subscribed topic names
  assignment: Vec<(topic, partition)>,
}
```

**Offsets:** durable store under `{data_dir}/__consumer_offsets/{group_id}/{topic}/{partition}` as a small file containing `u64 offset` (LE) + optional metadata string, OR a single append-only offsets log. **Prefer simple files** for Phase 3 clarity + fsync on commit.

Do **not** require a user-visible `__consumer_offsets` topic unless easy; file-backed offset store is acceptable if documented.

## New opcodes

| Opcode | Request | Response |
|--------|---------|----------|
| 6 | OffsetCommit | OffsetCommit |
| 7 | OffsetFetch | OffsetFetch |
| 8 | JoinGroup | JoinGroup |
| 9 | Heartbeat | Heartbeat |
| 10 | LeaveGroup | LeaveGroup |

Payloads: multi-byte integers **little-endian** (same as Phase 2). Strings: `u16 len` + UTF-8.

### Error codes (additions)

| Code | Name |
|------|------|
| 9 | RebalanceInProgress |
| 10 | UnknownMemberId |
| 11 | IllegalGeneration |
| 12 | InconsistentGroupProtocol |

Existing Error response frame still used for hard failures. Group RPCs also embed `error_code: u16` in success responses (0 = ok).

### JoinGroup request

```
group_id: string
member_id: string          # empty = new member
session_timeout_ms: u32    # e.g. 10000
topic_count: u32
topics: repeated string
```

### JoinGroup response

```
error_code: u16
generation: u32
member_id: string
assignment_count: u32
assignment: repeated {
  topic: string
  partition: u32
}
```

On join: add/update member, bump generation, **reassign all members** with range assignor, return this member's assignment.

### Heartbeat request

```
group_id: string
member_id: string
generation: u32
```

### Heartbeat response

```
error_code: u16   # 0 ok; 9 rebalance (generation mismatch / membership changed — client should JoinGroup again)
```

Update `last_heartbeat`. Background task expires members where `now - last_heartbeat > session_timeout_ms`, then rebalance.

### LeaveGroup request/response

```
// req
group_id: string
member_id: string

// resp
error_code: u16
```

Remove member, bump generation, reassign remaining.

### OffsetCommit request

```
group_id: string
member_id: string          # may be empty for admin commits in tests; prefer required
generation: u32            # 0 = skip generation check (admin/cli); else must match
entry_count: u32
entries: repeated {
  topic: string
  partition: u32
  offset: u64              # next offset to read (committed position)
  metadata: string         # may be empty
}
```

### OffsetCommit response

```
error_code: u16
```

Persist offsets; fsync.

### OffsetFetch request

```
group_id: string
// empty entry_count means all committed offsets for group
entry_count: u32
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
  offset: u64          # u64::MAX means unknown / not committed
  metadata: string
}
```

## Range assignor

For each subscribed topic with `n` partitions and `m` members subscribed to it (stable-sorted by member_id):

```
base = n / m
extra = n % m
member i gets `base + (i < extra ? 1 : 0)` partitions in order
```

Members not subscribed to a topic get nothing for that topic.

## Broker modules

```
volant-broker/
  group.rs       # GroupCoordinator
  offset_store.rs
  assignor.rs    # range_assign
  net.rs         # dispatch new opcodes
```

```rust
impl GroupCoordinator {
  pub fn join(...) -> JoinResult;
  pub fn heartbeat(...) -> HeartbeatResult;
  pub fn leave(...);
  pub fn commit_offsets(...);
  pub fn fetch_offsets(...);
  pub fn expire_sessions(&self); // called periodically
}
```

`Broker` holds `Arc<GroupCoordinator>` or embeds it. Net layer calls coordinator.

Session expiry: either tokio interval in `serve` loop, or check on each group RPC.

## Client API

```rust
pub struct GroupConsumer { ... }

impl GroupConsumer {
  pub async fn join(client: Arc<Client>, group_id, topics, session_timeout) -> Result<Self>;
  pub async fn poll(&mut self) -> Result<Vec<Record>>; // heartbeat + fetch assigned partitions
  pub async fn commit(&self) -> Result<()>;           // commit last+1 per partition
  pub async fn leave(self) -> Result<()>;
  pub fn assignment(&self) -> &[(String, u32)];
}
```

Poll loop:
1. Heartbeat; if rebalance error → re-JoinGroup, reset positions from OffsetFetch
2. For each assigned partition, Fetch from current offset
3. Track high-water positions for commit

On first assignment: OffsetFetch; if unknown use 0 (earliest).

## CLI

```
volant group commit --group G --topic T --partition P --offset N --broker ...
volant group fetch-offsets --group G [--topic T --partition P] --broker ...
volant consume ... --group G   # optional: join group, poll once or N messages, commit
```

## Tests (required)

1. Protocol roundtrip for all new request/response types
2. Range assignor unit tests (uneven partitions)
3. Two members join → disjoint full partition cover
4. Leave → remaining member gets all partitions
5. Offset commit + fetch durable across coordinator recreate / process reopen
6. E2E TCP: two GroupConsumers split partitions; commit; new consumer resumes

## Non-goals

- Cooperative sticky assignor (document as Phase 3.1)
- Static membership / incremental cooperative rebalance
- Cross-node coordinator
- Transactional offsets
