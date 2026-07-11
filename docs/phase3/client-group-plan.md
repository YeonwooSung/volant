# Phase 3 Client — Group-Aware Consumer Plan

## Scope

- `crates/volant-client/**` primary ownership
- Light protocol touches in `volant-protocol` so group opcodes 6–10 encode/decode per `PHASE3_SPEC.md`
- Light broker `net.rs` match arms so workspace still compiles (NotImplemented until group coordinator lands)

## Protocol (required for client compile)

### Opcodes

| Opcode | Request / Response |
|--------|--------------------|
| 6 | OffsetCommit |
| 7 | OffsetFetch |
| 8 | JoinGroup |
| 9 | Heartbeat |
| 10 | LeaveGroup |

### Wire layout (LE integers; strings `u16 len` + UTF-8)

Per PHASE3_SPEC payloads for JoinGroup / Heartbeat / LeaveGroup / OffsetCommit / OffsetFetch.

### Error codes (additions)

| Code | Name |
|------|------|
| 9 | RebalanceInProgress |
| 10 | UnknownMemberId |
| 11 | IllegalGeneration |
| 12 | InconsistentGroupProtocol |

## Client API

### Low-level `Client` methods

```rust
Client::join_group(group_id, member_id, session_timeout_ms, topics) -> JoinGroupResult
Client::heartbeat(group_id, member_id, generation) -> Result<()>  // Err on rebalance etc.
Client::leave_group(group_id, member_id) -> Result<()>
Client::offset_commit(group_id, member_id, generation, entries) -> Result<()>
Client::offset_fetch(group_id, entries /* empty = all */) -> Vec<OffsetFetchResult>
```

Result types:
- `JoinGroupResult { generation, member_id, assignment: Vec<(String, u32)> }`
- `OffsetCommitEntry { topic, partition, offset, metadata }`
- `OffsetFetchResult { topic, partition, offset /* MAX = unknown */, metadata }`

Rebalance (`error_code=9`) maps to `Error::Protocol` with marker `"rebalance_in_progress"` so `GroupConsumer` can re-join.

### `GroupConsumer`

```rust
pub struct GroupConsumerConfig {
    pub group_id: String,
    pub session_timeout_ms: u32,  // default 10_000
    pub max_messages: u32,        // per partition per poll, default 100
    pub max_wait_ms: u32,         // fetch long-poll, default 0
}

impl GroupConsumer {
    pub async fn join(client: Arc<Client>, topics, config) -> Result<Self>;
    pub async fn poll(&mut self) -> Result<Vec<Record>>;
    pub async fn commit(&self) -> Result<()>;
    pub async fn leave(self) -> Result<()>;
    pub fn assignment(&self) -> &[(String, u32)];
}
```

### Poll loop (PHASE3_SPEC)

1. Heartbeat; if rebalance / illegal generation / unknown member → re-`JoinGroup`, `OffsetFetch`, reset positions
2. For each assigned partition, `Fetch` from current position (`max_messages`)
3. Track next-read offsets (last+1) for commit
4. On first assignment: OffsetFetch; unknown (`u64::MAX`) → start at 0

### Commit

Commit current positions (next offset to read) for all assigned partitions with current generation/member_id.

## Config

- `GroupConsumerConfig`: `group_id`, `session_timeout_ms`, `max_messages`, `max_wait_ms`
- Existing `ClientConfig` unchanged

## Tests

1. Protocol payload roundtrip for all five group/offset request+response types
2. Client encode/RPC path against mock TCP server handling opcodes 6–10
3. GroupConsumer poll path: join → heartbeat ok → fetch → position advance → commit
4. GroupConsumer rebalance: heartbeat returns code 9 → re-join + offset_fetch + resume

## Iteration log

- Iteration 1: plan + implement protocol + client + GroupConsumer + mock tests
  - Result: all `cargo test -p volant-client` green (11 tests); protocol group roundtrips green; review complete; no further iterations needed.
