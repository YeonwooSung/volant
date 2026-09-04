# Phase 155 — openraft metadata SoT + native SyncGroup + Join retry

**Status:** Open — PR1–PR5 landed on main (crate **0.2.0**). Overlay membership still SoT; homemade 154 not deleted.  
**Theme:** Change the v0.2 product bets that leftover residuals could not
touch: replace homemade 150/152/154 as the cluster metadata story with
**openraft** (already in-tree, opt-in through v0.40), add native
**SyncGroup** opcodes **116/117**, retry JoinGroup only when it is
safe, and make Go `CreateTopic` return the topic id.

This **is** Phase 155. Residual **v0.155** (DeleteRecords wait config)
is unrelated.

## Goals

1. **Openraft is cluster metadata SoT.** With `--cluster-config`,
   `VOLANT_OPENRAFT_METADATA` defaults **on**. CreateTopic /
   DeleteTopic / CreatePartitions succeed only after
   `client_write(SetAssignment)` commits and applies.
   `{data_dir}/cluster/assignment.json` is the apply artifact, not
   the client-visible SoT.
2. **Single-node unchanged.** No cluster config → do not start
   openraft. `controller_id()` is the local `node_id`.
3. **Do not grow homemade 154.** No RequestVote, no InstallSnapshot,
   no compaction on `__metadata_raft`. `VOLANT_METADATA_RAFT` stays
   default **off**. Code remains for tests / escape hatch.
4. **Native SyncGroup 116/117** is a peek/confirm (same honesty as
   the Kafka shim key **14**, which already exists in the 38-key
   table). Not Kafka CompletingRebalance.
5. **JoinGroup retry** only when `member_id` or `group_instance_id`
   is non-empty. Empty first join is still one shot.
6. **Go `CreateTopic` / `CreateTopicDefault` return `(uint32, error)`.**
   `CreateTopicID` becomes an alias.

## Non-goals

| Deferred | Why |
|----------|-----|
| Homemade 154 RequestVote / InstallSnapshot | Freeze choice C; replace, do not finish |
| Overlay membership as raft voter SoT | Out of 155; `{data_dir}/cluster/membership.json` stays membership SoT |
| Kafka two-phase join/sync states | Coordinator rewrite; shim SyncGroup already ignores leader payload |
| JoinGroup native member list | Frozen; range still uses DescribeGroup |
| Retry empty-`member_id` first Join | Ghost member + generation++ |
| New Kafka API key / version ratchet | SyncGroup **14** is already in `SUPPORTED_APIS` (38 keys) |
| librdkafka / kafka-python / kcat claims | Still not claimed |
| Distributed EOS / windows / KIP-890 | Unchanged |
| Crate 0.3.0 | After 155 ships, not during |

## Semantics

### Openraft default

- Unset env in cluster mode → **on**.
- `0` / `false` / `off` / `no` → off (lowest-id + `assignment.json` write, v0.2 path).
- `1` / `true` / `on` / `yes` → on.
- Flag true + no cluster config → do not start the raft engine;
  `controller_id()` stays `node_id`.
- Cluster + on: `controller_id()` is the openraft leader (`0` if none yet).
- Cluster + on: `assignment_must_wait()` is **true**. `client_write`
  failure rolls back live assignment and returns native **15**
  (Kafka admin **19**).

### Native SyncGroup (116/117)

Request: `group_id`, `member_id`, `generation`, assignment bytes
(ignored; empty allowed).  
Response: `error_code`, `assignment[]` (same `Assignment` as JoinGroup).  
Broker: `heartbeat()` membership/generation check, then
`assignment()`. No new `GroupState`.  
Clients: `sync_group` / `SyncGroup` / `syncGroup`. GroupConsumer may
call it after join; default fetch set remains JoinGroup assignment.

### JoinGroup retry

Same transient set as Heartbeat (6/7/15/16 + TCP), `max_retries`
default **0**, error **14** on `max_redirects`. **9/10/11 not retried.**  
Skip the retry loop entirely when both `member_id` and
`group_instance_id` are empty.

### Go CreateTopic

```go
func (c *Client) CreateTopic(name string, partitions int) (uint32, error)
func (c *Client) CreateTopicDefault(name string) (uint32, error)
func (c *Client) CreateTopicID(name string, partitions int) (uint32, error) // alias
```

## Implementation order

1. Docs: this spec + `V02_FREEZE.md` amendment + living-doc ceiling.
2. Go `CreateTopic` return type.
3. Language + Rust JoinGroup retry (guarded).
4. Protocol 116/117 + broker peek + four clients.
5. Flip openraft cluster default + `assignment_must_wait` + tests.

## Honesty leftovers after 155

- Overlay membership is still SoT.
- Homemade 154 code is not deleted.
- SyncGroup is still peek, not CompletingRebalance (GroupConsumer now peeks after join: v0.207/v0.208).
- Range uses JoinGroup members trailer when present (v0.211); empty trailer still DescribeGroup.
- Empty first Join now sends a client-generated member_id (v0.209/v0.210) so retry is safe.
- Kafka stays 38 keys. No client-compat claim.
- Process-local EOS / windows / KIP-890 unchanged.

## Related

- [V02_FREEZE.md](./V02_FREEZE.md) — v0.2 shipped; post-v0.2 replace
- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade log (do not extend)
- [V11_SPEC.md](./V11_SPEC.md) … [V40_SPEC.md](./V40_SPEC.md) — opt-in openraft
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka SyncGroup honesty
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — shim matrix (key 14 already listed)
