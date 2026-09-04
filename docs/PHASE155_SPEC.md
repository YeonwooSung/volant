# Phase 155 — openraft metadata SoT + native SyncGroup + Join retry

**Status:** Open — PR1–PR5 landed on main (crate **0.2.0**). Overlay membership still SoT; homemade 154 hatch **deleted** (v0.222).  
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
3. **Do not grow homemade 154.** Hatch **deleted** (v0.222). No
   RequestVote, no InstallSnapshot, no `{data_dir}/__metadata_raft/`
   creation. Inbound 98 always `metadata raft not enabled`.
   `VOLANT_METADATA_RAFT` / `VOLANT_METADATA_RAFT_WAIT_COMMIT` warn
   once and ignore. Protocol 98/99 encode/decode stays.
4. **Native SyncGroup 116/117** applies assignment bytes when they
   decode (v0.248); empty/garbage still peeks Join (same as Kafka
   key **14**). Not Kafka CompletingRebalance. `SUPPORTED_APIS` is **64**.
5. **JoinGroup retry** only when `member_id` or `group_instance_id`
   is non-empty. Empty first join is still one shot.
6. **Go `CreateTopic` / `CreateTopicDefault` return `(uint32, error)`.**
   `CreateTopicID` becomes an alias.

## Non-goals

| Deferred | Why |
|----------|-----|
| Homemade 154 RequestVote / InstallSnapshot | Freeze choice C; replace, do not finish |
| Overlay membership as raft voter SoT | Out of 155; `{data_dir}/cluster/membership.json` stays membership SoT |
| Kafka two-phase join/sync states | Join parks (v0.227); still not PreparingRebalance / join-set wait |
| JoinGroup native member list | Frozen; range still uses DescribeGroup |
| Retry empty-`member_id` first Join | Ghost member + generation++ |
| New Kafka API key / version ratchet | `SUPPORTED_APIS` is **64**. No further keys |
| librdkafka / kafka-python / kcat claims | Still not claimed |
| Distributed EOS / windows / full KIP-890 | Opt-in TransactionLog v0 (v0.229); not TV2 / default-on |
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
(applied when they decode as a known assignment; empty/garbage peeks
Join — v0.248).  
Response: `error_code`, `assignment[]` (same `Assignment` as JoinGroup).  
Broker: `heartbeat()` membership/generation check, then apply-or-peek
`assignment()`. No new `GroupState`.  
Clients: `sync_group` / `SyncGroup` / `syncGroup`. GroupConsumer may
call it after join; default fetch set remains JoinGroup assignment
unless SyncGroup supplied a decodable assignment.

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

- Overlay is persist-after-joint on the openraft-on leader (v0.212) and apply artifact on followers (v0.216). In-process add/remove also persist after joint when raft is started (v0.217). Flag off stays v0.10.
- Homemade 154 hatch is **deleted** (v0.222). 98/99 still decode. Leftover `__metadata_raft/` files are unread.
- SyncGroup is a generation confirm fence (v0.215). List/Describe report CompletingRebalance while the fence is open (v0.218) and **PreparingRebalance** while a Join is parked (v0.230). Member OffsetCommit is 9 until sync (v0.219). GroupConsumer retries Join 9 when `max_retries>0` (v0.220/v0.221). Thin Client retries Join 9 on the same budget (v0.223/v0.224). New-member Join **parks** until SyncGroup or **rebalance** timeout (v0.227/v0.231; default 1000ms; mutex released). Still not join-set wait.
- Range uses JoinGroup members trailer when present (v0.211); empty trailer still DescribeGroup.
- Empty first Join now sends a client-generated member_id (v0.209/v0.210) so retry is safe.
- Kafka `SUPPORTED_APIS` is **74** (… + Vote **52**, Add/Remove/UpdateRaftVoter **80**/**81**/**82**, UnregisterController **94**). No stored quotas / KIP-584 features / token store / KIP-848 / unclean election / live reassignment. No client-compat claim.
- OffsetCommit v6+ stores `committed_leader_epoch`; OffsetFetch v5+ returns it (v0.262). Legacy files / native commits stay **-1**.
- OffsetFetch RequireStable (v0.256) returns **81** when the committed offset is still unstable. Not a wait.
- TxnOffsetCommit v3+ honors generation/member with the OffsetCommit fence (v0.254). Empty member or gen `< 0` still skips.
- SyncGroup applies decoded assignment bytes (v0.248). Empty/garbage still peeks Join. Still not join-set wait.
- ACL TransactionalId (native **4** / Kafka **5**) is Write-checked on txn APIs when the id is non-empty (v0.247).
- Opt-in `__transaction_state` records open≡abort (v0.226) and writes Kafka TransactionLogKey/Value v0 (v0.229) including open/prepared partitions (v0.232). JSON v1 still replays. Flag still default off. Not TV2 writes / not full KIP-890/939. Process-local EOS / windows unchanged.
- Native Fetch with group+member trailer honors assignment (v0.234). Kafka Fetch and empty-trailer Fetch stay unfiltered.
- Native SCRAM-SHA-512 is opt-in trailer **2** on ScramFirst (v0.238). Default/legacy stay SHA-256.
- Native ListOffsets timestamp trailer (v0.239) and isolation (v0.240): `-1` latest, `-2` earliest, isolation **1** latest = LSO. Kafka key 2 unchanged.
- Leftover `{data_dir}/__metadata_raft/` warn-once (v0.243); unread, not deleted.

## Related

- [V02_FREEZE.md](./V02_FREEZE.md) — v0.2 shipped; post-v0.2 replace
- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade log (do not extend)
- [V11_SPEC.md](./V11_SPEC.md) … [V40_SPEC.md](./V40_SPEC.md) — opt-in openraft
- [PHASE26_SPEC.md](./PHASE26_SPEC.md) — Kafka SyncGroup honesty
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — shim matrix (key 14 already listed)
