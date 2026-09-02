# v0.10 — Dynamic membership overlay (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Add or remove a broker **without rewriting `cluster.toml` and
restarting the whole cluster**, using a persisted overlay.

**Honesty:** this is **not** Raft joint consensus, **not** KRaft voter
reconfig, and **not** automatic replica move. Controller remains
**lowest live id**. Split-brain: two isolated nodes can both accept add
(no quorum). A down peer does **not** share `data_dir` — the best-effort
push is what propagates the overlay.

## Goals

1. **Overlay file** `{data_dir}/cluster/membership.json`:
   ```json
   {
     "generation": 1,
     "brokers": [
       {"id": 1, "host": "127.0.0.1", "port": 9092, "rack": null},
       {"id": 2, "host": "127.0.0.1", "port": 9093, "rack": null}
     ]
   }
   ```
   - File present on start → **membership SoT** (replaces `cluster.toml`
     brokers for live membership, majority N, and `broker_addr`).
   - Absent → toml brokers (today). First successful add/remove writes
     the overlay (seeded from current list + mutation).
2. **Native admin** (not Kafka keys; `SUPPORTED_APIS` stays 38):
   - `AddBroker` `{id, host, port, rack?}` — reject duplicate id.
   - `RemoveBroker` `{id}` — reject self; reject last remaining broker.
   - `ListMembers` — configured + live + generation.
3. **Majority N** uses the **effective** broker list. After add, N
   increases; after remove, N decreases. Existing topic replicas are
   **not** reassigned (new brokers do not get old partitions).
4. **Propagation:** after local persist, best-effort `MembershipPut`
   (opcode **100**) to currently configured peers. No majority wait.
   Apply only if `incoming.generation > local`.
5. **CLI:** `volant cluster add-broker|remove-broker|members`.

## Protocol

| Opcode | Direction | Name | Body |
|--------|-----------|------|------|
| **100** / **101** | inter-broker | `MembershipPut` | `generation:u64` + brokers (`id`, `host`, `port`, optional `rack`) → `error_code`, `applied_generation` |
| **102** / **103** | client/admin | `AddBroker` | one broker endpoint → `error_code`, `generation` |
| **104** / **105** | client/admin | `RemoveBroker` | `id:u32` → `error_code`, `generation` |
| **106** / **107** | client/admin | `ListMembers` | empty → `error_code`, `generation`, brokers[], live[] |

Do **not** reuse 96–99 (assignment consensus / metadata Raft).

## Live vs configured

- Add: insert endpoint immediately; broker becomes **live on heartbeat**
  (not optimistic like toml init).
- Remove: drop from config; `Membership` drops that id.
- Restart: overlay ids are marked live at start (same as toml init).

## Non-goals

| Deferred | Why |
|----------|-----|
| Raft joint consensus / KRaft voter reconfig | homemade Raft is frozen |
| Automatic replica reassignment | later slice |
| Kafka DescribeCluster / new API keys | shim frozen at 38 |
| Majority wait on overlay push | best-effort like `!must_wait` assignment notes |
| Shared `data_dir` reload | brokers do not share disks |

## Split-brain

Any node may accept add/remove. Two isolated controllers can both
increment generation and diverge. MVP does not solve this. On reconnect,
higher generation wins (`MembershipPut` ignore-if-stale). Concurrent
equal-generation forks are not merged.

## Tests

`crates/volant-broker/tests/v10_dynamic_membership.rs`:

1. Add on N=2 → overlay has 3 brokers, generation ≥ 1, configured N = 3.
2. Remove 3; cannot remove self; cannot remove last remaining.
3. Restart same `data_dir` loads overlay (3) even if toml still has 2.
4. Stale generation apply must not shrink the list.
5. Produce `acks=1` on an existing topic still works after add (new
   broker process not started).
