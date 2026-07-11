# Phase 6 Implementation Review

**Date:** 2026-07-11  
**Design:** Kafka-style static membership + controller + ISR (not Raft-per-partition)

## Delivered

| Area | Status | Notes |
|------|--------|-------|
| Protocol opcodes 20–25, errors 13–16 | ✅ | Codec roundtrip tests |
| PartitionInfo replicas/isr/epoch | ✅ | Metadata wire extended |
| Storage `append_with_offset` | ✅ | Gap rejected; batch helper |
| `cluster.toml` + `--node-id` | ✅ | Server flags wired |
| Controller = lowest live id | ✅ | HeartbeatBroker + membership |
| CreateTopic RF assignment | ✅ | Controller-only; NotController |
| ReplicaFetch + HWM/ISR | ✅ | Follower loop background task |
| acks=all + min_insync_replicas | ✅ | Async HWM wait in net.rs |
| Leader failover from ISR | ✅ | `on_broker_death` + elect |
| 3-node failover test | ✅ | `cluster_failover` integration |
| Single-node regression | ✅ | `cargo test --workspace` green |
| consistency.md linked | ✅ | README + ROADMAP |

## Test evidence

```text
cargo test --workspace
# All packages ok, including:
#   volant-protocol: 10 tests (phase6 codec + error codes)
#   volant-storage: append_with_offset + append_records_with_offsets
#   volant-broker unit: assignment, HWM min, ISR shrink, membership
#   volant-broker --test cluster_failover:
#     three_node_acks_all_survives_leader_kill ... ok
#     follower_rejects_produce ... ok
#   volant-client e2e_group / e2e_tcp ... ok
#   volant-stream e2e ... ok
```

## Architecture notes

- **Single-node:** no `--cluster-config` → `node_id=0`, RF=1, HWM=LEO, acks=all ≡ acks=1.
- **Multi-node:** `Broker::with_cluster`; background heartbeats, follower ReplicaFetch, membership tick.
- **HWM wait:** network path polls `committed_hwm` asynchronously (no `block_in_place`) so current-thread runtimes used by older e2e tests keep working.
- **Failover:** mark broker dead → recompute controller → elect leader from ISR ∩ live.

## Gaps / follow-ups

1. **Rolling restart automation** — not fully automated; document operational procedure.
2. **Client multi-broker routing** — retries NotLeader once on same connection; does not yet reconnect to the new leader host automatically.
3. **acks=all timeout surface** — returns Timeout error_code on produce response after append; client treats non-zero as error (data may exist on leader).
4. **ISR/assignment generation churn** — every ISR change bumps generation; fine for prototype scale.
5. **Rack-aware placement** — config field only.
6. **Process-level e2e** (spawn 3 `volant-server` + SIGKILL) — in-process TCP test covers the semantic exit criterion; OS-level chaos optional.

## Honest Phase 6 checkbox status

- [x] Spec + consistency.md
- [x] Static membership + controller
- [x] ReplicaFetch path + HWM/ISR
- [x] acks=all + min_insync_replicas
- [x] Leader failover elects from ISR
- [x] Test: leader kill → no loss of acks=all data
- [x] Single-node tests still pass
- [ ] Rolling restart e2e (deferred)
