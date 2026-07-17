# Phase 6 Implementation Plan

## Design (locked)

Kafka-style static membership + controller (lowest live broker id) + leader/follower ISR replication.
Not Raft-per-partition.

## Layers

### A. Protocol (`volant-protocol`)
- ErrorCode: NotLeaderForPartition=13, NotController=14, NotEnoughReplicas=15, BrokerNotAvailable=16
- Opcodes: ReplicaFetch 20/21, HeartbeatBroker 22/23, ClusterState 24/25
- Request/Response variants + encode/decode
- PartitionInfo: +replicas, +isr, +leader_epoch
- Unit codec tests

### B. Storage (`volant-storage`)
- `PartitionLog::append_with_offset(offset, message)` requiring offset == next_offset
- `append_records_with_offsets` batch helper
- `log_end_offset()` alias for LEO
- Tests

### C. Broker cluster + replica
```
cluster/{mod,config,membership,controller,assignment,state}.rs
replica/{mod,follower,leader}.rs
```
- ClusterConfig from TOML (serde + toml)
- Broker: node_id, optional ClusterHandle
- Partition: leader, replicas, isr, leader_epoch, committed_hwm, follower_leo
- produce: leadership check; acks=all waits for HWM; min_insync_replicas
- fetch: client capped at committed_hwm; ReplicaFetch up to LEO
- Background: heartbeats, follower ReplicaFetch loops, controller expiry
- Persist `data_dir/cluster/assignment.json`
- net.rs: dispatch new opcodes; metadata all brokers + replicas/isr

### D. Server
- `--node-id`, `--cluster-config` flags
- Start cluster background tasks when configured

### E. Client
- `produce_with_acks`; on NotLeaderForPartition refresh metadata once + retry
- Prefer connecting to partition leader when known

### F. Tests
- Unit: assignment round-robin, HWM min, ISR shrink
- Integration: 3 in-process brokers, acks=all, kill leader, fetch from new leader
- Regression: single-node workspace green

### G. Docs
- ROADMAP Phase 6 checkboxes (honest)
- README Phase 6 + consistency.md link
- examples/cluster.toml

## Single-node compatibility

No `--cluster-config` → node_id=0, RF=1, HWM=LEO, acks=all ≡ acks=1. All Phase 1–5 tests pass.

## Implementation order

1. Protocol + tests
2. Storage offsets + tests
3. Cluster config/assignment/membership (unit)
4. Partition replica state + produce/fetch HWM
5. net dispatch + server flags
6. Follower loop + controller failover
7. Integration test
8. Client retry + docs

## Exit criteria (minimum)

- [x] Static membership + controller
- [x] ReplicaFetch + HWM/ISR
- [x] acks=all + min_insync_replicas
- [x] Leader failover from ISR
- [x] Test: leader kill → no loss of acks=all data
- [x] Single-node tests still pass
