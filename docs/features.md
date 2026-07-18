# Native features (post-core)

Summary of Volant-native capabilities shipped after the core platform
(Phases **7–22** and related). Core formats: [PHASE1–6](./INDEX.md). Kafka
shim: [KAFKA_COMPAT.md](./KAFKA_COMPAT.md).

## Ops & packaging (7–9)

| Feature | Behavior |
|---------|----------|
| Metrics | Prometheus `GET /metrics` (`--metrics-addr`); optional Bearer token |
| Logs | `text` or `json` (`--log-format`) |
| Shared-token auth | `VOLANT_AUTH_TOKEN` on native protocol |
| Server TLS | Feature `tls`; cert/key; TLS-only listen |
| Client TLS | Feature `tls` on `volant-client` |
| Inter-broker TLS | On by default when server TLS enabled (Phase 9) |
| Leader redirect | Client refreshes Metadata + reconnects on `NotLeaderForPartition` |
| Deploy | Docker, compose, systemd, Helm (single or multi-node) |
| Fuzz scaffold | `fuzz/` targets for frame/request decode |

## Reliability (10–11)

| Feature | Behavior |
|---------|----------|
| Idempotent produce | PID / epoch / sequence de-dupe |
| Produce retries | Client-side with redirect awareness |
| Durable producer state | `{data_dir}/__producer_state/state.json` |
| Consumer lag | Metrics + `volant group lag` |
| Sticky assignor | Default partition assignor |

## Groups & topics (12–17)

| Feature | Behavior |
|---------|----------|
| Group admin | list / describe / delete-offsets; static membership |
| Topic configs | `retention.ms` / `retention.bytes` / `segment.bytes` |
| Topic catalog | Survives single-node restart |
| DeleteRecords | Truncate sealed segments before offset |
| CreatePartitions | Grow partition count (cannot shrink) |
| ListOffsets | Earliest / latest (+ Kafka specials on shim) |
| Compaction | `cleanup.policy=compact` on sealed segments |
| Cooperative rebalance | JoinGroup `revoked` list; sticky-retained positions |

## Transactions (18)

| Feature | Behavior |
|---------|----------|
| transactional_id fencing | Yes |
| Write-through (Phase 86) | Txn produces append immediately; LSO holds until EndTxn |
| Abort | Soft markers hide ranges (native + READ_COMMITTED); data stays on log for READ_UNCOMMITTED |
| Deferred offsets | Txn offset commits apply on commit only |
| Crash | Open write-through ranges ≡ abort (persisted `__txn_markers`) |
| READ_COMMITTED | MVP: LSO filtering + aborted list (soft markers, not control batches) |

## Security (19–22)

| Feature | Behavior |
|---------|----------|
| mTLS identity | Client cert CN/SAN as principal |
| Principal ACLs | Topic / group / cluster (+ Kafka User resource store-only); allow/deny; durable file |
| Super-users | Bypass ACL checks |
| SCRAM-SHA-256 | Durable users; **native** + Kafka SASL |
| SCRAM-SHA-512 | Dual hashes per user; **Kafka SASL only** |

## Stream processing (Phase 4+)

In-process `volant-stream`: map, filter, flat_map, reduce, windows, foreach.
**In-memory state only**; at-least-once. No durable state store, no distributed
workers.

## Open limitations (native)

- Multi-language clients deferred  
- No Raft metadata / dynamic membership  
- No Kafka control batches on the data log (soft markers only)  
- No real 2PC / prepared transactions  
- ACL store is single-node file (no consensus)  
- DeleteRecords does not fan out to cluster followers  
- Compaction simpler than Kafka (no tombstone retention window)  
- Inter-broker not ACL-gated; uses shared-token when configured  

See [ROADMAP.md](../ROADMAP.md) for the full deferred list.
