# Kafka compatibility matrix

**Living document** for the optional Kafka wire shim (`--kafka-listen`).
Ship history: Phases **23–86** (git HEAD product). Binding deep dives:
`PHASE23_SPEC.md` … `PHASE86_SPEC.md`. Overview: [WHITEPAPER.md](./WHITEPAPER.md).
Semantic rows below describe **shipped** behavior.

## Enable

```bash
volant-server \
  --data-dir ./data \
  --listen 127.0.0.1:9092 \
  --kafka-listen 127.0.0.1:9093
```

Native Volant protocol remains on `--listen`. Shared-token Auth does **not**
apply on the Kafka port (use SASL or anonymous + ACLs).

## Supported APIs (current)

From `SUPPORTED_APIS` in `crates/volant-broker/src/kafka/mod.rs`:

| Key | API | Versions | Notes |
|----:|-----|----------|-------|
| 0 | Produce | 0–13 | Classic 0–8; flex v9+; TopicId v13; KIP-951 CurrentLeader v10+ |
| 1 | Fetch | 0–18 | Classic 0–11; flex v12–18; TopicId v13+; isolation LSO/aborted (Phase 86); ReplicaState v15+ ignore; NodeEndpoints v16+; CurrentLeader tag v12+ |
| 2 | ListOffsets | 0–11 | Flex v6+; specials v7–11; READ_COMMITTED latest = LSO (Phase 86) |
| 3 | Metadata | 0–13 | Flex v9+; TopicId v10–13; top-level ErrorCode v13 |
| 8 | OffsetCommit | 0–10 | Flex v8+; TopicId v10 |
| 9 | OffsetFetch | 0–10 | Flex v6+; multi-group v8; TopicId v10 |
| 10 | FindCoordinator | 0–6 | Flex v3; batch v4–6; no share key_type; no TRANSACTION_ABORTABLE |
| 11 | JoinGroup | 0–9 | Flex v6+; ProtocolType/Reason/SkipAssignment v7–9 |
| 12 | Heartbeat | 0–4 | Flex v4 |
| 13 | LeaveGroup | 0–5 | Flex v4+; Reason v5 |
| 14 | SyncGroup | 0–5 | Flex v4+; ProtocolType/Name v5 |
| 15 | DescribeGroups | 0–6 | Flex v5; ErrorMessage v6 |
| 16 | ListGroups | 0–5 | Flex v3; StatesFilter v4; TypesFilter v5 (`classic`) |
| 17 | SaslHandshake | 0–1 | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 |
| 18 | ApiVersions | 0–5 | Flex v3–5; header always v0; empty feature tags; v5 ClusterId/NodeId ignored |
| 19 | CreateTopics | 0–7 | Flex v5+; TopicId response v7 |
| 20 | DeleteTopics | 0–6 | Flex v4+; ErrorMessage v5; TopicId v6 |
| 21 | DeleteRecords | 0–2 | Flex v2 |
| 22 | InitProducerId | 0–6 | Flex v2+; v6 Enable2Pc ignored; OngoingTxn* = -1 |
| 23 | OffsetForLeaderEpoch | 0–4 | Flex v4; no durable epoch history → HWM |
| 24 | AddPartitionsToTxn | 0–5 | Flex v3; batch v4–5 |
| 25 | AddOffsetsToTxn | 0–4 | Flex v3+; v4 = v3 wire (no TRANSACTION_ABORTABLE) |
| 26 | EndTxn | 0–5 | Flex v3+; v5 pid/epoch echo |
| 28 | TxnOffsetCommit | 0–6 | Flex v3+; TopicId v6 |
| 29–31 | ACL admin | 0–3 | Flex v2+; User resource v3; LITERAL only |
| 32 | DescribeConfigs | 0–4 | Flex v4; topic keys |
| 33 | AlterConfigs | 0–2 | Flex v2 |
| 36 | SaslAuthenticate | 0–2 | Flex v2 |
| 37 | CreatePartitions | 0–3 | Flex v2+; v3 = v2 wire (no KIP-599 quota) |
| 42 | DeleteGroups | 0–3 | Flex v2; ErrorMessage v3 |
| 44 | IncrementalAlterConfigs | 0–1 | SET/DELETE only |
| 47 | OffsetDelete | 0 | Classic only |
| 60 | DescribeCluster | 0–2 | Always flex; IsFenced always false |
| 61 | DescribeProducers | 0 | Always flex |
| 65 | DescribeTransactions | 0 | Always flex |
| 66 | ListTransactions | 0–2 | Pattern = simple `*` glob |

## Wire evolution (summary)

| Band | Phases | Theme |
|------|--------|-------|
| MVP | 23–27 | Produce/Fetch/Metadata/groups/admin surface |
| Formats | 28–34 | Compression, idempotence, SASL, SCRAM-512 |
| Classic max | 35–50 | Version ratchets for classic framing |
| Flexible | 51–66 | KIP-482 compact + modern admin |
| TopicId / modern | 67–85 | UUID topics, ListOffsets specials, KIP-890, KIP-951, group admin, CreatePartitions v3, FindCoordinator v5–6, AddOffsetsToTxn v4, ApiVersions 0–5, Fetch 0–18, ACL admin User resource v3 |
| READ_COMMITTED | 86 | Write-through txn + soft abort markers; true LSO; Fetch isolation filtering |

## Semantic honesty (open)

These are **current** product facts, not temporary docs lag:

| Area | Limitation |
|------|------------|
| Transactions | **Write-through** (Phase 86): data on log immediately; soft abort markers (not Kafka control batches); LSO may be `<` HWM while open; READ_COMMITTED filters aborted + caps at LSO; READ_UNCOMMITTED sees unstable/aborted; TxnOffsetCommit still deferred until EndTxn; markers in `__txn_markers` (crash ≡ abort open ranges) |
| 2PC | InitProducerId v6 flags parsed **and ignored**; OngoingTxn* always -1 |
| Epochs | No durable epoch history; eligible → HWM; leader_epoch often -1 |
| TopicId | Deterministic UUID from Volant id (`volant` + zeros + u32), not KRaft random |
| Groups | Coordinator-driven assignment; GroupType always `classic`; states Stable/Empty |
| Fetch sessions | Session header accepted; no real incremental sessions |
| Preferred replica | Always -1 |
| CreateTopics | Replica assignment arrays ignored; configs response often null |
| Storage | Log stores uncompressed Volant records; Fetch re-encodes |
| Auth | Kafka port: SASL or `kafka-anonymous`; no shared-token on Kafka port |
| ACLs | LITERAL only; host always `*`; User resource (v3) stored only (no SCRAM-admin gating); no TransactionalId/DelegationToken; no cluster ACL consensus |
| KIP-951 | CurrentLeader on leader errors; Produce NodeEndpoints v10+; Fetch NodeEndpoints v16+; empty tags on success |
| ApiVersions features | Empty SupportedFeatures / FinalizedFeatures / ZkMigrationReady tags; no REBOOTSTRAP_REQUIRED |
| Missing APIs | Large Kafka surface still unsupported (GSSAPI, OAUTH, broker configs, …) |

## Related

- [ops.md](./ops.md) — Kafka listen ops notes  
- [features.md](./features.md) — native txn/ACL/SCRAM behavior  
- [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) — phase index  
