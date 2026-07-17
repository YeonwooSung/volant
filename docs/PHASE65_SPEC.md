# Phase 65 — SaslAuthenticate flexible + DescribeCluster + ListTransactions

## Goals

1. **SaslAuthenticate** 0–2 (flexible **v2**): compact AuthBytes + response
   compact fields + response header v1; classic 0–1 unchanged
2. **DescribeCluster** (apiKey 60) **v0** only — always flexible (KIP-700)
3. **ListTransactions** (apiKey 66) **v0** only — always flexible; open
   buffer-until-commit transactions as `Ongoing`
4. Tests + docs honesty

## Non-goals

- SaslHandshake flexible (Kafka: flexibleVersions `"none"`)
- DescribeCluster v1 EndpointType / v2 fenced brokers
- ListTransactions v1 DurationFilter / v2 TransactionalIdPattern
- DescribeTransactions / DescribeProducers
- True multi-state txn coordinator (PrepareCommit, etc.)

## Wire summary

### SaslAuthenticate v2

**Request** (header v2): compact AuthBytes, tags.

**Response** (header v1): error, compact nullable ErrorMessage, compact
AuthBytes, SessionLifetimeMs (=0), tags.

Classic v0–1: classic bytes/strings; session_lifetime only on v1+.

### DescribeCluster v0

**Request** (header v2): IncludeClusterAuthorizedOperations (bool), tags.

**Response** (header v1): throttle, error, compact nullable error_message,
compact ClusterId (`volant`), ControllerId, compact Brokers[{id, host, port,
nullable rack, tags}], ClusterAuthorizedOperations, tags.

### ListTransactions v0

**Request:** compact StateFilters[] (strings), compact ProducerIdFilters[]
(int64), tags.

**Response:** throttle, error, compact UnknownStateFilters[], compact
TransactionStates[{TransactionalId, ProducerId, TransactionState, tags}], tags.

Open Volant txns report state `"Ongoing"`. Empty filters = all open txns.
Unknown state filter strings are echoed in UnknownStateFilters.

## Exit criteria

1. ApiVersions: SaslAuthenticate max **2**; DescribeCluster **0–0**; ListTransactions **0–0**
2. SaslAuthenticate v2 PLAIN success path
3. DescribeCluster returns cluster_id + at least one broker
4. ListTransactions empty list when no open txns; Ongoing after begin
5. Classic SaslAuthenticate v0 still works
6. Unsupported higher versions → header v1 + UnsupportedVersion
7. phase65 + phase30 green

## Honest limitations

- Empty tag buffers only
- SessionLifetimeMs always 0
- DescribeCluster no EndpointType / IsFenced
- ListTransactions only Ongoing open memory txns (no historical / prepare states)
- Duration / pattern filters unsupported (v1+)
