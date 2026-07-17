# Phase 70 — DescribeCluster v2 + ListTransactions v2

## Goals

1. **DescribeCluster** max **0–2** — IncludeFencedBrokers request; **IsFenced**
   on each broker (always false)
2. **ListTransactions** max **0–2** — **TransactionalIdPattern** filter
   (minimal `*` glob)
3. v0–1 paths unchanged
4. Tests + docs honesty

## Non-goals

- Real fenced broker membership / KRaft decommission state
- Full RE2J regex (Kafka uses RE2J; Volant: `*` glob only)
- DescribeTransactions / DescribeProducers version bumps
- DurationFilter enforcement (still ignored)
- Controller EndpointType

## Wire summary

### DescribeCluster v2

**Request:** IncludeClusterAuthorizedOperations, EndpointType (v1+),
**IncludeFencedBrokers (v2+)**, tags.

**Response brokers[]:** BrokerId, Host, Port, Rack, **IsFenced (v2+)**, tags.

Volant always reports `IsFenced=false`. IncludeFencedBrokers is parsed and
ignored (no fenced set to include).

### ListTransactions v2

**Request:** StateFilters[], ProducerIdFilters[], DurationFilter (v1+),
**TransactionalIdPattern (v2+, nullable compact string)**, tags.

Null/empty pattern = no id filter. Pattern matching: `*` matches any
sequence; other characters are literal (not full RE2J).

## Exit criteria

1. ApiVersions: DescribeCluster **0–2**, ListTransactions **0–2**
2. DescribeCluster v2 returns IsFenced=0 on brokers
3. ListTransactions v2 pattern filters transactional ids
4. DescribeCluster v1 still omits IsFenced
5. v3 → header v1 + UnsupportedVersion
6. phase70 + phase66 + phase65 green

## Honest limitations

- No fenced brokers ever returned
- Pattern is simple glob, not RE2J
- DurationFilter still ignored
- Only Ongoing open memory txns
