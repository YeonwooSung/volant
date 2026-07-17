# Phase 66 — DescribeTransactions + DescribeProducers + cluster/txn bumps

## Goals

1. **DescribeTransactions** (apiKey 65) **v0** — always flexible; per-id
   Empty/Ongoing state from Volant txn registry
2. **DescribeProducers** (apiKey 61) **v0** — always flexible; active producers
   from committed sequences + open-txn activity
3. **DescribeCluster** bump **0–1**: EndpointType (brokers only)
4. **ListTransactions** bump **0–1**: DurationFilter accepted, ignored
5. Tests + docs honesty

## Non-goals

- DescribeCluster v2 fenced brokers
- ListTransactions v2 TransactionalIdPattern
- True txn start-time / timeout tracking
- Coordinator epoch / log-start-offset fidelity for DescribeProducers
- Metadata TopicId (v10+)
- Control-marker READ_COMMITTED

## Wire summary

### DescribeTransactions v0

**Request:** compact TransactionalIds[], tags.

**Response** (header v1): throttle, compact TransactionStates[{error, id,
state, timeout_ms=0, start_time_ms=0, producer_id, epoch, compact
topics[{name, compact partitions[], tags}], tags}], tags.

- Unknown id → `TransactionalIdNotFound` (105)
- Known, no open txn → state `"Empty"`, empty topics
- Open txn → state `"Ongoing"`, partitions from buffered produces

### DescribeProducers v0

**Request:** compact topics[{name, compact partition_indexes[], tags}], tags.

**Response:** throttle, compact topics[{name, compact partitions[{index, error,
error_message, compact active_producers[{pid, epoch, last_seq, last_ts=-1,
coord_epoch=0, txn_start=-1, tags}], tags}], tags}], tags.

Unknown topic/partition → `UnknownTopicOrPartition`.

### DescribeCluster v1

Request adds EndpointType (int8, default 1=brokers). Response echoes
EndpointType after error_message. Type ≠ 1 → `UnsupportedEndpointType` (115).

### ListTransactions v1

Request adds DurationFilter (int64) after ProducerIdFilters; parsed and ignored
(no start-time tracking).

## Exit criteria

1. ApiVersions: DescribeTransactions **0–0**, DescribeProducers **0–0**,
   DescribeCluster **0–1**, ListTransactions **0–1**
2. DescribeTransactions Empty + Ongoing + unknown
3. DescribeProducers lists producer after open-txn produce buffer
4. DescribeCluster v1 brokers endpoint works; controllers rejected
5. ListTransactions v1 still lists Ongoing
6. Unsupported higher versions → header v1 + UnsupportedVersion
7. phase66 + phase65 green

## Honest limitations

- Empty tag buffers only
- timeout/start always 0; duration filter ignored
- No fenced brokers / controller endpoint
- DescribeProducers last_timestamp / coordinator_epoch / txn_start_offset
  placeholders
- No pattern filter (ListTransactions v2)
