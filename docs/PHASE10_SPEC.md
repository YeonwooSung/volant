# Phase 10 — Idempotent produce, retries, consumer lag (binding)

## Goals

1. **Idempotent produce** — `InitProducerId` + produce PID/epoch/sequence de-dupe on the broker
2. **Client retries** — configurable produce retries with backoff for transient errors
3. **Consumer lag** — Prometheus lag series + `volant group lag` CLI
4. Docs honesty in ROADMAP / ops / README

## Non-goals

- Transactions / exactly-once multi-partition
- Kafka wire shim, SCRAM, mTLS identity
- Sticky/cooperative assignor
- Persistent producer state across broker restarts (in-memory PID map)

## Wire protocol

### Opcodes

| Direction | Opcode | Name |
|-----------|--------|------|
| Req | 32 | `InitProducerId` |
| Resp | 33 | `InitProducerId` |

### Error codes

| Code | Name |
|------|------|
| 19 | `InvalidProducerEpoch` |
| 20 | `OutOfOrderSequence` |
| 21 | `UnknownProducerId` |

### `InitProducerId`

Request: empty payload (transactional id reserved for later).

Response:

```
producer_id: u64 LE
epoch: u16 LE
error_code: u16 LE
```

### Produce trailer (backward compatible)

After the existing message list, optional 14 bytes:

```
producer_id: u64 LE     # 0 = non-idempotent
producer_epoch: u16 LE
base_sequence: i32 LE   # -1 = non-idempotent
```

Decoders accept missing trailer as `(0, 0, -1)`.

### De-dupe rules (per producer_id, topic, partition)

- Unknown `producer_id` → error 21
- Epoch mismatch → error 19
- First batch for partition: accept any `base_sequence >= 0`
- Exact replay of last batch (`base_sequence` + count match) → return cached offsets (success, no re-append)
- Next expected: `base_sequence == last_base + last_count` → append
- Else → error 20

## Client (`ClientConfig`)

| Field | Default | Meaning |
|-------|---------|---------|
| `enable_idempotence` | `false` | Init PID + sequence produce |
| `max_retries` | `0` | Extra produce attempts on transient errors |
| `retry_backoff_ms` | `50` | Sleep between retries |

Transient: `Timeout`, `NotEnoughReplicas`, `BrokerNotAvailable`, `Io`, plus transport errors.

Idempotent produce resolves partition client-side (metadata + murmur2 / RR) so sequences bind to a known partition.

## Lag

- Metrics scrape: `volant_consumer_group_lag{group,topic,partition}` = max(0, hwm − committed)
- CLI: `volant group lag --group G [--topic T]`

## Exit criteria

1. Duplicate idempotent produce returns same `base_offset` without double-append
2. Out-of-order / wrong epoch surface error codes 19/20/21
3. Retries succeed after transient failure (unit/integration)
4. Lag visible in metrics text and CLI
5. `cargo test --workspace` green
