# Phase 23 — Kafka wire protocol shim (MVP)

## Goals

1. Optional **Kafka-compatible listen port** for basic produce/fetch/metadata
2. Support enough of the classic (non-flexible) Kafka protocol for discovery tools
   and a hand-written client / simple `kcat` flows
3. Map requests onto existing `Broker` APIs (no second storage path)
4. Tests + honest docs

## Non-goals

- Full Kafka API surface (JoinGroup, Txn, SASL handshake, etc.)
- Flexible versions / tagged fields (KIP-482)
- RecordBatch magic=2 (use legacy MessageSet magic 0/1)
- Kafka SASL/SCRAM on the shim port
- Idempotent / transactional Kafka produce
- Drop-in Redpanda/Kafka admin tooling parity

## Listen

| Flag | Meaning |
|------|---------|
| `--kafka-listen host:port` | Enable Kafka protocol accept loop (default: **disabled**) |

Native Volant protocol remains on `--listen`. Kafka is a **second** socket.

## Supported APIs

| API key | Name | Request versions | Response versions |
|--------:|------|------------------|-------------------|
| 0 | Produce | 0 | 0 |
| 1 | Fetch | 0 | 0 |
| 3 | Metadata | 0–1 | 0–1 |
| 18 | ApiVersions | 0 | 0 |

Unsupported API key / version → Kafka error `UNSUPPORTED_VERSION` (35) where
applicable, or close/connection error on unparseable frames.

## Framing

Classic Kafka TCP framing (big-endian):

```
request:  Int32 size | RequestHeader | body
response: Int32 size | ResponseHeader | body
```

RequestHeader v0/v1 (non-flexible):

```
api_key: i16
api_version: i16
correlation_id: i32
client_id: nullable string (i16 len; -1 = null)
```

ResponseHeader v0:

```
correlation_id: i32
```

## Produce (v0)

- `acks`: i16 (`0`, `1`, or `-1` → map `-1` to Volant acks=all `255`)
- `timeout_ms`: i32 (ignored for MVP except logged)
- Topics → partitions → **MessageSet** (magic 0 or 1)
- Append via `Broker::produce` (acks respected when possible)

MessageSet message:

```
offset: i64          # ignored on produce
message_size: i32
crc: i32
magic: i8            # 0 or 1
attributes: i8
[timestamp_ms: i64]  # magic 1 only
key: bytes           # i32 len; -1 = null
value: bytes
```

## Fetch (v0)

- Replica id, max wait, min bytes ignored for MVP (non-blocking fetch)
- Per-partition: topic, partition, fetch_offset, max_bytes
- Response MessageSet from broker records starting at `fetch_offset`

## Metadata (v0–1)

- Empty topic list = all topics
- Brokers from `Broker::metadata`
- v1 includes `controller_id` and `is_internal` topic flag (`false`)

## ApiVersions (v0)

Advertise the four supported APIs with their min/max versions.

## Auth / ACLs

- Kafka port has **no SASL** in this phase
- If Volant `auth_required` (token/SCRAM/mTLS gate on native port), Kafka port
  still accepts connections but ACL checks use principal `"kafka-anonymous"`
  when ACLs are enabled
- Operators should bind Kafka listen to localhost / private net, or leave it off

## Exit criteria

1. ApiVersions returns Produce/Fetch/Metadata/ApiVersions ranges
2. Metadata lists brokers + topics matching Volant catalog
3. Produce MessageSet → readable via Volant client Fetch
4. Kafka Fetch returns MessageSet of produced records
5. `cargo test --workspace` green
6. Docs list supported APIs and limitations

## Honest limitations

- No magic=2 RecordBatch (many modern producers default to it — use older
  client config or our test client)
- No consumer groups on Kafka port
- No Kafka SASL
- No CreateTopics / DeleteTopics on Kafka port (use Volant protocol/CLI)
- Single correlation, sequential request/response per connection
