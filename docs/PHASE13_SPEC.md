# Phase 13 — Topic configs & retention ops (binding)

## Goals

1. **Per-topic configs** — `retention.ms`, `retention.bytes`, `segment.bytes`
2. **Durable config store** under `data_dir`
3. **DescribeConfigs / AlterConfigs** protocol + CLI
4. **CreateTopic** accepts optional config trailer
5. **Background retention** applies size/time policies periodically
6. Docs honesty

## Non-goals

- Full Kafka AdminClient config catalog
- Dynamic partition count increase
- Compact cleanup policy
- Cooperative rebalance / transactions / Kafka shim

## Config keys

| Key | Type | Meaning |
|-----|------|---------|
| `retention.ms` | u64 or empty | Drop sealed segments older than this; empty = disabled |
| `retention.bytes` | u64 or empty | Drop oldest sealed segments until total ≤ limit; empty = disabled |
| `segment.bytes` | u64 or empty | Target segment roll size; empty = broker default |

## Durable layout

```
{data_dir}/__topic_configs/{sanitized_topic}.json
```

```json
{
  "retention_ms": 86400000,
  "retention_bytes": null,
  "segment_bytes": 1048576
}
```

## Protocol

### CreateTopic trailer (backward compatible)

After `name` + `partitions`, optional:

```
config_count: u32
  for each: key string, value string
```

Legacy payloads without trailer → empty configs.

### DescribeConfigs

| Dir | Opcode |
|-----|--------|
| Req | 40 |
| Resp | 41 |

Req: `topic` string  
Resp: `error_code u16`, `topic string`, `topic_id u32`, `partition_count u32`, `config_count` + key/value pairs

### AlterConfigs

| Dir | Opcode |
|-----|--------|
| Req | 42 |
| Resp | 43 |

Req: `topic`, `config_count` + key/value (empty value clears)  
Resp: `error_code`, `topic`

## CLI

```bash
volant topic create NAME --partitions N \
  [--retention-ms MS] [--retention-bytes B] [--segment-bytes B]
volant topic describe NAME
volant topic config set NAME --key retention.ms --value 3600000
volant topic config set NAME --key retention.ms --value ''   # clear
```

## Exit criteria

1. Create with retention.bytes + tiny segment.bytes; produce past limit; retention drops old segments
2. DescribeConfigs / CLI describe show configs + partition count
3. AlterConfigs updates live logs and durable file
4. `cargo test --workspace` green
