# Phase 14 — Durable topic catalog & DeleteRecords (binding)

## Goals

1. **Single-node topic catalog** — topic id + partition count survive broker restart
2. **Reload partition logs** on `Broker::new` from catalog + on-disk segments
3. **DeleteRecords** protocol (44/45) + client/CLI for admin truncate-by-offset
4. Docs honesty

## Non-goals

- Dynamic partition count increase
- Compact cleanup policy
- Coordinated multi-node DeleteRecords fan-out to followers
- Cooperative rebalance / transactions / Kafka shim / SCRAM

## Durable layout

```
{data_dir}/__topics/catalog.json
```

```json
{
  "next_id": 3,
  "topics": {
    "orders": { "id": 1, "partitions": 3 },
    "events": { "id": 2, "partitions": 1 }
  }
}
```

- Atomic write (temp + rename), same pattern as `__producer_state`.
- Multi-node continues to use `cluster/assignment.json` (catalog unused when clustered).
- Topic configs remain under `__topic_configs/` (Phase 13); applied on reload.

## Broker behavior

### CreateTopic (single-node)

After opening partitions: persist catalog (`next_id` + all topics).

### DeleteTopic (single-node)

Remove from catalog, drop config file, remove data dir (existing).

### Startup (`Broker::new`)

1. Open catalog (empty if missing).
2. For each topic: load config overlay, `Topic::create_with_config` (opens existing logs).
3. Set `next_topic_id` from catalog.

### DeleteRecords

| Dir | Opcode |
|-----|--------|
| Req | 44 |
| Resp | 45 |

Request:

```
topic: string
partition: u32
before_offset: u64
```

Response:

```
error_code: u16
topic: string
partition: u32
low_watermark: u64   # new log start offset
```

Semantics (matches storage): drop **whole sealed segments** entirely before
`before_offset`. Active segment is not partially truncated. Returns new log start.

- Leader only in cluster mode (`NotLeaderForPartition` on followers).
- Multi-node: leader applies locally; **followers are not notified** (honest limit —
  use retention or recreate for full cleanup).

## Client / CLI

```rust
client.delete_records(topic, partition, before_offset).await?; // -> low_watermark
```

```bash
volant topic delete-records NAME --partition P --before-offset N
```

## Exit criteria

1. Create + produce → drop broker → new broker on same `data_dir` → metadata lists topic and fetch returns data **without** recreate
2. DeleteRecords drops eligible segments; low watermark advances; fetch from old offsets fails / empty as appropriate
3. Delete topic removes catalog entry; restart does not resurrect topic
4. Protocol encode/decode roundtrip for 44/45
5. `cargo test --workspace` green

## Honest limitations

- Catalog is single-node only (cluster uses assignment.json)
- DeleteRecords does not coordinate follower truncation
- No compact policy; no partition count increase
