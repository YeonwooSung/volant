# Phase 15 — CreatePartitions & ListOffsets (binding)

## Goals

1. **CreatePartitions** — increase a topic’s partition count (single-node + controller)
2. **ListOffsets** — report earliest (log start) and latest (LEO) per partition
3. Client + CLI
4. Durable catalog / assignment updates
5. Docs honesty

## Non-goals

- Decreasing partition count
- Reassignment / preferred leader election APIs
- Kafka ListOffsets timestamp semantics (we return both ends always)
- Cooperative rebalance / transactions / Kafka shim / SCRAM / compact

## Protocol

### CreatePartitions

| Dir | Opcode |
|-----|--------|
| Req | 46 |
| Resp | 47 |

Request:

```
topic: string
total_count: u32   # must be > current partition count
```

Response:

```
error_code: u16
topic: string
partitions: u32    # new total (0 if error)
```

Errors: NotFound (2), InvalidArg (3), NotController (14) on multi-node non-controller.

### ListOffsets

| Dir | Opcode |
|-----|--------|
| Req | 48 |
| Resp | 49 |

Request:

```
topic: string
partition_count: u32   # 0 = all partitions
  for each: partition u32
```

Response:

```
error_code: u16
topic: string
entry_count: u32
  for each:
    partition: u32
    earliest: u64   # log start offset
    latest: u64     # log end offset (next write / LEO)
```

## Broker behavior

### CreatePartitions (single-node)

1. Topic must exist; `total_count > current`.
2. Open new partition logs `current..total_count-1` with topic config overlay.
3. Persist `__topics/catalog.json`.

### CreatePartitions (cluster)

1. Controller only.
2. Assign replicas for new partitions (same RF/placement as create).
3. Update `assignment.json` + generation; open local replicas via ensure_partition.

### ListOffsets

- Any broker that has the topic metadata may answer.
- For partitions without a local log (follower not replica): return earliest=0, latest=0
  or NotFound for that partition — prefer skip only unknown partitions; known assignment
  partitions without local log report earliest=0, latest=0 with still success.
- Single-node: always local.

## CLI

```bash
volant topic add-partitions NAME --total N
volant topic offsets NAME [--partition P]
```

## Exit criteria

1. Create topic with 2 partitions → add-partitions to 4 → metadata shows 4; produce to p3 works
2. Restart single-node → still 4 partitions (catalog)
3. ListOffsets returns earliest/latest matching produce/delete-records
4. Protocol roundtrips; `cargo test --workspace` green

## Honest limitations

- Cannot shrink partition count
- New partitions start empty (no data rebalance)
- Cluster CreatePartitions does not wait for all brokers to apply (best-effort via existing state sync)
- ListOffsets `latest` is LEO not client HWM (same on single-node; on leader equals HWM when caught up)
