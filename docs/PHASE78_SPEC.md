# Phase 78 — KIP-951 CurrentLeader / NodeEndpoints

## Goals

1. **Produce v10+**: populate partition **CurrentLeader** (tag 0) and top-level
   **NodeEndpoints** (tag 0) on leader-related errors
2. **Fetch v12+**: populate partition **CurrentLeader** (tag 1) on the same errors
3. Success / non-leader errors keep **empty** tag buffers (unchanged)
4. No ApiVersions max bumps (Produce stays 0–13, Fetch 0–13)
5. Tests + docs honesty

## Non-goals

- Fetch top-level NodeEndpoints (Kafka `taggedVersions: "16+"`; we max at 13)
- DivergingEpoch / SnapshotId tags on Fetch
- Emitting CurrentLeader on every response (only error cases)
- Changing NotLeader / fencing policy
- Real multi-broker leader redirect e2e beyond unit/integration tag checks

## When CurrentLeader is emitted

| Error | Code | Produce v10+ | Fetch v12+ |
|-------|-----:|:------------:|:----------:|
| NotLeaderForPartition | 6 | yes | yes |
| FencedLeaderEpoch | 74 | yes | yes |
| Other errors / success | * | empty tags | empty tags |

**CurrentLeader** value (`LeaderIdAndEpoch`):

```
LeaderId: int32      # partition leader from metadata (this node when leader)
LeaderEpoch: int32   # partition.leader_epoch
TAG_BUFFER empty
```

If partition metadata is missing, omit the tag (empty buffer) rather than invent ids.

## Wire summary

### Produce partition tags (v9+ flexible; CurrentLeader tag from v10+)

v9: always empty TAG_BUFFER.

v10–13 on NotLeader / FencedLeaderEpoch:

```
TAG_BUFFER {
  tag 0: CurrentLeader = LeaderIdAndEpoch{id, epoch, tags}
}
```

### Produce top-level tags (v9+)

When **any** partition in the response included CurrentLeader and version ≥ 10:

```
TAG_BUFFER {
  tag 0: NodeEndpoints = compact array of {
    NodeId: int32,
    Host: compact string,
    Port: int32,
    Rack: compact nullable string (null),
    tags empty
  }
}
```

Endpoints are the unique brokers referenced by emitted CurrentLeader ids,
resolved from `MetadataSnapshot.brokers` (fall back to self host/port).

Otherwise top-level tags remain empty.

### Fetch partition tags (v12+)

CurrentLeader is **tag 1** (tag 0 is DivergingEpoch, unused).

```
TAG_BUFFER {
  tag 1: CurrentLeader = LeaderIdAndEpoch{…}
}
```

No Fetch top-level NodeEndpoints (v16+ only).

## Exit criteria

1. Produce v10 success → empty partition + top-level tags
2. Produce v10 with NotLeader/Fenced → CurrentLeader tag + NodeEndpoints
3. Fetch v12 FencedLeaderEpoch → CurrentLeader tag 1
4. Produce v9 / Fetch classic unchanged
5. phase78 + phase53/54/71 green
6. ROADMAP / README / ops honesty

## Honest limitations

- Single-node: CurrentLeader almost always self; multi-node correct when metadata has leader
- No NodeEndpoints on Fetch (need v16)
- No DivergingEpoch
- Empty tags on success (Kafka also omits when not useful)
- Leader host/port from advertised metadata only
