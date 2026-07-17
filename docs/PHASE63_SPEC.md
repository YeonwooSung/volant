# Phase 63 — Flexible ListOffsets + OffsetForLeaderEpoch

## Goals

1. First flexible versions of offset-query APIs:
   - **ListOffsets** 0–6 (flexible **v6**)
   - **OffsetForLeaderEpoch** 0–4 (flexible **v4**)
2. Response header **v1** for those flexible versions
3. Compact strings/arrays + empty TAG_BUFFER
4. Classic paths unchanged
5. Tests + docs honesty

## Non-goals

- ListOffsets v7+ max-timestamp / local-log-start / tiered / remote storage
- Real leader-epoch history (still maps eligible epochs → HWM)
- Timestamp-based offset lookup beyond earliest/latest specials

## Wire summary

### ListOffsets v6

**Request:** replica_id, isolation_level, compact topics[{name, compact partitions[{partition, current_leader_epoch, timestamp, tags}], tags}], tags.

**Response** (header v1): throttle, compact topics[{name, compact partitions[{partition, error, timestamp, offset, leader_epoch, tags}], tags}], tags.

Semantics unchanged: `-1` latest, `-2` earliest; other timestamps → InvalidTimestamp; isolation ignored (LSO≡HWM); leader-epoch fencing as classic v4–5.

### OffsetForLeaderEpoch v4

**Request:** replica_id, compact topics[{name, compact partitions[{partition, current_leader_epoch, leader_epoch, tags}], tags}], tags.

**Response** (header v1): throttle, compact topics[{name, compact partitions[{error, partition, leader_epoch, end_offset, tags}], tags}], tags.

Semantics unchanged: no epoch history; eligible epochs → HWM + current epoch.

## Exit criteria

1. ApiVersions maxes: ListOffsets **6**, OffsetForLeaderEpoch **4**
2. ListOffsets v6 earliest + latest roundtrip
3. OFLE v4 returns HWM
4. Classic ListOffsets v2 still works
5. Unsupported higher versions → header v1 + UnsupportedVersion
6. phase63 + phase40 + phase39 green

## Honest limitations

- Empty tag buffers only
- No max-timestamp / tiered / remote ListOffsets
- No epoch history
