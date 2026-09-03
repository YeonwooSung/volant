# v0.167 — Go ReassignAllPartitions

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V59_SPEC.md](./V59_SPEC.md): Java
already has `reassignPartitions(topic, replicas)` (null partition).
Python `reassign_partitions(topic, replicas, partition=None)` already
reassigns all. Go only has `ReassignPartitions(topic, replicas,
partition *uint32)` — nil already means all
(`codec.ReassignAllPartitions` = 0xFFFFFFFF), but there is no named
all-partition helper matching Java.

Add `Client.ReassignAllPartitions`. Reuse `ReassignPartitions` (do not
reimplement the RPC). `ReassignPartitions(topic, replicas, partition)`
stays unchanged. This is **not** Kafka AlterPartitionReassignments.

This is residual **v0.167** (Go ReassignAllPartitions). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Rust, Python, or Java.

## Goals

1. Add public `func (c *Client) ReassignAllPartitions(topic string,
   replicas []uint32) (uint32, error)` that calls
   `ReassignPartitions(topic, replicas, nil)`.
2. Inherit retry / error **14** from `ReassignPartitions` (v0.72
   error 14 + v0.103 transient retry via `adminRoundTrip`). No new
   retry policy.
3. Do **not** change `ReassignPartitions(topic, replicas, partition)`.
4. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `ReassignPartitions(topic, replicas, partition)` | Frozen; nil already means all |
| Kafka AlterPartitionReassignments (API key 45) | Native opcode 114/115 only |
| Overlay / assignment wait-rollback | Broker-side (v0.18 / v0.39) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Java | Already have topic+replicas overloads (v0.59) |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// ReassignAllPartitions reassigns every partition of topic.
// Same as ReassignPartitions(topic, replicas, nil).
func (c *Client) ReassignAllPartitions(topic string, replicas []uint32) (uint32, error) {
    return c.ReassignPartitions(topic, replicas, nil)
}
```

```go
gen, _ := c.ReassignAllPartitions("events", []uint32{1, 2}) // all partitions
gen, _ = c.ReassignPartitions("events", []uint32{1, 2}, nil) // unchanged: same wire
part := uint32(0)
gen, _ = c.ReassignPartitions("events", []uint32{1, 2}, &part)
```

## Semantics

- Wire partition `codec.ReassignAllPartitions` (0xFFFFFFFF) = all
  partitions of the topic (same as today).
- `ReassignAllPartitions` is a named wrapper; it does not re-encode.
- `ReassignPartitions(topic, replicas, partition)` is unchanged
  (`nil` still means all).
- Nil / empty `replicas` is auto-place (same as today).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `ReassignPartitions` / `adminRoundTrip` (v0.103; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.72).
- Not Kafka AlterPartitionReassignments (no throttle, no
  per-partition error array, no TopicId).

## Tests

Fake TCP stub that records decoded ReassignPartitions partition (same
helper as existing `reassign_partitions_test.go`).

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `ReassignAllPartitions("events", []uint32{1, 2})` | wire partition == `codec.ReassignAllPartitions` (0xFFFFFFFF) |
| Existing `ReassignPartitions` nil / explicit / error cases | still pass |

Existing ReassignPartitions retry / 14 tests must still pass
(`ReassignPartitions` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `ReassignAllPartitions` wraps `ReassignPartitions(topic, replicas, nil)` |
| `clients/go/reassign_partitions_test.go` | all-partitions wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V167_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** AlterPartitionReassignments (API key 45). Native
  opcode **114/115** only. No throttle, per-partition errors, or
  TopicId.
- Nil `partition` still reassigns **all** partitions of the topic
  (`u32::MAX`).
- `ReassignPartitions(topic, replicas, partition)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the ReassignAllPartitions wrapper:

- **Keep the wrapper only.** Do not change `ReassignPartitions`.
- Do not change the ReassignPartitions send loop (v0.72 14 + v0.103
  transient retry).
- Do not change Python, Java, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `ReassignAllPartitions`
  after `ReassignPartitions`
- `clients/go/reassign_partitions_test.go`
- `clients/go/README.md`

## Related

- [V59_SPEC.md](./V59_SPEC.md) — language ReassignPartitions
- [V72_SPEC.md](./V72_SPEC.md) — language admin error 14
- [V103_SPEC.md](./V103_SPEC.md) — language admin_round_trip transient retry
- [V163_SPEC.md](./V163_SPEC.md) — Go ListOffsetsAll (same wrapper pattern)
- [V18_SPEC.md](./V18_SPEC.md) — native 114/115
