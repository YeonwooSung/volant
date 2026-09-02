# v0.65 — DeleteRecords leader redirect on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V52_SPEC.md](./V52_SPEC.md):
DeleteRecords error **13** (`NotLeaderForPartition`) is **not**
auto-redirected. v0.43 already redirects Produce/Fetch. Reuse the same
`redirect_to_leader` / `redirectToLeader` helper and `max_redirects`
budget on `delete_records`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. On DeleteRecords (opcode **44**): if `error_code == 13` **or**
   `BrokerError` / `BrokerException` with code 13, and redirect
   attempts remain (`1 + max_redirects`, same as produce/fetch), call
   the existing metadata redirect helper for `(topic, partition)` and
   retry the **same** DeleteRecords (same `wait_majority`).
2. If the redirect helper returns false / empty leader, raise the
   original 13.
3. Other errors (2 not found, etc.) still raise immediately.
4. Default `max_redirects=1` already on Client. `max_redirects=0`
   raises on the first 13 (no Metadata).
5. No new public methods. Document that error 13 now follows
   Produce/Fetch redirect.

## Non-goals

| Deferred | Why |
|----------|-----|
| Redirect on other admin RPCs | CreatePartitions / Describe-AlterConfigs / DeleteOffsets / ACL stay on the original connection |
| Kafka `NOT_LEADER_OR_FOLLOWER` on the Kafka shim | Redirect still uses native Metadata (v0.43) |
| Change wait-off dual-ACK (v0.45) | Broker env; this slice does not touch it |
| New native opcodes / Kafka API keys | Frozen |
| Broker / protocol / Rust client changes | Wire and Rust redirect already exist |
| Phase 155 / homemade Raft | Frozen |

## Semantics

Same budget as Produce/Fetch:

- Default: one initial send + one redirect (`max_redirects=1`).
- `max_redirects=0`: raise on the first 13; no Metadata.
- Helper false (unknown topic / unknown broker / empty host): raise 13.
- Retry uses the same encoded body, including the Phase 137
  `wait_majority` trailer.

## API

No new public methods. Existing:

```python
c.delete_records(topic, partition, before_offset, wait_majority=0)
```

```go
c.DeleteRecords(...)
c.DeleteRecordsWithWaitFlag(...)
```

```java
c.deleteRecords(...)
```

Error 13 now follows Produce/Fetch redirect. Other admin RPCs do not.

## Tests

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| First DeleteRecords returns 13; Metadata names a leader; second DeleteRecords ok | success, `low_watermark` parsed; two DeleteRecords RPCs |
| `max_redirects=0` and first 13 | raise 13; no second DeleteRecords; no Metadata |
| Redirect helper fails (unknown topic) | raise 13 |
| Success + `wait_majority` trailer | unchanged from v0.52 |

## Merge notes

Sibling slices **v0.61 / v0.64** also edit `Client`. When merging:

- **Keep produce/fetch redirect loops.** Only wrap `delete_records` /
  `DeleteRecords` / `deleteRecords`.
- Do not drop other admin RPCs or change their no-redirect behavior.
- Do not change the broker, Kafka shim, or Rust client in this merge.

## Honesty leftovers

- Redirect still uses Metadata (v0.43). Not Kafka
  `NOT_LEADER_OR_FOLLOWER` on the Kafka shim.
- Admin RPCs other than DeleteRecords still do not redirect.
- Cluster wait-off ACK (v0.45) is unchanged.
- No Kafka API keys / opcodes / Phase 155.

See [V43_SPEC.md](./V43_SPEC.md) (Produce/Fetch redirect),
[V52_SPEC.md](./V52_SPEC.md) (DeleteRecords on language clients),
[PHASE14_SPEC.md](./PHASE14_SPEC.md) (native 44/45), and
[V45_SPEC.md](./V45_SPEC.md) (wait-off dual ACK).
