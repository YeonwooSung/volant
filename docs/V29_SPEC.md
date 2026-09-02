# v0.29 — DeleteRecords wait-off safety

**Status:** Shipped (residual slice; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the clustered wait-off foot-gun: local-first DeleteRecords
truncate is irreversible. Default to wait-on when a broker has cluster state
unless the operator explicitly allows the old path.

## Goals

1. New env `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE` default **off** (`0`).
   `1`/`true`/`yes`/`on` (case-insensitive) re-enables today's local-first
   wait-off on clusters.
2. When the broker is in **cluster mode** (`ClusterState` present) **and**
   effective wait would be **off** **and** the env is off: **upgrade to
   wait-on** (Phase 148 majority-first). Majority miss → native **15** /
   Kafka **19**, no local truncate. Clients still succeed when majority is
   available.
3. When the env is on, wait-off stays the irreversible local-first path
   (flag `2`, flag `0` + knob off).
4. **Single-node** (no cluster): wait-off remains allowed (no majority exists).
5. Metric `volant_delete_records_wait_off_upgraded_total` ticks when a
   wait-off decision is upgraded to wait-on.
6. Default env unset = safe. Existing wait-on tests unchanged. Existing
   wait-off **cluster** tests opt into the old path via
   `set_delete_records_allow_irreversible(true)` (same as
   `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1`).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rollback after an allowed wait-off truncate | Segment delete is still irreversible |
| Change flag `1` / wait-on path | Phase 148 already correct |
| Kafka API keys / native trailer change | Flag 0/1/2 unchanged; safety is broker-side |
| Reject instead of upgrade | Prefer upgrade so clients succeed when majority is up |
| Phase 155 / Raft truncate | Out of scope |

## Config

| Knob | Default | Meaning |
|------|---------|---------|
| `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE` | **off** | Clustered wait-off stays local-first. Unset/`0`/`false`/`no`/`off` → upgrade to wait-on. |
| `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` | **off** | Unchanged Phase 135 broker default (merged with request flag first). |

Runtime setter: `Broker::set_delete_records_allow_irreversible` (tests / live).

## Path

```text
  request flag 0/1/2
       │
       ▼
  raw = effective merge (Phase 137)
       │
       ├─ raw ON  → wait-on (Phase 148; unchanged)
       │
       └─ raw OFF
            │
            ├─ no ClusterState → wait-off (single-node; unchanged)
            │
            ├─ ALLOW_IRREVERSIBLE on → wait-off (explicit ops)
            │
            └─ clustered + allow off
                 upgrade to wait-on
                 metric wait_off_upgraded++
                 majority first (Phase 148)
                 miss → 15 / Kafka 19, no truncate
```

## Tests

```bash
cargo test -p volant-broker --test v29_wait_off_safety -- --test-threads=1
cargo test -p volant-broker --test phase148_defer_truncate_majority -- --test-threads=1
cargo test -p volant-broker --test phase137_delete_records_request_wait_flag -- --test-threads=1
```

1. Cluster N=2, one dead, force wait-off (flag 2), env unset → **15**, no truncate.
2. Same + `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1` → local truncate (old path).
3. Single-node force wait-off, env unset → still truncates.
4. Phase 148 wait-on suite still green.

## Honesty leftovers

- Allowed wait-off is still **irreversible** (segment files already gone).
- Upgrade uses configured-N journal majority (N=2 one-down still cannot
  wait-succeed; that is the point of the default).
- Kafka v0–1 stay flag 0 (broker merge + this safety). Flex v2 tag 0 still
  the only per-request Kafka wait field.
- Direct `Broker::delete_records` (not the client/native dispatch path)
  still truncates locally; safety is on the request wait merge.

## Related

- [PHASE137_SPEC.md](./PHASE137_SPEC.md) — native trailer 0/1/2
- [PHASE148_SPEC.md](./PHASE148_SPEC.md) — majority-first truncate
- [PHASE135_SPEC.md](./PHASE135_SPEC.md) — broker wait knob
- [V06_SPEC.md](./V06_SPEC.md) — Kafka flex v2 tag 0
