# v0.45 — DeleteRecords wait-off second ACK

**Status:** Shipped (residual slice; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.29 honesty leftover: clustered wait-off is still
irreversible when `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1`. Operators
must now set a **second** ACK env. One env var is no longer enough to lose
an uncommitted tail.

## Goals

1. New env `VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK` default **off**
   (`0` / unset / `false` / `no` / `off`). On values: `1` / `true` /
   `yes` / `on` (same parser as ALLOW).
2. **Clustered** wait-off (flag `2`, or flag `0` + knob off) stays
   local-first **only if both** ALLOW **and** ACK are on.
3. ALLOW on + ACK off → still **upgrade to wait-on** (v0.29 default-safe
   path). Majority miss → native **15** / Kafka **19**, no local truncate.
4. ACK on + ALLOW off → still upgrade (ACK alone is not enough).
5. **Single-node** wait-off is unchanged: ACK is not required (no majority).
   ALLOW is irrelevant there today.
6. Runtime setter `Broker::set_delete_records_irreversible_ack` (mirrors
   ALLOW). Default from env at construct.
7. Metrics: existing `volant_delete_records_wait_off_upgraded_total` ticks
   on every clustered upgrade. New
   `volant_delete_records_wait_off_ack_missing_total` ticks when ALLOW is
   on and ACK is off.
8. Do **not** change wait-on behavior, majority miss errors, or the flag
   `0/1/2` trailer.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rollback after a double-gated wait-off truncate | Segment delete is still irreversible |
| Change flag `1` / wait-on path | Phase 148 already correct |
| Kafka API keys / native trailer change | Flag 0/1/2 unchanged; safety is broker-side |
| Reject instead of upgrade | Prefer upgrade so clients succeed when majority is up |
| Phase 155 / Raft truncate | Out of scope |
| Homemade 154 RequestVote / InstallSnapshot | Frozen |

## Config

| Knob | Default | Meaning |
|------|---------|---------|
| `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE` | **off** | First gate. Unset/`0`/`false`/`no`/`off` → clustered wait-off upgrades. |
| `VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK` | **off** | Second gate. Same on/off parser. Clustered wait-off stays local-first only with ALLOW **and** this. |
| `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` | **off** | Unchanged Phase 135 broker default (merged with request flag first). |

Runtime setters: `Broker::set_delete_records_allow_irreversible`,
`Broker::set_delete_records_irreversible_ack` (tests / live).

## Truth table

Clustered broker, raw wait-off (flag `2` or flag `0` + knob off):

| ALLOW | ACK | Result |
|-------|-----|--------|
| off | off | upgrade to wait-on; `wait_off_upgraded++` |
| on | off | upgrade to wait-on; `wait_off_upgraded++` **and** `wait_off_ack_missing++` |
| off | on | upgrade to wait-on; `wait_off_upgraded++` |
| on | on | wait-off (local-first, **irreversible**) |

Single-node (no `ClusterState`): wait-off stays wait-off for every row.
ACK is not required. Metrics do not tick.

Raw wait-on (flag `1`, or flag `0` + knob on) is unchanged on both
single-node and cluster.

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
            ├─ no ClusterState → wait-off (single-node; ACK unused)
            │
            ├─ clustered + ALLOW on + ACK on → wait-off (explicit double gate)
            │
            └─ clustered + (ALLOW off or ACK off)
                 upgrade to wait-on
                 metric wait_off_upgraded++
                 if ALLOW on and ACK off: wait_off_ack_missing++
                 majority first (Phase 148)
                 miss → 15 / Kafka 19, no truncate
```

## Tests

```bash
cargo test -p volant-broker --test v45_wait_off_ack -- --test-threads=1
cargo test -p volant-broker --test v29_wait_off_safety -- --test-threads=1
```

1. Default (no env): clustered wait-off still upgrades (v0.29 regression).
2. ALLOW=1, ACK unset: clustered wait-off **still upgrades**.
3. ALLOW=1, ACK=1: clustered wait-off stays wait-off.
4. ACK=1, ALLOW unset: clustered wait-off still upgrades.
5. Single-node wait-off does not require ACK (truncate / no upgrade).

v0.29 tests that opt into irreversible cluster truncate now set **both**
envs / setters.

## Honesty leftovers

- **Double gate is still irreversible** when both envs are on. Segment
  files are already gone; there is no rollback.
- This is **not Kafka**. Flag `0/1/2` is a Volant trailer / flex tag;
  librdkafka will not send it. Kafka v0–1 stay flag 0 (broker merge +
  this safety).
- Upgrade uses configured-N journal majority (N=2 one-down still cannot
  wait-succeed; that is the point of the default).
- Direct `Broker::delete_records` (not the client/native dispatch path)
  still truncates locally; safety is on the request wait merge.
- Single-node wait-off remains local-first with neither env.

## Related

- [V29_SPEC.md](./V29_SPEC.md) — first ALLOW gate
- [PHASE137_SPEC.md](./PHASE137_SPEC.md) — native trailer 0/1/2
- [PHASE148_SPEC.md](./PHASE148_SPEC.md) — majority-first truncate
- [PHASE135_SPEC.md](./PHASE135_SPEC.md) — broker wait knob
- [V06_SPEC.md](./V06_SPEC.md) — Kafka flex v2 tag 0
