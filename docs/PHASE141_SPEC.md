# Phase 141 — N=2 majority ops tooling

**Status:** ✅ Done  

**Theme:** Operator observability for the configured-N journal majority sharp
edge (especially **N=2** with one peer down). Does **not** change the majority
algorithm.

## Goals

1. **Health gauges** on `GET /metrics` (Prometheus text via
   [`render_metrics`](../crates/volant-broker/src/net.rs)):
   - `volant_cluster_configured_brokers` — configured membership size (`1` single-node)
   - `volant_cluster_live_brokers` — live membership size (`1` single-node)
   - `volant_cluster_majority_quorum` — `floor(N/2)+1` for **configured** N (`1` single-node)
   - `volant_cluster_majority_impossible` — **0 or 1**: `1` when
     `live < majority(configured)` (journal majority cannot succeed). Single-node
     always `0`.
2. **Broker API helpers** (public):
   - `configured_broker_count() -> u64`
   - `live_broker_count() -> u64`
   - `majority_quorum_size() -> u64` (same math as `TruncateJournal::majority`)
   - `majority_impossible() -> bool`
3. **Tests** `phase141_n2_majority_ops`: single-node; N=3 all live; N=2 both live;
   N=2 one dead → impossible; N=3 one dead → still reachable; metrics text values.
4. Living docs: ops sharp-edge callout, TODO, PHASE_HISTORY / ROADMAP / INDEX.

## Non-goals

| Deferred | Why |
|----------|-----|
| Live-only majority | Product residual; would change Phase 130 semantics |
| Rollback local truncate on majority fail | Hard; irreversible segment delete |
| Auto-reconfigure N | Ops policy, not broker auto |
| Alertmanager rules files | Operator-owned; metrics only |
| Metadata ISR lag | Phase 142 sibling / separate residual |
| New CLI `cluster health` | No existing cluster CLI surface; metrics-first |

## Semantics

```text
N        = configured static membership size (cluster.toml brokers)
live     = local Membership live set size
quorum   = floor(N/2)+1          # TruncateJournal::majority
impossible = live < quorum
```

Classic failure: **N=2**, one peer down → `live=1`, `quorum=2`,
`majority_impossible=1`. Prefer **odd N (3+)** for journal / DeleteRecords wait.

## Exit criteria

1. Single-node: all gauges `1` / impossible `0`  
2. N=2 both live → impossible `0`; one dead → impossible `1`  
3. N=3 one dead → live=2, quorum=2 → impossible `0`  
4. Metrics text includes four gauge names with expected values  
5. Docs point operators at the new series; majority algorithm unchanged  

## Honesty

- Gauges reflect **this broker’s local membership view**, not a cluster-wide
  consensus. Controllers and observers can briefly disagree during death detect.
- `majority_impossible=1` means journal note majority **cannot** complete with
  current live set under configured-N rules; it does not itself fail in-flight
  client RPCs (wait mode still uses Phase 135/137 paths).
