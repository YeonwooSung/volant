# Phase 145 — Rack-aware partition assignment + preferred polish (MVP)

**Status:** ✅ Done

**Theme:** Bounded residual of “full preferred selector / throttling / rack-aware
assignment.” Prefer **assignment depth**: place new topic replicas across racks
when `cluster.toml` brokers declare ≥2 distinct racks. Light preferred polish
is a counter only (no Kafka-style preferred redirect throttle).

## Goals

1. **Rack-aware `assign_replicas`** when ≥2 distinct configured `rack: Some`
   values exist among cluster brokers. Maximize rack diversity for successive
   replica slots; leader remains the first replica.
2. **Legacy round-robin preserved** when all brokers lack racks, only one
   distinct configured rack, single-node, or env disables diversity.
3. **Env:** `VOLANT_RACK_AWARE_ASSIGNMENT` default **on**; set `0` / `false` /
   `no` / `off` to force legacy placement even on multi-rack clusters.
4. **Wire** into `Broker::create_topic_cluster` and create-partitions expansion
   with racks from `ClusterConfig`.
5. **Metric:** `volant_rack_aware_assignment_total` when the diversity path is
   used on create / create-partitions.
6. **Tests** `phase145_rack_aware_assignment` + existing assignment unit tests.
7. Living docs honesty.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full Kafka preferred throttling / TCP probe | Product residual (beyond counter) |
| Rebalance / reassignment of existing topics | Orthogonal; create-time only |
| Phases 146–148 siblings | Out of scope |
| Serve-from-mirror / incremental MirrorPut | Separate P3 residuals |

## Algorithm (deterministic)

```text
brokers sorted by id
if env off OR distinct configured racks (rack: Some) < 2:
  legacy RR: start = (p + topic_hash) % N; take next rf ids
else:
  group by rack (None → unique pseudo-rack "norack-{id}")
  rack groups ordered by BTreeMap key
  for each partition p:
    start_rack = (p + topic_hash) % num_racks
    for slot in 0..rf:
      try successive rack groups from (start_rack + slot)
      pick next unused broker in group (ids ascending)
      if rf > racks, wrap and pick next unused in rack
```

## Exit criteria

1. Multi-rack a,a,b + RF=2 → every partition’s replicas span both racks  
2. No racks → identical to legacy `assign_replicas_round_robin`  
3. Single rack → RF filled; no panic; legacy path  
4. `VOLANT_RACK_AWARE_ASSIGNMENT=0` → legacy even with multi-rack  
5. Integration create_topic on 3-node multi-rack; metadata p0 rack-diverse;
   metric increments  
6. Docs honesty (TODO P3 slice closed; residual throttling/rebalance open)

## Honest residual

- Preferred selector still lacks full Kafka throttling / probe depth (Phases
  126/133/140/144 remain; this phase does not add redirect rate limits).
- Existing topics are not re-placed when racks are added later.
- Pseudo-racks for `rack: None` only participate when diversity is already
  active (≥2 real racks); they do not alone enable the diversity path.
