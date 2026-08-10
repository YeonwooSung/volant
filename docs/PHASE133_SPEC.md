# Phase 133 — Preferred read-replica selector polish (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Improve Phase 126 PreferredReadReplica selection honesty without full
Kafka selector / throttling. Complements Phase 134 (heartbeat mesh) but is
orthogonal in code.

## Goals

1. **Endpoint usability gate:** Prefer only peers that are **live** (existing)
   and have a resolvable configured address (`broker_addr` present / non-empty
   host+port). Skip unusable endpoints so redirects do not point at ghosts.
2. **Smarter ranking among eligible peers:** Among same-rack ISR followers with
   LEO ≥ HWM, prefer **highest observed LEO**, then **lowest broker id** as
   tiebreak (replaces pure lowest-id-only).
3. Keep Phase 126 gates: leader-only selection, empty rack → no redirect,
   READ_COMMITTED suppress (Phase 126 residual), follower `replica_id` gate.
4. Tests covering ranking + usability; living docs honesty when shipping.
5. Metric remains `volant_preferred_replica_redirect_total` (no new metric required).

## Non-goals

| Deferred | Why |
|----------|-----|
| Full Kafka preferred selector / throttling / out-of-sync preferred | Larger |
| Shared session store / preferred+session affinity | Orthogonal |
| Rack-aware partition placement | Orthogonal |
| READ_COMMITTED marker/LSO parity on preferred candidates | Still deferred |

## Design

```text
  candidates = ISR − self ∩ live ∩ same_rack ∩ LEO≥HWM ∩ usable_addr
  sort by (leo desc, id asc)
  return first or None
```

Touch points:
- `Broker::select_preferred_read_replica` in `broker.rs`
- Fetch path in `produce_fetch.rs` unchanged except via selector behavior
- Tests: `crates/volant-broker/tests/phase133_preferred_selector.rs`
- Keep `phase126_*` green

## Implementation (landed)

- **Usable endpoint:** candidate must have configured broker entry with
  non-empty host and non-zero port, and `broker_addr` non-empty after trim.
- **Ranking:** highest observed `follower_leo`, then lowest broker id (tiebreak).
- Existing gates retained: leader-only, non-empty client rack, same-rack ISR,
  live membership, LEO ≥ HWM. Fetch path still suppresses under READ_COMMITTED
  and follower `replica_id` / ReplicaState.
- Metric unchanged: `volant_preferred_replica_redirect_total`.

### Tests (`phase133_preferred_selector`)

| Test | Asserts |
|------|---------|
| `higher_leo_wins_over_lower_id` | Higher LEO beats lower-id peer; equal LEO → min id |
| `non_live_higher_leo_skipped` | Dead high-LEO peer skipped; remaining or None |
| `empty_addr_peer_skipped_when_other_eligible` | Empty-host peer never preferred |

## Exit criteria

1. Highest-LEO same-rack peer wins over lower-LEO lower-id peer ✅  
2. Non-live / missing-addr peer not selected when another eligible exists ✅  
3. phase126 preferred + isolation still green ✅  
4. Docs 0–133 when shipped ✅  

## Honest limitations

- Still no load/throttling; process-local LEO samples only  
- Usability is config+liveness, not active TCP probe  
