# Phase 143 — Fetch session promote claim fence (lowest-id)

**Status:** ✅ Shipped  
**Theme:** Deterministic dual-promote convergence: when multiple peers promote the
same equal-freshness mirror after owner death, **lowest non-zero `promoted_by`**
wins via existing MirrorPut exchange. Not Raft.

## Problem

After Phase 138/139, owner death can race multiple peers into
`promote_from_mirror(session_id)` on identical (or equal-gen) mirrors. Dual
primaries for one `session_id` later surface client **71**
(`INVALID_FETCH_SESSION_EPOCH` / fenced epoch). `mirror_gen` fences *stale*
puts but does not break ties when two peers promote the *same* snapshot.

## Goals

1. **`promoted_by: u32` on session** — durable + MirrorPut JSON; `0` = never
   claim-promoted (original create path); non-zero = node that last
   claim-promoted (or winning claim after fence).
2. **Claim compare** — after `session_is_newer` (gen/epoch/activity), equal
   freshness uses lowest non-zero `promoted_by`.
3. **`promote_from_mirror` stamps claim** — empty-primary promote sets
   `promoted_by = owner_node_id` (leave `0` on single-node).
4. **`apply_mirror_put` claim-aware** — losing put does not clobber; metric
   `volant_fetch_session_promote_claim_reject_total`.
5. Tests `phase143_promote_claim_fence` + phase138/139 still green.
6. Living docs; residual honesty: best-effort MirrorPut, not Raft.

## Non-goals

| Deferred | Why |
|----------|-----|
| Raft session registry | Larger product |
| Serve-from-mirror without promote | **Closed by Phase 147** (dual-epoch residual remains) |
| Re-encode session_id owner bits | Orthogonal |
| Preferred × session thrash | Phase 144 candidate |
| New inter-broker opcode | Claim travels in existing MirrorPut JSON |

## Design

```text
session_claim_wins(incoming, existing):
  if session_is_newer(incoming, existing) → true   // gen/epoch/activity
  if session_is_newer(existing, incoming) → false
  // equal freshness:
  both promoted_by != 0 → lower id wins
  both 0               → false (keep existing)
  incoming 0, existing claim → false
  incoming claim, existing 0 → true

promote_from_mirror(id):
  empty primary + mirror → stamp owner_node_id only if promoted_by==0; else keep
  primary + mirror wins claim → supersede (keep mirror claim or stamp local if 0)
  primary + mirror loses claim → drop mirror; claim_reject if equal-fresh

apply_mirror_put(snapshot):
  primary/mirror exists + !session_claim_wins → reject
    (stale_put if existing strictly newer; else claim_reject)
  else install/replace
```

### JSON field

`StoredFetchSession.promoted_by` (u32, default `0` on missing — old mirrors OK).

### Metrics

| Metric | Meaning |
|--------|---------|
| `volant_fetch_session_promote_claim_reject_total` | Dual-promote / claim-lose drop on put or promote |

## Honest limitations

- Convergence requires peers to **exchange MirrorPut** after promote (existing
  fan-out). Until exchange, dual primaries can briefly exist.
- Not Raft / not a shared session registry; claim fence is best-effort.
- Put lag/fail still **70**; session_id owner bits not re-encoded.

## Exit criteria

1. Dual-promote equal snapshot + mutual MirrorPut → single SoT = lowest claimer  
2. Lower id wins independent of apply order  
3. Higher claimer with strictly newer `mirror_gen` still wins (Phase 139)  
4. Old mirror JSON without `promoted_by` deserializes as `0`  
5. phase138 + phase139 green  
6. Docs updated  
