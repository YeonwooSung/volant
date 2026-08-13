# Phase 147 — Serve-from-mirror without promote (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** On owner miss, **serve** a foreign fetch-session mirror **without**
claiming ownership via `promote_from_mirror`. Keeps Phase 119 forward as the
live happy path and Phase 138 mirror fan-out. **Not** Raft / not single SoT.

## Problem

Phase 138 always `promote_from_mirror` then serves as primary on owner miss →
dual-promote risk (mitigated by Phase 143 claim fence but still claims
ownership). Residual: serve using foreign mirror without promoting into primary.

## Goals

1. **Read path from mirror** on `FetchSessionManager`:
   - `mirror_session_clone(session_id) -> Option<FetchSession>`
   - `has_servable_session(session_id) -> bool` = primary **or** mirror
   - `begin_incremental_from_any(session_id, epoch)` — primary first, else
     mirror epoch check / advance **without** mirror→primary move
   - Mutations used by `encode_fetch` (`merge_topics`, `note_returned`,
     `forget`, `close`, `snapshot_topics`): on mirror-only sessions apply
     **in-place** to the mirror table (keep foreign) **without**
     `queue_mirror_put` / primary insert
2. **`encode_fetch`:** incremental path uses `begin_incremental_from_any`
3. **`maybe_forward_kafka_fetch`:** on forward fail / missing owner addr:
   prefer serve-from-mirror (no promote); metric
   `volant_fetch_session_serve_from_mirror_total`
4. **Env knobs** (see below)
5. Tests `phase147_serve_from_mirror` + phase138/139 adjusted; phase143 green
6. Living docs honesty (dual-epoch residual)

## Non-goals

| Deferred | Why |
|----------|-----|
| Raft / controller session registry | Hot SoT; larger than MVP |
| Re-encode session_id to new owner | Client recreate; keep id stable |
| Incremental/delta MirrorPut | Chatty full put residual |
| Preferred re-home of session owner | Orthogonal |
| Single SoT across dual mirrors | Honest dual-epoch residual |

## Design

```text
  Client → B, A unreachable, B has foreign mirror:
       B forward fails
       default: record serve_from_mirror → return None
            encode_fetch uses begin_incremental_from_any on mirror
            mutations stay on mirror (no queue_mirror_put)
       PROMOTE_ON_MISS=1: promote_from_mirror → primary serve (Phase 138)
       SERVE_WITHOUT_PROMOTE=0: legacy promote path

  No mirror → empty Fetch top-level 70 (unchanged)
  Owner alive → Phase 119 forward (unchanged)
```

### Env knobs

| Env | Default | Effect |
|-----|---------|--------|
| `VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE` | **on** (`1`) | If mirror present on owner miss → serve without promote. `0`/`false`/`no`/`off` → legacy promote |
| `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS` | **off** (`0`) | If `1`/`true`/`yes` → force promote into primary when mirror present (overrides serve-without-promote) |

Runtime setters: `set_serve_mirror_without_promote` / `set_promote_on_miss`
(for tests and live reconfig of manager flags initialized from env at construct).

### Default behavior change (honesty)

**Before Phase 147:** owner miss + mirror → always promote → primary.  
**After Phase 147:** owner miss + mirror → **serve from mirror**, leave foreign;
`promote_total` unchanged. Promote remains available via
`VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1` or
`VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE=0`.

## Metrics

| Metric | Meaning |
|--------|---------|
| `volant_fetch_session_serve_from_mirror_total` | Owner-miss decisions that served foreign mirror without promote |

Existing promote / claim metrics unchanged.

## Honest limitations

- **Dual-epoch residual:** two peers may both serve their local mirrors without a
  single source of truth; epochs can diverge until the client recreates or a
  promote path re-converges.
- Best-effort mirror; put lag/fail still **70**.
- Not dual-master while owner is reachable (forward still preferred).
- Mirror-only mutations do **not** fan out MirrorPut (not claiming SoT).
- READ_COMMITTED / preferred residual unchanged.

## Exit criteria

1. [x] Mirror-only: `begin_incremental_from_any` + snapshot without promote  
2. [x] Owner dead + mirror → local serve; `promote_total` unchanged  
3. [x] `PROMOTE_ON_MISS=1` still promotes  
4. [x] No mirror → **70**  
5. [x] phase143 claim fence still green; phase138/139 updated for new default  
6. [x] Living docs honesty  

## Tests

- `crates/volant-broker/tests/phase147_serve_from_mirror.rs`
- Unit: `phase147_*` in `crates/volant-broker/src/kafka/fetch_session.rs`
- Adjusted: phase138 / phase139 owner-miss cases assert serve-without-promote

## Implementation notes (shipped)

- `FetchSessionManager::try_owner_miss_local_serve` centralizes the decision for
  `maybe_forward_kafka_fetch`.
- `produce_fetch` incremental uses `begin_incremental_from_any`.
- Prometheus scrape exposes `volant_fetch_session_serve_from_mirror_total`.
