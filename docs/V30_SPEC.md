# v0.30 — Fetch-session mirror-only self-converge

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the v0.25 honesty leftover “mirror-only pairs still do not
self-converge.” Two peers that only hold foreign mirrors (owner dead, Phase 147
serve-from-mirror) never sent MirrorPut, so they could diverge.

This is **best-effort mirror↔mirror converge**. It is **not** a Raft session
registry, does **not** promote unless Phase 147 `promote_on_miss` is already
on, and does **not** change the default single owner-miss serve-from-mirror
path.

## Goals

1. When serving **from a foreign mirror** (Phase 147) **or** on a short
   periodic / session-touch path, if this node has **no local primary** for
   that session id, **best-effort MirrorPut** the mirror snapshot to other live
   peers (reuse existing opcode 90 / `queue_mirror_put`).
2. `converge_dual_mirror` / `converge_dual_mirror_pair`: if **both** sides are
   mirrors (no primary) for the same session id, pick a winner with the **same
   order as v0.25** (`mirror_gen`, epoch, lowest non-zero `promoted_by` /
   owner id). Loser adopts the winner’s snapshot (overwrite mirror). Metric
   `volant_fetch_session_mirror_converge_total`.
3. Default **on**. Escape `VOLANT_SESSION_MIRROR_CONVERGE=0` (or `false` /
   `no` / `off`).
4. Do **not** promote to primary unless Phase 147 `promote_on_miss` is already
   on. This slice is **mirror↔mirror** only. A `from_mirror` put never
   replaces or demotes a local primary.

## Non-goals

| Deferred | Why |
|----------|-----|
| Raft / controller session registry | Hot SoT; larger than this residual |
| Re-encode `session_id` owner bits | Client recreate; keep id stable |
| Change 147 default on single owner-miss | Correctness hole is *diverged mirrors* |
| Promote on mirror converge | Explicitly out of scope |
| Kafka API keys / openraft / Phase 155 | Sibling slices |
| Incremental MirrorPut from a foreign mirror | Full snapshot is enough |

## Trigger

Converge runs on inbound MirrorPut (`from_mirror=true`) and the helper
`converge_dual_mirror` / `converge_dual_mirror_pair` when:

- Local has **no** primary for the session id
- Local already holds a foreign mirror
- Incoming is also a mirror snapshot (`from_mirror` on the wire, or the
  in-process helper)
- The knob is on

Fan-out is queued (best-effort) when:

- `try_owner_miss_local_serve` serves from a foreign mirror (no promote)
- `begin_incremental_from_any` advances a mirror-only session
- Phase 97 sweep calls `queue_foreign_mirror_puts` then
  `schedule_session_mirror_fanout`

Owner→peer puts (`from_mirror=false`) stay on the existing Phase 139/143 /
v0.25 path.

## Winner rule

Same as v0.25 `session_dual_epoch_wins` (no `last_activity_ms`):

1. Higher `mirror_gen` wins (beats a higher epoch on the other side).
2. Else higher session `epoch` wins.
3. Else lowest non-zero `promoted_by` / owner id wins (`0` = no claim, loses
   to a real id).
4. Else keep local.

Loser **overwrites** its mirror with the winner’s snapshot. Neither side
becomes primary.

## Config

| Knob | Default | Meaning |
|------|---------|---------|
| `VOLANT_SESSION_MIRROR_CONVERGE` | **on** | Mirror↔mirror overwrite + fan-out from 147/touch/sweep. `0`/`false`/`no`/`off` → no-op (today’s diverged mirrors) |

Runtime setter: `FetchSessionManager::set_mirror_converge` (tests / live).

## Path

```text
serve-from-mirror / begin_incremental_from_any (mirror) / sweep:
  if knob on and no local primary → queue_mirror_put (export from_mirror=true)

apply_mirror_put / converge_dual_mirror(local mirror, incoming):
  if incoming from_mirror and local has primary → ignore (do not demote)
  if knob off → existing 139 claim-wins on the mirror table
  both mirrors, no primary:
    incoming wins → overwrite local mirror;
                    increment mirror_converge_total
    local wins / tie → keep local
```

`converge_dual_mirror_pair(a, b, id)` compares two in-process managers and
overwrites only the loser (metric on the loser). True tie keeps both locals.
Either side holding a primary is a no-op.

## Metrics

| Metric | Meaning |
|--------|---------|
| `volant_fetch_session_mirror_converge_total` | Mirror-only loser overwrites (adopted winner snapshot) |

Existing serve-from-mirror / promote / dual-epoch / claim metrics unchanged.

## Tests

```bash
cargo test -p volant-broker --test v30_mirror_converge -- --test-threads=1
cargo test -p volant-broker --test v25_dual_epoch -- --test-threads=1
cargo test -p volant-broker --test phase147_serve_from_mirror -- --test-threads=1
```

## Honesty leftovers

- Not Raft / not a shared session registry. Convergence requires a MirrorPut
  or an explicit helper call; until then two foreign mirrors can diverge.
- Periodic fan-out is best-effort (sweep interval + Put debounce / coalesce).
  No client Fetch and a paused sweeper (`sweep_interval_ms=0`) still leave
  mirrors diverged until a serve/touch.
- `from_mirror` puts are full snapshots (no Phase 146 delta cache on the
  mirror table).
- Put lag/fail still **70**. Best-effort mirrors remain best-effort.
- `last_activity_ms` is not a dual-mirror tie-break (unlike Phase 139).
- Demote of unclaimed **primaries** remains v0.25; this slice does not
  promote and does not demote a primary from a mirror-only put.
- `session_id` owner bits are not re-encoded; Phase 119 may still forward
  to the encoded (dead) owner first.
