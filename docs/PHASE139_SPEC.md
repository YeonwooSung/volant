# Phase 139 — Session mirror polish (debounce + durable + fence)

**Status:** ✅ Shipped  
**Theme:** Polish Phase 138 best-effort session mirrors: coalesce/debounce Puts,
optional durable peer mirrors, light promote fencing. No new opcodes; not Raft.

## Goals

1. **Coalesce dirty ops:** at most one pending op per `session_id` (Put/Delete);
   Delete supersedes Put; Put after Delete wins. Drain order: Deletes then Puts.
2. **Debounced Puts:** `VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default
   **50**; `0` = coalesce only, flush immediately on schedule. **Delete** flushes
   immediately (no debounce wait).
3. **Optional durable mirrors:** `VOLANT_FETCH_SESSION_MIRROR_DURABLE=1` →
   `{data_dir}/__fetch_session_mirrors/state.json`; load with idle TTL filter.
   Default **off**.
4. **Light fencing via `mirror_gen`:** bump on primary mutations; `apply_mirror_put`
   / `promote_from_mirror` prefer higher `mirror_gen`, then epoch, then
   `last_activity_ms`. Stale puts do not clobber newer state.
5. Metrics: coalesced puts, mirrors restored, stale put rejects, promote supersede.
6. Tests `phase139_*` + living docs 0–139.

## Non-goals

| Deferred | Why |
|----------|-----|
| Incremental/delta MirrorPut wire | Coalesce only |
| Serve-from-mirror without promote | **Closed by Phase 147** (dual-epoch residual) |
| Raft / session_id re-encode | Out of scope |
| Full dual-promote elimination | Needs consensus |

## Design

```text
queue Put(id) → map[id]=Put (coalesce); schedule debounced fanout
queue Delete(id) → map[id]=Delete; schedule IMMEDIATE fanout

apply_mirror_put(snapshot with mirror_gen):
  if primary has id and incoming newer → replace primary (converge)
  elif mirror has id and incoming older → drop stale
  else install/replace mirror

promote_from_mirror(id):
  if primary missing → move mirror → primary
  if both: newer mirror supersedes primary; older mirror dropped
```

## Env knobs

| Env | Default |
|-----|---------|
| `VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` | 50 |
| `VOLANT_FETCH_SESSION_MIRROR_DURABLE` | off |

## Exit criteria

1. Multi-mutation before flush → one Put per session per drain  
2. Delete removes peer mirror without waiting min-interval  
3. Durable=1 peer restart restores non-idle mirrors; promote still works  
4. Stale put does not clobber newer primary/mirror  
5. phase138 green  
6. Docs updated  
