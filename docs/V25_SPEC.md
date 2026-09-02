# v0.25 — Fetch-session dual-epoch converge

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the Phase 147 honesty hole “two peers serve the same session
id without a promote (dual-epoch)” with a deterministic converge rule.

This is **best-effort session-mirror converge**. It is **not** a Raft session
registry and does **not** change the default Phase 147 serve-from-mirror path
for a single owner-miss.

## Goals

1. When **two peers** both hold a usable **unclaimed** primary (`promoted_by == 0`)
   for the same session id, inbound MirrorPut / `converge_dual_epoch` keeps
   **exactly one** owner and demotes the loser to a foreign mirror.
2. **Winner rule** (documented, tested):
   1. Higher `mirror_gen` wins (beats a higher epoch on the other side).
   2. Else higher session `epoch` wins.
   3. Else lowest non-zero `promoted_by` / owner id wins (`0` = no claim, loses
      to a real id).
   4. Else keep local.
3. Loser **does not serve as owner**. Increment
   `volant_fetch_session_dual_epoch_converge_total`.
4. Default **on**. `VOLANT_SESSION_DUAL_EPOCH_CONVERGE=0` (or `false`/`no`/`off`)
   disables (regression escape: both unclaimed primaries can remain).
5. Phase 147 single owner-miss serve-from-mirror **unchanged**.

## Non-goals

| Deferred | Why |
|----------|-----|
| Raft / controller session registry | Hot SoT; larger than this residual |
| Re-encode `session_id` owner bits | Client recreate; keep id stable |
| Change 147 default on single owner-miss | Correctness hole is *dual* unclaimed primary |
| Kafka API keys / openraft / Phase 155 | Sibling slices |
| Fan-out on demote | Best-effort; loser just stops being owner |

## Trigger

Converge runs on `apply_mirror_put` (inbound MirrorPut) and the helper
`converge_dual_epoch` / `converge_dual_epoch_pair` when:

- Local has a **primary** for the session id
- Incoming is also an unclaimed copy (`promoted_by == 0` on **both**)
- The knob is on

If either side has an exclusive promote claim (`promoted_by != 0`), the
existing Phase 143 replace-or-reject path applies (no demote).

Phase 147 owner-miss with **only** a foreign mirror (no local primary) still
serves from that mirror without promote.

## Winner rule vs older fences

| Compare | Order |
|---------|--------|
| `session_is_newer` (139) | `mirror_gen`, epoch, `last_activity_ms` |
| `session_claim_wins` (143) | `session_is_newer`, then lowest non-zero `promoted_by` |
| `session_dual_epoch_wins` (v0.25) | `mirror_gen`, epoch, lowest non-zero claim/owner. **No** activity |

Higher `mirror_gen` **beats** a higher epoch. That is intentional: gen is the
mutation fence; epoch can diverge independently when two peers serve without
promote.

## Config

| Knob | Default | Meaning |
|------|---------|---------|
| `VOLANT_SESSION_DUAL_EPOCH_CONVERGE` | **on** | Demote unclaimed dual-primary loser. `0`/`false`/`no`/`off` → no-op (today’s dual-primary) |

Runtime setter: `FetchSessionManager::set_dual_epoch_converge` (tests / live).

## Path

```text
apply_mirror_put / converge_dual_epoch(local primary, incoming):
  if knob off → existing 139/143 path (replace primary if claim-wins)
  if either promoted_by != 0 → Phase 143 claim fence (replace or reject)
  both unclaimed:
    incoming wins → remove local primary; install incoming as mirror;
                    increment dual_epoch_converge_total
    local wins / tie → keep primary; reject put
```

`converge_dual_epoch_pair(a, b, id)` compares two in-process managers and
demotes only the loser (metric on the loser). True tie keeps both locals.

## Metrics

| Metric | Meaning |
|--------|---------|
| `volant_fetch_session_dual_epoch_converge_total` | Unclaimed dual-primary demotes (loser became mirror) |

Existing serve-from-mirror / promote / claim metrics unchanged.

## Tests

```bash
cargo test -p volant-broker --test v25_dual_epoch -- --test-threads=1
cargo test -p volant-broker --test phase147_serve_from_mirror -- --test-threads=1
cargo test -p volant-broker --test phase143_promote_claim_fence -- --test-threads=1
```

## Honesty leftovers

- Not Raft / not a shared session registry. Convergence requires a MirrorPut
  or an explicit helper call; until then two unclaimed primaries can exist.
- Mirror-only 147 mutations still do **not** fan out MirrorPut, so two peers
  that *only* serve from mirrors (owner dead, no primary) do not self-converge
  until a put or helper runs. This slice closes **dual unclaimed primary**.
- Demote does not re-encode `session_id` owner bits; Phase 119 may still
  forward to the encoded owner.
- Put lag/fail still **70**. Best-effort mirrors remain best-effort.
- `last_activity_ms` is not a dual-epoch tie-break (unlike Phase 139).
