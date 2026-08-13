# Phase 146 — Incremental/delta MirrorPut wire (MVP)

**Status:** ✅ Shipped  
**Theme:** Shrink best-effort session mirror fan-out payloads when only a subset
of topics (or only metadata) changed. No new opcode; JSON `mode` field is enough.

## Goals

1. **Snapshot schema extension** on `StoredFetchSession` (MirrorPut JSON + durable
   shape):
   - `mode: String` — `"full"` | `"delta"`; **default `"full"`** for backward
     compat (`#[serde(default = "full")]`).
   - For delta: `topics` = **upserts only**; `remove_topic_keys: Vec<String>`
     (default empty).
   - Full mode: `topics` is the complete map; `remove_topic_keys` ignored/empty.
2. **Apply path** (`apply_mirror_put`):
   - Full (or missing mode): current replace/install via `session_claim_wins`.
   - Delta: load existing primary **or** mirror for id; merge upserts; apply
     removes; set epoch / activity / `mirror_gen` / `promoted_by` from payload;
     claim-fence as today. **Delta without base** installs upserts as full state.
3. **Export:**
   - `export_session_bytes` remains **full** (`mode=full`).
   - `export_session_delta_bytes(session_id, prev)`:
     - `prev == None` → full.
     - Topics equal → metadata-only delta (empty upserts/removes).
     - Topics differ → upserts + remove keys for gone topics.
     - Always includes current `mirror_gen` / epoch / activity / `promoted_by`.
4. **Fan-out prefers delta:** `export_mirror_put_bytes` uses in-memory
   `last_mirrored: Mutex<HashMap<i32, FetchSession>>`; first put full, later
   delta. Opcode **90** unchanged. After export schedule, `note_last_mirrored`.
5. **Metric:** `volant_fetch_session_mirror_delta_puts_total` (delta payloads
   sent **or** applied).
6. **Tests** `phase146_mirror_put_delta` + unit tests; phase138/139/143 green.
7. Living docs honesty.

## Non-goals

| Deferred | Why |
|----------|-----|
| New opcode | JSON `mode` is enough |
| Serve-from-mirror without promote | Dual-epoch design |
| Rack assignment / preferred residual | Orthogonal (144+) |
| Truncate defer / Raft session registry | Larger product |

## Wire merge notes

| Field | Default | Meaning |
|-------|---------|---------|
| `mode` | `"full"` | `"full"` = complete topics map; `"delta"` = upserts + removes |
| `remove_topic_keys` | `[]` | Topic map keys to drop (delta only) |
| `topics` | `[]` | Full map or upserts |
| `mirror_gen` / `epoch` / `last_activity_ms` / `promoted_by` | as today | Always present on both modes |

Old peers omit `mode` / `remove_topic_keys` → serde defaults → full install path.

## Design

```text
export (fan-out Put):
  if last_mirrored[id] present → export_session_delta_bytes(id, Some(prev))
  else → export full
  note_last_mirrored(id)  # primary clone

apply_mirror_put(snapshot):
  parse StoredFetchSession
  if mode == "delta":
    base = primary[id] or mirror[id] or empty topics
    merge upserts; remove remove_topic_keys
    session.meta = payload meta
  else:
    session = full topics from payload
  session_claim_wins → install primary or mirror
```

## Exit criteria

1. Full put still works (phase138 path)  
2. Delta upsert adds topic/partition to existing mirror  
3. Delta `remove_topic_keys` drops topic  
4. Old JSON without `mode` still applies  
5. Metadata-only delta bumps activity without wiping topics  
6. phase138 / phase139 / phase143 tests green  

## Honest residual

- Best-effort only: missed intermediate deltas can leave peers topic-stale until
  a later full export (no forced resync). `last_mirrored` is process-local.
- Not Raft; claim fence (143) still best-effort MirrorPut exchange.
- Serve-from-mirror without promote still deferred.
