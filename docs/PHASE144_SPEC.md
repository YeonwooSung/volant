# Phase 144 — Preferred × session thrash suppress

**Status:** ✅ Done  

**Theme:** Light suppress so PreferredReadReplica does not redirect a consumer
that already holds a Kafka fetch session (session owner = leader via Phase 119
encoding). Avoids owner-miss **forward thrash** (119) / promote path (138) when
the client would follow preferred to a same-rack follower with that `session_id`.

## Goals

1. **Suppress rule:** when Fetch already has a non-zero request session id
   (`req_session_id != 0`) and epoch is **not** `FINAL_EPOCH`, do **not** emit
   PreferredReadReplica redirects even if a same-rack candidate exists.
2. **Full fetch still prefers:** `session_id == 0` (and FINAL close path with
   zero id) may still preferred-redirect as in Phase 126/133/140.
3. **Metric:** `volant_preferred_replica_session_suppressed_total` — increment
   when a preferred candidate **would** have been selected but was suppressed
   due to established session.
4. **No double-count with RC:** READ_COMMITTED still uses Phase 140
   `volant_preferred_replica_suppressed_total` only; session suppress is the
   non-RC path.
5. **Tests** `phase144_preferred_session_suppress` + regression 126/133/140.
6. Living docs honesty.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full preferred selector / throttling | Product residual |
| Promote claim fence | Phase 143 sibling |
| Session ownership / re-encode on preferred | Larger design |
| Suppress preferred on first session-less fetch forever | First full fetch may still redirect |
| Serve-from-mirror without promote | **Closed by Phase 147** |

## Rule (encode path)

```text
if version >= 11 && replica_id < 0 {
  if let Some(pref) = select_preferred_read_replica(...) {
    if READ_COMMITTED {
      note_preferred_replica_suppressed();          // Phase 140
      // serve locally
    } else if req_session_id != 0 && req_session_epoch != FINAL {
      note_preferred_replica_session_suppressed();  // Phase 144
      // serve locally; preferred_read_replica = -1
    } else {
      // redirect (empty records)
    }
  }
}
```

## Exit criteria

1. Established session + rack match → preferred = -1; session suppress metric++  
2. `session_id == 0` full fetch still preferred-redirects when eligible  
3. READ_COMMITTED still increments RC suppress only  
4. phase126 / phase133 / phase140 tests green  
5. Docs honesty (KAFKA_COMPAT / features / ops / TODO)

## Honest residual

- First full fetch with rack may still preferred-redirect **and** create a
  session on the leader in the same response; the client can still thrash if it
  immediately uses that new `session_id` on the preferred broker. Suppress
  covers the common sticky-session-then-preferred case (`req_session_id != 0`).
