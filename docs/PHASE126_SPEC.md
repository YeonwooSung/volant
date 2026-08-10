# Phase 126 — PreferredReadReplica / rack-aware Fetch (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** `select_preferred_read_replica` + Fetch PreferredReadReplica emit — **landed**  
- **PR2** Metadata / DescribeCluster / NodeEndpoints rack from `cluster.toml` — **landed**  
- **PR3** multi-node + single-node tests (`phase126_preferred_replica`) — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** KIP-392 subset — when a consumer advertises a rack on Fetch v11+,
the **partition leader** may return `PreferredReadReplica` pointing at a
caught-up same-rack ISR follower so clients can offload read traffic.

## Goals

1. **Parse client `rack_id`** on Kafka Fetch v11+ (classic + flexible).
2. **Select preferred replica** when this broker is the partition leader:
   - Client rack non-empty
   - Cluster mode with broker racks in `cluster.toml`
   - Candidate ∈ local ISR, ≠ leader, rack matches client rack
   - Observed follower LEO ≥ partition HWM (can serve committed data)
   - Deterministic ranking: **Phase 126 MVP used lowest broker id only**;
     **Phase 133** ranks **highest LEO then lowest id**, plus usable-address gate
3. **Response honesty (Kafka-like redirect):** on redirect, emit
   `PreferredReadReplica = id`, **empty records**, still fill HWM/LSO/log_start;
   never omit-unchanged away a preferred redirect.
4. **Advertise rack** on Metadata (classic + flexible), DescribeCluster, and
   Fetch NodeEndpoints when known.
5. **Metric:** `volant_preferred_replica_redirect_total`.
6. Integration tests + living-docs honesty.

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka replica selector / throttling / out-of-sync preferred | MVP is static rack match + LEO≥HWM; Phase 133 adds LEO-desc + usable-addr ranking only |
| Shared multi-broker session store | Orthogonal (Phase 119 forward remains) |
| Rack-aware partition placement / assignor | Placement still round-robin |
| Client-side consumer rack config in `volant-client` | Wire path only; clients send rack_id |
| Dynamic rack / broker reconfiguration | Static `cluster.toml` only |
| Multi-lang / chaos-mesh / long fuzz | Orthogonal |
| Rewrite of Phase 114–125 history | Forbidden |

## Problem (today — post Phase 125)

```text
  Fetch v11+ rack_id parsed but ignored
  PreferredReadReplica always -1
  Metadata rack always null
  All consumer reads hit the leader even when a same-rack follower is caught up
```

## Design principles

1. **Smallest honest design** — no new consensus; use existing ISR + follower_leo.
2. **Leader decides redirect; follower serves local HWM-capped data** (existing
   `fetch_kafka` has no leader gate).
3. **Empty rack / single-node / non-leader → no redirect** (PreferredReadReplica = -1).
4. **Conservative eligibility** — require LEO ≥ HWM observation; no redirect on
   cold followers without LEO samples.
5. **Static membership + static racks** only.

---

## Architecture

### Selection (`Broker::select_preferred_read_replica`)

```text
  client_rack empty?          → None
  no cluster?                 → None
  not partition leader?       → None
  for each id in local ISR:
    id == self?               skip
    not live?                 skip
    usable addr (host+port)?  skip if missing/empty  (Phase 133)
    config.broker(id).rack != client_rack?  skip
    follower_leo[id] < HWM?   skip
  rank by (leo desc, id asc)  (Phase 133; was pure min id in 126 MVP)
  return first or None
```

### Fetch path (`encode_fetch`)

On success path before local `fetch_kafka`, if version ≥ 11, `replica_id < 0`
(consumer), **`!read_committed`**, and selection returns `Some(id)`:

- note metric
- push partition response with preferred = id, empty records, omit = false

**READ_COMMITTED (isolation=1):** preferred redirect is **suppressed** — leader
serves with aborted-marker / LSO filter. Followers may lack a complete soft-abort
marker view; redirect would risk filter divergence vs the leader (MVP residual
vs full marker/LSO parity on candidates).

### Rack advertisement

| Surface | Behavior |
|---------|----------|
| Metadata brokers[] | `broker_rack(id)` from cluster.toml |
| DescribeCluster | same |
| NodeEndpoints (Fetch v16+) | same when emitting CurrentLeader endpoints |

---

## Config

```toml
[[brokers]]
id = 1
host = "10.0.0.1"
port = 9092
rack = "us-east-1a"   # optional; Phase 126 uses for preferred match
```

Unset rack → broker never selected as preferred (and Metadata rack null).

---

## Tests (`phase126_preferred_replica`)

1. Single-node + rack → preferred -1, records present  
2. Multi-node same-rack caught-up follower → preferred id, empty records, metric  
3. Unknown / empty client rack → preferred -1, records present  
4. Preferred broker accepts consumer Fetch without partition error  
5. Metadata v1 emits configured racks  
6. Follower `replica_id ≥ 0` / Fetch v15 ReplicaState → no preferred redirect  
7. **READ_COMMITTED (isolation=1)** suppresses preferred even when selector would return `Some`; leader serves records; **READ_UNCOMMITTED** contrast still redirects  

---

## Honest limitations

- Not full Kafka `replica.selector.class` / throttled preferred logic  
- Follower may have empty local log if ReplicaFetch has not copied bytes yet
  even when LEO was force-advanced in tests; production relies on real
  replication for follower serve usefulness  
- No rack-aware produce / partition assignment  
- Preferred never set for non-leader receivers  
- **No preferred when isolation = READ_COMMITTED** — leader always serves (aborted-marker filter honesty); full marker/LSO parity on preferred candidates deferred  
- Shared session store / preferred+session affinity still deferred  

## Exit criteria

1. Fetch v11+ PreferredReadReplica non--1 when leader + same-rack ISR peer LEO≥HWM  
2. Redirect responses empty records + HWM filled  
3. Metadata rack non-null when configured  
4. Single-node / empty rack remain PreferredReadReplica = -1  
5. `phase126_preferred_replica` passes  
6. Living docs (ROADMAP / features / KAFKA_COMPAT / INDEX / PHASE_HISTORY / README) honest  
