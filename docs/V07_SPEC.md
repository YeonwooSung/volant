# v0.7 — Preferred replica throttle + TCP probe

**Status:** landed (bounded MVP)  
**Crate:** 0.2.0 (unchanged)  
**Does not open Phase 155.** Does not add Kafka API keys or version ratchets.

## Goals

1. **Redirect throttle (opt-in, default off).**  
   `VOLANT_PREFERRED_REPLICA_THROTTLE_MS` — parse as `u32` milliseconds.
   Unset / `0` / invalid → **0** (today’s Fetch `throttle_time_ms` unchanged).
   When Fetch **selects** a preferred replica (`Some(id)`) **and** actually
   emits a PreferredReadReplica redirect, and `throttle_ms > 0`, set the
   Fetch response **top-level** `throttle_time_ms` to
   `max(existing, configured)`. When preferred is **not** selected (no rack,
   single-node, RC/session suppress, no eligible peer), do not add this throttle.
   Metric: `volant_preferred_replica_throttled_total` (incremented when this
   throttle is applied).

2. **TCP / connect probe (opt-in, default off).**  
   `VOLANT_PREFERRED_REPLICA_TCP_PROBE` — `1` / `true` / `yes` / `on` enables;
   default **off**. When enabled, `select_preferred_read_replica` skips a
   candidate whose advertised `host:port` fails a short TCP connect
   (`TcpStream::connect_timeout`, ~75ms). Unresolvable addr / connect fail /
   timeout → skip that peer (same as not usable). Metric:
   `volant_preferred_replica_probe_fail_total` on **each** failed probe.

3. Default off means Phase 126 / 133 / 140 / 144 tests stay green with no env.

## Algorithm addition

```text
candidates = ISR − self ∩ live ∩ usable_addr ∩ same_rack
             ∩ LEO≥HWM ∩ (optional lag ≤ max_leo_lag)
             ∩ (optional TCP connect to advertised host:port)
rank (leo desc, id asc)
```

Probe is an **additional** filter. It does not replace rack, LEO, ISR, live,
or usable-addr gates.

## Env

| Env | Default | Meaning |
|-----|---------|---------|
| `VOLANT_PREFERRED_REPLICA_THROTTLE_MS` | unset → **0** | Fetch top-level throttle on preferred **redirect** |
| `VOLANT_PREFERRED_REPLICA_TCP_PROBE` | unset → **off** | Skip peers that fail a short advertised-addr TCP connect |

Existing: `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` (Phase 140) unchanged.

## Metrics

| Series | When |
|--------|------|
| `volant_preferred_replica_throttled_total` | Fetch applied the configured preferred-redirect throttle |
| `volant_preferred_replica_probe_fail_total` | One increment per failed advertised-addr probe |

Registered next to the existing preferred series in `broker/mod.rs` +
`net/metrics_http.rs`.

## Tests

`crates/volant-broker/tests/v07_preferred_throttle_probe.rs`

- Default no env → same eligible peer; Fetch v11+ `throttle_time_ms == 0`
- Throttle env set → redirect Fetch has configured throttle + metric++;
  no-redirect Fetch stays 0
- Probe off → peer selectable without a live accept
- Probe on → listening advertised port selected; closed port / `127.0.0.1:1`
  skipped; probe-fail metric increments
- Rack / LEO / ISR gates still apply

## Non-goals (honesty)

| Deferred | Why |
|----------|-----|
| Kafka client-quota / produce-fetch quota throttle | This is a **redirect** `throttle_time_ms` hint only |
| Kafka broker-to-broker Fetch probe / replica fetcher health | Connect probe only; not a Fetch RPC |
| Async probe cache / background liveness | Sync `connect_timeout` inside the selector is enough for MVP |
| Preferred replica reassignment / leadership move | Selector + empty-records redirect only |
| Chaos-mesh / partition injection | Out of scope |
| Change rack-aware assignment (145) | Orthogonal |
| Homemade Raft / new Kafka API keys | Frozen |

This is **not** a full Kafka preferred-replica selector and **not** a quota
implementation.
