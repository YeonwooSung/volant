# Phase 110 — Non-controller auto-death from heartbeat alive-set diffs (MVP)

## Goals

1. Non-controller brokers detect dead peers from the controller
   `HeartbeatBroker` response `alive_brokers` set **without** waiting for a
   generation-bumped `ClusterState` pull.
2. On an alive-set gap, call [`on_broker_death`] / local ISR shrink + HWM
   recompute (Phase 108 path) so partition leaders that are not the controller
   still unblock `acks=all` promptly.
3. Local membership `expire` on **every** observer (not only the controller)
   also runs death handling — backup when controller heartbeats fail.
4. Tests + living docs honesty.

## Non-goals

- Changing controller election (still lowest live id)
- Lag-based ISR shrink threshold changes
- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Straddle marker clip
- Multi-broker BROKER config fan-out
- Gossip-style peer-to-peer heartbeats (controller remains membership SoT)

## Problem

Phase 108 made `on_broker_death` shrink **local** ISR on every observer, but
non-controllers rarely called it:

```text
controller: expire / kill → on_broker_death → generation bump
non-controller: note_peer_live(all alive_brokers) only
                → wait for ClusterState pull to learn ISR shrink
```

If the partition leader is not the controller, `acks=all` still waited on a
stale dead-follower LEO until the next ClusterState apply (or forever if pull
lagged). Local `tick_cluster` expired peers from membership on non-controllers
but **skipped** `on_broker_death`, so ISR stayed dirty even after session
timeout.

## Fix

| Change | Why |
|--------|-----|
| `Broker::apply_controller_alive_set` | Diff previous live set vs controller `alive_brokers`; `on_broker_death` for gaps; heartbeat-touch survivors |
| `heartbeat_to_controller` uses it | Every successful controller heartbeat reconciles death immediately |
| `tick_cluster` death on all observers | Backup when heartbeats fail / controller unreachable |
| `live_brokers` / `local_partition_isr` | Inspect membership + local ISR (assignment metadata may lag) |

```text
HeartbeatBroker response { alive_brokers }:
  missing = prev_live \ alive  (except self)
  for d in missing: on_broker_death(d)   // Phase 108 local ISR + HWM
  for id in alive: note_peer_live(id)
  if generation advanced: pull ClusterState (unchanged)
```

Controller path unchanged: it still expires on heartbeat/tick and updates the
durable assignment + generation inside `on_broker_death`.

## Contract preserved

- `acks=all` waits only on **remaining live** ISR LEOs
- `|ISR| < min_insync_replicas` → `NotEnoughReplicas` (15)
- Empty ISR restore-last-known on controller assignment path (Phase 108)
- Self is never marked dead from an alive-set gap

## Tests

`crates/volant-broker/tests/phase110_alive_set_death.rs`:

1. Non-controller alive-set diff drops peer from local ISR + membership
2. Leader alive-set death unblocks `acks=all` with stale dead-follower LEO
3. Non-controller `tick_cluster` expire shrinks local ISR
4. Unchanged alive-set is idempotent

## Files

| Path | Change |
|------|--------|
| `crates/volant-broker/src/broker.rs` | `apply_controller_alive_set`, `live_brokers`, `local_partition_isr`; tick death for all |
| `crates/volant-broker/src/net.rs` | Heartbeat response uses alive-set reconcile |
| `crates/volant-broker/tests/phase110_alive_set_death.rs` | Integration tests |
| `docs/PHASE110_SPEC.md` | This spec |
| `ROADMAP.md`, `docs/history/PHASE_HISTORY.md`, `docs/INDEX.md`, `README.md` | Living docs |
| `docs/consistency.md`, `docs/ops.md` | Death observation note |

## Honest limitations

- Membership SoT is still the **controller** alive set (no peer gossip)
- Assignment / Metadata ISR on non-controllers may lag until ClusterState pull;
  **local** partition ISR (produce / HWM) updates immediately
- Controller unreachable → death only after local session timeout via tick
- Does not re-add peers to ISR when they reappear (rejoin / lag path unchanged)

## Still deferred after this

- Straddle marker clip
- Multi-broker 2PC / multi-lang / fuzz CI
- Multi-broker BROKER config fan-out
- Multi-broker session affinity
