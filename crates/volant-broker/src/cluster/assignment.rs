//! Replica placement for new topics (round-robin + optional rack diversity).

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

/// Stable topic hash for placement (FNV-1a 32-bit).
pub fn topic_hash(name: &str) -> u32 {
    const FNV_OFFSET: u32 = 0x811c_9dc5;
    const FNV_PRIME: u32 = 0x0100_0193;
    let mut h = FNV_OFFSET;
    for b in name.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Env flag: auto-expand under-replicated topics after `AddBroker` (v0.18).
pub const ENV_REASSIGN_ON_ADD: &str = "VOLANT_REASSIGN_ON_ADD";

/// Whether the controller should expand replica sets after a successful add.
///
/// Default **off**. Set `VOLANT_REASSIGN_ON_ADD=1` (or `true`/`yes`/`on`) to
/// opt in. New replicas start empty (no live segment copy).
pub fn reassign_on_add_enabled() -> bool {
    match std::env::var(ENV_REASSIGN_ON_ADD) {
        Ok(s) => {
            let t = s.trim();
            t == "1"
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("yes")
                || t.eq_ignore_ascii_case("on")
        }
        Err(_) => false,
    }
}

/// Whether rack-aware diversity is enabled (Phase 145).
///
/// Default **on**. Set `VOLANT_RACK_AWARE_ASSIGNMENT=0` (or `false`/`no`/`off`)
/// to force legacy round-robin even when multi-rack brokers are configured.
pub fn rack_aware_assignment_enabled() -> bool {
    match std::env::var("VOLANT_RACK_AWARE_ASSIGNMENT") {
        Ok(s) => {
            let t = s.trim();
            !(t == "0"
                || t.eq_ignore_ascii_case("false")
                || t.eq_ignore_ascii_case("no")
                || t.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    }
}

/// Count distinct configured rack strings among brokers that set `rack: Some`.
///
/// Empty/whitespace racks are ignored. Used to decide multi-rack diversity.
pub fn distinct_configured_racks<'a, I>(brokers: I) -> usize
where
    I: IntoIterator<Item = (u32, Option<&'a str>)>,
{
    let mut racks = HashSet::new();
    for (_id, rack) in brokers {
        if let Some(r) = rack.map(str::trim).filter(|s| !s.is_empty()) {
            racks.insert(r.to_owned());
        }
    }
    racks.len()
}

/// Whether the rack-diversity path would be used for this broker set (Phase 145).
pub fn will_use_rack_aware_assignment<'a, I>(brokers: I) -> bool
where
    I: IntoIterator<Item = (u32, Option<&'a str>)>,
{
    rack_aware_assignment_enabled() && distinct_configured_racks(brokers) >= 2
}

/// Assign `rf` replicas for `num_partitions`.
///
/// `brokers` is `(id, optional rack)` — ids need not be pre-sorted; empty list panics.
///
/// * **Legacy round-robin** when fewer than two distinct configured racks
///   (`rack: Some`), env disables diversity, or only one broker: for partition
///   `p`, replicas start at index `(p + topic_hash) % N` over ids sorted
///   ascending, then the next `rf - 1` brokers (wrapping).
/// * **Rack-aware diversity** (Phase 145) when ≥2 distinct `rack: Some` values
///   and env allows: maximize rack diversity across successive replica slots.
///   Brokers with `rack: None` get a unique pseudo-rack `norack-{id}`.
///
/// Returns `(replica_sets, rack_aware_used)` — one replica list per partition;
/// leader is still the first replica.
pub fn assign_replicas<'a, I>(
    topic_name: &str,
    num_partitions: u32,
    brokers: I,
    rf: u32,
) -> (Vec<Vec<u32>>, bool)
where
    I: IntoIterator<Item = (u32, Option<&'a str>)>,
{
    let mut list: Vec<(u32, Option<String>)> = brokers
        .into_iter()
        .map(|(id, rack)| {
            let r = rack
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_owned());
            (id, r)
        })
        .collect();
    assert!(!list.is_empty(), "brokers must not be empty");
    list.sort_by_key(|(id, _)| *id);
    // De-dupe by id (keep first rack after sort).
    list.dedup_by_key(|(id, _)| *id);

    let ids: Vec<u32> = list.iter().map(|(id, _)| *id).collect();
    let rack_refs: Vec<(u32, Option<&str>)> = list
        .iter()
        .map(|(id, r)| (*id, r.as_deref()))
        .collect();
    let use_rack = will_use_rack_aware_assignment(rack_refs.iter().copied());
    if use_rack {
        (
            assign_replicas_rack_aware(topic_name, num_partitions, &list, rf),
            true,
        )
    } else {
        (
            assign_replicas_round_robin(topic_name, num_partitions, &ids, rf),
            false,
        )
    }
}

/// Pure round-robin over sorted broker ids (legacy / single-rack / env-off).
pub fn assign_replicas_round_robin(
    topic_name: &str,
    num_partitions: u32,
    broker_ids: &[u32],
    rf: u32,
) -> Vec<Vec<u32>> {
    assert!(!broker_ids.is_empty(), "broker_ids must not be empty");
    let mut ids = broker_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let n = ids.len() as u32;
    let rf = rf.min(n).max(1);
    let th = topic_hash(topic_name);
    let mut out = Vec::with_capacity(num_partitions as usize);
    for p in 0..num_partitions {
        let start = (p.wrapping_add(th)) % n;
        let mut replicas = Vec::with_capacity(rf as usize);
        for i in 0..rf {
            let idx = (start + i) % n;
            replicas.push(ids[idx as usize]);
        }
        out.push(replicas);
    }
    out
}

/// Rack-diversity placement (callers should gate multi-rack + env themselves).
fn assign_replicas_rack_aware(
    topic_name: &str,
    num_partitions: u32,
    brokers: &[(u32, Option<String>)],
    rf: u32,
) -> Vec<Vec<u32>> {
    // Group by rack key; None → unique pseudo-rack per broker.
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    for (id, rack) in brokers {
        let key = match rack {
            Some(r) => r.clone(),
            None => format!("norack-{id}"),
        };
        groups.entry(key).or_default().push(*id);
    }
    // Brokers within each rack already sorted (input sorted by id).
    for v in groups.values_mut() {
        v.sort_unstable();
        v.dedup();
    }
    let rack_keys: Vec<String> = groups.keys().cloned().collect();
    let num_racks = rack_keys.len();
    assert!(num_racks >= 1);
    let n_brokers = brokers.len() as u32;
    let rf = rf.min(n_brokers).max(1);
    let th = topic_hash(topic_name);
    let mut out = Vec::with_capacity(num_partitions as usize);

    for p in 0..num_partitions {
        let start = (p.wrapping_add(th) as usize) % num_racks;
        let mut next_in_rack: Vec<usize> = vec![0; num_racks];
        let mut chosen: HashSet<u32> = HashSet::with_capacity(rf as usize);
        let mut replicas = Vec::with_capacity(rf as usize);

        for slot in 0..rf as usize {
            let mut picked: Option<u32> = None;
            for attempt in 0..num_racks {
                let ri = (start + slot + attempt) % num_racks;
                let members = groups.get(&rack_keys[ri]).expect("rack key");
                while next_in_rack[ri] < members.len() {
                    let bid = members[next_in_rack[ri]];
                    next_in_rack[ri] += 1;
                    if !chosen.contains(&bid) {
                        picked = Some(bid);
                        break;
                    }
                }
                if picked.is_some() {
                    break;
                }
            }
            if picked.is_none() {
                // Fallback: any remaining broker (sorted id order).
                for (id, _) in brokers {
                    if !chosen.contains(id) {
                        picked = Some(*id);
                        break;
                    }
                }
            }
            let bid = picked.expect("enough brokers for rf");
            chosen.insert(bid);
            replicas.push(bid);
        }
        out.push(replicas);
    }
    out
}

/// Compute high watermark as min LEO among ISR members.
///
/// `leo_of` maps broker_id → LEO. Missing entries are treated as 0.
pub fn compute_hwm(isr: &[u32], leo_of: impl Fn(u32) -> u64) -> u64 {
    if isr.is_empty() {
        return 0;
    }
    isr.iter().map(|id| leo_of(*id)).min().unwrap_or(0)
}

/// Shrink ISR: remove followers whose lag exceeds `max_lag`.
///
/// Leader is always kept. `leader_leo` is the leader's LEO; `leo_of` for each member.
pub fn shrink_isr(
    leader: u32,
    isr: &[u32],
    leader_leo: u64,
    max_lag: u64,
    leo_of: impl Fn(u32) -> u64,
) -> Vec<u32> {
    let mut out = Vec::with_capacity(isr.len());
    out.push(leader);
    for &id in isr {
        if id == leader {
            continue;
        }
        let leo = leo_of(id);
        let lag = leader_leo.saturating_sub(leo);
        if lag <= max_lag {
            out.push(id);
        }
    }
    out
}

/// Shrink ISR: remove followers whose last caught-up observation is older than
/// `max_lag_ms` (Phase 125).
///
/// Leader is always kept. `max_lag_ms == 0` disables time shrink (returns a copy
/// of `isr` with leader first). Missing `last_caught_up` keeps the member (no
/// evidence of staleness yet); offset lag still applies via [`shrink_isr`].
pub fn shrink_isr_by_time(
    leader: u32,
    isr: &[u32],
    max_lag_ms: u64,
    now: Instant,
    last_caught_up: impl Fn(u32) -> Option<Instant>,
) -> Vec<u32> {
    let mut out = Vec::with_capacity(isr.len());
    out.push(leader);
    if max_lag_ms == 0 {
        for &id in isr {
            if id != leader && !out.contains(&id) {
                out.push(id);
            }
        }
        return out;
    }
    let max_dur = std::time::Duration::from_millis(max_lag_ms);
    for &id in isr {
        if id == leader {
            continue;
        }
        match last_caught_up(id) {
            Some(at) if now.saturating_duration_since(at) > max_dur => {
                // Timed out — drop.
            }
            _ => out.push(id),
        }
    }
    out
}

/// Whether a replica is eligible to (re)join the ISR (Phase 118).
///
/// Requires membership in the replica set, lag ≤ `max_lag` vs leader LEO, and
/// LEO ≥ committed HWM so rejoin cannot pin HWM to a pre-commit frontier.
pub fn isr_rejoin_eligible(
    replica_id: u32,
    replicas: &[u32],
    leader_leo: u64,
    replica_leo: u64,
    committed_hwm: u64,
    max_lag: u64,
) -> bool {
    if !replicas.contains(&replica_id) {
        return false;
    }
    let lag = leader_leo.saturating_sub(replica_leo);
    lag <= max_lag && replica_leo >= committed_hwm
}

/// Add `replica_id` to ISR when eligible (Phase 118 rejoin).
///
/// Leader is always present first. Idempotent if already in `isr`.
pub fn expand_isr(
    leader: u32,
    isr: &[u32],
    replicas: &[u32],
    replica_id: u32,
    leader_leo: u64,
    replica_leo: u64,
    committed_hwm: u64,
    max_lag: u64,
) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::with_capacity(isr.len() + 1);
    out.push(leader);
    for &id in isr {
        if id != leader && !out.contains(&id) {
            out.push(id);
        }
    }
    if replica_id != leader
        && !out.contains(&replica_id)
        && isr_rejoin_eligible(
            replica_id,
            replicas,
            leader_leo,
            replica_leo,
            committed_hwm,
            max_lag,
        )
    {
        out.push(replica_id);
    }
    out
}

/// Full ISR reconcile after observing a follower LEO (Phase 118 + 125).
///
/// 1. Offset lag-shrink current members.
/// 2. Time lag-shrink members with stale last-caught-up (Phase 125; `max_lag_ms == 0` off).
/// 3. Rejoin `fetching_replica` when eligible (LEO ≥ HWM and lag ≤ max).
/// 4. Ensure leader remains in the set.
///
/// Returns `(new_isr, time_shrink_count)` where `time_shrink_count` is how many
/// members were removed solely by the time-lag step (for metrics).
pub fn reconcile_isr(
    leader: u32,
    isr: &[u32],
    replicas: &[u32],
    leader_leo: u64,
    committed_hwm: u64,
    max_lag: u64,
    max_lag_ms: u64,
    now: Instant,
    fetching_replica: Option<(u32, u64)>,
    leo_of: impl Fn(u32) -> u64,
    last_caught_up: impl Fn(u32) -> Option<Instant>,
) -> (Vec<u32>, u64) {
    let after_offset = shrink_isr(leader, isr, leader_leo, max_lag, &leo_of);
    let after_time =
        shrink_isr_by_time(leader, &after_offset, max_lag_ms, now, &last_caught_up);
    let mut time_shrink = 0u64;
    for &id in &after_offset {
        if id != leader && !after_time.contains(&id) {
            time_shrink += 1;
        }
    }
    let mut out = if let Some((rid, rleo)) = fetching_replica {
        expand_isr(
            leader,
            &after_time,
            replicas,
            rid,
            leader_leo,
            rleo,
            committed_hwm,
            max_lag,
        )
    } else {
        after_time
    };
    if !out.contains(&leader) {
        out.insert(0, leader);
    }
    (out, time_shrink)
}

/// Elect a new leader from ISR ∩ live, preferring the first live replica in the
/// replica list order.
pub fn elect_leader(replicas: &[u32], isr: &[u32], live: &[u32]) -> Option<u32> {
    for &r in replicas {
        if isr.contains(&r) && live.contains(&r) {
            return Some(r);
        }
    }
    // Fallback: any live ISR member.
    for &r in isr {
        if live.contains(&r) {
            return Some(r);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_placement() {
        // No racks → legacy round-robin (identical to assign_replicas_round_robin).
        let brokers: Vec<(u32, Option<&str>)> =
            vec![(1, None), (2, None), (3, None)];
        let (parts, rack_aware) = assign_replicas("events", 3, brokers.iter().copied(), 3);
        assert!(!rack_aware);
        let legacy = assign_replicas_round_robin("events", 3, &[1, 2, 3], 3);
        assert_eq!(parts, legacy);
        assert_eq!(parts.len(), 3);
        for p in &parts {
            assert_eq!(p.len(), 3);
            // all brokers present for RF=3
            let mut s = p.clone();
            s.sort();
            assert_eq!(s, vec![1, 2, 3]);
        }
        // Different partitions should rotate starting broker when hash allows.
        let start0 = parts[0][0];
        let start1 = parts[1][0];
        // With RF=N, all have same set but order differs by start index.
        assert_ne!(start0, start1);
    }

    #[test]
    fn multi_rack_spans_racks_when_possible() {
        // 3 brokers: racks a,a,b — RF=2 must span a and b on every partition.
        let brokers: Vec<(u32, Option<&str>)> = vec![
            (1, Some("a")),
            (2, Some("a")),
            (3, Some("b")),
        ];
        if !rack_aware_assignment_enabled() {
            // External env may disable; skip diversity assert.
            return;
        }
        let (parts, rack_aware) =
            assign_replicas("rack-topic", 6, brokers.iter().copied(), 2);
        assert!(rack_aware);
        let rack_of = |id: u32| match id {
            1 | 2 => "a",
            3 => "b",
            _ => panic!("bad id"),
        };
        for p in &parts {
            assert_eq!(p.len(), 2);
            assert_ne!(rack_of(p[0]), rack_of(p[1]), "replicas must span racks: {p:?}");
        }
    }

    #[test]
    fn single_rack_fills_rf() {
        let brokers: Vec<(u32, Option<&str>)> = vec![
            (1, Some("r1")),
            (2, Some("r1")),
            (3, Some("r1")),
        ];
        let (parts, rack_aware) =
            assign_replicas("single-rack", 4, brokers.iter().copied(), 2);
        assert!(!rack_aware, "single configured rack → legacy");
        for p in &parts {
            assert_eq!(p.len(), 2);
            assert!(p[0] != p[1] || p.len() == 1);
        }
    }

    #[test]
    fn hwm_is_min_leo() {
        let isr = vec![1u32, 2, 3];
        let hwm = compute_hwm(&isr, |id| match id {
            1 => 10,
            2 => 8,
            3 => 9,
            _ => 0,
        });
        assert_eq!(hwm, 8);
    }

    #[test]
    fn isr_shrink_on_lag() {
        let isr = vec![1u32, 2, 3];
        let shrunk = shrink_isr(1, &isr, 100, 10, |id| match id {
            1 => 100,
            2 => 95,  // lag 5 — ok
            3 => 50,  // lag 50 — out
            _ => 0,
        });
        assert_eq!(shrunk, vec![1, 2]);
    }

    #[test]
    fn isr_rejoin_requires_hwm_and_lag() {
        let replicas = vec![1u32, 2, 3];
        // lag 5 ≤ 10 but LEO 90 < HWM 95 → not eligible
        assert!(!isr_rejoin_eligible(3, &replicas, 100, 90, 95, 10));
        // lag 5 ≤ 10 and LEO ≥ HWM → eligible
        assert!(isr_rejoin_eligible(3, &replicas, 100, 95, 95, 10));
        // lag 20 > 10 → not eligible even if ≥ HWM
        assert!(!isr_rejoin_eligible(3, &replicas, 100, 80, 80, 10));
        // not in replica set
        assert!(!isr_rejoin_eligible(9, &replicas, 100, 100, 100, 10));
    }

    #[test]
    fn reconcile_rejoin_after_shrink() {
        let replicas = vec![1u32, 2, 3];
        // 3 was out of ISR; fetches at HWM with small lag → rejoin
        let isr = vec![1u32, 2];
        let now = Instant::now();
        let (out, time_n) = reconcile_isr(
            1,
            &isr,
            &replicas,
            100,
            90,
            10,
            0, // time shrink off
            now,
            Some((3, 95)),
            |id| match id {
                1 => 100,
                2 => 98,
                3 => 95,
                _ => 0,
            },
            |_| None,
        );
        assert_eq!(time_n, 0);
        assert!(out.contains(&3), "rejoin: {out:?}");
        assert!(out.contains(&1) && out.contains(&2));
    }

    #[test]
    fn reconcile_lag_shrink_alive_slow() {
        let replicas = vec![1u32, 2, 3];
        let isr = vec![1u32, 2, 3];
        // 3 lag 50 > 10; fetch from 2 triggers full reconcile
        let now = Instant::now();
        let (out, time_n) = reconcile_isr(
            1,
            &isr,
            &replicas,
            100,
            50,
            10,
            0,
            now,
            Some((2, 98)),
            |id| match id {
                1 => 100,
                2 => 98,
                3 => 50,
                _ => 0,
            },
            |_| None,
        );
        assert_eq!(time_n, 0);
        assert_eq!(out, vec![1, 2]);
    }

    #[test]
    fn shrink_by_time_drops_stale() {
        let isr = vec![1u32, 2, 3];
        let now = Instant::now();
        let stale = now - std::time::Duration::from_millis(500);
        let fresh = now - std::time::Duration::from_millis(10);
        let out = shrink_isr_by_time(1, &isr, 100, now, |id| match id {
            2 => Some(fresh),
            3 => Some(stale),
            _ => None,
        });
        assert_eq!(out, vec![1, 2], "stale id=3 must leave: {out:?}");
    }

    #[test]
    fn shrink_by_time_disabled_when_zero() {
        let isr = vec![1u32, 2, 3];
        let now = Instant::now();
        let stale = now - std::time::Duration::from_secs(3600);
        let out = shrink_isr_by_time(1, &isr, 0, now, |_| Some(stale));
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn reconcile_time_shrink_then_rejoin() {
        let replicas = vec![1u32, 2, 3];
        let isr = vec![1u32, 2, 3];
        let now = Instant::now();
        let stale = now - std::time::Duration::from_millis(200);
        // 3 is within message lag but time-stale; fetch from 2 triggers drop.
        let (out, time_n) = reconcile_isr(
            1,
            &isr,
            &replicas,
            100,
            90,
            50,  // large message lag
            50,  // 50ms time max
            now,
            Some((2, 100)),
            |id| match id {
                1 => 100,
                2 => 100,
                3 => 99, // lag 1 ≤ 50
                _ => 0,
            },
            |id| match id {
                2 => Some(now),
                3 => Some(stale),
                _ => None,
            },
        );
        assert_eq!(time_n, 1);
        assert_eq!(out, vec![1, 2]);
        // Same member catches up → rejoin (time does not block once LEO ok).
        let (out2, time_n2) = reconcile_isr(
            1,
            &out,
            &replicas,
            100,
            90,
            50,
            50,
            now,
            Some((3, 100)),
            |id| match id {
                1 => 100,
                2 => 100,
                3 => 100,
                _ => 0,
            },
            |id| match id {
                2 => Some(now),
                3 => Some(now), // fresh stamp on catch-up path
                _ => None,
            },
        );
        assert_eq!(time_n2, 0);
        assert!(out2.contains(&3), "rejoin after catch-up: {out2:?}");
    }

    #[test]
    fn elect_prefers_replica_order() {
        let replicas = vec![1u32, 2, 3];
        let isr = vec![1, 2, 3];
        // Leader 1 is dead
        let live = vec![2u32, 3];
        assert_eq!(elect_leader(&replicas, &isr, &live), Some(2));
    }
}
