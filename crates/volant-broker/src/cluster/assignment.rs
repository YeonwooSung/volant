//! Replica placement for new topics.

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

/// Assign `rf` replicas for `num_partitions` using round-robin over `broker_ids`.
///
/// For partition `p`, replicas start at index `(p + topic_hash) % N`, then the next
/// `rf - 1` brokers (wrapping). Returns `Vec` of replica lists (one per partition).
pub fn assign_replicas(
    topic_name: &str,
    num_partitions: u32,
    broker_ids: &[u32],
    rf: u32,
) -> Vec<Vec<u32>> {
    assert!(!broker_ids.is_empty(), "broker_ids must not be empty");
    let n = broker_ids.len() as u32;
    let rf = rf.min(n).max(1);
    let th = topic_hash(topic_name);
    let mut out = Vec::with_capacity(num_partitions as usize);
    for p in 0..num_partitions {
        let start = (p.wrapping_add(th)) % n;
        let mut replicas = Vec::with_capacity(rf as usize);
        for i in 0..rf {
            let idx = (start + i) % n;
            replicas.push(broker_ids[idx as usize]);
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
        let brokers = vec![1u32, 2, 3];
        let parts = assign_replicas("events", 3, &brokers, 3);
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
    fn elect_prefers_replica_order() {
        let replicas = vec![1u32, 2, 3];
        let isr = vec![1, 2, 3];
        // Leader 1 is dead
        let live = vec![2u32, 3];
        assert_eq!(elect_leader(&replicas, &isr, &live), Some(2));
    }
}
