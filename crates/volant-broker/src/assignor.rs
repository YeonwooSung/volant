//! Partition assignors for consumer groups (range + sticky).

use std::collections::{HashMap, HashSet};

/// Assign partitions of a topic to members using the Kafka-style range assignor.
///
/// Members are sorted by `member_id`. For `n` partitions and `m` members:
/// `base = n / m`, `extra = n % m`; member `i` gets `base + (i < extra ? 1 : 0)`
/// partitions in order.
///
/// Returns a parallel vector of partition lists (same order as sorted members).
pub fn range_assign(num_partitions: u32, member_ids: &[String]) -> Vec<Vec<u32>> {
    let m = member_ids.len();
    if m == 0 || num_partitions == 0 {
        return vec![Vec::new(); m];
    }

    // Sort by member_id for stable assignment.
    let mut indexed: Vec<(usize, &String)> = member_ids.iter().enumerate().collect();
    indexed.sort_by(|a, b| a.1.cmp(b.1));

    let base = num_partitions / m as u32;
    let extra = num_partitions % m as u32;

    let mut result = vec![Vec::new(); m];
    let mut next = 0u32;
    for (rank, (orig_idx, _)) in indexed.into_iter().enumerate() {
        let count = base + if (rank as u32) < extra { 1 } else { 0 };
        let parts: Vec<u32> = (next..next + count).collect();
        next += count;
        result[orig_idx] = parts;
    }
    result
}

/// Assign multiple topics: each topic independently range-assigned to members
/// subscribed to that topic.
///
/// `member_topics[i]` is the subscription list for `member_ids[i]`.
/// `partition_counts` maps topic name → partition count.
/// Returns `assignments[i]` = list of (topic, partition) for member i.
pub fn range_assign_multi(
    member_ids: &[String],
    member_topics: &[Vec<String>],
    partition_counts: &std::collections::HashMap<String, u32>,
) -> Vec<Vec<(String, u32)>> {
    assert_eq!(member_ids.len(), member_topics.len());
    let m = member_ids.len();
    let mut out = vec![Vec::new(); m];

    // Union of all subscribed topics.
    let mut topics: Vec<String> = member_topics
        .iter()
        .flat_map(|t| t.iter().cloned())
        .collect();
    topics.sort();
    topics.dedup();

    for topic in topics {
        let n = match partition_counts.get(&topic) {
            Some(&n) => n,
            None => continue, // unknown topic → skip
        };
        // Members subscribed to this topic.
        let mut sub_ids = Vec::new();
        let mut sub_orig = Vec::new();
        for (i, topics) in member_topics.iter().enumerate() {
            if topics.iter().any(|t| t == &topic) {
                sub_ids.push(member_ids[i].clone());
                sub_orig.push(i);
            }
        }
        if sub_ids.is_empty() {
            continue;
        }
        let parts = range_assign(n, &sub_ids);
        for (j, ps) in parts.into_iter().enumerate() {
            let orig = sub_orig[j];
            for p in ps {
                out[orig].push((topic.clone(), p));
            }
        }
    }

    // Stable order within each assignment: by topic then partition.
    for a in &mut out {
        a.sort();
    }
    out
}

/// Sticky assign one topic: keep previous ownership when possible, then fill free
/// partitions onto the least-loaded members (stable member-id tie-break).
///
/// `previous[i]` is the prior partition list for `member_ids[i]` (may be empty).
/// Returns partition lists in the same order as `member_ids`.
pub fn sticky_assign(
    num_partitions: u32,
    member_ids: &[String],
    previous: &[Vec<u32>],
) -> Vec<Vec<u32>> {
    let m = member_ids.len();
    if m == 0 || num_partitions == 0 {
        return vec![Vec::new(); m];
    }
    assert_eq!(member_ids.len(), previous.len());

    let all: HashSet<u32> = (0..num_partitions).collect();
    let mut owned: Vec<Vec<u32>> = vec![Vec::new(); m];
    let mut claimed: HashSet<u32> = HashSet::new();

    // Preserve previous ownership if still valid and unique.
    for (i, prev) in previous.iter().enumerate() {
        for &p in prev {
            if p < num_partitions && !claimed.contains(&p) {
                owned[i].push(p);
                claimed.insert(p);
            }
        }
    }

    let mut free: Vec<u32> = all.difference(&claimed).copied().collect();
    free.sort_unstable();

    // Target sizes: base or base+1 for first `extra` members in sorted-id order.
    let base = num_partitions / m as u32;
    let extra = num_partitions % m as u32;
    let mut sorted_idx: Vec<usize> = (0..m).collect();
    sorted_idx.sort_by(|&a, &b| member_ids[a].cmp(&member_ids[b]));
    let mut target = vec![base; m];
    for (rank, &idx) in sorted_idx.iter().enumerate() {
        if (rank as u32) < extra {
            target[idx] = base + 1;
        }
    }

    // Strip excess from over-assigned members (highest partitions first for stability).
    for i in 0..m {
        if owned[i].len() as u32 > target[i] {
            owned[i].sort_unstable();
            while owned[i].len() as u32 > target[i] {
                if let Some(p) = owned[i].pop() {
                    free.push(p);
                    claimed.remove(&p);
                }
            }
        }
    }
    free.sort_unstable();
    free.dedup();

    // Fill free partitions onto members under target, fewest first, then member_id.
    while let Some(p) = free.first().copied() {
        let mut candidates: Vec<usize> = (0..m)
            .filter(|&i| (owned[i].len() as u32) < target[i])
            .collect();
        if candidates.is_empty() {
            // Should not happen if targets sum to n; dump remaining fairly.
            candidates = (0..m).collect();
        }
        candidates.sort_by(|&a, &b| {
            owned[a]
                .len()
                .cmp(&owned[b].len())
                .then(member_ids[a].cmp(&member_ids[b]))
        });
        let i = candidates[0];
        free.remove(0);
        owned[i].push(p);
    }

    for o in &mut owned {
        o.sort_unstable();
    }
    owned
}

/// Sticky multi-topic assignor. `previous[i]` is the full prior assignment for member i.
pub fn sticky_assign_multi(
    member_ids: &[String],
    member_topics: &[Vec<String>],
    partition_counts: &HashMap<String, u32>,
    previous: &[Vec<(String, u32)>],
) -> Vec<Vec<(String, u32)>> {
    assert_eq!(member_ids.len(), member_topics.len());
    assert_eq!(member_ids.len(), previous.len());
    let m = member_ids.len();
    let mut out = vec![Vec::new(); m];

    let mut topics: Vec<String> = member_topics
        .iter()
        .flat_map(|t| t.iter().cloned())
        .collect();
    topics.sort();
    topics.dedup();

    for topic in topics {
        let n = match partition_counts.get(&topic) {
            Some(&n) => n,
            None => continue,
        };
        let mut sub_ids = Vec::new();
        let mut sub_orig = Vec::new();
        let mut sub_prev = Vec::new();
        for (i, topics) in member_topics.iter().enumerate() {
            if topics.iter().any(|t| t == &topic) {
                sub_ids.push(member_ids[i].clone());
                sub_orig.push(i);
                let prev_parts: Vec<u32> = previous[i]
                    .iter()
                    .filter(|(t, _)| t == &topic)
                    .map(|(_, p)| *p)
                    .collect();
                sub_prev.push(prev_parts);
            }
        }
        if sub_ids.is_empty() {
            continue;
        }
        let parts = sticky_assign(n, &sub_ids, &sub_prev);
        for (j, ps) in parts.into_iter().enumerate() {
            let orig = sub_orig[j];
            for p in ps {
                out[orig].push((topic.clone(), p));
            }
        }
    }

    for a in &mut out {
        a.sort();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uneven_partitions() {
        // 5 partitions, 2 members → 3 and 2
        let members = vec!["a".into(), "b".into()];
        let parts = range_assign(5, &members);
        assert_eq!(parts[0].len() + parts[1].len(), 5);
        // Sorted: a gets first 3, b gets last 2
        assert_eq!(parts[0], vec![0, 1, 2]);
        assert_eq!(parts[1], vec![3, 4]);
    }

    #[test]
    fn even_split() {
        let members = vec!["m0".into(), "m1".into()];
        let parts = range_assign(4, &members);
        assert_eq!(parts[0], vec![0, 1]);
        assert_eq!(parts[1], vec![2, 3]);
    }

    #[test]
    fn single_member_gets_all() {
        let members = vec!["solo".into()];
        let parts = range_assign(3, &members);
        assert_eq!(parts[0], vec![0, 1, 2]);
    }

    #[test]
    fn three_members_seven_partitions() {
        // base=2, extra=1 → first sorted member gets 3, others 2
        let members = vec!["c".into(), "a".into(), "b".into()];
        let parts = range_assign(7, &members);
        // sorted order: a, b, c → indices 1, 2, 0
        // a (orig 1): 0,1,2 ; b (orig 2): 3,4 ; c (orig 0): 5,6
        assert_eq!(parts[1], vec![0, 1, 2]);
        assert_eq!(parts[2], vec![3, 4]);
        assert_eq!(parts[0], vec![5, 6]);
    }

    #[test]
    fn multi_topic_disjoint_cover() {
        let members = vec!["m1".into(), "m2".into()];
        let subs = vec![
            vec!["t".into()],
            vec!["t".into()],
        ];
        let mut counts = HashMap::new();
        counts.insert("t".into(), 4u32);
        let assigns = range_assign_multi(&members, &subs, &counts);
        let mut all = HashSet::new();
        for a in &assigns {
            for (topic, p) in a {
                assert_eq!(topic, "t");
                assert!(all.insert(*p), "duplicate partition {p}");
            }
        }
        assert_eq!(all, HashSet::from([0, 1, 2, 3]));
        // Disjoint
        let set0: HashSet<_> = assigns[0].iter().map(|(_, p)| *p).collect();
        let set1: HashSet<_> = assigns[1].iter().map(|(_, p)| *p).collect();
        assert!(set0.is_disjoint(&set1));
    }

    #[test]
    fn sticky_keeps_previous_when_member_stays() {
        let members = vec!["a".into(), "b".into()];
        let prev = vec![vec![0, 1], vec![2, 3]];
        let next = sticky_assign(4, &members, &prev);
        assert_eq!(next[0], vec![0, 1]);
        assert_eq!(next[1], vec![2, 3]);
    }

    #[test]
    fn sticky_rebalances_when_member_joins() {
        // Solo a held all 4; b joins → balanced 2/2, a keeps lower partitions when possible.
        let members = vec!["a".into(), "b".into()];
        let prev = vec![vec![0, 1, 2, 3], vec![]];
        let next = sticky_assign(4, &members, &prev);
        assert_eq!(next[0].len(), 2);
        assert_eq!(next[1].len(), 2);
        let mut all: Vec<u32> = next.iter().flatten().copied().collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3]);
        // a should retain a subset of its prior partitions.
        for p in &next[0] {
            assert!(*p <= 3);
        }
    }

    #[test]
    fn sticky_multi_covers_all() {
        let members = vec!["m0".into(), "m1".into()];
        let subs = vec![vec!["t".into()], vec!["t".into()]];
        let mut counts = HashMap::new();
        counts.insert("t".into(), 3);
        let prev = vec![
            vec![("t".into(), 0), ("t".into(), 1)],
            vec![("t".into(), 2)],
        ];
        let assigns = sticky_assign_multi(&members, &subs, &counts, &prev);
        assert_eq!(assigns[0], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(assigns[1], vec![("t".into(), 2)]);
    }
}
