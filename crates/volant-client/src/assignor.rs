//! Kafka-style range assignor (matches `volant_broker::range_assign`).
//!
//! Copied into this crate so `GroupConsumer` does not depend on `volant-broker`.

use std::collections::HashMap;

/// Assign partitions of a topic to members using the Kafka-style range assignor.
///
/// Members are sorted by `member_id`. For `n` partitions and `m` members:
/// `base = n / m`, `extra = n % m`; member `i` gets `base + (i < extra ? 1 : 0)`
/// partitions in order.
///
/// Returns a parallel vector of partition lists (same order as the input members).
pub(crate) fn range_assign(num_partitions: u32, member_ids: &[String]) -> Vec<Vec<u32>> {
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
pub(crate) fn range_assign_multi(
    member_ids: &[String],
    member_topics: &[Vec<String>],
    partition_counts: &HashMap<String, u32>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn uneven_partitions() {
        let members = vec!["a".into(), "b".into()];
        let parts = range_assign(5, &members);
        assert_eq!(parts[0].len() + parts[1].len(), 5);
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
        let members = vec!["c".into(), "a".into(), "b".into()];
        let parts = range_assign(7, &members);
        assert_eq!(parts[1], vec![0, 1, 2]);
        assert_eq!(parts[2], vec![3, 4]);
        assert_eq!(parts[0], vec![5, 6]);
    }

    #[test]
    fn multi_topic_disjoint_cover() {
        let members = vec!["m1".into(), "m2".into()];
        let subs = vec![vec!["t".into()], vec!["t".into()]];
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
        let set0: HashSet<_> = assigns[0].iter().map(|(_, p)| *p).collect();
        let set1: HashSet<_> = assigns[1].iter().map(|(_, p)| *p).collect();
        assert!(set0.is_disjoint(&set1));
        assert_eq!(assigns[0], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(assigns[1], vec![("t".into(), 2), ("t".into(), 3)]);
    }
}
