package volant

import "sort"

// RangeAssign assigns numPartitions to memberIDs using the Kafka-style
// range algorithm (same as volant_broker::range_assign).
//
// Members are sorted by id. For n partitions and m members:
// base = n/m, extra = n%m; sorted member i gets base+(i<extra ? 1 : 0)
// consecutive partitions. The returned slice is parallel to memberIDs
// (original order). Empty members or 0 partitions yields an empty list
// per member.
func RangeAssign(numPartitions uint32, memberIDs []string) [][]uint32 {
	m := len(memberIDs)
	result := make([][]uint32, m)
	for i := range result {
		result[i] = []uint32{}
	}
	if m == 0 || numPartitions == 0 {
		return result
	}

	type pair struct {
		orig int
		id   string
	}
	indexed := make([]pair, m)
	for i, id := range memberIDs {
		indexed[i] = pair{orig: i, id: id}
	}
	sort.SliceStable(indexed, func(i, j int) bool {
		return indexed[i].id < indexed[j].id
	})

	base := numPartitions / uint32(m)
	extra := numPartitions % uint32(m)
	var next uint32
	for rank, p := range indexed {
		count := base
		if uint32(rank) < extra {
			count++
		}
		parts := make([]uint32, count)
		for i := uint32(0); i < count; i++ {
			parts[i] = next + i
		}
		next += count
		result[p.orig] = parts
	}
	return result
}

// RangeAssignMulti range-assigns each topic independently to members
// subscribed to that topic (same as volant_broker::range_assign_multi).
//
// memberTopics[i] is the subscription list for memberIDs[i].
// partitionCounts maps topic → partition count. Topics missing from
// the map are skipped. Returns assignments[i] as (topic, partition)
// pairs for member i, sorted by topic then partition.
func RangeAssignMulti(memberIDs []string, memberTopics [][]string, partitionCounts map[string]uint32) [][]Assignment {
	if len(memberIDs) != len(memberTopics) {
		panic("RangeAssignMulti: memberIDs and memberTopics must have the same length")
	}
	m := len(memberIDs)
	out := make([][]Assignment, m)
	for i := range out {
		out[i] = []Assignment{}
	}
	if m == 0 {
		return out
	}

	var topics []string
	for _, subscribed := range memberTopics {
		topics = append(topics, subscribed...)
	}
	sort.Strings(topics)
	deduped := topics[:0]
	for _, topic := range topics {
		if len(deduped) == 0 || deduped[len(deduped)-1] != topic {
			deduped = append(deduped, topic)
		}
	}
	topics = deduped

	for _, topic := range topics {
		n, ok := partitionCounts[topic]
		if !ok {
			continue
		}
		var subIDs []string
		var subOrig []int
		for i, subscribed := range memberTopics {
			for _, t := range subscribed {
				if t == topic {
					subIDs = append(subIDs, memberIDs[i])
					subOrig = append(subOrig, i)
					break
				}
			}
		}
		if len(subIDs) == 0 {
			continue
		}
		parts := RangeAssign(n, subIDs)
		for j, ps := range parts {
			orig := subOrig[j]
			for _, p := range ps {
				out[orig] = append(out[orig], Assignment{Topic: topic, Partition: p})
			}
		}
	}

	for i := range out {
		sort.Slice(out[i], func(a, b int) bool {
			if out[i][a].Topic != out[i][b].Topic {
				return out[i][a].Topic < out[i][b].Topic
			}
			return out[i][a].Partition < out[i][b].Partition
		})
	}
	return out
}
