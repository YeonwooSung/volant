package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Kafka-style range partition assignor (matches {@code volant_broker::range_assign}).
 *
 * <p>Not cooperative-sticky and not a Kafka consumer assignor. The helper is
 * public so apps can pass a known member set. {@link GroupConsumer} optional
 * {@code assignor="range"} uses JoinGroup members, else DescribeGroup
 * (still no SyncGroup).
 */
public final class RangeAssignor {
    private RangeAssignor() {}

    /**
     * Assign {@code numPartitions} to {@code memberIds} using the range algorithm.
     *
     * <p>Members are sorted by id. For {@code n} partitions and {@code m} members:
     * {@code base = n / m}, {@code extra = n % m}; sorted member {@code i} gets
     * {@code base + (i < extra ? 1 : 0)} consecutive partitions.
     *
     * <p>Returns a parallel list of partition lists in the original member order.
     * Empty members or {@code numPartitions <= 0} yields an empty list per member.
     */
    public static List<List<Integer>> rangeAssign(int numPartitions, List<String> memberIds) {
        final List<String> ids = memberIds == null ? Collections.emptyList() : memberIds;
        int m = ids.size();
        List<List<Integer>> result = new ArrayList<>(m);
        for (int i = 0; i < m; i++) {
            result.add(new ArrayList<>());
        }
        if (m == 0 || numPartitions <= 0) {
            return result;
        }

        List<int[]> indexed = new ArrayList<>(m);
        for (int i = 0; i < m; i++) {
            indexed.add(new int[] {i});
        }
        indexed.sort(Comparator.comparing(pair -> ids.get(pair[0])));

        int base = numPartitions / m;
        int extra = numPartitions % m;
        int next = 0;
        for (int rank = 0; rank < indexed.size(); rank++) {
            int origIdx = indexed.get(rank)[0];
            int count = base + (rank < extra ? 1 : 0);
            List<Integer> parts = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                parts.add(next + i);
            }
            next += count;
            result.set(origIdx, parts);
        }
        return result;
    }

    /**
     * Range-assign each topic independently to members subscribed to that topic.
     *
     * <p>{@code memberTopics.get(i)} is the subscription list for
     * {@code memberIds.get(i)}. {@code partitionCounts} maps topic → partition
     * count. Topics missing from the map are skipped. Returns
     * {@code assignments.get(i)} as {@link Codec.Assignment} pairs for member
     * {@code i}, sorted by topic then partition.
     */
    public static List<List<Codec.Assignment>> rangeAssignMulti(
            List<String> memberIds,
            List<List<String>> memberTopics,
            Map<String, Integer> partitionCounts) {
        if (memberIds == null) {
            memberIds = Collections.emptyList();
        }
        if (memberTopics == null) {
            memberTopics = Collections.emptyList();
        }
        if (memberIds.size() != memberTopics.size()) {
            throw new IllegalArgumentException("memberIds and memberTopics must have the same length");
        }
        int m = memberIds.size();
        List<List<Codec.Assignment>> out = new ArrayList<>(m);
        for (int i = 0; i < m; i++) {
            out.add(new ArrayList<>());
        }
        if (m == 0) {
            return out;
        }

        List<String> topics = new ArrayList<>();
        for (List<String> subscribed : memberTopics) {
            if (subscribed != null) {
                topics.addAll(subscribed);
            }
        }
        Collections.sort(topics);
        List<String> deduped = new ArrayList<>();
        for (String topic : topics) {
            if (deduped.isEmpty() || !deduped.get(deduped.size() - 1).equals(topic)) {
                deduped.add(topic);
            }
        }
        topics = deduped;

        Map<String, Integer> counts = partitionCounts == null ? Collections.emptyMap() : partitionCounts;
        for (String topic : topics) {
            Integer n = counts.get(topic);
            if (n == null) {
                continue;
            }
            List<String> subIds = new ArrayList<>();
            List<Integer> subOrig = new ArrayList<>();
            for (int i = 0; i < m; i++) {
                List<String> subscribed = memberTopics.get(i);
                if (subscribed != null && subscribed.contains(topic)) {
                    subIds.add(memberIds.get(i));
                    subOrig.add(i);
                }
            }
            if (subIds.isEmpty()) {
                continue;
            }
            List<List<Integer>> parts = rangeAssign(n, subIds);
            for (int j = 0; j < parts.size(); j++) {
                int orig = subOrig.get(j);
                for (int p : parts.get(j)) {
                    out.get(orig).add(new Codec.Assignment(topic, p));
                }
            }
        }

        Comparator<Codec.Assignment> byTopicThenPart =
                Comparator.comparing((Codec.Assignment a) -> a.topic).thenComparingInt(a -> a.partition);
        for (List<Codec.Assignment> assignment : out) {
            assignment.sort(byTopicThenPart);
        }
        return out;
    }
}
