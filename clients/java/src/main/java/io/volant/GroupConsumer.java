package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * High-level consumer that joins a group, polls assigned partitions, and commits.
 *
 * <p>Same semantics as the Rust {@code GroupConsumer}: join, OffsetFetch
 * assigned partitions, heartbeat on {@link #poll(int)}, commit with
 * member+generation, rejoin on heartbeat error 9 (and 10/11, matching Rust).
 *
 * <pre>
 * GroupConsumer g = GroupConsumer.join(c, "g", List.of("t"), 10_000);
 * List&lt;Record&gt; recs = g.poll(500);
 * g.commit();
 * g.close();
 * </pre>
 */
public final class GroupConsumer implements AutoCloseable {
    /** Wire sentinel: unknown / not-committed offset ({@code u64::MAX}). */
    static final long OFFSET_UNKNOWN = 0xFFFFFFFFFFFFFFFFL;

    /** Native rebalance / stale-membership codes (Rust {@code needs_rebalance}). */
    static final int ERR_REBALANCE = 9;
    static final int ERR_UNKNOWN_MEMBER = 10;
    static final int ERR_ILLEGAL_GENERATION = 11;

    private static final int POLL_MAX_MESSAGES = 100;

    private final Backend backend;
    private final String groupId;
    private final List<String> topics;
    private final int sessionTimeoutMs;
    private String memberId = "";
    private long generation;
    private List<Codec.Assignment> assignment = Collections.emptyList();
    private List<Codec.Assignment> lastRevoked = Collections.emptyList();
    private final Map<Tp, Long> positions = new LinkedHashMap<>();
    private boolean closed;

    GroupConsumer(Backend backend, String groupId, List<String> topics, int sessionTimeoutMs) {
        this.backend = backend;
        this.groupId = groupId;
        this.topics = topics == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(topics));
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    /** Join a consumer group on the given topics. {@code sessionTimeoutMs} 0 defaults to 10000. */
    public static GroupConsumer join(Client client, String group, List<String> topics, int sessionTimeoutMs) {
        return join(new ClientBackend(client), group, topics, sessionTimeoutMs);
    }

    static GroupConsumer join(Backend backend, String group, List<String> topics, int sessionTimeoutMs) {
        int timeout = sessionTimeoutMs == 0 ? 10_000 : sessionTimeoutMs;
        GroupConsumer g = new GroupConsumer(backend, group, topics, timeout);
        g.doJoin();
        return g;
    }

    private void doJoin() {
        List<Codec.Assignment> previous = new ArrayList<>(assignment);
        JoinGroupResult result = backend.joinGroup(groupId, memberId, topics, sessionTimeoutMs);
        memberId = result.memberId;
        generation = result.generation;
        List<Codec.Assignment> newAssignment = new ArrayList<>(result.assignment);

        Set<Tp> oldSet = toSet(previous);
        Set<Tp> newSet = toSet(newAssignment);

        List<Tp> revoked = new ArrayList<>();
        for (Tp tp : oldSet) {
            if (!newSet.contains(tp)) {
                revoked.add(tp);
            }
        }
        for (Codec.Assignment a : result.revoked) {
            Tp tp = new Tp(a.topic, a.partition);
            if (!revoked.contains(tp)) {
                revoked.add(tp);
            }
        }
        Collections.sort(revoked);

        List<Tp> added = new ArrayList<>();
        for (Tp tp : newSet) {
            if (!oldSet.contains(tp)) {
                added.add(tp);
            }
        }

        for (Tp tp : revoked) {
            positions.remove(tp);
        }

        assignment = Collections.unmodifiableList(newAssignment);
        List<Codec.Assignment> revokedAssign = new ArrayList<>();
        for (Tp tp : revoked) {
            revokedAssign.add(new Codec.Assignment(tp.topic, tp.partition));
        }
        lastRevoked = Collections.unmodifiableList(revokedAssign);

        if (!added.isEmpty() || (positions.isEmpty() && !assignment.isEmpty())) {
            List<Tp> toFetch = previous.isEmpty() ? toList(newSet) : added;
            fetchPositionsFor(toFetch);
        }

        for (Codec.Assignment a : assignment) {
            positions.putIfAbsent(new Tp(a.topic, a.partition), 0L);
        }
    }

    private void fetchPositionsFor(List<Tp> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        List<Codec.OffsetEntry> entries = new ArrayList<>();
        for (Tp tp : partitions) {
            entries.add(new Codec.OffsetEntry(tp.topic, tp.partition));
        }
        List<Codec.OffsetFetchEntry> fetched = backend.fetchOffsets(groupId, entries);
        for (Codec.OffsetFetchEntry e : fetched) {
            long pos = e.offset == OFFSET_UNKNOWN ? 0L : e.offset;
            positions.put(new Tp(e.topic, e.partition), pos);
        }
    }

    /**
     * Heartbeat, then fetch from all assigned partitions.
     *
     * <p>{@code timeoutMs} is Fetch {@code max_wait_ms} on the first assigned
     * partition (0 = non-blocking). Rejoins on heartbeat error 9/10/11.
     */
    public List<Record> poll(int timeoutMs) {
        ensureOpen();
        try {
            backend.heartbeat(groupId, memberId, generation);
        } catch (BrokerException e) {
            if (needsRebalance(e.code)) {
                doJoin();
            } else {
                throw e;
            }
        }

        List<Record> out = new ArrayList<>();
        List<Codec.Assignment> assigned = new ArrayList<>(assignment);
        boolean waited = false;
        long wait = timeoutMs < 0 ? 0 : timeoutMs;
        for (Codec.Assignment a : assigned) {
            long from = positions.getOrDefault(new Tp(a.topic, a.partition), 0L);
            long maxWait = 0;
            if (!waited && wait > 0) {
                maxWait = wait;
                waited = true;
            }
            List<Record> recs = backend.fetch(a.topic, a.partition, from, POLL_MAX_MESSAGES, maxWait);
            for (Record r : recs) {
                long next = r.offset == Long.MAX_VALUE ? Long.MAX_VALUE : r.offset + 1;
                positions.put(new Tp(a.topic, a.partition), next);
                out.add(r);
            }
        }
        return out;
    }

    /** Commit last+1 positions for all assigned partitions (member + generation). */
    public void commit() {
        ensureOpen();
        if (positions.isEmpty()) {
            return;
        }
        List<Codec.OffsetCommitEntry> entries = new ArrayList<>();
        for (Map.Entry<Tp, Long> e : positions.entrySet()) {
            entries.add(new Codec.OffsetCommitEntry(e.getKey().topic, e.getKey().partition, e.getValue(), ""));
        }
        backend.commitOffsets(groupId, memberId, generation, entries);
    }

    /** Leave the group. Does not close the underlying {@link Client}. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (memberId != null && !memberId.isEmpty()) {
            backend.leaveGroup(groupId, memberId);
        }
    }

    public String groupId() {
        return groupId;
    }

    public String memberId() {
        return memberId;
    }

    public long generation() {
        return generation;
    }

    public List<Codec.Assignment> assignment() {
        return assignment;
    }

    public List<Codec.Assignment> lastRevoked() {
        return lastRevoked;
    }

    /** Current next-read positions as an unmodifiable snapshot. */
    public Map<Codec.Assignment, Long> positions() {
        Map<Codec.Assignment, Long> out = new LinkedHashMap<>();
        for (Map.Entry<Tp, Long> e : positions.entrySet()) {
            out.put(new Codec.Assignment(e.getKey().topic, e.getKey().partition), e.getValue());
        }
        return Collections.unmodifiableMap(out);
    }

    private void ensureOpen() {
        if (closed) {
            throw new ProtocolException("consumer closed");
        }
    }

    static boolean needsRebalance(int code) {
        return code == ERR_REBALANCE || code == ERR_UNKNOWN_MEMBER || code == ERR_ILLEGAL_GENERATION;
    }

    private static Set<Tp> toSet(List<Codec.Assignment> items) {
        Set<Tp> out = new HashSet<>();
        for (Codec.Assignment a : items) {
            out.add(new Tp(a.topic, a.partition));
        }
        return out;
    }

    private static List<Tp> toList(Set<Tp> items) {
        return new ArrayList<>(items);
    }

    static final class Tp implements Comparable<Tp> {
        final String topic;
        final int partition;

        Tp(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Tp)) {
                return false;
            }
            Tp other = (Tp) o;
            return partition == other.partition && Objects.equals(topic, other.topic);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hashCode(topic) + partition;
        }

        @Override
        public int compareTo(Tp o) {
            int c = topic.compareTo(o.topic);
            if (c != 0) {
                return c;
            }
            return Integer.compare(partition, o.partition);
        }
    }

    /** Package-visible so unit tests can inject a fake broker. */
    interface Backend {
        JoinGroupResult joinGroup(String group, String memberId, List<String> topics, int sessionTimeoutMs);

        void heartbeat(String group, String memberId, long generation);

        void leaveGroup(String group, String memberId);

        List<Record> fetch(String topic, int partition, long offset, int maxMessages, long maxWaitMs);

        void commitOffsets(String group, String memberId, long generation, List<Codec.OffsetCommitEntry> entries);

        List<Codec.OffsetFetchEntry> fetchOffsets(String group, List<Codec.OffsetEntry> entries);
    }

    static final class ClientBackend implements Backend {
        private final Client client;

        ClientBackend(Client client) {
            this.client = client;
        }

        @Override
        public JoinGroupResult joinGroup(String group, String memberId, List<String> topics, int sessionTimeoutMs) {
            return client.joinGroup(group, memberId, topics, sessionTimeoutMs);
        }

        @Override
        public void heartbeat(String group, String memberId, long generation) {
            client.heartbeat(group, memberId, generation);
        }

        @Override
        public void leaveGroup(String group, String memberId) {
            client.leaveGroup(group, memberId);
        }

        @Override
        public List<Record> fetch(String topic, int partition, long offset, int maxMessages, long maxWaitMs) {
            return client.fetch(topic, partition, offset, maxMessages, maxWaitMs);
        }

        @Override
        public void commitOffsets(
                String group, String memberId, long generation, List<Codec.OffsetCommitEntry> entries) {
            client.offsetCommit(group, memberId, generation, entries);
        }

        @Override
        public List<Codec.OffsetFetchEntry> fetchOffsets(String group, List<Codec.OffsetEntry> entries) {
            return client.offsetFetchEntries(group, entries);
        }
    }
}
