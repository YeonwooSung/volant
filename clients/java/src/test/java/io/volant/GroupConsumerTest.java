package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** GroupConsumer unit tests against a fake backend (no broker). */
class GroupConsumerTest {
    @Test
    void joinFetchesCommittedOffsets() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), 5L);

        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        assertEquals("m1", g.memberId());
        assertEquals(1, g.generation());
        assertEquals(1, g.assignment().size());
        assertEquals(5L, g.positions().values().iterator().next());
        assertEquals(1, fake.joinCount);
        assertEquals(1, fake.fetchOffsetCount);
        g.close();
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void unknownOffsetUsesListOffsetsEarliest() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), GroupConsumer.OFFSET_UNKNOWN);

        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        assertEquals(0L, g.positions().values().iterator().next());
        assertEquals("earliest", g.autoOffsetReset());
        assertEquals(1, fake.listOffsetCount);
        assertEquals("t", fake.lastListOffsetTopic);
        assertEquals(List.of(0), fake.lastListOffsetPartitions);
        g.close();
    }

    @Test
    void pollHeartbeatsFetchesAndAdvances() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(0, -1L, null, "hello".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));

        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        List<Record> recs = g.poll(500);
        assertEquals(1, recs.size());
        assertEquals(0L, recs.get(0).offset);
        assertEquals(1L, g.positions().values().iterator().next());
        assertEquals(1, fake.heartbeatCount);
        assertEquals(1, fake.fetchCount);
        assertEquals(500L, fake.lastMaxWaitMs);
        assertEquals(0, fake.commitCount);

        g.commit();
        assertEquals(1, fake.lastCommit.size());
        assertEquals("t", fake.lastCommit.get(0).topic);
        assertEquals(0, fake.lastCommit.get(0).partition);
        assertEquals(1L, fake.lastCommit.get(0).offset);
        assertEquals("m1", fake.lastCommitMember);
        assertEquals(1L, fake.lastCommitGeneration);
        g.close();
    }

    @Test
    void pollRejoinsOnError9() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        assertEquals(1, fake.joinCount);

        fake.heartbeatCode = 9;
        fake.nextJoin = joinResult("m1", 2, assign("t", 1));
        fake.committed.put(tp("t", 1), 3L);

        List<Record> recs = g.poll(0);
        assertTrue(recs.isEmpty());
        assertEquals(2, fake.joinCount);
        assertEquals(2, g.generation());
        assertEquals(1, g.assignment().get(0).partition);
        assertEquals(3L, g.positions().values().iterator().next());
        g.close();
    }

    @Test
    void pollRejoinsOnIllegalGeneration() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        fake.heartbeatCode = 11;
        fake.nextJoin = joinResult("m1", 3, assign("t", 0));
        g.poll(0);
        assertEquals(3, g.generation());
        assertEquals(2, fake.joinCount);
        g.close();
    }

    @Test
    void pollOtherBrokerErrorPropagates() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        fake.heartbeatCode = 1;
        BrokerException ex = assertThrows(BrokerException.class, () -> g.poll(0));
        assertEquals(1, ex.code);
        assertEquals(1, fake.joinCount);
        g.close();
    }

    @Test
    void cooperativeDropsRevokedKeepsSticky() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult(
                "m1",
                1,
                List.of(new Codec.Assignment("t", 0), new Codec.Assignment("t", 1)));
        fake.committed.put(tp("t", 0), 10L);
        fake.committed.put(tp("t", 1), 20L);
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        assertEquals(10L, pos(g, "t", 0));
        assertEquals(20L, pos(g, "t", 1));

        fake.heartbeatCode = 9;
        fake.nextJoin = new JoinGroupResult(
                "m1",
                2,
                List.of(new Codec.Assignment("t", 0), new Codec.Assignment("t", 2)),
                List.of(new Codec.Assignment("t", 1)));
        fake.committed.put(tp("t", 2), 7L);
        int fetchesBefore = fake.fetchOffsetCount;
        g.poll(0);

        assertEquals(10L, pos(g, "t", 0));
        assertEquals(7L, pos(g, "t", 2));
        assertEquals(1, g.lastRevoked().size());
        assertEquals(1, g.lastRevoked().get(0).partition);
        assertEquals(fetchesBefore + 1, fake.fetchOffsetCount);
        g.close();
    }

    @Test
    void pollAfterCloseThrows() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        g.close();
        assertThrows(ProtocolException.class, () -> g.poll(0));
        g.close();
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void sessionTimeoutZeroDefaultsTo10000() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 0, false);
        assertEquals(10_000, fake.lastSessionTimeoutMs);
        g.close();
    }

    @Test
    void joinStaticSendsGroupInstanceId() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, "inst-1");
        assertEquals("inst-1", g.groupInstanceId());
        assertEquals("inst-1", fake.lastGroupInstanceId);
        assertEquals("", fake.lastJoinMemberId);
        assertEquals(1, fake.joinCount);
        g.close();
    }

    @Test
    void joinDefaultIsDynamic() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
        assertEquals("", g.groupInstanceId());
        assertEquals("", fake.lastGroupInstanceId);
        g.close();
    }

    @Test
    void rejoinKeepsGroupInstanceId() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, "inst-1");
        fake.heartbeatCode = 9;
        fake.nextJoin = joinResult("m1", 2, assign("t", 0));
        g.poll(0);
        assertEquals(2, fake.joinCount);
        assertEquals(List.of("inst-1", "inst-1"), fake.joinInstanceIds);
        assertEquals("m1", fake.lastJoinMemberId);
        assertEquals("inst-1", g.groupInstanceId());
        g.close();
    }

    @Test
    void heartbeatIntervalClamped() {
        assertEquals(100L, GroupConsumer.heartbeatIntervalMs(0));
        assertEquals(100L, GroupConsumer.heartbeatIntervalMs(150));
        assertEquals(100L, GroupConsumer.heartbeatIntervalMs(300));
        assertEquals(300L, GroupConsumer.heartbeatIntervalMs(900));
        assertEquals(3000L, GroupConsumer.heartbeatIntervalMs(10_000));
    }

    @Test
    void backgroundHeartbeatWithoutPoll() throws Exception {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 300, true);
        try {
            long deadline = System.nanoTime() + 1_000_000_000L;
            while (System.nanoTime() < deadline && fake.heartbeats() == 0) {
                Thread.sleep(20);
            }
            assertTrue(fake.heartbeats() > 0);
            assertEquals(0, fake.fetchCount);
        } finally {
            g.close();
        }
        int n = fake.heartbeats();
        Thread.sleep(350);
        assertEquals(n, fake.heartbeats());
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void backgroundHeartbeatRejoinsOnError9() throws Exception {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 300, true);
        fake.heartbeatCode = 9;
        fake.nextJoin = joinResult("m1", 2, assign("t", 0));
        try {
            long deadline = System.nanoTime() + 1_000_000_000L;
            while (System.nanoTime() < deadline && g.generation() < 2) {
                Thread.sleep(20);
            }
            assertEquals(2, g.generation());
            assertEquals(2, fake.joinCount);
        } finally {
            g.close();
        }
    }

    @Test
    void pollDoesNotAutoCommitByDefault() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(0, -1L, null, "a".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        List<Record> recs = g.poll(0);
        assertEquals(1, recs.size());
        assertEquals(0, fake.commitCount);
        g.close();
        assertEquals(0, fake.commitCount);
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void autoCommitIntervalZeroCommitsAfterPoll() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(0, -1L, null, "a".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));
        GroupConsumer g = GroupConsumer.joinWithAutoCommit(fake, "g", List.of("t"), 10_000, 0);
        List<Record> recs = g.poll(0);
        assertEquals(1, recs.size());
        assertEquals(1, fake.commitCount);
        assertEquals("m1", fake.lastCommitMember);
        assertEquals(1L, fake.lastCommitGeneration);
        assertEquals(1, fake.lastCommit.size());
        assertEquals(1L, fake.lastCommit.get(0).offset);
        g.close();
    }

    @Test
    void autoCommitIntervalFirstPollOnly() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(0, -1L, null, "a".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));
        GroupConsumer g = GroupConsumer.joinWithAutoCommit(fake, "g", List.of("t"), 10_000, 10_000);
        assertEquals(1, g.poll(0).size());
        assertEquals(1, fake.commitCount);
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(1, -1L, null, "b".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));
        assertEquals(1, g.poll(0).size());
        assertEquals(1, fake.commitCount);
        g.close();
    }

    @Test
    void autoCommitCloseCommitsPendingThenLeaves() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(0, -1L, null, "a".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));
        GroupConsumer g = GroupConsumer.joinWithAutoCommit(fake, "g", List.of("t"), 10_000, 10_000);
        g.poll(0);
        assertEquals(1, fake.commitCount);
        fake.records.put(
                tp("t", 0),
                Collections.singletonList(
                        new Record(1, -1L, null, "b".getBytes(StandardCharsets.UTF_8), Collections.emptyList())));
        g.poll(0);
        assertEquals(1, fake.commitCount);
        g.close();
        assertEquals(2, fake.commitCount);
        assertEquals(1L, fake.lastCommitGeneration);
        assertEquals("m1", fake.lastCommitMember);
        assertEquals(2L, fake.lastCommit.get(0).offset);
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void earliestUnknownUsesListOffsetsEarliest() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), GroupConsumer.OFFSET_UNKNOWN);
        fake.listOffsetEntries.put(tp("t", 0), new OffsetListing(0, 7, 20));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        assertEquals(7L, g.positions().values().iterator().next());
        assertEquals(1, fake.listOffsetCount);
        assertEquals("t", fake.lastListOffsetTopic);
        assertEquals(List.of(0), fake.lastListOffsetPartitions);
        assertEquals(0, fake.fetchCount);
        g.close();
    }

    @Test
    void earliestListOffsetsMissingPartitionRaises() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), GroupConsumer.OFFSET_UNKNOWN);
        fake.listOffsetOmit.add(tp("t", 0));
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> GroupConsumer.join(fake, "g", List.of("t"), 10_000, false));
        assertTrue(ex.getMessage().contains("list_offsets missing partition"));
        assertEquals(1, fake.listOffsetCount);
        assertEquals(0, fake.fetchCount);
    }

    @Test
    void latestUnknownUsesListOffsets() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), GroupConsumer.OFFSET_UNKNOWN);
        fake.listOffsetEntries.put(tp("t", 0), new OffsetListing(0, 7, 20));
        GroupConsumer g = GroupConsumer.joinWithOffsetReset(fake, "g", List.of("t"), 10_000, "latest");
        assertEquals(20L, g.positions().values().iterator().next());
        assertEquals(1, fake.listOffsetCount);
        assertEquals("t", fake.lastListOffsetTopic);
        assertEquals(List.of(0), fake.lastListOffsetPartitions);
        assertEquals(0, fake.fetchCount);
        g.close();
    }

    @Test
    void latestCommittedSkipsListOffsets() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), 5L);
        fake.listOffsetEntries.put(tp("t", 0), new OffsetListing(0, 7, 20));
        GroupConsumer g = GroupConsumer.joinWithOffsetReset(fake, "g", List.of("t"), 10_000, "latest");
        assertEquals(5L, g.positions().values().iterator().next());
        assertEquals(0, fake.listOffsetCount);
        g.close();
    }

    @Test
    void earliestCommittedSkipsListOffsets() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), 3L);
        fake.listOffsetEntries.put(tp("t", 0), new OffsetListing(0, 7, 20));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, false);
        assertEquals(3L, g.positions().values().iterator().next());
        assertEquals(0, fake.listOffsetCount);
        g.close();
    }

    @Test
    void noneUnknownRaisesWithoutFetch() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), GroupConsumer.OFFSET_UNKNOWN);
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> GroupConsumer.joinWithOffsetReset(fake, "g", List.of("t"), 10_000, "none"));
        assertTrue(ex.getMessage().contains("auto_offset_reset"));
        assertEquals(0, fake.fetchCount);
        assertEquals(0, fake.listOffsetCount);
    }

    @Test
    void invalidAutoOffsetResetThrowsBeforeJoin() {
        FakeBackend fake = new FakeBackend();
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> GroupConsumer.joinWithOffsetReset(fake, "g", List.of("t"), 10_000, "foo"));
        assertTrue(ex.getMessage().contains("unknown auto_offset_reset"));
        assertEquals(0, fake.joinCount);
        assertEquals(0, fake.listOffsetCount);
    }

    @Test
    void latestEmptyAssignmentSkipsListOffsets() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, Collections.emptyList());
        GroupConsumer g = GroupConsumer.joinWithOffsetReset(fake, "g", List.of("t"), 10_000, "latest");
        assertTrue(g.positions().isEmpty());
        assertEquals(0, fake.listOffsetCount);
        assertEquals(0, fake.fetchOffsetCount);
        g.close();
    }

    @Test
    void heartbeatFalseIsPollOnly() throws Exception {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 300, false);
        Thread.sleep(350);
        assertEquals(0, fake.heartbeats());
        g.close();
    }

    private static long pos(GroupConsumer g, String topic, int partition) {
        for (Map.Entry<Codec.Assignment, Long> e : g.positions().entrySet()) {
            if (topic.equals(e.getKey().topic) && e.getKey().partition == partition) {
                return e.getValue();
            }
        }
        throw new AssertionError("missing position " + topic + "-" + partition);
    }

    private static String tp(String topic, int partition) {
        return topic + "\0" + partition;
    }

    private static List<Codec.Assignment> assign(String topic, int partition) {
        return Collections.singletonList(new Codec.Assignment(topic, partition));
    }

    private static JoinGroupResult joinResult(String memberId, long generation, List<Codec.Assignment> assignment) {
        return new JoinGroupResult(memberId, generation, assignment, Collections.emptyList());
    }

    static final class FakeBackend implements GroupConsumer.Backend {
        private final Object lock = new Object();
        JoinGroupResult nextJoin;
        int heartbeatCode;
        int joinCount;
        int heartbeatCount;
        int leaveCount;
        int fetchCount;
        int fetchOffsetCount;
        int listOffsetCount;
        String lastListOffsetTopic = "";
        List<Integer> lastListOffsetPartitions = Collections.emptyList();
        final Map<String, OffsetListing> listOffsetEntries = new LinkedHashMap<>();
        final Set<String> listOffsetOmit = new HashSet<>();
        long lastMaxWaitMs;
        int lastSessionTimeoutMs;
        String lastGroupInstanceId = "";
        String lastJoinMemberId = "";
        final List<String> joinInstanceIds = new ArrayList<>();
        String lastCommitMember;
        long lastCommitGeneration;
        int commitCount;
        List<Codec.OffsetCommitEntry> lastCommit = Collections.emptyList();
        final Map<String, Long> committed = new LinkedHashMap<>();
        final Map<String, List<Record>> records = new LinkedHashMap<>();
        Metadata metadata = new Metadata(Collections.emptyList(), Collections.emptyList());
        int metadataCount;
        DescribeGroupResult describeGroupResult;
        RuntimeException describeGroupError;
        int describeGroupCount;

        int heartbeats() {
            synchronized (lock) {
                return heartbeatCount;
            }
        }

        @Override
        public JoinGroupResult joinGroup(
                String group, String memberId, List<String> topics, int sessionTimeoutMs, String groupInstanceId) {
            synchronized (lock) {
                joinCount++;
                lastSessionTimeoutMs = sessionTimeoutMs;
                lastGroupInstanceId = groupInstanceId == null ? "" : groupInstanceId;
                lastJoinMemberId = memberId == null ? "" : memberId;
                joinInstanceIds.add(lastGroupInstanceId);
                return nextJoin;
            }
        }

        @Override
        public void heartbeat(String group, String memberId, long generation) {
            int code;
            synchronized (lock) {
                heartbeatCount++;
                code = heartbeatCode;
                heartbeatCode = 0;
            }
            if (code != 0) {
                throw new BrokerException(code, "", "heartbeat");
            }
        }

        @Override
        public void leaveGroup(String group, String memberId) {
            synchronized (lock) {
                leaveCount++;
            }
        }

        @Override
        public List<Record> fetch(String topic, int partition, long offset, int maxMessages, long maxWaitMs) {
            fetchCount++;
            lastMaxWaitMs = maxWaitMs;
            List<Record> recs = records.remove(tp(topic, partition));
            return recs == null ? Collections.emptyList() : recs;
        }

        @Override
        public void commitOffsets(
                String group, String memberId, long generation, List<Codec.OffsetCommitEntry> entries) {
            lastCommitMember = memberId;
            lastCommitGeneration = generation;
            lastCommit = new ArrayList<>(entries);
            commitCount++;
        }

        @Override
        public List<Codec.OffsetFetchEntry> fetchOffsets(String group, List<Codec.OffsetEntry> entries) {
            fetchOffsetCount++;
            List<Codec.OffsetFetchEntry> out = new ArrayList<>();
            for (Codec.OffsetEntry e : entries) {
                Long off = committed.get(tp(e.topic, e.partition));
                out.add(new Codec.OffsetFetchEntry(
                        e.topic, e.partition, off == null ? GroupConsumer.OFFSET_UNKNOWN : off, ""));
            }
            return out;
        }

        @Override
        public List<OffsetListing> listOffsets(String topic, int... partitions) {
            listOffsetCount++;
            lastListOffsetTopic = topic == null ? "" : topic;
            lastListOffsetPartitions = new ArrayList<>();
            List<OffsetListing> out = new ArrayList<>();
            if (partitions != null) {
                for (int p : partitions) {
                    lastListOffsetPartitions.add(p);
                    if (listOffsetOmit.contains(tp(topic, p))) {
                        continue;
                    }
                    OffsetListing e = listOffsetEntries.get(tp(topic, p));
                    if (e == null) {
                        e = new OffsetListing(p, 0, 0);
                    }
                    out.add(e);
                }
            }
            return out;
        }

        @Override
        public Metadata metadata() {
            metadataCount++;
            return metadata;
        }

        @Override
        public DescribeGroupResult describeGroup(String group) {
            describeGroupCount++;
            if (describeGroupError != null) {
                throw describeGroupError;
            }
            if (describeGroupResult != null) {
                return describeGroupResult;
            }
            return new DescribeGroupResult(group, 1, Collections.emptyList());
        }
    }
}
