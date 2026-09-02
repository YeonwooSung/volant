package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** GroupConsumer unit tests against a fake backend (no broker). */
class GroupConsumerTest {
    @Test
    void joinFetchesCommittedOffsets() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), 5L);

        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
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
    void unknownOffsetStartsAtZero() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.committed.put(tp("t", 0), GroupConsumer.OFFSET_UNKNOWN);

        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
        assertEquals(0L, g.positions().values().iterator().next());
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

        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
        List<Record> recs = g.poll(500);
        assertEquals(1, recs.size());
        assertEquals(0L, recs.get(0).offset);
        assertEquals(1L, g.positions().values().iterator().next());
        assertEquals(1, fake.heartbeatCount);
        assertEquals(1, fake.fetchCount);
        assertEquals(500L, fake.lastMaxWaitMs);

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
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
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
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
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
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
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
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
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
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
        g.close();
        assertThrows(ProtocolException.class, () -> g.poll(0));
        g.close();
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void sessionTimeoutZeroDefaultsTo10000() {
        FakeBackend fake = new FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 0);
        assertEquals(10_000, fake.lastSessionTimeoutMs);
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
        JoinGroupResult nextJoin;
        int heartbeatCode;
        int joinCount;
        int heartbeatCount;
        int leaveCount;
        int fetchCount;
        int fetchOffsetCount;
        long lastMaxWaitMs;
        int lastSessionTimeoutMs;
        String lastCommitMember;
        long lastCommitGeneration;
        List<Codec.OffsetCommitEntry> lastCommit = Collections.emptyList();
        final Map<String, Long> committed = new LinkedHashMap<>();
        final Map<String, List<Record>> records = new LinkedHashMap<>();

        @Override
        public JoinGroupResult joinGroup(String group, String memberId, List<String> topics, int sessionTimeoutMs) {
            joinCount++;
            lastSessionTimeoutMs = sessionTimeoutMs;
            return nextJoin;
        }

        @Override
        public void heartbeat(String group, String memberId, long generation) {
            heartbeatCount++;
            if (heartbeatCode != 0) {
                int code = heartbeatCode;
                heartbeatCode = 0;
                throw new BrokerException(code, "", "heartbeat");
            }
        }

        @Override
        public void leaveGroup(String group, String memberId) {
            leaveCount++;
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
    }
}
