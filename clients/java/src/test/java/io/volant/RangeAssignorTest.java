package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RangeAssignorTest {
    @Test
    void unevenPartitions() {
        List<List<Integer>> parts = RangeAssignor.rangeAssign(5, List.of("a", "b"));
        assertEquals(5, parts.get(0).size() + parts.get(1).size());
        assertEquals(List.of(0, 1, 2), parts.get(0));
        assertEquals(List.of(3, 4), parts.get(1));
    }

    @Test
    void evenSplit() {
        List<List<Integer>> parts = RangeAssignor.rangeAssign(4, List.of("m0", "m1"));
        assertEquals(List.of(0, 1), parts.get(0));
        assertEquals(List.of(2, 3), parts.get(1));
    }

    @Test
    void singleMemberGetsAll() {
        assertEquals(List.of(List.of(0, 1, 2)), RangeAssignor.rangeAssign(3, List.of("solo")));
    }

    @Test
    void threeMembersSevenPartitions() {
        List<List<Integer>> parts = RangeAssignor.rangeAssign(7, List.of("c", "a", "b"));
        assertEquals(List.of(0, 1, 2), parts.get(1));
        assertEquals(List.of(3, 4), parts.get(2));
        assertEquals(List.of(5, 6), parts.get(0));
    }

    @Test
    void emptyMembersOrZeroPartitions() {
        assertTrue(RangeAssignor.rangeAssign(5, List.of()).isEmpty());
        List<List<Integer>> zero = RangeAssignor.rangeAssign(0, List.of("a", "b"));
        assertEquals(2, zero.size());
        assertTrue(zero.get(0).isEmpty());
        assertTrue(zero.get(1).isEmpty());
    }

    @Test
    void multiTopicDisjointCover() {
        Map<String, Integer> counts = new HashMap<>();
        counts.put("t", 4);
        List<List<Codec.Assignment>> assigns =
                RangeAssignor.rangeAssignMulti(List.of("m1", "m2"), List.of(List.of("t"), List.of("t")), counts);
        assertAssigns(assigns.get(0), "t", 0, 1);
        assertAssigns(assigns.get(1), "t", 2, 3);
    }

    @Test
    void multiSkipsMissingTopic() {
        Map<String, Integer> counts = new HashMap<>();
        counts.put("t", 2);
        List<List<Codec.Assignment>> assigns = RangeAssignor.rangeAssignMulti(
                List.of("solo"), List.of(List.of("missing", "t")), counts);
        assertAssigns(assigns.get(0), "t", 0, 1);
    }

    @Test
    void multiEmptyMembers() {
        assertTrue(RangeAssignor.rangeAssignMulti(List.of(), List.of(), Map.of()).isEmpty());
    }

    @Test
    void multiLengthMismatch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RangeAssignor.rangeAssignMulti(List.of("a"), List.of(), Map.of()));
    }

    @Test
    void groupConsumerRangeFetchesEveryPartitionFromMetadata() {
        GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.metadata = topicMeta("t", 3);
        fake.records.put(tp("t", 0), recs(0, "p0"));
        fake.records.put(tp("t", 1), recs(0, "p1"));
        fake.records.put(tp("t", 2), recs(0, "p2"));

        GroupConsumer g = GroupConsumer.joinWithAssignor(fake, "g", List.of("t"), 10_000, "range");
        assertEquals("range", g.assignor());
        assertEquals(3, g.assignment().size());
        assertEquals(0, g.assignment().get(0).partition);
        assertEquals(1, g.assignment().get(1).partition);
        assertEquals(2, g.assignment().get(2).partition);
        assertEquals(1, fake.metadataCount);
        assertEquals(1, fake.describeGroupCount);

        List<Record> recs = g.poll(0);
        assertEquals(3, recs.size());
        assertEquals(3, fake.fetchCount);
        g.close();
        assertEquals(1, fake.leaveCount);
    }

    @Test
    void groupConsumerBrokerDoesNotCallMetadata() {
        GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        fake.metadata = topicMeta("t", 3);
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000);
        assertEquals("broker", g.assignor());
        assertEquals(1, g.assignment().size());
        assertEquals(0, g.assignment().get(0).partition);
        assertEquals(0, fake.metadataCount);
        assertEquals(0, fake.describeGroupCount);
        g.close();
    }

    @Test
    void groupConsumerRangeDescribeTwoMembersSplitsHalf() {
        for (String member : List.of("m-a", "m-b")) {
            GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
            fake.nextJoin = joinResult(member, 1, assign("t", 0));
            fake.metadata = topicMeta("t", 4);
            fake.describeGroupResult = new DescribeGroupResult(
                    "g",
                    1,
                    List.of(
                            new Codec.GroupMemberInfo("m-a", List.of("t"), List.of()),
                            new Codec.GroupMemberInfo("m-b", List.of("t"), List.of())));
            GroupConsumer g = GroupConsumer.joinWithAssignor(fake, "g", List.of("t"), 10_000, "range");
            if (member.equals("m-a")) {
                assertAssigns(g.assignment(), "t", 0, 1);
            } else {
                assertAssigns(g.assignment(), "t", 2, 3);
            }
            assertEquals(1, fake.describeGroupCount);
            g.close();
        }
    }

    @Test
    void groupConsumerRangeDescribeErrorFallsBackToSolo() {
        GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
        fake.nextJoin = joinResult("m-a", 1, assign("t", 0));
        fake.metadata = topicMeta("t", 4);
        fake.describeGroupError = new BrokerException(2, "", "describe_group");
        GroupConsumer g = GroupConsumer.joinWithAssignor(fake, "g", List.of("t"), 10_000, "range");
        assertAssigns(g.assignment(), "t", 0, 1, 2, 3);
        assertEquals(1, fake.describeGroupCount);
        g.close();
    }

    @Test
    void groupConsumerRangeDescribeOmitsSelfStillIncludes() {
        GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
        fake.nextJoin = joinResult("m-b", 1, assign("t", 0));
        fake.metadata = topicMeta("t", 4);
        fake.describeGroupResult = new DescribeGroupResult(
                "g", 1, List.of(new Codec.GroupMemberInfo("m-a", List.of("t"), List.of())));
        GroupConsumer g = GroupConsumer.joinWithAssignor(fake, "g", List.of("t"), 10_000, "range");
        assertAssigns(g.assignment(), "t", 2, 3);
        assertEquals(1, fake.describeGroupCount);
        g.close();
    }

    @Test
    void groupConsumerEmptyAssignorIsBroker() {
        GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
        fake.nextJoin = joinResult("m1", 1, assign("t", 0));
        GroupConsumer g = GroupConsumer.join(fake, "g", List.of("t"), 10_000, "");
        assertEquals(1, g.assignment().size());
        assertEquals(0, fake.metadataCount);
        assertEquals(0, fake.describeGroupCount);
        g.close();
    }

    @Test
    void groupConsumerRangeJoinMembersSkipsDescribe() {
        for (String member : List.of("m-a", "m-b")) {
            GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
            fake.nextJoin = new JoinGroupResult(
                    member, 1, assign("t", 0), Collections.emptyList(), List.of("m-a", "m-b"));
            fake.metadata = topicMeta("t", 4);
            fake.describeGroupError = new BrokerException(2, "", "describe_group");
            GroupConsumer g = GroupConsumer.joinWithAssignor(fake, "g", List.of("t"), 10_000, "range");
            if (member.equals("m-a")) {
                assertAssigns(g.assignment(), "t", 0, 1);
            } else {
                assertAssigns(g.assignment(), "t", 2, 3);
            }
            assertEquals(0, fake.describeGroupCount);
            g.close();
        }
    }

    @Test
    void groupConsumerUnknownAssignorThrows() {
        GroupConsumerTest.FakeBackend fake = new GroupConsumerTest.FakeBackend();
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> GroupConsumer.joinWithAssignor(fake, "g", List.of("t"), 10_000, "sticky"));
        assertTrue(ex.getMessage().contains("unknown assignor"));
        assertEquals(0, fake.joinCount);
    }

    private static void assertAssigns(List<Codec.Assignment> got, String topic, int... parts) {
        assertEquals(parts.length, got.size());
        for (int i = 0; i < parts.length; i++) {
            assertEquals(topic, got.get(i).topic);
            assertEquals(parts[i], got.get(i).partition);
        }
    }

    private static List<Codec.Assignment> assign(String topic, int partition) {
        return Collections.singletonList(new Codec.Assignment(topic, partition));
    }

    private static JoinGroupResult joinResult(String memberId, long generation, List<Codec.Assignment> assignment) {
        return new JoinGroupResult(memberId, generation, assignment, Collections.emptyList());
    }

    private static String tp(String topic, int partition) {
        return topic + "\0" + partition;
    }

    private static List<Record> recs(long offset, String value) {
        return Collections.singletonList(
                new Record(offset, -1L, null, value.getBytes(StandardCharsets.UTF_8), Collections.emptyList()));
    }

    private static Metadata topicMeta(String name, int n) {
        List<Metadata.PartitionInfo> parts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            parts.add(new Metadata.PartitionInfo(i, 0, 0, List.of(), List.of(), 0));
        }
        return new Metadata(List.of(), List.of(new Metadata.TopicInfo(name, 1, 0, parts)));
    }
}
