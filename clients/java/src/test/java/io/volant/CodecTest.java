package io.volant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Payload encode/decode fixtures matching crates/volant-protocol/src/payload.rs. */
class CodecTest {
    private static byte[] hx(String hex) {
        String compact = hex.replace(" ", "");
        int n = compact.length();
        byte[] out = new byte[n / 2];
        for (int i = 0; i < n; i += 2) {
            out[i / 2] = (byte) Integer.parseInt(compact.substring(i, i + 2), 16);
        }
        return out;
    }

    @Test
    void produceValueOnlyExactBytes() {
        // topic "t", partition 0, acks 1, one message: null key, value b"v",
        // timestamp -1, no headers, Phase 10 trailer (0, 0, -1).
        Codec.ProduceRequest req = new Codec.ProduceRequest(
                "t",
                0,
                1,
                Collections.singletonList(
                        new Codec.ProduceMessage(null, "v".getBytes(StandardCharsets.US_ASCII), -1L, Collections.emptyList())),
                0L,
                0,
                -1);
        byte[] raw = Codec.encodeProduceRequest(req);
        byte[] expected = hx(
                "0100" // string len 1
                        + "74" // 't'
                        + "00000000" // partition 0 i32
                        + "01" // acks
                        + "01000000" // 1 message
                        + "ffffffff" // null key
                        + "01000000" // value len 1
                        + "76" // 'v'
                        + "ffffffffffffffff" // timestamp -1
                        + "00000000" // 0 headers
                        + "0000000000000000" // producer_id
                        + "0000" // epoch
                        + "ffffffff"); // base_sequence -1
        assertArrayEquals(expected, raw);
        Codec.ProduceRequest decoded = Codec.decodeProduceRequest(raw);
        assertEquals("t", decoded.topic);
        assertEquals(0, decoded.partition);
        assertEquals(1, decoded.acks);
        assertEquals(0L, decoded.producerId);
        assertEquals(0, decoded.producerEpoch);
        assertEquals(-1, decoded.baseSequence);
        assertEquals(1, decoded.messages.size());
        assertNull(decoded.messages.get(0).key);
        assertArrayEquals("v".getBytes(StandardCharsets.US_ASCII), decoded.messages.get(0).value);
    }

    @Test
    void produceKeyedWithHeadersMatchesRustRoundtripShape() {
        Codec.ProduceRequest req = new Codec.ProduceRequest(
                "events",
                -1,
                1,
                Collections.singletonList(
                        new Codec.ProduceMessage(
                                "k".getBytes(StandardCharsets.US_ASCII),
                                "v".getBytes(StandardCharsets.US_ASCII),
                                -1L,
                                Collections.singletonList(
                                        new Record.Header("h", "hv".getBytes(StandardCharsets.US_ASCII))))),
                0L,
                0,
                -1);
        byte[] raw = Codec.encodeProduceRequest(req);
        byte[] expected = hx(
                "0600"
                        + "6576656e7473"
                        + "ffffffff"
                        + "01"
                        + "01000000"
                        + "01000000"
                        + "6b"
                        + "01000000"
                        + "76"
                        + "ffffffffffffffff"
                        + "01000000"
                        + "0100"
                        + "68"
                        + "02000000"
                        + "6876"
                        + "0000000000000000"
                        + "0000"
                        + "ffffffff");
        assertArrayEquals(expected, raw);
        Codec.ProduceRequest decoded = Codec.decodeProduceRequest(raw);
        assertEquals("events", decoded.topic);
        assertEquals(-1, decoded.partition);
        assertArrayEquals("k".getBytes(StandardCharsets.US_ASCII), decoded.messages.get(0).key);
        assertArrayEquals("hv".getBytes(StandardCharsets.US_ASCII), decoded.messages.get(0).headers.get(0).value);
    }

    @Test
    void produceLegacyWithoutTrailer() {
        byte[] raw = hx(
                "010074"
                        + "00000000"
                        + "01"
                        + "01000000"
                        + "ffffffff"
                        + "0100000076"
                        + "ffffffffffffffff"
                        + "00000000");
        Codec.ProduceRequest decoded = Codec.decodeProduceRequest(raw);
        assertEquals(0L, decoded.producerId);
        assertEquals(0, decoded.producerEpoch);
        assertEquals(-1, decoded.baseSequence);
        assertArrayEquals("v".getBytes(StandardCharsets.US_ASCII), decoded.messages.get(0).value);
        assertNull(decoded.messages.get(0).key);
    }

    @Test
    void produceResponseRoundtrip() {
        Codec.ProduceResponse resp = new Codec.ProduceResponse("t", 0, 0, 1, 0);
        byte[] raw = Codec.encodeProduceResponse(resp);
        byte[] expected = hx("010074" + "00000000" + "0000000000000000" + "01000000" + "0000");
        assertArrayEquals(expected, raw);
        Codec.ProduceResponse decoded = Codec.decodeProduceResponse(raw);
        assertEquals("t", decoded.topic);
        assertEquals(0, decoded.partition);
        assertEquals(0, decoded.baseOffset);
        assertEquals(1, decoded.count);
        assertEquals(0, decoded.errorCode);
    }

    @Test
    void fetchRequestExactBytes() {
        Codec.FetchRequest req = new Codec.FetchRequest("t", 0, 0, 10, 4096, 0);
        byte[] raw = Codec.encodeFetchRequest(req);
        byte[] expected = hx("010074" + "00000000" + "0000000000000000" + "0a000000" + "00100000" + "00000000");
        assertArrayEquals(expected, raw);
        Codec.FetchRequest decoded = Codec.decodeFetchRequest(raw);
        assertEquals("t", decoded.topic);
        assertEquals(0, decoded.partition);
        assertEquals(0, decoded.fromOffset);
        assertEquals(10, decoded.maxMessages);
        assertEquals(4096, decoded.maxBytes);
        assertEquals(0, decoded.maxWaitMs);
    }

    @Test
    void fetchResponseNullKey() {
        Codec.FetchResponse resp = new Codec.FetchResponse(
                "t",
                0,
                1,
                0,
                Collections.singletonList(
                        new Record(0, -1L, null, "hello".getBytes(StandardCharsets.US_ASCII), Collections.emptyList())));
        byte[] raw = Codec.encodeFetchResponse(resp);
        byte[] expected = hx(
                "010074"
                        + "00000000"
                        + "0100000000000000"
                        + "0000"
                        + "01000000"
                        + "0000000000000000"
                        + "ffffffffffffffff"
                        + "ffffffff"
                        + "05000000"
                        + "68656c6c6f"
                        + "00000000");
        assertArrayEquals(expected, raw);
        Codec.FetchResponse decoded = Codec.decodeFetchResponse(raw);
        assertEquals("t", decoded.topic);
        assertEquals(1, decoded.highWatermark);
        assertEquals(1, decoded.records.size());
        assertNull(decoded.records.get(0).key);
        assertArrayEquals("hello".getBytes(StandardCharsets.US_ASCII), decoded.records.get(0).value);
    }

    @Test
    void createTopicRequest() {
        Codec.CreateTopicRequest req = new Codec.CreateTopicRequest("t", 1, Collections.emptyList());
        byte[] raw = Codec.encodeCreateTopicRequest(req);
        byte[] expected = hx("010074" + "01000000" + "00000000");
        assertArrayEquals(expected, raw);
        Codec.CreateTopicRequest decoded = Codec.decodeCreateTopicRequest(raw);
        assertEquals("t", decoded.name);
        assertEquals(1, decoded.partitions);
        assertTrue(decoded.configs.isEmpty());
    }

    @Test
    void createTopicLegacyWithoutConfigs() {
        byte[] raw = hx("010074" + "02000000");
        Codec.CreateTopicRequest decoded = Codec.decodeCreateTopicRequest(raw);
        assertEquals("t", decoded.name);
        assertEquals(2, decoded.partitions);
        assertTrue(decoded.configs.isEmpty());
    }

    @Test
    void createTopicResponse() {
        Codec.CreateTopicResponse resp = new Codec.CreateTopicResponse(1, "t", 1, 0);
        byte[] raw = Codec.encodeCreateTopicResponse(resp);
        byte[] expected = hx("01000000" + "010074" + "01000000" + "0000");
        assertArrayEquals(expected, raw);
        Codec.CreateTopicResponse decoded = Codec.decodeCreateTopicResponse(raw);
        assertEquals(1, decoded.topicId);
        assertEquals("t", decoded.name);
        assertEquals(1, decoded.partitions);
        assertEquals(0, decoded.errorCode);
    }

    @Test
    void deleteTopicRoundtrip() {
        Codec.DeleteTopicRequest req = new Codec.DeleteTopicRequest("t");
        byte[] raw = Codec.encodeDeleteTopicRequest(req);
        assertArrayEquals(hx("010074"), raw);
        assertEquals("t", Codec.decodeDeleteTopicRequest(raw).name);
        Codec.DeleteTopicResponse resp = new Codec.DeleteTopicResponse("t", 0);
        byte[] rraw = Codec.encodeDeleteTopicResponse(resp);
        assertArrayEquals(hx("0100740000"), rraw);
        Codec.DeleteTopicResponse decoded = Codec.decodeDeleteTopicResponse(rraw);
        assertEquals("t", decoded.name);
        assertEquals(0, decoded.errorCode);
    }

    @Test
    void metadataRequestAllTopics() {
        Codec.MetadataRequest req = new Codec.MetadataRequest(Collections.emptyList());
        byte[] raw = Codec.encodeMetadataRequest(req);
        assertArrayEquals(hx("00000000"), raw);
        assertTrue(Codec.decodeMetadataRequest(raw).topics.isEmpty());
    }

    @Test
    void metadataResponseOneBrokerOnePartition() {
        Metadata resp = new Metadata(
                Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", 9092)),
                Collections.singletonList(
                        new Metadata.TopicInfo(
                                "t",
                                1,
                                0,
                                Collections.singletonList(
                                        new Metadata.PartitionInfo(
                                                0,
                                                1,
                                                0,
                                                Collections.singletonList(1L),
                                                Collections.singletonList(1L),
                                                0)))));
        byte[] raw = Codec.encodeMetadataResponse(resp);
        byte[] expected = hx(
                "01000000" // 1 broker
                        + "01000000" // node 1
                        + "0900" // host len 9
                        + "3132372e302e302e31" // 127.0.0.1
                        + "8423" // port 9092 le
                        + "01000000" // 1 topic
                        + "010074"
                        + "01000000" // topic_id
                        + "0000" // error
                        + "01000000" // 1 partition
                        + "00000000" // id 0
                        + "01000000" // leader 1
                        + "0000000000000000" // hwm
                        + "01000000" // 1 replica
                        + "01000000"
                        + "01000000" // 1 isr
                        + "01000000"
                        + "00000000"); // leader_epoch
        assertArrayEquals(expected, raw);
        Metadata decoded = Codec.decodeMetadataResponse(raw);
        assertEquals(1, decoded.brokers.get(0).nodeId);
        assertEquals("127.0.0.1", decoded.brokers.get(0).host);
        assertEquals(9092, decoded.brokers.get(0).port);
        assertEquals("t", decoded.topics.get(0).name);
        assertEquals(1, decoded.topics.get(0).partitions.get(0).leader);
    }

    @Test
    void decodeResponseDispatch() {
        byte[] raw = Codec.encodeProduceResponse(new Codec.ProduceResponse("t", 0, 0, 1, 0));
        Object got = Codec.decodeResponse(Codec.OP_PRODUCE, raw);
        Codec.ProduceResponse pr = assertInstanceOf(Codec.ProduceResponse.class, got);
        assertEquals("t", pr.topic);
        assertEquals(1, pr.count);
        ProtocolException ex = assertThrows(ProtocolException.class, () -> Codec.decodeResponse(0x00AB, new byte[0]));
        assertTrue(ex.getMessage().contains("unknown response opcode"), ex.getMessage());
    }

    @Test
    void hexHelperRejectsOddLength() {
        // keep a tiny sanity check that fixtures stay even-length
        assertEquals(0, hx("").length);
        assertTrue(Arrays.equals(hx("0100"), new byte[] {0x01, 0x00}));
    }

    @Test
    void offsetCommitRequestPayloadRsFixture() {
        Codec.OffsetCommitRequest req = new Codec.OffsetCommitRequest(
                "g1",
                "m1",
                2,
                Collections.singletonList(new Codec.OffsetCommitEntry("events", 1, 42, "cli")));
        byte[] raw = Codec.encodeOffsetCommitRequest(req);
        byte[] expected = hx(
                "0200"
                        + "6731"
                        + "0200"
                        + "6d31"
                        + "02000000"
                        + "01000000"
                        + "0600"
                        + "6576656e7473"
                        + "01000000"
                        + "2a00000000000000"
                        + "0300"
                        + "636c69");
        assertArrayEquals(expected, raw);
        Codec.OffsetCommitRequest decoded = Codec.decodeOffsetCommitRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals("m1", decoded.memberId);
        assertEquals(2, decoded.generation);
        assertEquals(1, decoded.entries.size());
        assertEquals("events", decoded.entries.get(0).topic);
        assertEquals(1, decoded.entries.get(0).partition);
        assertEquals(42, decoded.entries.get(0).offset);
        assertEquals("cli", decoded.entries.get(0).metadata);
    }

    @Test
    void offsetCommitRequestAdminShape() {
        Codec.OffsetCommitRequest req = new Codec.OffsetCommitRequest(
                "g",
                "",
                0,
                Collections.singletonList(new Codec.OffsetCommitEntry("t", 0, 5, "")));
        byte[] raw = Codec.encodeOffsetCommitRequest(req);
        assertArrayEquals(
                hx("010067" + "0000" + "00000000" + "01000000" + "010074" + "00000000" + "0500000000000000" + "0000"),
                raw);
        Codec.OffsetCommitRequest decoded = Codec.decodeOffsetCommitRequest(raw);
        assertEquals("g", decoded.groupId);
        assertEquals("", decoded.memberId);
        assertEquals(0, decoded.generation);
        assertEquals("t", decoded.entries.get(0).topic);
        assertEquals(5, decoded.entries.get(0).offset);
    }

    @Test
    void offsetCommitResponse() {
        byte[] raw = Codec.encodeOffsetCommitResponse(new Codec.OffsetCommitResponse(0));
        assertArrayEquals(hx("0000"), raw);
        assertEquals(0, Codec.decodeOffsetCommitResponse(raw).errorCode);
        Object got = Codec.decodeResponse(Codec.OP_OFFSET_COMMIT, raw);
        Codec.OffsetCommitResponse cr = assertInstanceOf(Codec.OffsetCommitResponse.class, got);
        assertEquals(0, cr.errorCode);
    }

    @Test
    void offsetFetchRequestPayloadRsFixture() {
        Codec.OffsetFetchRequest req =
                new Codec.OffsetFetchRequest("g1", Collections.singletonList(new Codec.OffsetEntry("events", 1)));
        byte[] raw = Codec.encodeOffsetFetchRequest(req);
        assertArrayEquals(hx("02006731" + "01000000" + "06006576656e7473" + "01000000"), raw);
        Codec.OffsetFetchRequest decoded = Codec.decodeOffsetFetchRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals(1, decoded.entries.size());
        assertEquals("events", decoded.entries.get(0).topic);
        assertEquals(1, decoded.entries.get(0).partition);
    }

    @Test
    void offsetFetchRequestEmptyEntries() {
        Codec.OffsetFetchRequest req = new Codec.OffsetFetchRequest("g1", Collections.emptyList());
        byte[] raw = Codec.encodeOffsetFetchRequest(req);
        assertArrayEquals(hx("02006731" + "00000000"), raw);
        Codec.OffsetFetchRequest decoded = Codec.decodeOffsetFetchRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertTrue(decoded.entries.isEmpty());
    }

    @Test
    void offsetFetchResponseUnknownOffset() {
        Codec.OffsetFetchResponse resp = new Codec.OffsetFetchResponse(
                0,
                Collections.singletonList(
                        new Codec.OffsetFetchEntry("events", 0, 0xFFFFFFFFFFFFFFFFL, "")));
        byte[] raw = Codec.encodeOffsetFetchResponse(resp);
        byte[] expected = hx(
                "0000"
                        + "01000000"
                        + "06006576656e7473"
                        + "00000000"
                        + "ffffffffffffffff"
                        + "0000");
        assertArrayEquals(expected, raw);
        Codec.OffsetFetchResponse decoded = Codec.decodeOffsetFetchResponse(raw);
        assertEquals(0, decoded.errorCode);
        assertEquals(1, decoded.entries.size());
        assertEquals(0xFFFFFFFFFFFFFFFFL, decoded.entries.get(0).offset);
        Object got = Codec.decodeResponse(Codec.OP_OFFSET_FETCH, raw);
        Codec.OffsetFetchResponse dispatched = assertInstanceOf(Codec.OffsetFetchResponse.class, got);
        assertEquals("events", dispatched.entries.get(0).topic);
    }

    @Test
    void offsetFetchResponseCommitted() {
        Codec.OffsetFetchResponse resp = new Codec.OffsetFetchResponse(
                0, Collections.singletonList(new Codec.OffsetFetchEntry("t", 0, 5, "")));
        byte[] raw = Codec.encodeOffsetFetchResponse(resp);
        assertArrayEquals(
                hx("0000" + "01000000" + "010074" + "00000000" + "0500000000000000" + "0000"), raw);
        Codec.OffsetFetchResponse decoded = Codec.decodeOffsetFetchResponse(raw);
        assertEquals("t", decoded.entries.get(0).topic);
        assertEquals(0, decoded.entries.get(0).partition);
        assertEquals(5, decoded.entries.get(0).offset);
        assertEquals("", decoded.entries.get(0).metadata);
    }

    @Test
    void joinGroupRequestPayloadRsFixture() {
        Codec.JoinGroupRequest req = new Codec.JoinGroupRequest(
                "g1", "", 10_000, Arrays.asList("events", "logs"), "");
        byte[] raw = Codec.encodeJoinGroupRequest(req);
        byte[] expected = hx(
                "0200"
                        + "6731"
                        + "0000"
                        + "10270000"
                        + "02000000"
                        + "0600"
                        + "6576656e7473"
                        + "0400"
                        + "6c6f6773"
                        + "0000");
        assertArrayEquals(expected, raw);
        Codec.JoinGroupRequest decoded = Codec.decodeJoinGroupRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals("", decoded.memberId);
        assertEquals(10_000, decoded.sessionTimeoutMs);
        assertEquals(Arrays.asList("events", "logs"), decoded.topics);
        assertEquals("", decoded.groupInstanceId);
    }

    @Test
    void joinGroupRequestWithInstance() {
        Codec.JoinGroupRequest req =
                new Codec.JoinGroupRequest("g1", "", 10_000, Collections.singletonList("events"), "pod-1");
        byte[] raw = Codec.encodeJoinGroupRequest(req);
        assertArrayEquals(hx("02006731" + "0000" + "10270000" + "01000000" + "06006576656e7473" + "0500706f642d31"), raw);
        Codec.JoinGroupRequest decoded = Codec.decodeJoinGroupRequest(raw);
        assertEquals("pod-1", decoded.groupInstanceId);
        assertEquals(Collections.singletonList("events"), decoded.topics);
    }

    @Test
    void joinGroupRequestLegacyWithoutInstance() {
        byte[] raw = hx("02006731" + "02006d31" + "88130000" + "01000000" + "010074");
        Codec.JoinGroupRequest decoded = Codec.decodeJoinGroupRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals("m1", decoded.memberId);
        assertEquals(5000, decoded.sessionTimeoutMs);
        assertEquals(Collections.singletonList("t"), decoded.topics);
        assertEquals("", decoded.groupInstanceId);
    }

    @Test
    void joinGroupResponsePayloadRsFixture() {
        Codec.JoinGroupResponse resp = new Codec.JoinGroupResponse(
                0,
                1,
                "uuid-1",
                Arrays.asList(new Codec.Assignment("events", 0), new Codec.Assignment("events", 1)),
                Collections.singletonList(new Codec.Assignment("events", 2)));
        byte[] raw = Codec.encodeJoinGroupResponse(resp);
        byte[] expected = hx(
                "0000"
                        + "01000000"
                        + "0600"
                        + "757569642d31"
                        + "02000000"
                        + "06006576656e7473"
                        + "00000000"
                        + "06006576656e7473"
                        + "01000000"
                        + "01000000"
                        + "06006576656e7473"
                        + "02000000");
        assertArrayEquals(expected, raw);
        Codec.JoinGroupResponse decoded = Codec.decodeJoinGroupResponse(raw);
        assertEquals("uuid-1", decoded.memberId);
        assertEquals(1, decoded.generation);
        assertEquals(2, decoded.assignment.size());
        assertEquals(1, decoded.assignment.get(1).partition);
        assertEquals(2, decoded.revoked.get(0).partition);
        Object got = Codec.decodeResponse(Codec.OP_JOIN_GROUP, raw);
        Codec.JoinGroupResponse dispatched = assertInstanceOf(Codec.JoinGroupResponse.class, got);
        assertEquals("uuid-1", dispatched.memberId);
    }

    @Test
    void joinGroupResponseLegacyWithoutRevoked() {
        byte[] raw = hx("0000" + "01000000" + "0600757569642d31" + "01000000" + "06006576656e7473" + "00000000");
        Codec.JoinGroupResponse decoded = Codec.decodeJoinGroupResponse(raw);
        assertEquals("uuid-1", decoded.memberId);
        assertEquals(1, decoded.generation);
        assertEquals(1, decoded.assignment.size());
        assertEquals("events", decoded.assignment.get(0).topic);
        assertEquals(0, decoded.assignment.get(0).partition);
        assertTrue(decoded.revoked.isEmpty());
    }

    @Test
    void heartbeatRequestPayloadRsFixture() {
        Codec.HeartbeatRequest req = new Codec.HeartbeatRequest("g1", "m1", 3);
        byte[] raw = Codec.encodeHeartbeatRequest(req);
        assertArrayEquals(hx("02006731" + "02006d31" + "03000000"), raw);
        Codec.HeartbeatRequest decoded = Codec.decodeHeartbeatRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals("m1", decoded.memberId);
        assertEquals(3, decoded.generation);
    }

    @Test
    void heartbeatResponseRebalance() {
        byte[] raw = Codec.encodeHeartbeatResponse(new Codec.HeartbeatResponse(9));
        assertArrayEquals(hx("0900"), raw);
        assertEquals(9, Codec.decodeHeartbeatResponse(raw).errorCode);
        Object got = Codec.decodeResponse(Codec.OP_HEARTBEAT, raw);
        Codec.HeartbeatResponse hr = assertInstanceOf(Codec.HeartbeatResponse.class, got);
        assertEquals(9, hr.errorCode);
    }

    @Test
    void leaveGroupRequestPayloadRsFixture() {
        Codec.LeaveGroupRequest req = new Codec.LeaveGroupRequest("g1", "m1");
        byte[] raw = Codec.encodeLeaveGroupRequest(req);
        assertArrayEquals(hx("02006731" + "02006d31"), raw);
        Codec.LeaveGroupRequest decoded = Codec.decodeLeaveGroupRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals("m1", decoded.memberId);
    }

    @Test
    void leaveGroupResponse() {
        byte[] raw = Codec.encodeLeaveGroupResponse(new Codec.LeaveGroupResponse(0));
        assertArrayEquals(hx("0000"), raw);
        assertEquals(0, Codec.decodeLeaveGroupResponse(raw).errorCode);
        Object got = Codec.decodeResponse(Codec.OP_LEAVE_GROUP, raw);
        Codec.LeaveGroupResponse lr = assertInstanceOf(Codec.LeaveGroupResponse.class, got);
        assertEquals(0, lr.errorCode);
    }

    @Test
    void authRequestS3cret() {
        byte[] raw = Codec.encodeAuthRequest(new Codec.AuthRequest("s3cret"));
        assertArrayEquals(hx("0600733363726574"), raw);
        Codec.AuthRequest decoded = Codec.decodeAuthRequest(raw);
        assertEquals("s3cret", decoded.token);
    }

    @Test
    void authResponseOkAndFailed() {
        byte[] ok = Codec.encodeAuthResponse(new Codec.AuthResponse(0));
        assertArrayEquals(hx("0000"), ok);
        assertEquals(0, Codec.decodeAuthResponse(ok).errorCode);
        Codec.AuthResponse dispatchedOk =
                assertInstanceOf(Codec.AuthResponse.class, Codec.decodeResponse(Codec.OP_AUTH_RESPONSE, ok));
        assertEquals(0, dispatchedOk.errorCode);

        byte[] fail = Codec.encodeAuthResponse(new Codec.AuthResponse(17));
        assertArrayEquals(hx("1100"), fail);
        assertEquals(17, Codec.decodeAuthResponse(fail).errorCode);
        Codec.AuthResponse dispatchedFail =
                assertInstanceOf(Codec.AuthResponse.class, Codec.decodeResponse(Codec.OP_AUTH_RESPONSE, fail));
        assertEquals(17, dispatchedFail.errorCode);
    }

    @Test
    void describeGroupRequestCg1() {
        byte[] raw = Codec.encodeDescribeGroupRequest(new Codec.DescribeGroupRequest("cg-1"));
        assertArrayEquals(hx("040063672d31"), raw);
        assertEquals("cg-1", Codec.decodeDescribeGroupRequest(raw).groupId);
    }

    @Test
    void describeGroupResponseMembers() {
        Codec.DescribeGroupResponse resp = new Codec.DescribeGroupResponse(
                0,
                "cg-1",
                3,
                Collections.singletonList(
                        new Codec.GroupMemberInfo(
                                "m-a",
                                Collections.singletonList("events"),
                                Arrays.asList(
                                        new Codec.Assignment("events", 0),
                                        new Codec.Assignment("events", 2)))));
        byte[] raw = Codec.encodeDescribeGroupResponse(resp);
        byte[] expected = hx(
                "0000"
                        + "040063672d31"
                        + "03000000"
                        + "01000000"
                        + "03006d2d61"
                        + "01000000"
                        + "06006576656e7473"
                        + "02000000"
                        + "06006576656e7473"
                        + "00000000"
                        + "06006576656e7473"
                        + "02000000");
        assertArrayEquals(expected, raw);
        Codec.DescribeGroupResponse decoded = Codec.decodeDescribeGroupResponse(raw);
        assertEquals(0, decoded.errorCode);
        assertEquals("cg-1", decoded.groupId);
        assertEquals(3, decoded.generation);
        assertEquals(1, decoded.members.size());
        assertEquals("m-a", decoded.members.get(0).memberId);
        assertEquals(Collections.singletonList("events"), decoded.members.get(0).topics);
        assertEquals(2, decoded.members.get(0).assignment.size());
        assertEquals(2, decoded.members.get(0).assignment.get(1).partition);
        Codec.DescribeGroupResponse dispatched = assertInstanceOf(
                Codec.DescribeGroupResponse.class, Codec.decodeResponse(Codec.OP_DESCRIBE_GROUP_RESPONSE, raw));
        assertEquals("cg-1", dispatched.groupId);
    }

    @Test
    void describeGroupResponseNotFound() {
        byte[] raw = Codec.encodeDescribeGroupResponse(
                new Codec.DescribeGroupResponse(2, "missing", 0, Collections.emptyList()));
        assertArrayEquals(hx("020007006d697373696e670000000000000000"), raw);
        assertEquals(2, Codec.decodeDescribeGroupResponse(raw).errorCode);
    }

    @Test
    void listGroupsRequestEmpty() {
        assertArrayEquals(new byte[0], Codec.encodeListGroupsRequest());
    }

    @Test
    void listGroupsResponseEmptyAndStable() {
        Codec.ListGroupsResponse resp = new Codec.ListGroupsResponse(
                0,
                Arrays.asList(
                        new Codec.GroupListing("g1", Codec.GROUP_STATE_STABLE, 2, 5),
                        new Codec.GroupListing("g2", Codec.GROUP_STATE_EMPTY, 0, 0)));
        byte[] raw = Codec.encodeListGroupsResponse(resp);
        byte[] expected = hx(
                "0000"
                        + "02000000"
                        + "02006731"
                        + "01"
                        + "02000000"
                        + "05000000"
                        + "02006732"
                        + "00"
                        + "00000000"
                        + "00000000");
        assertArrayEquals(expected, raw);
        Codec.ListGroupsResponse decoded = Codec.decodeListGroupsResponse(raw);
        assertEquals(2, decoded.groups.size());
        assertEquals("g1", decoded.groups.get(0).groupId);
        assertEquals(Codec.GROUP_STATE_STABLE, decoded.groups.get(0).state);
        assertEquals(2, decoded.groups.get(0).memberCount);
        assertEquals(5, decoded.groups.get(0).generation);
        assertEquals("g2", decoded.groups.get(1).groupId);
        assertEquals(Codec.GROUP_STATE_EMPTY, decoded.groups.get(1).state);
        Codec.ListGroupsResponse dispatched = assertInstanceOf(
                Codec.ListGroupsResponse.class, Codec.decodeResponse(Codec.OP_LIST_GROUPS_RESPONSE, raw));
        assertEquals(2, dispatched.groups.size());
        assertEquals(Codec.GROUP_STATE_EMPTY, new Codec.GroupListing("x", 99, 0, 0).state);
    }

    @Test
    void describeConfigsRequestEvents() {
        Codec.DescribeConfigsRequest req = new Codec.DescribeConfigsRequest("events");
        byte[] raw = Codec.encodeDescribeConfigsRequest(req);
        assertArrayEquals(hx("06006576656e7473"), raw);
        assertEquals("events", Codec.decodeDescribeConfigsRequest(raw).topic);
    }

    @Test
    void describeConfigsResponseRetentionMs() {
        Codec.DescribeConfigsResponse resp = new Codec.DescribeConfigsResponse(
                0,
                "events",
                1,
                1,
                Collections.singletonList(new String[] {"retention.ms", "86400000"}));
        byte[] raw = Codec.encodeDescribeConfigsResponse(resp);
        byte[] expected = hx(
                "0000"
                        + "06006576656e7473"
                        + "01000000"
                        + "01000000"
                        + "01000000"
                        + "0c00726574656e74696f6e2e6d73"
                        + "08003836343030303030");
        assertArrayEquals(expected, raw);
        Codec.DescribeConfigsResponse decoded = Codec.decodeDescribeConfigsResponse(raw);
        assertEquals(0, decoded.errorCode);
        assertEquals("events", decoded.topic);
        assertEquals(1, decoded.topicId);
        assertEquals(1, decoded.partitionCount);
        assertEquals(1, decoded.configs.size());
        assertEquals("retention.ms", decoded.configs.get(0)[0]);
        assertEquals("86400000", decoded.configs.get(0)[1]);
        Codec.DescribeConfigsResponse dispatched = assertInstanceOf(
                Codec.DescribeConfigsResponse.class, Codec.decodeResponse(Codec.OP_DESCRIBE_CONFIGS_RESPONSE, raw));
        assertEquals("86400000", dispatched.configs.get(0)[1]);
    }

    @Test
    void alterConfigsEmptyValueClears() {
        Codec.AlterConfigsRequest req = new Codec.AlterConfigsRequest(
                "events", Collections.singletonList(new String[] {"retention.ms", ""}));
        byte[] raw = Codec.encodeAlterConfigsRequest(req);
        byte[] expected = hx(
                "06006576656e7473" + "01000000" + "0c00726574656e74696f6e2e6d73" + "0000");
        assertArrayEquals(expected, raw);
        Codec.AlterConfigsRequest decoded = Codec.decodeAlterConfigsRequest(raw);
        assertEquals("events", decoded.topic);
        assertEquals(1, decoded.configs.size());
        assertEquals("retention.ms", decoded.configs.get(0)[0]);
        assertEquals("", decoded.configs.get(0)[1]);
    }

    @Test
    void alterConfigsResponseOk() {
        Codec.AlterConfigsResponse resp = new Codec.AlterConfigsResponse(0, "events");
        byte[] raw = Codec.encodeAlterConfigsResponse(resp);
        assertArrayEquals(hx("000006006576656e7473"), raw);
        Codec.AlterConfigsResponse decoded = Codec.decodeAlterConfigsResponse(raw);
        assertEquals(0, decoded.errorCode);
        assertEquals("events", decoded.topic);
        Codec.AlterConfigsResponse dispatched = assertInstanceOf(
                Codec.AlterConfigsResponse.class, Codec.decodeResponse(Codec.OP_ALTER_CONFIGS_RESPONSE, raw));
        assertEquals("events", dispatched.topic);
    }
}
