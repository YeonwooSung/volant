package io.volant;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Little-endian native payload encode/decode.
 *
 * <p>Matches {@code crates/volant-protocol/src/payload.rs} for the MVP opcodes:
 * Produce, Fetch, CreateTopic, Metadata, DeleteTopic, OffsetCommit,
 * OffsetFetch, JoinGroup, Heartbeat, LeaveGroup, Auth, DescribeGroup,
 * ListGroups, ListOffsets, DeleteRecords, InitProducerId, Scram.
 *
 * <p>Header fields are big-endian (see {@link Frame}); <strong>payload</strong>
 * integers and length prefixes are little-endian.
 */
public final class Codec {
    public static final int OP_PRODUCE = 1;
    public static final int OP_FETCH = 2;
    public static final int OP_CREATE_TOPIC = 3;
    public static final int OP_METADATA = 4;
    public static final int OP_DELETE_TOPIC = 5;
    public static final int OP_OFFSET_COMMIT = 6;
    public static final int OP_OFFSET_FETCH = 7;
    public static final int OP_JOIN_GROUP = 8;
    public static final int OP_HEARTBEAT = 9;
    public static final int OP_LEAVE_GROUP = 10;
    public static final int OP_AUTH = 30;
    public static final int OP_AUTH_RESPONSE = 31;
    public static final int OP_INIT_PRODUCER_ID = 32;
    public static final int OP_INIT_PRODUCER_ID_RESPONSE = 33;
    public static final int OP_SCRAM_FIRST = 60;
    public static final int OP_SCRAM_FIRST_RESPONSE = 61;
    public static final int OP_SCRAM_FINAL = 62;
    public static final int OP_SCRAM_FINAL_RESPONSE = 63;
    public static final int OP_DESCRIBE_GROUP = 34;
    public static final int OP_DESCRIBE_GROUP_RESPONSE = 35;
    public static final int OP_LIST_GROUPS = 36;
    public static final int OP_LIST_GROUPS_RESPONSE = 37;
    public static final int OP_LIST_OFFSETS = 48;
    public static final int OP_LIST_OFFSETS_RESPONSE = 49;
    public static final int OP_DELETE_RECORDS = 44;
    public static final int OP_DELETE_RECORDS_RESPONSE = 45;
    public static final int OP_ERROR = 0xFFFF;

    /** ListGroups state: offsets only, no live members. */
    public static final int GROUP_STATE_EMPTY = 0;
    /** ListGroups state: at least one live member. */
    public static final int GROUP_STATE_STABLE = 1;

    static final long NULL_LEN = 0xFFFFFFFFL;

    private Codec() {}

    // --- request / response types ------------------------------------------

    public static final class ProduceMessage {
        public final byte[] key;
        public final byte[] value;
        public final long timestampMs;
        public final List<Record.Header> headers;

        public ProduceMessage(byte[] key, byte[] value) {
            this(key, value, -1L, Collections.emptyList());
        }

        public ProduceMessage(byte[] key, byte[] value, long timestampMs, List<Record.Header> headers) {
            this.key = key;
            this.value = value == null ? new byte[0] : value;
            this.timestampMs = timestampMs;
            this.headers = headers == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(headers));
        }
    }

    public static final class ProduceRequest {
        public final String topic;
        public final int partition;
        public final int acks;
        public final List<ProduceMessage> messages;
        public final long producerId;
        public final int producerEpoch;
        public final int baseSequence;

        public ProduceRequest(
                String topic,
                int partition,
                int acks,
                List<ProduceMessage> messages,
                long producerId,
                int producerEpoch,
                int baseSequence) {
            this.topic = topic;
            this.partition = partition;
            this.acks = acks;
            this.messages = messages == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(messages));
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.baseSequence = baseSequence;
        }
    }

    public static final class ProduceResponse {
        public final String topic;
        public final long partition;
        public final long baseOffset;
        public final long count;
        public final int errorCode;

        public ProduceResponse(String topic, long partition, long baseOffset, long count, int errorCode) {
            this.topic = topic;
            this.partition = partition;
            this.baseOffset = baseOffset;
            this.count = count;
            this.errorCode = errorCode;
        }
    }

    public static final class FetchRequest {
        public final String topic;
        public final long partition;
        public final long fromOffset;
        public final long maxMessages;
        public final long maxBytes;
        public final long maxWaitMs;

        public FetchRequest(
                String topic,
                long partition,
                long fromOffset,
                long maxMessages,
                long maxBytes,
                long maxWaitMs) {
            this.topic = topic;
            this.partition = partition;
            this.fromOffset = fromOffset;
            this.maxMessages = maxMessages;
            this.maxBytes = maxBytes;
            this.maxWaitMs = maxWaitMs;
        }
    }

    public static final class FetchResponse {
        public final String topic;
        public final long partition;
        public final long highWatermark;
        public final int errorCode;
        public final List<Record> records;

        public FetchResponse(
                String topic, long partition, long highWatermark, int errorCode, List<Record> records) {
            this.topic = topic;
            this.partition = partition;
            this.highWatermark = highWatermark;
            this.errorCode = errorCode;
            this.records = records == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(records));
        }
    }

    public static final class CreateTopicRequest {
        public final String name;
        public final long partitions;
        public final List<String[]> configs;

        public CreateTopicRequest(String name, long partitions, List<String[]> configs) {
            this.name = name;
            this.partitions = partitions;
            this.configs = configs == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(configs));
        }
    }

    public static final class CreateTopicResponse {
        public final long topicId;
        public final String name;
        public final long partitions;
        public final int errorCode;

        public CreateTopicResponse(long topicId, String name, long partitions, int errorCode) {
            this.topicId = topicId;
            this.name = name;
            this.partitions = partitions;
            this.errorCode = errorCode;
        }
    }

    public static final class DeleteTopicRequest {
        public final String name;

        public DeleteTopicRequest(String name) {
            this.name = name;
        }
    }

    public static final class DeleteTopicResponse {
        public final String name;
        public final int errorCode;

        public DeleteTopicResponse(String name, int errorCode) {
            this.name = name;
            this.errorCode = errorCode;
        }
    }

    public static final class MetadataRequest {
        public final List<String> topics;

        public MetadataRequest(List<String> topics) {
            this.topics = topics == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(topics));
        }
    }

    public static final class ErrorResponse {
        public final int code;
        public final String message;

        public ErrorResponse(int code, String message) {
            this.code = code;
            this.message = message == null ? "" : message;
        }
    }

    public static final class OffsetCommitEntry {
        public final String topic;
        public final int partition;
        public final long offset;
        public final String metadata;

        public OffsetCommitEntry(String topic, int partition, long offset, String metadata) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.metadata = metadata == null ? "" : metadata;
        }
    }

    public static final class OffsetCommitRequest {
        public final String groupId;
        public final String memberId;
        public final long generation;
        public final List<OffsetCommitEntry> entries;

        public OffsetCommitRequest(
                String groupId, String memberId, long generation, List<OffsetCommitEntry> entries) {
            this.groupId = groupId;
            this.memberId = memberId == null ? "" : memberId;
            this.generation = generation;
            this.entries = entries == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(entries));
        }
    }

    public static final class OffsetCommitResponse {
        public final int errorCode;

        public OffsetCommitResponse(int errorCode) {
            this.errorCode = errorCode;
        }
    }

    public static final class OffsetEntry {
        public final String topic;
        public final int partition;

        public OffsetEntry(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }
    }

    public static final class OffsetFetchRequest {
        public final String groupId;
        public final List<OffsetEntry> entries;

        public OffsetFetchRequest(String groupId, List<OffsetEntry> entries) {
            this.groupId = groupId;
            this.entries = entries == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(entries));
        }
    }

    public static final class OffsetFetchEntry {
        public final String topic;
        public final int partition;
        public final long offset;
        public final String metadata;

        public OffsetFetchEntry(String topic, int partition, long offset, String metadata) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.metadata = metadata == null ? "" : metadata;
        }
    }

    public static final class OffsetFetchResponse {
        public final int errorCode;
        public final List<OffsetFetchEntry> entries;

        public OffsetFetchResponse(int errorCode, List<OffsetFetchEntry> entries) {
            this.errorCode = errorCode;
            this.entries = entries == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(entries));
        }
    }

    public static final class Assignment {
        public final String topic;
        public final int partition;

        public Assignment(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }
    }

    public static final class JoinGroupRequest {
        public final String groupId;
        public final String memberId;
        public final long sessionTimeoutMs;
        public final List<String> topics;
        public final String groupInstanceId;

        public JoinGroupRequest(
                String groupId,
                String memberId,
                long sessionTimeoutMs,
                List<String> topics,
                String groupInstanceId) {
            this.groupId = groupId;
            this.memberId = memberId == null ? "" : memberId;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.topics = topics == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(topics));
            this.groupInstanceId = groupInstanceId == null ? "" : groupInstanceId;
        }
    }

    public static final class JoinGroupResponse {
        public final int errorCode;
        public final long generation;
        public final String memberId;
        public final List<Assignment> assignment;
        public final List<Assignment> revoked;

        public JoinGroupResponse(
                int errorCode,
                long generation,
                String memberId,
                List<Assignment> assignment,
                List<Assignment> revoked) {
            this.errorCode = errorCode;
            this.generation = generation;
            this.memberId = memberId;
            this.assignment = assignment == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(assignment));
            this.revoked = revoked == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(revoked));
        }
    }

    public static final class HeartbeatRequest {
        public final String groupId;
        public final String memberId;
        public final long generation;

        public HeartbeatRequest(String groupId, String memberId, long generation) {
            this.groupId = groupId;
            this.memberId = memberId;
            this.generation = generation;
        }
    }

    public static final class HeartbeatResponse {
        public final int errorCode;

        public HeartbeatResponse(int errorCode) {
            this.errorCode = errorCode;
        }
    }

    public static final class LeaveGroupRequest {
        public final String groupId;
        public final String memberId;

        public LeaveGroupRequest(String groupId, String memberId) {
            this.groupId = groupId;
            this.memberId = memberId;
        }
    }

    public static final class LeaveGroupResponse {
        public final int errorCode;

        public LeaveGroupResponse(int errorCode) {
            this.errorCode = errorCode;
        }
    }

    public static final class AuthRequest {
        public final String token;

        public AuthRequest(String token) {
            this.token = token == null ? "" : token;
        }
    }

    public static final class AuthResponse {
        public final int errorCode;

        public AuthResponse(int errorCode) {
            this.errorCode = errorCode;
        }
    }

    public static final class GroupListing {
        public final String groupId;
        public final int state;
        public final long memberCount;
        public final long generation;

        public GroupListing(String groupId, int state, long memberCount, long generation) {
            this.groupId = groupId;
            this.state = state == 1 ? GROUP_STATE_STABLE : GROUP_STATE_EMPTY;
            this.memberCount = memberCount;
            this.generation = generation;
        }
    }

    public static final class GroupMemberInfo {
        public final String memberId;
        public final List<String> topics;
        public final List<Assignment> assignment;

        public GroupMemberInfo(String memberId, List<String> topics, List<Assignment> assignment) {
            this.memberId = memberId;
            this.topics = topics == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(topics));
            this.assignment = assignment == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(assignment));
        }
    }

    public static final class InitProducerIdRequest {
        public final String transactionalId;

        public InitProducerIdRequest(String transactionalId) {
            this.transactionalId = transactionalId == null ? "" : transactionalId;
        }
    }

    public static final class InitProducerIdResponse {
        public final long producerId;
        public final int epoch;
        public final int errorCode;

        public InitProducerIdResponse(long producerId, int epoch, int errorCode) {
            this.producerId = producerId;
            this.epoch = epoch;
            this.errorCode = errorCode;
        }
    }

    public static final class ScramFirstRequest {
        public final String username;
        public final String clientNonce;

        public ScramFirstRequest(String username, String clientNonce) {
            this.username = username == null ? "" : username;
            this.clientNonce = clientNonce == null ? "" : clientNonce;
        }
    }

    public static final class ScramFirstResponse {
        public final int errorCode;
        public final String combinedNonce;
        public final byte[] salt;
        public final long iterations;

        public ScramFirstResponse(int errorCode, String combinedNonce, byte[] salt, long iterations) {
            this.errorCode = errorCode;
            this.combinedNonce = combinedNonce == null ? "" : combinedNonce;
            this.salt = salt == null ? new byte[0] : salt;
            this.iterations = iterations;
        }
    }

    public static final class ScramFinalRequest {
        public final String username;
        public final String combinedNonce;
        public final byte[] clientProof;

        public ScramFinalRequest(String username, String combinedNonce, byte[] clientProof) {
            this.username = username == null ? "" : username;
            this.combinedNonce = combinedNonce == null ? "" : combinedNonce;
            this.clientProof = clientProof == null ? new byte[0] : clientProof;
        }
    }

    public static final class ScramFinalResponse {
        public final int errorCode;
        public final byte[] serverSignature;

        public ScramFinalResponse(int errorCode, byte[] serverSignature) {
            this.errorCode = errorCode;
            this.serverSignature = serverSignature == null ? new byte[0] : serverSignature;
        }
    }

    public static final class DescribeGroupRequest {
        public final String groupId;

        public DescribeGroupRequest(String groupId) {
            this.groupId = groupId == null ? "" : groupId;
        }
    }

    public static final class DescribeGroupResponse {
        public final int errorCode;
        public final String groupId;
        public final long generation;
        public final List<GroupMemberInfo> members;

        public DescribeGroupResponse(
                int errorCode, String groupId, long generation, List<GroupMemberInfo> members) {
            this.errorCode = errorCode;
            this.groupId = groupId == null ? "" : groupId;
            this.generation = generation;
            this.members = members == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(members));
        }
    }

    public static final class ListGroupsResponse {
        public final int errorCode;
        public final List<GroupListing> groups;

        public ListGroupsResponse(int errorCode, List<GroupListing> groups) {
            this.errorCode = errorCode;
            this.groups = groups == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(groups));
        }
    }

    // --- wire helpers ------------------------------------------------------

    public static final class ListOffsetsRequest {
        public final String topic;
        public final List<Integer> partitions;

        public ListOffsetsRequest(String topic, List<Integer> partitions) {
            this.topic = topic;
            this.partitions = partitions == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(partitions));
        }
    }

    public static final class ListOffsetsResponse {
        public final int errorCode;
        public final String topic;
        public final List<OffsetListing> entries;

        public ListOffsetsResponse(int errorCode, String topic, List<OffsetListing> entries) {
            this.errorCode = errorCode;
            this.topic = topic;
            this.entries = entries == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(entries));
        }
    }

    public static final class DeleteRecordsRequest {
        public final String topic;
        public final long partition;
        public final long beforeOffset;
        public final int waitMajority;

        public DeleteRecordsRequest(String topic, long partition, long beforeOffset, int waitMajority) {
            this.topic = topic;
            this.partition = partition;
            this.beforeOffset = beforeOffset;
            this.waitMajority = waitMajority;
        }
    }

    public static final class DeleteRecordsResponse {
        public final int errorCode;
        public final String topic;
        public final long partition;
        public final long lowWatermark;

        public DeleteRecordsResponse(int errorCode, String topic, long partition, long lowWatermark) {
            this.errorCode = errorCode;
            this.topic = topic;
            this.partition = partition;
            this.lowWatermark = lowWatermark;
        }
    }

    static final class Writer {
        private byte[] buf = new byte[256];
        private int pos;

        void u8(int v) {
            ensure(1);
            buf[pos++] = (byte) v;
        }

        void u16(int v) {
            ensure(2);
            buf[pos++] = (byte) v;
            buf[pos++] = (byte) (v >>> 8);
        }

        void u32(long v) {
            ensure(4);
            buf[pos++] = (byte) v;
            buf[pos++] = (byte) (v >>> 8);
            buf[pos++] = (byte) (v >>> 16);
            buf[pos++] = (byte) (v >>> 24);
        }

        void i32(int v) {
            u32(v);
        }

        void u64(long v) {
            ensure(8);
            buf[pos++] = (byte) v;
            buf[pos++] = (byte) (v >>> 8);
            buf[pos++] = (byte) (v >>> 16);
            buf[pos++] = (byte) (v >>> 24);
            buf[pos++] = (byte) (v >>> 32);
            buf[pos++] = (byte) (v >>> 40);
            buf[pos++] = (byte) (v >>> 48);
            buf[pos++] = (byte) (v >>> 56);
        }

        void i64(long v) {
            u64(v);
        }

        void raw(byte[] b) {
            if (b == null || b.length == 0) {
                return;
            }
            ensure(b.length);
            System.arraycopy(b, 0, buf, pos, b.length);
            pos += b.length;
        }

        byte[] finish() {
            byte[] out = new byte[pos];
            System.arraycopy(buf, 0, out, 0, pos);
            return out;
        }

        private void ensure(int extra) {
            int need = pos + extra;
            if (need <= buf.length) {
                return;
            }
            int cap = buf.length;
            while (cap < need) {
                cap *= 2;
            }
            byte[] next = new byte[cap];
            System.arraycopy(buf, 0, next, 0, pos);
            buf = next;
        }
    }

    static final class Reader {
        private final byte[] data;
        private int i;

        Reader(byte[] data) {
            this.data = data == null ? new byte[0] : data;
        }

        int remaining() {
            return data.length - i;
        }

        private void need(int n, String msg) {
            if (remaining() < n) {
                throw new ProtocolException(msg);
            }
        }

        int u8() {
            need(1, "truncated u8");
            return data[i++] & 0xFF;
        }

        int u16() {
            need(2, "truncated u16");
            int v = (data[i] & 0xFF) | ((data[i + 1] & 0xFF) << 8);
            i += 2;
            return v;
        }

        long u32() {
            need(4, "truncated u32");
            long v = (data[i] & 0xFFL)
                    | ((data[i + 1] & 0xFFL) << 8)
                    | ((data[i + 2] & 0xFFL) << 16)
                    | ((data[i + 3] & 0xFFL) << 24);
            i += 4;
            return v;
        }

        int i32() {
            return (int) u32();
        }

        long u64() {
            need(8, "truncated u64");
            long v = (data[i] & 0xFFL)
                    | ((data[i + 1] & 0xFFL) << 8)
                    | ((data[i + 2] & 0xFFL) << 16)
                    | ((data[i + 3] & 0xFFL) << 24)
                    | ((data[i + 4] & 0xFFL) << 32)
                    | ((data[i + 5] & 0xFFL) << 40)
                    | ((data[i + 6] & 0xFFL) << 48)
                    | ((data[i + 7] & 0xFFL) << 56);
            i += 8;
            return v;
        }

        long i64() {
            return u64();
        }

        byte[] take(int n, String msg) {
            need(n, msg);
            byte[] out = new byte[n];
            System.arraycopy(data, i, out, 0, n);
            i += n;
            return out;
        }
    }

    static void putString(Writer w, String s) {
        byte[] raw = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
        if (raw.length > 0xFFFF) {
            throw new ProtocolException("string too long: " + raw.length + " bytes");
        }
        w.u16(raw.length);
        w.raw(raw);
    }

    static String getString(Reader r) {
        int n = r.u16();
        byte[] raw = r.take(n, "truncated string body");
        return new String(raw, StandardCharsets.UTF_8);
    }

    static void putBytes(Writer w, byte[] b) {
        if (b == null) {
            b = new byte[0];
        }
        w.u32(b.length);
        w.raw(b);
    }

    static byte[] getBytes(Reader r) {
        long n = r.u32();
        if (n == NULL_LEN) {
            throw new ProtocolException("unexpected optional null in required bytes");
        }
        if (n > Integer.MAX_VALUE) {
            throw new ProtocolException("bytes length too large: " + n);
        }
        return r.take((int) n, "truncated bytes body");
    }

    static void putOptionalBytes(Writer w, byte[] b) {
        if (b == null) {
            w.u32(NULL_LEN);
            return;
        }
        putBytes(w, b);
    }

    static byte[] getOptionalBytes(Reader r) {
        long n = r.u32();
        if (n == NULL_LEN) {
            return null;
        }
        if (n > Integer.MAX_VALUE) {
            throw new ProtocolException("optional bytes length too large: " + n);
        }
        return r.take((int) n, "truncated optional bytes body");
    }

    static void putHeaders(Writer w, List<Record.Header> headers) {
        if (headers == null) {
            w.u32(0);
            return;
        }
        w.u32(headers.size());
        for (Record.Header h : headers) {
            putString(w, h.name);
            putBytes(w, h.value);
        }
    }

    static List<Record.Header> getHeaders(Reader r) {
        long count = r.u32();
        if (count > Integer.MAX_VALUE) {
            throw new ProtocolException("header count too large: " + count);
        }
        List<Record.Header> out = new ArrayList<>((int) count);
        for (int i = 0; i < count; i++) {
            String name = getString(r);
            byte[] value = getBytes(r);
            out.add(new Record.Header(name, value));
        }
        return out;
    }

    // --- produce -----------------------------------------------------------

    public static byte[] encodeProduceRequest(ProduceRequest req) {
        Writer w = new Writer();
        putString(w, req.topic);
        w.i32(req.partition);
        w.u8(req.acks);
        w.u32(req.messages.size());
        for (ProduceMessage m : req.messages) {
            putOptionalBytes(w, m.key);
            putBytes(w, m.value);
            w.i64(m.timestampMs);
            putHeaders(w, m.headers);
        }
        // Phase 10 idempotent trailer (always written by current encoders).
        w.u64(req.producerId);
        w.u16(req.producerEpoch);
        w.i32(req.baseSequence);
        return w.finish();
    }

    public static ProduceRequest decodeProduceRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String topic = getString(r);
        int partition = r.i32();
        int acks = r.u8();
        long n = r.u32();
        List<ProduceMessage> messages = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            byte[] key = getOptionalBytes(r);
            byte[] value = getBytes(r);
            long ts = r.i64();
            List<Record.Header> headers = getHeaders(r);
            messages.add(new ProduceMessage(key, value, ts, headers));
        }
        long producerId = 0;
        int producerEpoch = 0;
        int baseSequence = -1;
        if (r.remaining() >= 8 + 2 + 4) {
            producerId = r.u64();
            producerEpoch = r.u16();
            baseSequence = r.i32();
        }
        return new ProduceRequest(topic, partition, acks, messages, producerId, producerEpoch, baseSequence);
    }

    public static byte[] encodeProduceResponse(ProduceResponse resp) {
        Writer w = new Writer();
        putString(w, resp.topic);
        w.u32(resp.partition);
        w.u64(resp.baseOffset);
        w.u32(resp.count);
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static ProduceResponse decodeProduceResponse(byte[] payload) {
        Reader r = new Reader(payload);
        String topic = getString(r);
        return new ProduceResponse(topic, r.u32(), r.u64(), r.u32(), r.u16());
    }

    // --- fetch -------------------------------------------------------------

    public static byte[] encodeFetchRequest(FetchRequest req) {
        Writer w = new Writer();
        putString(w, req.topic);
        w.u32(req.partition);
        w.u64(req.fromOffset);
        w.u32(req.maxMessages);
        w.u32(req.maxBytes);
        w.u32(req.maxWaitMs);
        return w.finish();
    }

    public static FetchRequest decodeFetchRequest(byte[] payload) {
        Reader r = new Reader(payload);
        return new FetchRequest(getString(r), r.u32(), r.u64(), r.u32(), r.u32(), r.u32());
    }

    public static byte[] encodeFetchResponse(FetchResponse resp) {
        Writer w = new Writer();
        putString(w, resp.topic);
        w.u32(resp.partition);
        w.u64(resp.highWatermark);
        w.u16(resp.errorCode);
        w.u32(resp.records.size());
        for (Record rec : resp.records) {
            w.u64(rec.offset);
            w.i64(rec.timestampMs);
            putOptionalBytes(w, rec.key);
            putBytes(w, rec.value);
            putHeaders(w, rec.headers);
        }
        return w.finish();
    }

    public static FetchResponse decodeFetchResponse(byte[] payload) {
        Reader r = new Reader(payload);
        String topic = getString(r);
        long partition = r.u32();
        long hwm = r.u64();
        int errorCode = r.u16();
        long n = r.u32();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            records.add(new Record(r.u64(), r.i64(), getOptionalBytes(r), getBytes(r), getHeaders(r)));
        }
        return new FetchResponse(topic, partition, hwm, errorCode, records);
    }

    // --- create / delete topic ---------------------------------------------

    public static byte[] encodeCreateTopicRequest(CreateTopicRequest req) {
        Writer w = new Writer();
        putString(w, req.name);
        w.u32(req.partitions);
        // Phase 13 config trailer (always written by current encoders).
        w.u32(req.configs.size());
        for (String[] kv : req.configs) {
            putString(w, kv[0]);
            putString(w, kv[1]);
        }
        return w.finish();
    }

    public static CreateTopicRequest decodeCreateTopicRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String name = getString(r);
        long partitions = r.u32();
        List<String[]> configs = new ArrayList<>();
        if (r.remaining() >= 4) {
            long n = r.u32();
            for (int i = 0; i < n; i++) {
                configs.add(new String[] {getString(r), getString(r)});
            }
        }
        return new CreateTopicRequest(name, partitions, configs);
    }

    public static byte[] encodeCreateTopicResponse(CreateTopicResponse resp) {
        Writer w = new Writer();
        w.u32(resp.topicId);
        putString(w, resp.name);
        w.u32(resp.partitions);
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static CreateTopicResponse decodeCreateTopicResponse(byte[] payload) {
        Reader r = new Reader(payload);
        long topicId = r.u32();
        String name = getString(r);
        return new CreateTopicResponse(topicId, name, r.u32(), r.u16());
    }

    public static byte[] encodeDeleteTopicRequest(DeleteTopicRequest req) {
        Writer w = new Writer();
        putString(w, req.name);
        return w.finish();
    }

    public static DeleteTopicRequest decodeDeleteTopicRequest(byte[] payload) {
        return new DeleteTopicRequest(getString(new Reader(payload)));
    }

    public static byte[] encodeDeleteTopicResponse(DeleteTopicResponse resp) {
        Writer w = new Writer();
        putString(w, resp.name);
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static DeleteTopicResponse decodeDeleteTopicResponse(byte[] payload) {
        Reader r = new Reader(payload);
        return new DeleteTopicResponse(getString(r), r.u16());
    }

    // --- metadata ----------------------------------------------------------

    public static byte[] encodeMetadataRequest(MetadataRequest req) {
        Writer w = new Writer();
        w.u32(req.topics.size());
        for (String t : req.topics) {
            putString(w, t);
        }
        return w.finish();
    }

    public static MetadataRequest decodeMetadataRequest(byte[] payload) {
        Reader r = new Reader(payload);
        long n = r.u32();
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            topics.add(getString(r));
        }
        return new MetadataRequest(topics);
    }

    public static byte[] encodeMetadataResponse(Metadata resp) {
        Writer w = new Writer();
        w.u32(resp.brokers.size());
        for (Metadata.BrokerInfo b : resp.brokers) {
            w.u32(b.nodeId);
            putString(w, b.host);
            w.u16(b.port);
        }
        w.u32(resp.topics.size());
        for (Metadata.TopicInfo t : resp.topics) {
            putString(w, t.name);
            w.u32(t.topicId);
            w.u16(t.errorCode);
            w.u32(t.partitions.size());
            for (Metadata.PartitionInfo p : t.partitions) {
                w.u32(p.partitionId);
                w.u32(p.leader);
                w.u64(p.hwm);
                w.u32(p.replicas.size());
                for (Long replica : p.replicas) {
                    w.u32(replica);
                }
                w.u32(p.isr.size());
                for (Long replica : p.isr) {
                    w.u32(replica);
                }
                w.u32(p.leaderEpoch);
            }
        }
        return w.finish();
    }

    public static Metadata decodeMetadataResponse(byte[] payload) {
        Reader r = new Reader(payload);
        long nBrokers = r.u32();
        List<Metadata.BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < nBrokers; i++) {
            long nodeId = r.u32();
            String host = getString(r);
            int port = r.u16();
            brokers.add(new Metadata.BrokerInfo(nodeId, host, port));
        }
        long nTopics = r.u32();
        List<Metadata.TopicInfo> topics = new ArrayList<>();
        for (int i = 0; i < nTopics; i++) {
            String name = getString(r);
            long topicId = r.u32();
            int errorCode = r.u16();
            long nParts = r.u32();
            List<Metadata.PartitionInfo> parts = new ArrayList<>();
            for (int j = 0; j < nParts; j++) {
                long partitionId = r.u32();
                long leader = r.u32();
                long hwm = r.u64();
                long nRep = r.u32();
                List<Long> replicas = new ArrayList<>();
                for (int k = 0; k < nRep; k++) {
                    replicas.add(r.u32());
                }
                long nIsr = r.u32();
                List<Long> isr = new ArrayList<>();
                for (int k = 0; k < nIsr; k++) {
                    isr.add(r.u32());
                }
                long leaderEpoch = r.u32();
                parts.add(new Metadata.PartitionInfo(partitionId, leader, hwm, replicas, isr, leaderEpoch));
            }
            topics.add(new Metadata.TopicInfo(name, topicId, errorCode, parts));
        }
        return new Metadata(brokers, topics);
    }

    // --- offset commit / fetch ---------------------------------------------

    public static byte[] encodeOffsetCommitRequest(OffsetCommitRequest req) {
        Writer w = new Writer();
        putString(w, req.groupId);
        putString(w, req.memberId);
        w.u32(req.generation);
        w.u32(req.entries.size());
        for (OffsetCommitEntry e : req.entries) {
            putString(w, e.topic);
            w.u32(e.partition);
            w.u64(e.offset);
            putString(w, e.metadata);
        }
        return w.finish();
    }

    public static OffsetCommitRequest decodeOffsetCommitRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String groupId = getString(r);
        String memberId = getString(r);
        long generation = r.u32();
        long n = r.u32();
        List<OffsetCommitEntry> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String topic = getString(r);
            int partition = (int) r.u32();
            long offset = r.u64();
            String metadata = getString(r);
            entries.add(new OffsetCommitEntry(topic, partition, offset, metadata));
        }
        return new OffsetCommitRequest(groupId, memberId, generation, entries);
    }

    public static byte[] encodeOffsetCommitResponse(OffsetCommitResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static OffsetCommitResponse decodeOffsetCommitResponse(byte[] payload) {
        return new OffsetCommitResponse(new Reader(payload).u16());
    }

    public static byte[] encodeOffsetFetchRequest(OffsetFetchRequest req) {
        Writer w = new Writer();
        putString(w, req.groupId);
        w.u32(req.entries.size());
        for (OffsetEntry e : req.entries) {
            putString(w, e.topic);
            w.u32(e.partition);
        }
        return w.finish();
    }

    public static OffsetFetchRequest decodeOffsetFetchRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String groupId = getString(r);
        long n = r.u32();
        List<OffsetEntry> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String topic = getString(r);
            int partition = (int) r.u32();
            entries.add(new OffsetEntry(topic, partition));
        }
        return new OffsetFetchRequest(groupId, entries);
    }

    public static byte[] encodeOffsetFetchResponse(OffsetFetchResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        w.u32(resp.entries.size());
        for (OffsetFetchEntry e : resp.entries) {
            putString(w, e.topic);
            w.u32(e.partition);
            w.u64(e.offset);
            putString(w, e.metadata);
        }
        return w.finish();
    }

    public static OffsetFetchResponse decodeOffsetFetchResponse(byte[] payload) {
        Reader r = new Reader(payload);
        int errorCode = r.u16();
        long n = r.u32();
        List<OffsetFetchEntry> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String topic = getString(r);
            int partition = (int) r.u32();
            long offset = r.u64();
            String metadata = getString(r);
            entries.add(new OffsetFetchEntry(topic, partition, offset, metadata));
        }
        return new OffsetFetchResponse(errorCode, entries);
    }

    // --- join / heartbeat / leave ------------------------------------------

    static void putAssignments(Writer w, List<Assignment> items) {
        if (items == null) {
            w.u32(0);
            return;
        }
        w.u32(items.size());
        for (Assignment a : items) {
            putString(w, a.topic);
            w.u32(a.partition);
        }
    }

    static List<Assignment> getAssignments(Reader r) {
        long n = r.u32();
        List<Assignment> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String topic = getString(r);
            int partition = (int) r.u32();
            out.add(new Assignment(topic, partition));
        }
        return out;
    }

    public static byte[] encodeJoinGroupRequest(JoinGroupRequest req) {
        Writer w = new Writer();
        putString(w, req.groupId);
        putString(w, req.memberId);
        w.u32(req.sessionTimeoutMs);
        w.u32(req.topics.size());
        for (String t : req.topics) {
            putString(w, t);
        }
        // Phase 12 trailing field (always written by current encoders).
        putString(w, req.groupInstanceId);
        return w.finish();
    }

    public static JoinGroupRequest decodeJoinGroupRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String groupId = getString(r);
        String memberId = getString(r);
        long timeout = r.u32();
        long n = r.u32();
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            topics.add(getString(r));
        }
        String instanceId = r.remaining() > 0 ? getString(r) : "";
        return new JoinGroupRequest(groupId, memberId, timeout, topics, instanceId);
    }

    public static byte[] encodeJoinGroupResponse(JoinGroupResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        w.u32(resp.generation);
        putString(w, resp.memberId);
        putAssignments(w, resp.assignment);
        // Phase 17 trailing revoked list (always written by current encoders).
        putAssignments(w, resp.revoked);
        return w.finish();
    }

    public static JoinGroupResponse decodeJoinGroupResponse(byte[] payload) {
        Reader r = new Reader(payload);
        int errorCode = r.u16();
        long generation = r.u32();
        String memberId = getString(r);
        List<Assignment> assignment = getAssignments(r);
        List<Assignment> revoked = r.remaining() >= 4 ? getAssignments(r) : Collections.emptyList();
        return new JoinGroupResponse(errorCode, generation, memberId, assignment, revoked);
    }

    public static byte[] encodeHeartbeatRequest(HeartbeatRequest req) {
        Writer w = new Writer();
        putString(w, req.groupId);
        putString(w, req.memberId);
        w.u32(req.generation);
        return w.finish();
    }

    public static HeartbeatRequest decodeHeartbeatRequest(byte[] payload) {
        Reader r = new Reader(payload);
        return new HeartbeatRequest(getString(r), getString(r), r.u32());
    }

    public static byte[] encodeHeartbeatResponse(HeartbeatResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static HeartbeatResponse decodeHeartbeatResponse(byte[] payload) {
        return new HeartbeatResponse(new Reader(payload).u16());
    }

    public static byte[] encodeLeaveGroupRequest(LeaveGroupRequest req) {
        Writer w = new Writer();
        putString(w, req.groupId);
        putString(w, req.memberId);
        return w.finish();
    }

    public static LeaveGroupRequest decodeLeaveGroupRequest(byte[] payload) {
        Reader r = new Reader(payload);
        return new LeaveGroupRequest(getString(r), getString(r));
    }

    public static byte[] encodeLeaveGroupResponse(LeaveGroupResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static LeaveGroupResponse decodeLeaveGroupResponse(byte[] payload) {
        return new LeaveGroupResponse(new Reader(payload).u16());
    }

    public static byte[] encodeAuthRequest(AuthRequest req) {
        Writer w = new Writer();
        putString(w, req.token);
        return w.finish();
    }

    public static AuthRequest decodeAuthRequest(byte[] payload) {
        return new AuthRequest(getString(new Reader(payload)));
    }

    public static byte[] encodeAuthResponse(AuthResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static AuthResponse decodeAuthResponse(byte[] payload) {
        return new AuthResponse(new Reader(payload).u16());
    }

    public static byte[] encodeDescribeGroupRequest(DescribeGroupRequest req) {
        Writer w = new Writer();
        putString(w, req.groupId);
        return w.finish();
    }

    public static DescribeGroupRequest decodeDescribeGroupRequest(byte[] payload) {
        return new DescribeGroupRequest(getString(new Reader(payload)));
    }

    public static byte[] encodeDescribeGroupResponse(DescribeGroupResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        putString(w, resp.groupId);
        w.u32(resp.generation);
        w.u32(resp.members.size());
        for (GroupMemberInfo m : resp.members) {
            putString(w, m.memberId);
            w.u32(m.topics.size());
            for (String t : m.topics) {
                putString(w, t);
            }
            putAssignments(w, m.assignment);
        }
        return w.finish();
    }

    public static DescribeGroupResponse decodeDescribeGroupResponse(byte[] payload) {
        Reader r = new Reader(payload);
        int errorCode = r.u16();
        String groupId = getString(r);
        long generation = r.u32();
        long n = r.u32();
        List<GroupMemberInfo> members = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String memberId = getString(r);
            long nTopics = r.u32();
            List<String> topics = new ArrayList<>();
            for (int j = 0; j < nTopics; j++) {
                topics.add(getString(r));
            }
            List<Assignment> assignment = getAssignments(r);
            members.add(new GroupMemberInfo(memberId, topics, assignment));
        }
        return new DescribeGroupResponse(errorCode, groupId, generation, members);
    }

    public static byte[] encodeListGroupsRequest() {
        return new byte[0];
    }

    public static byte[] encodeListGroupsResponse(ListGroupsResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        w.u32(resp.groups.size());
        for (GroupListing g : resp.groups) {
            putString(w, g.groupId);
            w.u8(g.state);
            w.u32(g.memberCount);
            w.u32(g.generation);
        }
        return w.finish();
    }

    public static ListGroupsResponse decodeListGroupsResponse(byte[] payload) {
        Reader r = new Reader(payload);
        int errorCode = r.u16();
        long n = r.u32();
        List<GroupListing> groups = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String groupId = getString(r);
            int state = r.u8();
            long memberCount = r.u32();
            long generation = r.u32();
            groups.add(new GroupListing(groupId, state, memberCount, generation));
        }
        return new ListGroupsResponse(errorCode, groups);
    }

    // --- error opcode ------------------------------------------------------

    public static byte[] encodeListOffsetsRequest(ListOffsetsRequest req) {
        Writer w = new Writer();
        putString(w, req.topic);
        w.u32(req.partitions.size());
        for (Integer p : req.partitions) {
            w.u32(p);
        }
        return w.finish();
    }

    public static ListOffsetsRequest decodeListOffsetsRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String topic = getString(r);
        long n = r.u32();
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            partitions.add((int) r.u32());
        }
        return new ListOffsetsRequest(topic, partitions);
    }

    public static byte[] encodeListOffsetsResponse(ListOffsetsResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        putString(w, resp.topic);
        w.u32(resp.entries.size());
        for (OffsetListing e : resp.entries) {
            w.u32(e.partition);
            w.u64(e.earliest);
            w.u64(e.latest);
        }
        return w.finish();
    }

    public static ListOffsetsResponse decodeListOffsetsResponse(byte[] payload) {
        Reader r = new Reader(payload);
        int errorCode = r.u16();
        String topic = getString(r);
        long n = r.u32();
        List<OffsetListing> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            entries.add(new OffsetListing((int) r.u32(), r.u64(), r.u64()));
        }
        return new ListOffsetsResponse(errorCode, topic, entries);
    }

    public static byte[] encodeDeleteRecordsRequest(DeleteRecordsRequest req) {
        Writer w = new Writer();
        putString(w, req.topic);
        w.u32(req.partition);
        w.u64(req.beforeOffset);
        // Phase 137: always write the wait_majority trailer.
        w.u8(req.waitMajority);
        return w.finish();
    }

    public static DeleteRecordsRequest decodeDeleteRecordsRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String topic = getString(r);
        long partition = r.u32();
        long beforeOffset = r.u64();
        // Phase 137: optional wait_majority trailer (absent → 0).
        int waitMajority = r.remaining() >= 1 ? r.u8() : 0;
        return new DeleteRecordsRequest(topic, partition, beforeOffset, waitMajority);
    }

    public static byte[] encodeDeleteRecordsResponse(DeleteRecordsResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        putString(w, resp.topic);
        w.u32(resp.partition);
        w.u64(resp.lowWatermark);
        return w.finish();
    }

    public static DeleteRecordsResponse decodeDeleteRecordsResponse(byte[] payload) {
        Reader r = new Reader(payload);
        int errorCode = r.u16();
        String topic = getString(r);
        long partition = r.u32();
        long lowWatermark = r.u64();
        return new DeleteRecordsResponse(errorCode, topic, partition, lowWatermark);
    }

    // --- error opcode ------------------------------------------------------

    public static byte[] encodeErrorResponse(ErrorResponse resp) {
        Writer w = new Writer();
        w.u16(resp.code);
        putString(w, resp.message);
        return w.finish();
    }

    public static ErrorResponse decodeErrorResponse(byte[] payload) {
        Reader r = new Reader(payload);
        return new ErrorResponse(r.u16(), getString(r));
    }

    /** Dispatch a response payload by opcode. */
    public static byte[] encodeInitProducerIdRequest(InitProducerIdRequest req) {
        Writer w = new Writer();
        // Always write the string; empty transactional_id = non-transactional PID.
        // Legacy empty body still decodes as "".
        putString(w, req.transactionalId);
        return w.finish();
    }

    public static InitProducerIdRequest decodeInitProducerIdRequest(byte[] payload) {
        Reader r = new Reader(payload);
        String txn = r.remaining() > 0 ? getString(r) : "";
        return new InitProducerIdRequest(txn);
    }

    public static byte[] encodeInitProducerIdResponse(InitProducerIdResponse resp) {
        Writer w = new Writer();
        w.u64(resp.producerId);
        w.u16(resp.epoch);
        w.u16(resp.errorCode);
        return w.finish();
    }

    public static InitProducerIdResponse decodeInitProducerIdResponse(byte[] payload) {
        Reader r = new Reader(payload);
        return new InitProducerIdResponse(r.u64(), r.u16(), r.u16());
    }

    // --- error opcode ------------------------------------------------------

    public static byte[] encodeScramFirstRequest(ScramFirstRequest req) {
        Writer w = new Writer();
        putString(w, req.username);
        putString(w, req.clientNonce);
        return w.finish();
    }

    public static ScramFirstRequest decodeScramFirstRequest(byte[] payload) {
        Reader r = new Reader(payload);
        return new ScramFirstRequest(getString(r), getString(r));
    }

    public static byte[] encodeScramFirstResponse(ScramFirstResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        putString(w, resp.combinedNonce);
        putBytes(w, resp.salt);
        w.u32(resp.iterations);
        return w.finish();
    }

    public static ScramFirstResponse decodeScramFirstResponse(byte[] payload) {
        Reader r = new Reader(payload);
        return new ScramFirstResponse(r.u16(), getString(r), getBytes(r), r.u32());
    }

    public static byte[] encodeScramFinalRequest(ScramFinalRequest req) {
        Writer w = new Writer();
        putString(w, req.username);
        putString(w, req.combinedNonce);
        putBytes(w, req.clientProof);
        return w.finish();
    }

    public static ScramFinalRequest decodeScramFinalRequest(byte[] payload) {
        Reader r = new Reader(payload);
        return new ScramFinalRequest(getString(r), getString(r), getBytes(r));
    }

    public static byte[] encodeScramFinalResponse(ScramFinalResponse resp) {
        Writer w = new Writer();
        w.u16(resp.errorCode);
        putBytes(w, resp.serverSignature);
        return w.finish();
    }

    public static ScramFinalResponse decodeScramFinalResponse(byte[] payload) {
        Reader r = new Reader(payload);
        return new ScramFinalResponse(r.u16(), getBytes(r));
    }

    // --- error opcode ------------------------------------------------------

    public static Object decodeResponse(int opcode, byte[] payload) {
        switch (opcode) {
            case OP_PRODUCE:
                return decodeProduceResponse(payload);
            case OP_FETCH:
                return decodeFetchResponse(payload);
            case OP_CREATE_TOPIC:
                return decodeCreateTopicResponse(payload);
            case OP_METADATA:
                return decodeMetadataResponse(payload);
            case OP_DELETE_TOPIC:
                return decodeDeleteTopicResponse(payload);
            case OP_OFFSET_COMMIT:
                return decodeOffsetCommitResponse(payload);
            case OP_OFFSET_FETCH:
                return decodeOffsetFetchResponse(payload);
            case OP_JOIN_GROUP:
                return decodeJoinGroupResponse(payload);
            case OP_HEARTBEAT:
                return decodeHeartbeatResponse(payload);
            case OP_LEAVE_GROUP:
                return decodeLeaveGroupResponse(payload);
            case OP_AUTH_RESPONSE:
                return decodeAuthResponse(payload);
            case OP_INIT_PRODUCER_ID_RESPONSE:
                return decodeInitProducerIdResponse(payload);
            case OP_SCRAM_FIRST_RESPONSE:
                return decodeScramFirstResponse(payload);
            case OP_SCRAM_FINAL_RESPONSE:
                return decodeScramFinalResponse(payload);
            case OP_DESCRIBE_GROUP_RESPONSE:
                return decodeDescribeGroupResponse(payload);
            case OP_LIST_GROUPS_RESPONSE:
                return decodeListGroupsResponse(payload);
            case OP_LIST_OFFSETS_RESPONSE:
                return decodeListOffsetsResponse(payload);
            case OP_DELETE_RECORDS_RESPONSE:
                return decodeDeleteRecordsResponse(payload);
            case OP_ERROR:
                return decodeErrorResponse(payload);
            default:
                throw new ProtocolException("unknown response opcode " + opcode);
        }
    }
}
