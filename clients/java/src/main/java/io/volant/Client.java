package io.volant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sync TCP client for the native Volant protocol (MVP).
 *
 * <p>This is not {@code kafka-clients} and does not speak the Kafka shim
 * ({@code --kafka-listen}).
 *
 * <pre>
 * try (Client c = Client.connect("127.0.0.1", 9092)) {
 *   c.createTopic("t", 1);
 *   long off = c.produce("t", 0, null, "hello".getBytes(UTF_8));
 *   List&lt;Record&gt; recs = c.fetch("t", 0, 0);
 *   c.offsetCommit("g", "t", 0, 5);
 *   List&lt;Offset&gt; offs = c.offsetFetch("g", "t");
 *   List&lt;OffsetListing&gt; bounds = c.listOffsets("t");
 *   JoinGroupResult j = c.joinGroup("g", java.util.List.of("t"), 10000);
 *   c.heartbeat("g", j.memberId, j.generation);
 *   c.leaveGroup("g", j.memberId);
 *   GroupConsumer g = GroupConsumer.join(c, "g", java.util.List.of("t"), 10_000);
 *   List&lt;Record&gt; recs = g.poll(500);
 *   g.commit();
 *   g.close();
 *   Metadata meta = c.metadata();
 * }
 * // Optional TLS (v0.27):
 * try (Client c = Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("ca.pem"))) {
 *   Metadata meta = c.metadata();
 * }
 * // Optional shared-token Auth (v0.42):
 * Client.connect("127.0.0.1", 9092, "s3cret");
 * Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("ca.pem"), "s3cret");
 * </pre>
 */
public final class Client implements AutoCloseable {
    /** Client library version (crate stays 0.2.0). */
    public static final String VERSION = "0.2.0";

    private static final int DEFAULT_TIMEOUT_MS = 10_000;
    /** Native {@code ErrorCode::NotLeaderForPartition}. */
    static final int NOT_LEADER_FOR_PARTITION = 13;

    private String addr;
    private Socket socket;
    private final int timeoutMs;
    private final boolean tls;
    private final TlsOptions tlsOptions;
    private final String authToken;
    private int maxRedirects = 1;
    private long nextCorr = 1;
    private byte[] buf = new byte[0];

    private Client(String addr, Socket socket, int timeoutMs, TlsOptions tlsOptions, String authToken) {
        this.addr = addr;
        this.socket = socket;
        this.timeoutMs = timeoutMs;
        this.tls = tlsOptions != null;
        this.tlsOptions = tlsOptions;
        this.authToken = authToken;
    }

    /** Connect to a native Volant listener with a 10s timeout. */
    public static Client connect(String host, int port) {
        return connect(host, port, DEFAULT_TIMEOUT_MS, null);
    }

    /** Connect with an explicit dial / RPC timeout in milliseconds. */
    public static Client connect(String host, int port, int timeoutMs) {
        return connect(host, port, timeoutMs, null);
    }

    /**
     * Connect and send shared-token Auth when {@code authToken} is non-empty.
     * Null or empty skips Auth (same as {@link #connect(String, int)}).
     */
    public static Client connect(String host, int port, String authToken) {
        return connect(host, port, DEFAULT_TIMEOUT_MS, authToken);
    }

    /** Connect with timeout and optional shared-token Auth. */
    public static Client connect(String host, int port, int timeoutMs, String authToken) {
        Socket s = openSocket(host, port, timeoutMs, null);
        Client c = new Client(formatAddr(host, port), s, timeoutMs, null, authToken);
        return finishConnect(c);
    }

    /** Connect with TLS after TCP (v0.27). See {@link TlsOptions#ca}. */
    public static Client connectTls(String host, int port, TlsOptions tls) {
        return connectTls(host, port, tls, DEFAULT_TIMEOUT_MS, null);
    }

    /** Connect with TLS and an explicit dial / handshake / RPC timeout. */
    public static Client connectTls(String host, int port, TlsOptions tls, int timeoutMs) {
        return connectTls(host, port, tls, timeoutMs, null);
    }

    /**
     * Connect with TLS and optional shared-token Auth after the handshake.
     * Null or empty {@code authToken} skips Auth.
     */
    public static Client connectTls(String host, int port, TlsOptions tls, String authToken) {
        return connectTls(host, port, tls, DEFAULT_TIMEOUT_MS, authToken);
    }

    /** Connect with TLS, timeout, and optional shared-token Auth. */
    public static Client connectTls(String host, int port, TlsOptions tls, int timeoutMs, String authToken) {
        if (tls == null) {
            throw new IllegalArgumentException("tls options are required; use connect() for plaintext");
        }
        Socket s = openSocket(host, port, timeoutMs, tls);
        Client c = new Client(formatAddr(host, port), s, timeoutMs, tls, authToken);
        return finishConnect(c);
    }

    /**
     * Extra Produce/Fetch attempts after {@code NotLeaderForPartition} (13).
     * Default is 1 (one initial send + one redirect). {@code 0} disables
     * redirect and raises on the first 13. Negative values are treated as 0.
     */
    public void setMaxRedirects(int n) {
        this.maxRedirects = Math.max(0, n);
    }

    public int maxRedirects() {
        return maxRedirects;
    }

    private static Client finishConnect(Client c) {
        try {
            c.maybeAuthenticate();
            return c;
        } catch (RuntimeException e) {
            try {
                c.close();
            } catch (RuntimeException ignored) {
                // best-effort
            }
            throw e;
        }
    }

    /** Whether this connection is TLS-wrapped. */
    public boolean isTls() {
        return tls;
    }

    public String addr() {
        return addr;
    }

    @Override
    public void close() {
        Socket s = socket;
        socket = null;
        if (s != null) {
            try {
                s.close();
            } catch (IOException e) {
                throw new ProtocolException("close failed: " + e.getMessage(), e);
            }
        }
    }

    private long nextCorrelation() {
        long corr = nextCorr;
        nextCorr = (nextCorr + 1) & 0xFFFFFFFFL;
        if (nextCorr == 0) {
            nextCorr = 1;
        }
        return corr;
    }

    private long send(int opcode, byte[] payload) {
        if (socket == null) {
            throw new ProtocolException("client closed");
        }
        long corr = nextCorrelation();
        byte[] raw = Frame.encode(opcode, corr, payload);
        try {
            OutputStream out = socket.getOutputStream();
            out.write(raw);
            out.flush();
        } catch (IOException e) {
            throw new ProtocolException("write failed: " + e.getMessage(), e);
        }
        return corr;
    }

    private Frame recvFrame() {
        if (socket == null) {
            throw new ProtocolException("client closed");
        }
        try {
            InputStream in = socket.getInputStream();
            while (true) {
                Frame.Decode d = Frame.tryDecode(buf);
                if (d.frame != null) {
                    buf = d.rest;
                    return d.frame;
                }
                int need;
                if (buf.length >= Frame.HEADER_LEN) {
                    long payloadLen = ((buf[8] & 0xFFL) << 24)
                            | ((buf[9] & 0xFFL) << 16)
                            | ((buf[10] & 0xFFL) << 8)
                            | (buf[11] & 0xFFL);
                    if (payloadLen > Frame.MAX_PAYLOAD) {
                        throw new ProtocolException(
                                "payload too large: " + payloadLen + " > " + Frame.MAX_PAYLOAD);
                    }
                    need = Frame.HEADER_LEN + (int) payloadLen - buf.length;
                } else {
                    need = Frame.HEADER_LEN - buf.length;
                }
                if (need < 4096) {
                    need = 4096;
                }
                byte[] tmp = new byte[need];
                int n = in.read(tmp);
                if (n < 0) {
                    throw new ProtocolException("connection closed while reading frame");
                }
                byte[] next = new byte[buf.length + n];
                System.arraycopy(buf, 0, next, 0, buf.length);
                System.arraycopy(tmp, 0, next, buf.length, n);
                buf = next;
            }
        } catch (IOException e) {
            throw new ProtocolException("read failed: " + e.getMessage(), e);
        }
    }

    private Object roundTrip(int opcode, byte[] payload) {
        long corr = send(opcode, payload);
        Frame frame = recvFrame();
        if (frame.correlationId != corr) {
            throw new ProtocolException(
                    "correlation mismatch: sent " + corr + ", got " + frame.correlationId);
        }
        if (frame.version != Frame.PROTOCOL_VERSION) {
            throw new ProtocolException("unsupported protocol version: " + frame.version);
        }
        Object decoded = Codec.decodeResponse(frame.opcode, frame.payload);
        if (decoded instanceof Codec.ErrorResponse) {
            Codec.ErrorResponse er = (Codec.ErrorResponse) decoded;
            throw new BrokerException(er.code, er.message);
        }
        return decoded;
    }

    private static void check(int errorCode, String op) {
        if (errorCode != 0) {
            throw new BrokerException(errorCode, "", op);
        }
    }

    private void maybeAuthenticate() {
        if (authToken == null || authToken.isEmpty()) {
            return;
        }
        byte[] payload = Codec.encodeAuthRequest(new Codec.AuthRequest(authToken));
        Object decoded = roundTrip(Codec.OP_AUTH, payload);
        if (!(decoded instanceof Codec.AuthResponse)) {
            throw new ProtocolException("unexpected response for auth: " + typeName(decoded));
        }
        Codec.AuthResponse resp = (Codec.AuthResponse) decoded;
        check(resp.errorCode, "auth");
    }

    private static String formatAddr(String host, int port) {
        if (host != null && host.indexOf(':') >= 0 && !host.startsWith("[")) {
            return "[" + host + "]:" + port;
        }
        return host + ":" + port;
    }

    private static Socket openSocket(String host, int port, int timeoutMs, TlsOptions tls) {
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(host, port), timeoutMs);
            s.setSoTimeout(timeoutMs);
            s.setTcpNoDelay(true);
            if (tls != null) {
                Socket wrapped = Tls.wrap(s, host, port, tls);
                wrapped.setSoTimeout(timeoutMs);
                try {
                    wrapped.setTcpNoDelay(true);
                } catch (IOException ignored) {
                    // some SSL sockets reject this
                }
                return wrapped;
            }
            return s;
        } catch (IOException | RuntimeException e) {
            try {
                s.close();
            } catch (IOException ignored) {
                // best-effort
            }
            if (e instanceof ProtocolException) {
                throw (ProtocolException) e;
            }
            String prefix = tls != null ? "tls connect failed: " : "connect failed: ";
            throw new ProtocolException(prefix + e.getMessage(), e);
        }
    }

    private void reconnect(String host, int port) {
        Socket old = socket;
        socket = null;
        buf = new byte[0];
        if (old != null) {
            try {
                old.close();
            } catch (IOException ignored) {
                // best-effort
            }
        }
        socket = openSocket(host, port, timeoutMs, tlsOptions);
        addr = formatAddr(host, port);
        maybeAuthenticate();
    }

    /**
     * Metadata → reconnect to the partition leader.
     *
     * @return true when the caller should retry (redirected or already on
     *     that host); false when Metadata has no leader / unknown broker /
     *     empty host (raise the original error 13).
     */
    private boolean redirectToLeader(String topic, int partition) {
        Metadata meta = metadata();
        Long leaderId = null;
        for (Metadata.TopicInfo t : meta.topics) {
            if (!topic.equals(t.name)) {
                continue;
            }
            for (Metadata.PartitionInfo p : t.partitions) {
                if (p.partitionId == (partition & 0xFFFFFFFFL)) {
                    leaderId = p.leader;
                    break;
                }
            }
            if (leaderId != null) {
                break;
            }
        }
        if (leaderId == null) {
            return false;
        }
        Metadata.BrokerInfo broker = null;
        for (Metadata.BrokerInfo b : meta.brokers) {
            if (b.nodeId == leaderId) {
                broker = b;
                break;
            }
        }
        if (broker == null || broker.host == null || broker.host.isEmpty()) {
            return false;
        }
        String next = formatAddr(broker.host, broker.port);
        if (next.equals(addr)) {
            return true;
        }
        reconnect(broker.host, broker.port);
        return true;
    }

    /** Create a topic. Returns the broker-assigned topic id. */
    public int createTopic(String name, int partitions) {
        byte[] payload = Codec.encodeCreateTopicRequest(
                new Codec.CreateTopicRequest(name, partitions, Collections.emptyList()));
        Object decoded = roundTrip(Codec.OP_CREATE_TOPIC, payload);
        if (!(decoded instanceof Codec.CreateTopicResponse)) {
            throw new ProtocolException("unexpected response for create_topic: " + typeName(decoded));
        }
        Codec.CreateTopicResponse resp = (Codec.CreateTopicResponse) decoded;
        check(resp.errorCode, "create_topic");
        return (int) resp.topicId;
    }

    /** Delete a topic by name. */
    public void deleteTopic(String name) {
        byte[] payload = Codec.encodeDeleteTopicRequest(new Codec.DeleteTopicRequest(name));
        Object decoded = roundTrip(Codec.OP_DELETE_TOPIC, payload);
        if (!(decoded instanceof Codec.DeleteTopicResponse)) {
            throw new ProtocolException("unexpected response for delete_topic: " + typeName(decoded));
        }
        Codec.DeleteTopicResponse resp = (Codec.DeleteTopicResponse) decoded;
        check(resp.errorCode, "delete_topic");
    }

    /**
     * Produce one message (null key when {@code key} is null) with acks=1.
     * Idempotent produce is not implemented; trailer is {@code (0, 0, -1)}.
     *
     * @return the broker-assigned base offset
     */
    public long produce(String topic, int partition, byte[] key, byte[] value) {
        if (value == null) {
            value = new byte[0];
        }
        byte[] payload = Codec.encodeProduceRequest(
                new Codec.ProduceRequest(
                        topic,
                        partition,
                        1,
                        Collections.singletonList(new Codec.ProduceMessage(key, value, -1L, Collections.emptyList())),
                        0L,
                        0,
                        -1));
        int maxAttempts = 1 + maxRedirects;
        int attempt = 0;
        while (true) {
            attempt++;
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_PRODUCE, payload);
            } catch (BrokerException e) {
                if (e.code == NOT_LEADER_FOR_PARTITION
                        && attempt < maxAttempts
                        && partition >= 0
                        && redirectToLeader(topic, partition)) {
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.ProduceResponse)) {
                throw new ProtocolException("unexpected response for produce: " + typeName(decoded));
            }
            Codec.ProduceResponse resp = (Codec.ProduceResponse) decoded;
            if (resp.errorCode == NOT_LEADER_FOR_PARTITION
                    && attempt < maxAttempts
                    && redirectToLeader(resp.topic, (int) resp.partition)) {
                continue;
            }
            check(resp.errorCode, "produce");
            return resp.baseOffset;
        }
    }

    /**
     * Fetch records from topic/partition starting at {@code offset}.
     * Defaults match the Python/Go clients: max_messages=128, max_bytes=4MiB,
     * max_wait_ms=0.
     */
    public List<Record> fetch(String topic, int partition, long offset) {
        return fetch(topic, partition, offset, 128, 0);
    }

    List<Record> fetch(String topic, int partition, long offset, int maxMessages, long maxWaitMs) {
        byte[] payload = Codec.encodeFetchRequest(
                new Codec.FetchRequest(topic, partition, offset, maxMessages, 4L * 1024 * 1024, maxWaitMs));
        int maxAttempts = 1 + maxRedirects;
        int attempt = 0;
        while (true) {
            attempt++;
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_FETCH, payload);
            } catch (BrokerException e) {
                if (e.code == NOT_LEADER_FOR_PARTITION
                        && attempt < maxAttempts
                        && redirectToLeader(topic, partition)) {
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.FetchResponse)) {
                throw new ProtocolException("unexpected response for fetch: " + typeName(decoded));
            }
            Codec.FetchResponse resp = (Codec.FetchResponse) decoded;
            if (resp.errorCode == NOT_LEADER_FOR_PARTITION
                    && attempt < maxAttempts
                    && redirectToLeader(resp.topic, (int) resp.partition)) {
                continue;
            }
            check(resp.errorCode, "fetch");
            return resp.records;
        }
    }

    /**
     * Admin OffsetCommit (empty {@code memberId}, {@code generation = 0}).
     * Commits the next offset to read for one topic/partition.
     */
    public void offsetCommit(String group, String topic, int partition, long offset) {
        offsetCommit(group, topic, partition, offset, "", 0);
    }

    /**
     * OffsetCommit with member + generation (joined consumer path).
     * {@code generation = 0} skips the broker generation check.
     */
    public void offsetCommit(
            String group, String topic, int partition, long offset, String memberId, long generation) {
        offsetCommit(
                group,
                memberId,
                generation,
                Collections.singletonList(new Codec.OffsetCommitEntry(topic, partition, offset, "")));
    }

    void offsetCommit(String group, String memberId, long generation, List<Codec.OffsetCommitEntry> entries) {
        byte[] payload = Codec.encodeOffsetCommitRequest(
                new Codec.OffsetCommitRequest(group, memberId, generation, entries));
        Object decoded = roundTrip(Codec.OP_OFFSET_COMMIT, payload);
        if (!(decoded instanceof Codec.OffsetCommitResponse)) {
            throw new ProtocolException("unexpected response for offset_commit: " + typeName(decoded));
        }
        Codec.OffsetCommitResponse resp = (Codec.OffsetCommitResponse) decoded;
        check(resp.errorCode, "offset_commit");
    }

    /**
     * List earliest/latest offsets for every partition of {@code topic}
     * (native opcode 48; empty partition list on the wire).
     */
    public List<OffsetListing> listOffsets(String topic) {
        return listOffsets(topic, new int[0]);
    }

    /**
     * List earliest/latest offsets for {@code topic} (native opcode 48).
     *
     * <p>An empty {@code partitions} array means all partitions (wire count 0).
     * Non-zero {@code error_code} is {@link BrokerException}. This is not Kafka
     * ListOffsets (no timestamp or isolation); both ends of each log are
     * returned.
     */
    public List<OffsetListing> listOffsets(String topic, int... partitions) {
        List<Integer> parts = new ArrayList<>();
        if (partitions != null) {
            for (int p : partitions) {
                parts.add(p);
            }
        }
        byte[] payload = Codec.encodeListOffsetsRequest(new Codec.ListOffsetsRequest(topic, parts));
        Object decoded = roundTrip(Codec.OP_LIST_OFFSETS, payload);
        if (!(decoded instanceof Codec.ListOffsetsResponse)) {
            throw new ProtocolException("unexpected response for list_offsets: " + typeName(decoded));
        }
        Codec.ListOffsetsResponse resp = (Codec.ListOffsetsResponse) decoded;
        check(resp.errorCode, "list_offsets");
        return resp.entries;
    }

    /**
     * Fetch committed offsets for {@code topic}.
     *
     * <p>Sends empty OffsetFetch entries (all group offsets) and filters to
     * {@code topic} client-side (same as the CLI / Python / Go).
     */
    public List<Offset> offsetFetch(String group, String topic) {
        List<Codec.OffsetFetchEntry> entries = offsetFetchEntries(group, Collections.emptyList());
        List<Offset> out = new ArrayList<>();
        for (Codec.OffsetFetchEntry e : entries) {
            if (topic.equals(e.topic)) {
                out.add(new Offset(e.partition, e.offset));
            }
        }
        return out;
    }

    List<Codec.OffsetFetchEntry> offsetFetchEntries(String group, List<Codec.OffsetEntry> entries) {
        byte[] payload = Codec.encodeOffsetFetchRequest(new Codec.OffsetFetchRequest(group, entries));
        Object decoded = roundTrip(Codec.OP_OFFSET_FETCH, payload);
        if (!(decoded instanceof Codec.OffsetFetchResponse)) {
            throw new ProtocolException("unexpected response for offset_fetch: " + typeName(decoded));
        }
        Codec.OffsetFetchResponse resp = (Codec.OffsetFetchResponse) decoded;
        check(resp.errorCode, "offset_fetch");
        return resp.entries;
    }

    /**
     * Join a consumer group. First join sends empty {@code memberId}
     * (broker assigns one). {@code sessionTimeoutMs} 0 defaults to 10000.
     */
    public JoinGroupResult joinGroup(String group, List<String> topics, int sessionTimeoutMs) {
        return joinGroup(group, "", topics, sessionTimeoutMs, "");
    }

    JoinGroupResult joinGroup(String group, String memberId, List<String> topics, int sessionTimeoutMs) {
        return joinGroup(group, memberId, topics, sessionTimeoutMs, "");
    }

    JoinGroupResult joinGroup(
            String group, String memberId, List<String> topics, int sessionTimeoutMs, String groupInstanceId) {
        long timeout = sessionTimeoutMs == 0 ? 10_000L : sessionTimeoutMs;
        if (topics == null) {
            topics = Collections.emptyList();
        }
        byte[] payload = Codec.encodeJoinGroupRequest(
                new Codec.JoinGroupRequest(
                        group,
                        memberId == null ? "" : memberId,
                        timeout,
                        topics,
                        groupInstanceId == null ? "" : groupInstanceId));
        Object decoded = roundTrip(Codec.OP_JOIN_GROUP, payload);
        if (!(decoded instanceof Codec.JoinGroupResponse)) {
            throw new ProtocolException("unexpected response for join_group: " + typeName(decoded));
        }
        Codec.JoinGroupResponse resp = (Codec.JoinGroupResponse) decoded;
        check(resp.errorCode, "join_group");
        return new JoinGroupResult(resp.memberId, resp.generation, resp.assignment, resp.revoked);
    }

    /** Heartbeat for group membership. Non-zero error_code is BrokerException. */
    public void heartbeat(String group, String memberId, long generation) {
        byte[] payload = Codec.encodeHeartbeatRequest(new Codec.HeartbeatRequest(group, memberId, generation));
        Object decoded = roundTrip(Codec.OP_HEARTBEAT, payload);
        if (!(decoded instanceof Codec.HeartbeatResponse)) {
            throw new ProtocolException("unexpected response for heartbeat: " + typeName(decoded));
        }
        Codec.HeartbeatResponse resp = (Codec.HeartbeatResponse) decoded;
        check(resp.errorCode, "heartbeat");
    }

    /** Leave a consumer group. */
    public void leaveGroup(String group, String memberId) {
        byte[] payload = Codec.encodeLeaveGroupRequest(new Codec.LeaveGroupRequest(group, memberId));
        Object decoded = roundTrip(Codec.OP_LEAVE_GROUP, payload);
        if (!(decoded instanceof Codec.LeaveGroupResponse)) {
            throw new ProtocolException("unexpected response for leave_group: " + typeName(decoded));
        }
        Codec.LeaveGroupResponse resp = (Codec.LeaveGroupResponse) decoded;
        check(resp.errorCode, "leave_group");
    }

    /** Cluster brokers and topics (all topics). */
    public Metadata metadata() {
        byte[] payload = Codec.encodeMetadataRequest(new Codec.MetadataRequest(Collections.emptyList()));
        Object decoded = roundTrip(Codec.OP_METADATA, payload);
        if (!(decoded instanceof Metadata)) {
            throw new ProtocolException("unexpected response for metadata: " + typeName(decoded));
        }
        return (Metadata) decoded;
    }

    private static String typeName(Object o) {
        return o == null ? "null" : o.getClass().getName();
    }
}
