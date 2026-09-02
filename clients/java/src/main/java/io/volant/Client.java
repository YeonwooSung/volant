package io.volant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sync TCP client for the native Volant protocol (MVP).
 *
 * <p>This is not {@code kafka-clients} and does not speak the Kafka shim
 * ({@code --kafka-listen}).
 *
 * <pre>
 * try (Client c = Client.connect("127.0.0.1", 9092)) {
 *   c.createTopic("t", 1);
 *   int parts = c.createPartitions("t", 2);
 *   long off = c.produce("t", 0, null, "hello".getBytes(UTF_8));
 *   List&lt;Record&gt; recs = c.fetch("t", 0, 0);
 *   c.offsetCommit("g", "t", 0, 5);
 *   List&lt;Offset&gt; offs = c.offsetFetch("g", "t");
 *   List&lt;OffsetListing&gt; bounds = c.listOffsets("t");
 *   c.createAcls(java.util.List.of(new AclBinding("User:alice", 0, "events", 3, 1)));
 *   List&lt;AclBinding&gt; acls = c.listAcls();
 *   int removed = c.deleteAcls(acls);
 *   DeleteRecordsResult cut = c.deleteRecords("t", 0, 100);
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
 * // Optional SCRAM-SHA-256 (v0.46). Token wins if both are set.
 * Client.connectScram("127.0.0.1", 9092, "alice", "s3cret");
 * Client.connectTlsScram("127.0.0.1", 9092, TlsOptions.ca("ca.pem"), "alice", "s3cret");
 * </pre>
 */
public final class Client implements AutoCloseable {
    /** Client library version (crate stays 0.2.0). */
    public static final String VERSION = "0.2.0";

    private static final int DEFAULT_TIMEOUT_MS = 10_000;
    /** Native {@code ErrorCode::NotLeaderForPartition}. */
    static final int NOT_LEADER_FOR_PARTITION = 13;
    /** Native {@code ErrorCode::UnknownProducerId}. */
    static final int UNKNOWN_PRODUCER_ID = 21;

    private String addr;
    private Socket socket;
    private final int timeoutMs;
    private final boolean tls;
    private final TlsOptions tlsOptions;
    private final String authToken;
    private final String scramUsername;
    private final String scramPassword;
    private int maxRedirects = 1;
    private boolean enableIdempotence = false;
    private long producerId = 0L;
    private int producerEpoch = 0;
    private boolean producerReady = false;
    private final Map<String, Integer> nextSeq = new HashMap<>();
    private long nextCorr = 1;
    private byte[] buf = new byte[0];

    private Client(String addr, Socket socket, int timeoutMs, TlsOptions tlsOptions, String authToken) {
        this(addr, socket, timeoutMs, tlsOptions, authToken, null, null);
    }

    private Client(
            String addr,
            Socket socket,
            int timeoutMs,
            TlsOptions tlsOptions,
            String authToken,
            String scramUsername,
            String scramPassword) {
        this.addr = addr;
        this.socket = socket;
        this.timeoutMs = timeoutMs;
        this.tls = tlsOptions != null;
        this.tlsOptions = tlsOptions;
        this.authToken = authToken;
        this.scramUsername = scramUsername;
        this.scramPassword = scramPassword;
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
     * Connect and run SCRAM-SHA-256 (opcodes 60–63) after TCP.
     * Username and password must both be non-empty.
     */
    public static Client connectScram(String host, int port, String user, String pass) {
        return connectScram(host, port, DEFAULT_TIMEOUT_MS, user, pass);
    }

    /** Connect with timeout and SCRAM-SHA-256. */
    public static Client connectScram(String host, int port, int timeoutMs, String user, String pass) {
        requireScramPair(user, pass);
        Socket s = openSocket(host, port, timeoutMs, null);
        Client c = new Client(formatAddr(host, port), s, timeoutMs, null, null, user, pass);
        return finishConnect(c);
    }

    /**
     * Connect with TLS and SCRAM-SHA-256 after the handshake.
     * Username and password must both be non-empty.
     */
    public static Client connectTlsScram(String host, int port, TlsOptions tls, String user, String pass) {
        return connectTlsScram(host, port, tls, DEFAULT_TIMEOUT_MS, user, pass);
    }

    /** Connect with TLS, timeout, and SCRAM-SHA-256. */
    public static Client connectTlsScram(
            String host, int port, TlsOptions tls, int timeoutMs, String user, String pass) {
        if (tls == null) {
            throw new IllegalArgumentException("tls options are required; use connectScram() for plaintext");
        }
        requireScramPair(user, pass);
        Socket s = openSocket(host, port, timeoutMs, tls);
        Client c = new Client(formatAddr(host, port), s, timeoutMs, tls, null, user, pass);
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

    /**
     * Opt-in InitProducerId + per-partition produce sequences. Default is
     * off (trailer {@code (0, 0, -1)}). Call before the first produce.
     */
    public void setEnableIdempotence(boolean enable) {
        this.enableIdempotence = enable;
    }

    public boolean enableIdempotence() {
        return enableIdempotence;
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

    private static void requireScramPair(String user, String pass) {
        boolean hasUser = user != null && !user.isEmpty();
        boolean hasPass = pass != null && !pass.isEmpty();
        if (!hasUser || !hasPass) {
            throw new IllegalArgumentException("scram username and password must both be set");
        }
    }

    private void maybeAuthenticate() {
        if (authToken != null && !authToken.isEmpty()) {
            authenticate(authToken);
            return;
        }
        if (scramUsername != null
                && !scramUsername.isEmpty()
                && scramPassword != null
                && !scramPassword.isEmpty()) {
            authenticateScram(scramUsername, scramPassword);
        }
    }

    private void authenticate(String token) {
        byte[] payload = Codec.encodeAuthRequest(new Codec.AuthRequest(token));
        Object decoded = roundTrip(Codec.OP_AUTH, payload);
        if (!(decoded instanceof Codec.AuthResponse)) {
            throw new ProtocolException("unexpected response for auth: " + typeName(decoded));
        }
        Codec.AuthResponse resp = (Codec.AuthResponse) decoded;
        check(resp.errorCode, "auth");
    }

    private void authenticateScram(String username, String password) {
        String clientNonce = Scram.generateClientNonce();
        byte[] payload =
                Codec.encodeScramFirstRequest(new Codec.ScramFirstRequest(username, clientNonce));
        Object decoded = roundTrip(Codec.OP_SCRAM_FIRST, payload);
        if (!(decoded instanceof Codec.ScramFirstResponse)) {
            throw new ProtocolException("unexpected response for scram first: " + typeName(decoded));
        }
        Codec.ScramFirstResponse first = (Codec.ScramFirstResponse) decoded;
        check(first.errorCode, "scram first");
        if (first.iterations <= 0 || first.iterations > Integer.MAX_VALUE) {
            throw new ProtocolException("scram iterations out of range: " + first.iterations);
        }
        Scram.Proof proof = Scram.clientProofAndServerSig(
                username,
                password,
                clientNonce,
                first.combinedNonce,
                first.salt,
                (int) first.iterations);
        payload = Codec.encodeScramFinalRequest(
                new Codec.ScramFinalRequest(username, first.combinedNonce, proof.clientProof));
        decoded = roundTrip(Codec.OP_SCRAM_FINAL, payload);
        if (!(decoded instanceof Codec.ScramFinalResponse)) {
            throw new ProtocolException("unexpected response for scram final: " + typeName(decoded));
        }
        Codec.ScramFinalResponse finalResp = (Codec.ScramFinalResponse) decoded;
        check(finalResp.errorCode, "scram final");
        if (!Scram.signaturesEqual(finalResp.serverSignature, proof.serverSignature)) {
            throw new ProtocolException("scram server signature mismatch");
        }
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
     * Grow {@code topic} to {@code totalCount} partitions (native opcode 46).
     *
     * <p>{@code totalCount} must exceed the current count. Returns the new
     * total. Non-zero {@code error_code} is {@link BrokerException}. This is
     * not Kafka CreatePartitions (API key 37).
     */
    public int createPartitions(String topic, int totalCount) {
        byte[] payload = Codec.encodeCreatePartitionsRequest(
                new Codec.CreatePartitionsRequest(topic, totalCount));
        Object decoded = roundTrip(Codec.OP_CREATE_PARTITIONS, payload);
        if (!(decoded instanceof Codec.CreatePartitionsResponse)) {
            throw new ProtocolException(
                    "unexpected response for create_partitions: " + typeName(decoded));
        }
        Codec.CreatePartitionsResponse resp = (Codec.CreatePartitionsResponse) decoded;
        check(resp.errorCode, "create_partitions");
        return (int) resp.partitions;
    }

    /**
     * Produce one message (null key when {@code key} is null) with acks=1.
     * Default trailer is {@code (0, 0, -1)}. After {@link #setEnableIdempotence}
     * the first produce sends InitProducerId (empty transactional_id) and later
     * produces attach pid/epoch/seq.
     *
     * @return the broker-assigned base offset
     */
    public long produce(String topic, int partition, byte[] key, byte[] value) {
        if (value == null) {
            value = new byte[0];
        }
        int reinitBudget = enableIdempotence ? 1 : 0;
        while (true) {
            byte[] payload = encodeProduce(topic, partition, key, value);
            int maxAttempts = 1 + maxRedirects;
            int attempt = 0;
            boolean retriedUnknown = false;
            while (true) {
                attempt++;
                Object decoded;
                try {
                    decoded = roundTrip(Codec.OP_PRODUCE, payload);
                } catch (BrokerException e) {
                    if (e.code == UNKNOWN_PRODUCER_ID && reinitBudget > 0) {
                        reinitBudget--;
                        resetProducerId();
                        retriedUnknown = true;
                        break;
                    }
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
                if (resp.errorCode == UNKNOWN_PRODUCER_ID && reinitBudget > 0) {
                    reinitBudget--;
                    resetProducerId();
                    retriedUnknown = true;
                    break;
                }
                if (resp.errorCode == NOT_LEADER_FOR_PARTITION
                        && attempt < maxAttempts
                        && redirectToLeader(resp.topic, (int) resp.partition)) {
                    continue;
                }
                check(resp.errorCode, "produce");
                int seqPart = partition < 0 ? (int) resp.partition : partition;
                noteProduceSuccess(topic, seqPart, 1);
                return resp.baseOffset;
            }
            if (!retriedUnknown) {
                throw new ProtocolException("produce loop exited");
            }
        }
    }

    private byte[] encodeProduce(String topic, int partition, byte[] key, byte[] value) {
        long[] trailer = produceTrailer(topic, partition);
        return Codec.encodeProduceRequest(
                new Codec.ProduceRequest(
                        topic,
                        partition,
                        1,
                        Collections.singletonList(new Codec.ProduceMessage(key, value, -1L, Collections.emptyList())),
                        trailer[0],
                        (int) trailer[1],
                        (int) trailer[2]));
    }

    private long[] produceTrailer(String topic, int partition) {
        if (!enableIdempotence) {
            return new long[] {0L, 0L, -1L};
        }
        ensureProducerId();
        Integer seq = nextSeq.get(seqKey(topic, partition));
        return new long[] {producerId, producerEpoch, seq == null ? 0 : seq};
    }

    private void noteProduceSuccess(String topic, int partition, int count) {
        if (!enableIdempotence) {
            return;
        }
        String key = seqKey(topic, partition);
        Integer cur = nextSeq.get(key);
        nextSeq.put(key, (cur == null ? 0 : cur) + count);
    }

    private void resetProducerId() {
        producerReady = false;
        producerId = 0L;
        producerEpoch = 0;
        nextSeq.clear();
    }

    private void ensureProducerId() {
        if (producerReady) {
            return;
        }
        byte[] payload = Codec.encodeInitProducerIdRequest(new Codec.InitProducerIdRequest(""));
        Object decoded = roundTrip(Codec.OP_INIT_PRODUCER_ID, payload);
        if (!(decoded instanceof Codec.InitProducerIdResponse)) {
            throw new ProtocolException("unexpected response for init_producer_id: " + typeName(decoded));
        }
        Codec.InitProducerIdResponse resp = (Codec.InitProducerIdResponse) decoded;
        check(resp.errorCode, "init_producer_id");
        producerId = resp.producerId;
        producerEpoch = resp.epoch;
        producerReady = true;
    }

    private static String seqKey(String topic, int partition) {
        return topic + "\0" + partition;
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
     * Delete records before {@code beforeOffset} (native opcode 44).
     * Sends {@code wait_majority=0} (broker default). Error 13 is not
     * redirected (Produce/Fetch only). This is not Kafka DeleteRecords.
     */

    /**
     * Describe topic configuration (native opcode 40/41).
     *
     * <p>Topic configs only (not Kafka DescribeConfigs / BROKER). Empty values
     * mean the key is unset. Non-zero {@code error_code} is
     * {@link BrokerException} with {@code op="describe_configs"}.
     */

    /**
     * Delete every committed offset for {@code group} (native opcode 38;
     * empty entry list on the wire).
     */
    public int deleteOffsets(String group) {
        return deleteOffsets(group, Collections.emptyList());
    }


    /**
     * Delete committed offsets for {@code group} (native opcode 38).
     *
     * <p>{@code null} or empty {@code entries} deletes all offsets for the
     * group (wire count 0). Returns the number of offset files removed.
     * Non-zero {@code error_code} is {@link BrokerException}. This is not
     * Kafka OffsetDelete.
     */
    public int deleteOffsets(String group, List<Codec.OffsetEntry> entries) {
        byte[] payload = Codec.encodeDeleteOffsetsRequest(new Codec.DeleteOffsetsRequest(group, entries));
        Object decoded = roundTrip(Codec.OP_DELETE_OFFSETS, payload);
        if (!(decoded instanceof Codec.DeleteOffsetsResponse)) {
            throw new ProtocolException("unexpected response for delete_offsets: " + typeName(decoded));
        }
        Codec.DeleteOffsetsResponse resp = (Codec.DeleteOffsetsResponse) decoded;
        check(resp.errorCode, "delete_offsets");
        return resp.deletedCount;
    }

    public DescribeConfigsResult describeConfigs(String topic) {
        byte[] payload = Codec.encodeDescribeConfigsRequest(new Codec.DescribeConfigsRequest(topic));
        Object decoded = roundTrip(Codec.OP_DESCRIBE_CONFIGS, payload);
        if (!(decoded instanceof Codec.DescribeConfigsResponse)) {
            throw new ProtocolException("unexpected response for describe_configs: " + typeName(decoded));
        }
        Codec.DescribeConfigsResponse resp = (Codec.DescribeConfigsResponse) decoded;
        check(resp.errorCode, "describe_configs");
        return new DescribeConfigsResult(resp.topic, resp.topicId, resp.partitionCount, resp.configs);
    }


    /**
     * Alter topic configuration (native opcode 42/43).
     *
     * <p>Empty value clears that key (same as Rust). Topic configs only.
     * Non-zero {@code error_code} is {@link BrokerException} with
     * {@code op="alter_configs"}.
     */
    public void alterConfigs(String topic, List<String[]> configs) {
        byte[] payload = Codec.encodeAlterConfigsRequest(new Codec.AlterConfigsRequest(topic, configs));
        Object decoded = roundTrip(Codec.OP_ALTER_CONFIGS, payload);
        if (!(decoded instanceof Codec.AlterConfigsResponse)) {
            throw new ProtocolException("unexpected response for alter_configs: " + typeName(decoded));
        }
        Codec.AlterConfigsResponse resp = (Codec.AlterConfigsResponse) decoded;
        check(resp.errorCode, "alter_configs");
    }

    public DeleteRecordsResult deleteRecords(String topic, int partition, long beforeOffset) {
        return deleteRecords(topic, partition, beforeOffset, 0);
    }

    /**
     * Delete records with the Phase 137 majority-wait trailer.
     *
     * <p>{@code waitMajority}: 0 = broker default, 1 = force wait, 2 = force
     * no-wait. Always written on the wire. Non-zero {@code error_code} is
     * {@link BrokerException} with {@code op="delete_records"}.
     */
    public DeleteRecordsResult deleteRecords(String topic, int partition, long beforeOffset, int waitMajority) {
        byte[] payload = Codec.encodeDeleteRecordsRequest(
                new Codec.DeleteRecordsRequest(topic, partition & 0xFFFFFFFFL, beforeOffset, waitMajority));
        Object decoded = roundTrip(Codec.OP_DELETE_RECORDS, payload);
        if (!(decoded instanceof Codec.DeleteRecordsResponse)) {
            throw new ProtocolException("unexpected response for delete_records: " + typeName(decoded));
        }
        Codec.DeleteRecordsResponse resp = (Codec.DeleteRecordsResponse) decoded;
        check(resp.errorCode, "delete_records");
        return new DeleteRecordsResult(resp.topic, (int) resp.partition, resp.lowWatermark);
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

    /**
     * Describe a live consumer group (native opcode 34/35).
     * Error 2 (NotFound, no live members) is a {@link BrokerException}.
     */
    public DescribeGroupResult describeGroup(String group) {
        byte[] payload = Codec.encodeDescribeGroupRequest(new Codec.DescribeGroupRequest(group));
        Object decoded = roundTrip(Codec.OP_DESCRIBE_GROUP, payload);
        if (!(decoded instanceof Codec.DescribeGroupResponse)) {
            throw new ProtocolException("unexpected response for describe_group: " + typeName(decoded));
        }
        Codec.DescribeGroupResponse resp = (Codec.DescribeGroupResponse) decoded;
        check(resp.errorCode, "describe_group");
        return new DescribeGroupResult(resp.groupId, resp.generation, resp.members);
    }

    /** List known consumer groups (native opcode 36/37). */
    public List<Codec.GroupListing> listGroups() {
        Object decoded = roundTrip(Codec.OP_LIST_GROUPS, Codec.encodeListGroupsRequest());
        if (!(decoded instanceof Codec.ListGroupsResponse)) {
            throw new ProtocolException("unexpected response for list_groups: " + typeName(decoded));
        }
        Codec.ListGroupsResponse resp = (Codec.ListGroupsResponse) decoded;
        check(resp.errorCode, "list_groups");
        return resp.groups;
    }

    /**
     * Create or replace a SCRAM user (native opcode 64/65) with broker-default
     * iterations (0 → 4096). Password is sent in the clear (use TLS).
     */
    public void createScramUser(String username, String password) {
        createScramUser(username, password, 0);
    }

    /**
     * Create or replace a SCRAM user (native opcode 64/65). {@code iterations}
     * 0 means the broker default (4096). This is not the v0.46 handshake
     * (60–63).
     */
    public void createScramUser(String username, String password, int iterations) {
        byte[] payload = Codec.encodeCreateScramUserRequest(
                new Codec.CreateScramUserRequest(username, password, iterations));
        Object decoded = roundTrip(Codec.OP_CREATE_SCRAM_USER, payload);
        if (!(decoded instanceof Codec.CreateScramUserResponse)) {
            throw new ProtocolException("unexpected response for create_scram_user: " + typeName(decoded));
        }
        Codec.CreateScramUserResponse resp = (Codec.CreateScramUserResponse) decoded;
        check(resp.errorCode, "create_scram_user");
    }

    /** Delete a SCRAM user (native opcode 66/67). */
    public void deleteScramUser(String username) {
        byte[] payload = Codec.encodeDeleteScramUserRequest(new Codec.DeleteScramUserRequest(username));
        Object decoded = roundTrip(Codec.OP_DELETE_SCRAM_USER, payload);
        if (!(decoded instanceof Codec.DeleteScramUserResponse)) {
            throw new ProtocolException("unexpected response for delete_scram_user: " + typeName(decoded));
        }
        Codec.DeleteScramUserResponse resp = (Codec.DeleteScramUserResponse) decoded;
        check(resp.errorCode, "delete_scram_user");
    }

    /** List SCRAM usernames (native opcode 68/69). */
    public List<String> listScramUsers() {
        Object decoded = roundTrip(Codec.OP_LIST_SCRAM_USERS, Codec.encodeListScramUsersRequest());
        if (!(decoded instanceof Codec.ListScramUsersResponse)) {
            throw new ProtocolException("unexpected response for list_scram_users: " + typeName(decoded));
        }
        Codec.ListScramUsersResponse resp = (Codec.ListScramUsersResponse) decoded;
        check(resp.errorCode, "list_scram_users");
        return resp.usernames;
    }

    /**
     * Create ACL bindings (native opcode 54/55). This is not Kafka CreateAcls
     * (API key 30).
     */
    public void createAcls(List<AclBinding> entries) {
        byte[] payload = Codec.encodeCreateAclsRequest(new Codec.CreateAclsRequest(entries));
        Object decoded = roundTrip(Codec.OP_CREATE_ACLS, payload);
        if (!(decoded instanceof Codec.CreateAclsResponse)) {
            throw new ProtocolException("unexpected response for create_acls: " + typeName(decoded));
        }
        Codec.CreateAclsResponse resp = (Codec.CreateAclsResponse) decoded;
        check(resp.errorCode, "create_acls");
    }

    /**
     * Delete exact-matching ACL bindings (native opcode 56/57). Returns the
     * number of entries removed. No filter-delete.
     */
    public int deleteAcls(List<AclBinding> entries) {
        byte[] payload = Codec.encodeDeleteAclsRequest(new Codec.DeleteAclsRequest(entries));
        Object decoded = roundTrip(Codec.OP_DELETE_ACLS, payload);
        if (!(decoded instanceof Codec.DeleteAclsResponse)) {
            throw new ProtocolException("unexpected response for delete_acls: " + typeName(decoded));
        }
        Codec.DeleteAclsResponse resp = (Codec.DeleteAclsResponse) decoded;
        check(resp.errorCode, "delete_acls");
        return resp.removed;
    }

    /** List all ACL bindings (empty filters: any principal / type / resource). */
    public List<AclBinding> listAcls() {
        return listAcls("", 255, "");
    }

    /**
     * List ACL bindings with optional filters (native opcode 58/59). Empty
     * {@code principal} / {@code resource} = any. {@code resourceType} 255 =
     * any type.
     */
    public List<AclBinding> listAcls(String principal, int resourceType, String resource) {
        byte[] payload = Codec.encodeListAclsRequest(
                new Codec.ListAclsRequest(principal, resourceType, resource));
        Object decoded = roundTrip(Codec.OP_LIST_ACLS, payload);
        if (!(decoded instanceof Codec.ListAclsResponse)) {
            throw new ProtocolException("unexpected response for list_acls: " + typeName(decoded));
        }
        Codec.ListAclsResponse resp = (Codec.ListAclsResponse) decoded;
        check(resp.errorCode, "list_acls");
        return resp.entries;
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
