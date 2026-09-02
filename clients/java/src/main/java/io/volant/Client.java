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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 *   int gen = c.reassignPartitions("t", new int[] {1, 2}); // all partitions
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
    /** Native {@code ErrorCode::NotController} (controller-gated admin). */
    static final int NOT_CONTROLLER = 14;
    private static final Pattern CONTROLLER_ID = Pattern.compile("controller_id=(\\d+)");
    /** Native {@code ErrorCode::UnknownProducerId}. */
    static final int UNKNOWN_PRODUCER_ID = 21;
    /** Native {@code ErrorCode::InvalidTxnState}. */
    static final int INVALID_TXN_STATE = 22;

    private String addr;
    private Socket socket;
    private final int timeoutMs;
    private final boolean tls;
    private final TlsOptions tlsOptions;
    private final String authToken;
    private final String scramUsername;
    private final String scramPassword;
    private int maxRedirects = 1;
    private int maxRetries = 0;
    private long retryBackoffMs = 50;
    /** Test-only: next N fetch RPCs throw a transient transport error. */
    int injectFetchTransportFails;
    /** Test-only: next N heartbeat RPCs throw a transient transport error. */
    int injectHeartbeatTransportFails;
    private boolean enableIdempotence = false;
    private String transactionalId = null;
    private long producerId = 0L;
    private int producerEpoch = 0;
    private boolean producerReady = false;
    private boolean inTransaction = false;
    private final Map<String, Integer> nextSeq = new HashMap<>();
    private final Map<String, Integer> seqAtBegin = new HashMap<>();
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
     * Extra Produce/Fetch/DeleteRecords attempts after
     * {@code NotLeaderForPartition} (13). Default is 1 (one initial send +
     * one redirect). {@code 0} disables redirect and raises on the first
     * 13. Negative values are treated as 0.
     */
    public void setMaxRedirects(int n) {
        this.maxRedirects = Math.max(0, n);
    }

    public int maxRedirects() {
        return maxRedirects;
    }

    /**
     * Extra Produce/Fetch/Heartbeat attempts after the first on transient
     * broker/transport errors. Default is 0. Error 13 stays on
     * {@link #setMaxRedirects}; error 21 stays on the one re-Init.
     * Heartbeat rebalance codes 9 / 10 / 11 are not retried.
     */
    public void setMaxRetries(int n) {
        this.maxRetries = Math.max(0, n);
    }

    public int maxRetries() {
        return maxRetries;
    }

    /** Sleep between produce/fetch retries, milliseconds. Default 50. Zero allowed. */
    public void setRetryBackoffMs(long ms) {
        this.retryBackoffMs = Math.max(0, ms);
    }

    public long retryBackoffMs() {
        return retryBackoffMs;
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

    /**
     * Native transactional_id used on InitProducerId (opcode 32) and
     * required by {@link #beginTransaction}. Null or empty means
     * non-transactional (v0.47 empty-id idempotence still works).
     */
    public void setTransactionalId(String id) {
        this.transactionalId = (id == null || id.isEmpty()) ? null : id;
    }

    public String transactionalId() {
        return transactionalId;
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

    private static Long parseControllerId(String message) {
        if (message == null || message.isEmpty()) {
            return null;
        }
        Matcher m = CONTROLLER_ID.matcher(message);
        if (!m.find()) {
            return null;
        }
        try {
            return Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private boolean maybeRedirectController(int code, String message, int attempt, int maxAttempts) {
        if (code != NOT_CONTROLLER || attempt >= maxAttempts) {
            return false;
        }
        return redirectToController(parseControllerId(message));
    }

    /**
     * Metadata → reconnect to the controller.
     *
     * <p>If {@code controllerId} is known (parsed from {@code controller_id=N}
     * in a 14 Error message, or Metadata's v0.77 trailer when non-zero), look
     * that node up in Metadata brokers, then {@link #listMembers()} if
     * Metadata has no matching id. Otherwise pick the first advertised broker
     * whose host:port is not this connection.
     *
     * @return true when the caller should retry; false on no other broker /
     *     lookup miss / empty host / reconnect fail (raise the original 14).
     */
    private boolean redirectToController(Long controllerId) {
        Metadata meta = metadata();
        if (controllerId == null && meta.controllerId != 0) {
            controllerId = meta.controllerId;
        }
        String host = null;
        int port = 0;
        if (controllerId != null) {
            for (Metadata.BrokerInfo b : meta.brokers) {
                if (b.nodeId == controllerId) {
                    host = b.host;
                    port = b.port;
                    break;
                }
            }
            if (host == null) {
                try {
                    MembershipList members = listMembers();
                    for (MembershipBroker b : members.brokers) {
                        if ((b.id & 0xFFFFFFFFL) == controllerId) {
                            host = b.host;
                            port = b.port;
                            break;
                        }
                    }
                } catch (RuntimeException ignored) {
                    return false;
                }
            }
            if (host == null || host.isEmpty()) {
                return false;
            }
        } else {
            for (Metadata.BrokerInfo b : meta.brokers) {
                if (b.host == null || b.host.isEmpty()) {
                    continue;
                }
                String next = formatAddr(b.host, b.port);
                if (!next.equals(addr)) {
                    host = b.host;
                    port = b.port;
                    break;
                }
            }
            if (host == null) {
                return false;
            }
        }
        String next = formatAddr(host, port);
        if (next.equals(addr)) {
            return true;
        }
        try {
            reconnect(host, port);
        } catch (RuntimeException e) {
            return false;
        }
        return true;
    }

    /**
     * Controller-gated admin RPC. Error 14 follows {@code maxRedirects}
     * (not counted as a transient retry). Transient 6 / 7 / 15 / 16 and
     * TCP/IO retry up to {@code maxRetries} extra times (default 0).
     */
    private Object adminRoundTrip(int opcode, byte[] payload, Class<?> expect, String op) {
        int maxAttempts = 1 + maxRedirects;
        int attempt = 0;
        int retryAttempt = 0;
        while (true) {
            attempt++;
            Object decoded;
            try {
                decoded = roundTrip(opcode, payload);
            } catch (BrokerException e) {
                if (maybeRedirectController(e.code, e.message, attempt, maxAttempts)) {
                    continue;
                }
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    attempt--;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    attempt--;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!expect.isInstance(decoded)) {
                throw new ProtocolException("unexpected response for " + op + ": " + typeName(decoded));
            }
            int code = typedAdminErrorCode(decoded);
            if (maybeRedirectController(code, "", attempt, maxAttempts)) {
                continue;
            }
            if (isTransientBroker(code) && retryAttempt < maxRetries) {
                retryAttempt++;
                attempt--;
                sleepProduceRetry();
                continue;
            }
            check(code, op);
            return decoded;
        }
    }

    private static int typedAdminErrorCode(Object decoded) {
        if (decoded instanceof Codec.CreateTopicResponse) {
            return ((Codec.CreateTopicResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.DeleteTopicResponse) {
            return ((Codec.DeleteTopicResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.CreatePartitionsResponse) {
            return ((Codec.CreatePartitionsResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.ReassignPartitionsResponse) {
            return ((Codec.ReassignPartitionsResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.CreateAclsResponse) {
            return ((Codec.CreateAclsResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.DeleteAclsResponse) {
            return ((Codec.DeleteAclsResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.CreateScramUserResponse) {
            return ((Codec.CreateScramUserResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.DeleteScramUserResponse) {
            return ((Codec.DeleteScramUserResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.ListScramUsersResponse) {
            return ((Codec.ListScramUsersResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.ListAclsResponse) {
            return ((Codec.ListAclsResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.AddBrokerResponse) {
            return ((Codec.AddBrokerResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.RemoveBrokerResponse) {
            return ((Codec.RemoveBrokerResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.DescribeConfigsResponse) {
            return ((Codec.DescribeConfigsResponse) decoded).errorCode;
        }
        if (decoded instanceof Codec.AlterConfigsResponse) {
            return ((Codec.AlterConfigsResponse) decoded).errorCode;
        }
        return 0;
    }

    /** Create a topic. Returns the broker-assigned topic id.
     * Error 14 (NotController) follows {@code maxRedirects}.
     * Transient 6 / 7 / 15 / 16 and TCP/IO follow {@code maxRetries}
     * (default 0); 14 is not a retry. */
    public int createTopic(String name, int partitions) {
        byte[] payload = Codec.encodeCreateTopicRequest(
                new Codec.CreateTopicRequest(name, partitions, Collections.emptyList()));
        Codec.CreateTopicResponse resp = (Codec.CreateTopicResponse) adminRoundTrip(
                Codec.OP_CREATE_TOPIC, payload, Codec.CreateTopicResponse.class, "create_topic");
        return (int) resp.topicId;
    }

    /** Delete a topic by name. Error 14 follows {@code maxRedirects}. */
    public void deleteTopic(String name) {
        byte[] payload = Codec.encodeDeleteTopicRequest(new Codec.DeleteTopicRequest(name));
        adminRoundTrip(Codec.OP_DELETE_TOPIC, payload, Codec.DeleteTopicResponse.class, "delete_topic");
    }

    /**
     * Grow {@code topic} to {@code totalCount} partitions (native opcode 46).
     *
     * <p>{@code totalCount} must exceed the current count. Returns the new
     * total. Non-zero {@code error_code} is {@link BrokerException}. Error 14
     * follows {@code maxRedirects}. This is not Kafka CreatePartitions (API
     * key 37).
     */
    public int createPartitions(String topic, int totalCount) {
        byte[] payload = Codec.encodeCreatePartitionsRequest(
                new Codec.CreatePartitionsRequest(topic, totalCount));
        Codec.CreatePartitionsResponse resp = (Codec.CreatePartitionsResponse) adminRoundTrip(
                Codec.OP_CREATE_PARTITIONS,
                payload,
                Codec.CreatePartitionsResponse.class,
                "create_partitions");
        return (int) resp.partitions;
    }

    /**
     * Reassign replicas for every partition of {@code topic} (native opcode 114).
     *
     * <p>Empty {@code replicas} asks the controller to auto-place with current
     * membership. Returns the assignment generation. This is not Kafka
     * AlterPartitionReassignments (API key 45).
     */

    /**
     * Add a broker endpoint to the membership overlay (native 102/103) with no
     * rack. Returns the overlay generation.
     */
    public long addBroker(int id, String host, int port) {
        return addBroker(id, host, port, null);
    }


    /**
     * Add a broker endpoint to the membership overlay (native 102/103).
     * {@code rack == null} is absent on the wire (flag 0). Returns the overlay
     * generation. Overlay is still SoT; this is not Kafka broker catalog.
     * Error 14 follows {@code maxRedirects} when the broker cannot forward.
     */
    public long addBroker(int id, String host, int port, String rack) {
        byte[] payload = Codec.encodeAddBrokerRequest(new Codec.AddBrokerRequest(id, host, port, rack));
        Codec.AddBrokerResponse resp = (Codec.AddBrokerResponse) adminRoundTrip(
                Codec.OP_ADD_BROKER, payload, Codec.AddBrokerResponse.class, "add_broker");
        return resp.generation;
    }


    /**
     * Remove a broker from the membership overlay (native 104/105). Returns
     * the overlay generation. Error 14 follows {@code maxRedirects} when the
     * broker cannot forward.
     */
    public long removeBroker(int id) {
        byte[] payload = Codec.encodeRemoveBrokerRequest(new Codec.RemoveBrokerRequest(id));
        Codec.RemoveBrokerResponse resp = (Codec.RemoveBrokerResponse) adminRoundTrip(
                Codec.OP_REMOVE_BROKER, payload, Codec.RemoveBrokerResponse.class, "remove_broker");
        return resp.generation;
    }


    /**
     * List configured + live membership (native opcode 106/107). Overlay is
     * still SoT. Transient broker/transport errors retry up to {@code
     * maxRetries} extra times (default 0). Error 2 / 9 / 10 / 11 / 13 / 14
     * are not retried.
     */
    public MembershipList listMembers() {
        byte[] payload = Codec.encodeListMembersRequest();
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_LIST_MEMBERS, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.ListMembersResponse)) {
                throw new ProtocolException("unexpected response for list_members: " + typeName(decoded));
            }
            Codec.ListMembersResponse resp = (Codec.ListMembersResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "list_members");
            return new MembershipList(resp.generation, resp.brokers, resp.live);
        }
    }

    public int reassignPartitions(String topic, int... replicas) {
        return reassignPartitions(topic, (Integer) null, replicas);
    }

    /**
     * Reassign replicas for {@code topic} (native opcode 114).
     *
     * <p>{@code partition == null} updates every partition (wire {@code
     * u32::MAX}). Empty {@code replicas} asks the controller to auto-place
     * with the current membership. Returns the assignment generation.
     * Non-zero {@code error_code} is {@link BrokerException}. Error 14
     * follows {@code maxRedirects}. This is not Kafka
     * AlterPartitionReassignments (API key 45).
     */
    public int reassignPartitions(String topic, Integer partition, int... replicas) {
        long wirePartition = partition == null
                ? Codec.REASSIGN_ALL_PARTITIONS
                : (partition.longValue() & 0xFFFFFFFFL);
        List<Long> ids = new ArrayList<>();
        if (replicas != null) {
            for (int id : replicas) {
                ids.add(id & 0xFFFFFFFFL);
            }
        }
        byte[] payload = Codec.encodeReassignPartitionsRequest(
                new Codec.ReassignPartitionsRequest(topic, wirePartition, ids));
        Codec.ReassignPartitionsResponse resp = (Codec.ReassignPartitionsResponse) adminRoundTrip(
                Codec.OP_REASSIGN_PARTITIONS,
                payload,
                Codec.ReassignPartitionsResponse.class,
                "reassign_partitions");
        return (int) resp.generation;
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
        return produce(topic, partition, key, value, 1);
    }

    /**
     * Produce with an explicit acks byte. {@code 1} = leader only;
     * {@code 255} = acks=all (ISR). Same as the Rust client / Python {@code acks=}.
     */
    public long produce(String topic, int partition, byte[] key, byte[] value, int acks) {
        if (value == null) {
            value = new byte[0];
        }
        return produce(
                topic,
                partition,
                Collections.singletonList(new Codec.ProduceMessage(key, value, -1L, Collections.emptyList())),
                acks);
    }

    /**
     * Produce {@code messages} in one Produce RPC. {@code acks}: 1 = leader,
     * 255 = all. Empty / null batch is an error (no opcode 1).
     *
     * @return the broker-assigned base offset of the batch
     */
    public long produce(String topic, int partition, List<Codec.ProduceMessage> messages, int acks) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("produce batch is empty");
        }
        int reinitBudget = usesPid() ? 1 : 0;
        int retryAttempt = 0;
        while (true) {
            byte[] payload = encodeProduce(topic, partition, messages, acks);
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
                    if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                        retryAttempt++;
                        attempt--;
                        sleepProduceRetry();
                        continue;
                    }
                    throw e;
                } catch (RuntimeException e) {
                    if (isTransientTransport(e) && retryAttempt < maxRetries) {
                        retryAttempt++;
                        attempt--;
                        sleepProduceRetry();
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
                if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    attempt--;
                    sleepProduceRetry();
                    continue;
                }
                check(resp.errorCode, "produce");
                int seqPart = partition < 0 ? (int) resp.partition : partition;
                noteProduceSuccess(topic, seqPart, messages.size());
                return resp.baseOffset;
            }
            if (!retriedUnknown) {
                throw new ProtocolException("produce loop exited");
            }
        }
    }

    static boolean isTransientBroker(int code) {
        return code == 6 || code == 7 || code == 15 || code == 16;
    }

    static boolean isTransientTransport(Throwable e) {
        if (e instanceof BrokerException || e instanceof ProtocolException) {
            return false;
        }
        return e instanceof java.io.UncheckedIOException
                || e instanceof RuntimeException && e.getCause() instanceof java.io.IOException;
    }

    private void sleepProduceRetry() {
        if (retryBackoffMs <= 0) {
            return;
        }
        try {
            Thread.sleep(retryBackoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private byte[] encodeProduce(String topic, int partition, List<Codec.ProduceMessage> messages, int acks) {
        long[] trailer = produceTrailer(topic, partition);
        return Codec.encodeProduceRequest(
                new Codec.ProduceRequest(
                        topic,
                        partition,
                        acks,
                        messages,
                        trailer[0],
                        (int) trailer[1],
                        (int) trailer[2]));
    }

    private boolean usesPid() {
        return enableIdempotence || (transactionalId != null && !transactionalId.isEmpty());
    }

    private long[] produceTrailer(String topic, int partition) {
        if (!usesPid()) {
            return new long[] {0L, 0L, -1L};
        }
        ensureProducerId();
        Integer seq = nextSeq.get(seqKey(topic, partition));
        return new long[] {producerId, producerEpoch, seq == null ? 0 : seq};
    }

    private void noteProduceSuccess(String topic, int partition, int count) {
        if (!usesPid()) {
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
        inTransaction = false;
        nextSeq.clear();
        seqAtBegin.clear();
    }

    private void ensureProducerId() {
        if (producerReady) {
            return;
        }
        String txn = transactionalId == null ? "" : transactionalId;
        byte[] payload = Codec.encodeInitProducerIdRequest(new Codec.InitProducerIdRequest(txn));
        Object decoded = roundTrip(Codec.OP_INIT_PRODUCER_ID, payload);
        if (!(decoded instanceof Codec.InitProducerIdResponse)) {
            throw new ProtocolException("unexpected response for init_producer_id: " + typeName(decoded));
        }
        Codec.InitProducerIdResponse resp = (Codec.InitProducerIdResponse) decoded;
        check(resp.errorCode, "init_producer_id");
        producerId = resp.producerId;
        producerEpoch = resp.epoch;
        producerReady = true;
        inTransaction = false;
        nextSeq.clear();
    }

    /**
     * Open a native transaction (opcode 50). Requires {@link #setTransactionalId}.
     * Transient broker/transport errors retry up to {@code maxRetries} extra
     * times (default 0). InvalidTxnState (22) and txn fence / epoch errors
     * are not retried.
     */
    public void beginTransaction() {
        if (transactionalId == null || transactionalId.isEmpty()) {
            throw new IllegalStateException("transactional_id not configured");
        }
        ensureProducerId();
        byte[] payload = Codec.encodeBeginTxnRequest(new Codec.BeginTxnRequest(producerId, producerEpoch));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_BEGIN_TXN, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.BeginTxnResponse)) {
                throw new ProtocolException("unexpected response for begin_txn: " + typeName(decoded));
            }
            Codec.BeginTxnResponse resp = (Codec.BeginTxnResponse) decoded;
            if (resp.errorCode == INVALID_TXN_STATE) {
                check(resp.errorCode, "begin_txn");
            }
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "begin_txn");
            seqAtBegin.clear();
            seqAtBegin.putAll(nextSeq);
            inTransaction = true;
            return;
        }
    }

    /** Commit the open transaction (opcode 52, committed=1). */
    public List<TxnProduceResult> commitTransaction() {
        return commitTransaction(Collections.emptyList());
    }

    /** Commit the open transaction with optional deferred offset commits. */
    public List<TxnProduceResult> commitTransaction(List<TxnOffsetCommit> offsets) {
        return endTransaction(true, offsets);
    }

    /** Abort the open transaction (opcode 52, committed=0) and rewind sequences. */
    public void abortTransaction() {
        endTransaction(false, Collections.emptyList());
    }

    private List<TxnProduceResult> endTransaction(boolean committed, List<TxnOffsetCommit> offsets) {
        if (!producerReady) {
            throw new IllegalStateException("producer id not initialized");
        }
        byte[] payload = Codec.encodeEndTxnRequest(
                new Codec.EndTxnRequest(producerId, producerEpoch, committed, offsets));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_END_TXN, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.EndTxnResponse)) {
                throw new ProtocolException("unexpected response for end_txn: " + typeName(decoded));
            }
            Codec.EndTxnResponse resp = (Codec.EndTxnResponse) decoded;
            if (resp.errorCode == INVALID_TXN_STATE) {
                check(resp.errorCode, "end_txn");
            }
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "end_txn");
            inTransaction = false;
            if (!committed) {
                nextSeq.clear();
                nextSeq.putAll(seqAtBegin);
            }
            seqAtBegin.clear();
            return resp.results;
        }
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
        return fetch(topic, partition, offset, 128, 4L * 1024 * 1024, 0);
    }

    /**
     * Fetch with explicit {@code max_messages}, {@code max_bytes}, and
     * {@code max_wait_ms}.
     */
    public List<Record> fetch(
            String topic, int partition, long offset, int maxMessages, long maxBytes, long maxWaitMs) {
        return fetchAt(topic, partition, offset, maxMessages, maxBytes, maxWaitMs);
    }

    List<Record> fetch(String topic, int partition, long offset, int maxMessages, long maxWaitMs) {
        return fetch(topic, partition, offset, maxMessages, 4L * 1024 * 1024, maxWaitMs);
    }

    private List<Record> fetchAt(
            String topic, int partition, long offset, int maxMessages, long maxBytes, long maxWaitMs) {
        byte[] payload = Codec.encodeFetchRequest(
                new Codec.FetchRequest(topic, partition, offset, maxMessages, maxBytes, maxWaitMs));
        int retryAttempt = 0;
        while (true) {
            int maxAttempts = 1 + maxRedirects;
            int attempt = 0;
            boolean retried = false;
            while (true) {
                attempt++;
                Object decoded;
                try {
                    if (injectFetchTransportFails > 0) {
                        injectFetchTransportFails--;
                        throw new RuntimeException(new java.io.IOException("injected transport"));
                    }
                    decoded = roundTrip(Codec.OP_FETCH, payload);
                } catch (BrokerException e) {
                    if (e.code == NOT_LEADER_FOR_PARTITION
                            && attempt < maxAttempts
                            && redirectToLeader(topic, partition)) {
                        continue;
                    }
                    if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                        retryAttempt++;
                        sleepProduceRetry();
                        retried = true;
                        break;
                    }
                    throw e;
                } catch (RuntimeException e) {
                    if (isTransientTransport(e) && retryAttempt < maxRetries) {
                        retryAttempt++;
                        sleepProduceRetry();
                        retried = true;
                        break;
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
                if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    retried = true;
                    break;
                }
                check(resp.errorCode, "fetch");
                return resp.records;
            }
            if (!retried) {
                throw new ProtocolException("fetch loop exited");
            }
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
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_OFFSET_COMMIT, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.OffsetCommitResponse)) {
                throw new ProtocolException("unexpected response for offset_commit: " + typeName(decoded));
            }
            Codec.OffsetCommitResponse resp = (Codec.OffsetCommitResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "offset_commit");
            return;
        }
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
     * Non-zero {@code error_code} is {@link BrokerException}. Transient
     * broker/transport errors retry up to {@code maxRetries} extra times
     * (default 0). This is not Kafka ListOffsets (no timestamp or isolation);
     * both ends of each log are returned.
     */
    public List<OffsetListing> listOffsets(String topic, int... partitions) {
        List<Integer> parts = new ArrayList<>();
        if (partitions != null) {
            for (int p : partitions) {
                parts.add(p);
            }
        }
        byte[] payload = Codec.encodeListOffsetsRequest(new Codec.ListOffsetsRequest(topic, parts));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_LIST_OFFSETS, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.ListOffsetsResponse)) {
                throw new ProtocolException("unexpected response for list_offsets: " + typeName(decoded));
            }
            Codec.ListOffsetsResponse resp = (Codec.ListOffsetsResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "list_offsets");
            return resp.entries;
        }
    }

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
     * Non-zero {@code error_code} is {@link BrokerException}. Error 14
     * follows {@code maxRedirects}. Transient broker/transport errors
     * retry up to {@code maxRetries} extra times (default 0). This is not
     * Kafka OffsetDelete.
     */
    public int deleteOffsets(String group, List<Codec.OffsetEntry> entries) {
        byte[] payload = Codec.encodeDeleteOffsetsRequest(new Codec.DeleteOffsetsRequest(group, entries));
        int retryAttempt = 0;
        int redirectAttempt = 0;
        int maxAttempts = 1 + maxRedirects;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_DELETE_OFFSETS, payload);
            } catch (BrokerException e) {
                if (maybeRedirectController(e.code, e.message, redirectAttempt + 1, maxAttempts)) {
                    redirectAttempt++;
                    continue;
                }
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.DeleteOffsetsResponse)) {
                throw new ProtocolException("unexpected response for delete_offsets: " + typeName(decoded));
            }
            Codec.DeleteOffsetsResponse resp = (Codec.DeleteOffsetsResponse) decoded;
            if (maybeRedirectController(resp.errorCode, null, redirectAttempt + 1, maxAttempts)) {
                redirectAttempt++;
                continue;
            }
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "delete_offsets");
            return resp.deletedCount;
        }
    }

    /** Topic configs only. Error 14 follows {@code maxRedirects}. */
    public DescribeConfigsResult describeConfigs(String topic) {
        byte[] payload = Codec.encodeDescribeConfigsRequest(new Codec.DescribeConfigsRequest(topic));
        Codec.DescribeConfigsResponse resp = (Codec.DescribeConfigsResponse) adminRoundTrip(
                Codec.OP_DESCRIBE_CONFIGS,
                payload,
                Codec.DescribeConfigsResponse.class,
                "describe_configs");
        return new DescribeConfigsResult(resp.topic, resp.topicId, resp.partitionCount, resp.configs);
    }


    /**
     * Alter topic configuration (native opcode 42/43).
     *
     * <p>Empty value clears that key (same as Rust). Topic configs only.
     * Non-zero {@code error_code} is {@link BrokerException} with
     * {@code op="alter_configs"}. Error 14 follows {@code maxRedirects}.
     */
    public void alterConfigs(String topic, List<String[]> configs) {
        byte[] payload = Codec.encodeAlterConfigsRequest(new Codec.AlterConfigsRequest(topic, configs));
        adminRoundTrip(
                Codec.OP_ALTER_CONFIGS, payload, Codec.AlterConfigsResponse.class, "alter_configs");
    }

    /**
     * Delete records before {@code beforeOffset} (native opcode 44).
     * Sends {@code wait_majority=0} (broker default). Error 13 follows
     * Produce/Fetch redirect. This is not Kafka DeleteRecords.
     */
    public DeleteRecordsResult deleteRecords(String topic, int partition, long beforeOffset) {
        return deleteRecords(topic, partition, beforeOffset, 0);
    }

    /**
     * Delete records with the Phase 137 majority-wait trailer.
     *
     * <p>{@code waitMajority}: 0 = broker default, 1 = force wait, 2 = force
     * no-wait. Always written on the wire. Non-zero {@code error_code} is
     * {@link BrokerException} with {@code op="delete_records"}. Error 13
     * follows Produce/Fetch redirect ({@code maxRedirects}).
     */
    public DeleteRecordsResult deleteRecords(String topic, int partition, long beforeOffset, int waitMajority) {
        byte[] payload = Codec.encodeDeleteRecordsRequest(
                new Codec.DeleteRecordsRequest(topic, partition & 0xFFFFFFFFL, beforeOffset, waitMajority));
        int maxAttempts = 1 + maxRedirects;
        int attempt = 0;
        while (true) {
            attempt++;
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_DELETE_RECORDS, payload);
            } catch (BrokerException e) {
                if (e.code == NOT_LEADER_FOR_PARTITION
                        && attempt < maxAttempts
                        && redirectToLeader(topic, partition)) {
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.DeleteRecordsResponse)) {
                throw new ProtocolException("unexpected response for delete_records: " + typeName(decoded));
            }
            Codec.DeleteRecordsResponse resp = (Codec.DeleteRecordsResponse) decoded;
            if (resp.errorCode == NOT_LEADER_FOR_PARTITION
                    && attempt < maxAttempts
                    && redirectToLeader(resp.topic, (int) resp.partition)) {
                continue;
            }
            check(resp.errorCode, "delete_records");
            return new DeleteRecordsResult(resp.topic, (int) resp.partition, resp.lowWatermark);
        }
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
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_OFFSET_FETCH, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.OffsetFetchResponse)) {
                throw new ProtocolException("unexpected response for offset_fetch: " + typeName(decoded));
            }
            Codec.OffsetFetchResponse resp = (Codec.OffsetFetchResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "offset_fetch");
            return resp.entries;
        }
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

    /**
     * Heartbeat for group membership. Non-zero error_code is BrokerException.
     * Transient broker/transport errors retry up to {@code maxRetries} extra
     * times (default 0). Rebalance codes 9 / 10 / 11 are not retried.
     */
    public void heartbeat(String group, String memberId, long generation) {
        byte[] payload = Codec.encodeHeartbeatRequest(new Codec.HeartbeatRequest(group, memberId, generation));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                if (injectHeartbeatTransportFails > 0) {
                    injectHeartbeatTransportFails--;
                    throw new RuntimeException(new java.io.IOException("injected transport"));
                }
                decoded = roundTrip(Codec.OP_HEARTBEAT, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.HeartbeatResponse)) {
                throw new ProtocolException("unexpected response for heartbeat: " + typeName(decoded));
            }
            Codec.HeartbeatResponse resp = (Codec.HeartbeatResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "heartbeat");
            return;
        }
    }

    /**
     * Describe a live consumer group (native opcode 34/35).
     * Error 2 (NotFound, no live members) is a {@link BrokerException}.
     * Transient broker/transport errors retry up to {@code maxRetries}
     * extra times (default 0). Error 2 / 9 / 10 / 11 / 13 / 14 are not
     * retried.
     */
    public DescribeGroupResult describeGroup(String group) {
        byte[] payload = Codec.encodeDescribeGroupRequest(new Codec.DescribeGroupRequest(group));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_DESCRIBE_GROUP, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.DescribeGroupResponse)) {
                throw new ProtocolException("unexpected response for describe_group: " + typeName(decoded));
            }
            Codec.DescribeGroupResponse resp = (Codec.DescribeGroupResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "describe_group");
            return new DescribeGroupResult(resp.groupId, resp.generation, resp.members);
        }
    }

    /**
     * List known consumer groups (native opcode 36/37). Transient
     * broker/transport errors retry up to {@code maxRetries} extra times
     * (default 0). Error 2 / 9 / 10 / 11 / 13 / 14 are not retried.
     */
    public List<Codec.GroupListing> listGroups() {
        byte[] payload = Codec.encodeListGroupsRequest();
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_LIST_GROUPS, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.ListGroupsResponse)) {
                throw new ProtocolException("unexpected response for list_groups: " + typeName(decoded));
            }
            Codec.ListGroupsResponse resp = (Codec.ListGroupsResponse) decoded;
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "list_groups");
            return resp.groups;
        }
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
     * (60–63). Error 14 follows {@code maxRedirects}.
     */
    public void createScramUser(String username, String password, int iterations) {
        byte[] payload = Codec.encodeCreateScramUserRequest(
                new Codec.CreateScramUserRequest(username, password, iterations));
        adminRoundTrip(
                Codec.OP_CREATE_SCRAM_USER, payload, Codec.CreateScramUserResponse.class, "create_scram_user");
    }

    /** Delete a SCRAM user (native opcode 66/67). Error 14 follows {@code maxRedirects}. */
    public void deleteScramUser(String username) {
        byte[] payload = Codec.encodeDeleteScramUserRequest(new Codec.DeleteScramUserRequest(username));
        adminRoundTrip(
                Codec.OP_DELETE_SCRAM_USER, payload, Codec.DeleteScramUserResponse.class, "delete_scram_user");
    }

    /** List SCRAM usernames (native opcode 68/69). Error 14 follows {@code maxRedirects}. */
    public List<String> listScramUsers() {
        Codec.ListScramUsersResponse resp = (Codec.ListScramUsersResponse) adminRoundTrip(
                Codec.OP_LIST_SCRAM_USERS,
                Codec.encodeListScramUsersRequest(),
                Codec.ListScramUsersResponse.class,
                "list_scram_users");
        return resp.usernames;
    }

    /**
     * Create ACL bindings (native opcode 54/55). This is not Kafka CreateAcls
     * (API key 30). Error 14 follows {@code maxRedirects}.
     */
    public void createAcls(List<AclBinding> entries) {
        byte[] payload = Codec.encodeCreateAclsRequest(new Codec.CreateAclsRequest(entries));
        adminRoundTrip(Codec.OP_CREATE_ACLS, payload, Codec.CreateAclsResponse.class, "create_acls");
    }

    /**
     * Delete exact-matching ACL bindings (native opcode 56/57). Returns the
     * number of entries removed. No filter-delete. Error 14 follows
     * {@code maxRedirects}.
     */
    public int deleteAcls(List<AclBinding> entries) {
        byte[] payload = Codec.encodeDeleteAclsRequest(new Codec.DeleteAclsRequest(entries));
        Codec.DeleteAclsResponse resp = (Codec.DeleteAclsResponse) adminRoundTrip(
                Codec.OP_DELETE_ACLS, payload, Codec.DeleteAclsResponse.class, "delete_acls");
        return resp.removed;
    }

    /** List all ACL bindings (empty filters: any principal / type / resource). */
    public List<AclBinding> listAcls() {
        return listAcls("", 255, "");
    }

    /**
     * List ACL bindings with optional filters (native opcode 58/59). Empty
     * {@code principal} / {@code resource} = any. {@code resourceType} 255 =
     * any type. Error 14 follows {@code maxRedirects}.
     */
    public List<AclBinding> listAcls(String principal, int resourceType, String resource) {
        byte[] payload = Codec.encodeListAclsRequest(
                new Codec.ListAclsRequest(principal, resourceType, resource));
        Codec.ListAclsResponse resp = (Codec.ListAclsResponse) adminRoundTrip(
                Codec.OP_LIST_ACLS, payload, Codec.ListAclsResponse.class, "list_acls");
        return resp.entries;
    }

    /**
     * Leave a consumer group. Transient broker/transport errors retry up
     * to {@code maxRetries} extra times (default 0). Error 10
     * (UnknownMemberId) is success (already left). Rebalance 9 /
     * IllegalGeneration 11 / 13 / 14 / not-found 2 are not retried.
     */
    public void leaveGroup(String group, String memberId) {
        byte[] payload = Codec.encodeLeaveGroupRequest(new Codec.LeaveGroupRequest(group, memberId));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_LEAVE_GROUP, payload);
            } catch (BrokerException e) {
                if (e.code == 10) {
                    return;
                }
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Codec.LeaveGroupResponse)) {
                throw new ProtocolException("unexpected response for leave_group: " + typeName(decoded));
            }
            Codec.LeaveGroupResponse resp = (Codec.LeaveGroupResponse) decoded;
            if (resp.errorCode == 10) {
                return;
            }
            if (isTransientBroker(resp.errorCode) && retryAttempt < maxRetries) {
                retryAttempt++;
                sleepProduceRetry();
                continue;
            }
            check(resp.errorCode, "leave_group");
            return;
        }
    }

    /**
     * Cluster brokers and topics (all topics). Transient broker/transport
     * errors retry up to {@code maxRetries} extra times (default 0). Native
     * Metadata has no top-level {@code error_code}; failures arrive as Error
     * opcode / transport. Error 2 / 9 / 10 / 11 / 13 / 14 are not retried.
     */
    public Metadata metadata() {
        byte[] payload = Codec.encodeMetadataRequest(new Codec.MetadataRequest(Collections.emptyList()));
        int retryAttempt = 0;
        while (true) {
            Object decoded;
            try {
                decoded = roundTrip(Codec.OP_METADATA, payload);
            } catch (BrokerException e) {
                if (isTransientBroker(e.code) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            } catch (RuntimeException e) {
                if (isTransientTransport(e) && retryAttempt < maxRetries) {
                    retryAttempt++;
                    sleepProduceRetry();
                    continue;
                }
                throw e;
            }
            if (!(decoded instanceof Metadata)) {
                throw new ProtocolException("unexpected response for metadata: " + typeName(decoded));
            }
            return (Metadata) decoded;
        }
    }

    private static String typeName(Object o) {
        return o == null ? "null" : o.getClass().getName();
    }
}
