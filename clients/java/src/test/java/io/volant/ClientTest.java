package io.volant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Leader-redirect tests against a scripted TCP broker (no live volant-server). */
class ClientTest {
    private static final int NOT_LEADER = Client.NOT_LEADER_FOR_PARTITION;
    private static final int TIMEOUT = 7;
    private static final int REBALANCE = 9;
    private static final int UNKNOWN_MEMBER = 10;
    private static final int NOT_FOUND = 2;
    private static final int UNKNOWN_PRODUCER = 21;

    @Test
    void produceRedirectsToLeader() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);
            leader.produceCodes.add(0);

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                long off = c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
                assertEquals(7L, off);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.produceCount.get());
        }
    }

    @Test
    void maxRedirectsZeroRaisesOnFirst13() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", 9);

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(NOT_LEADER, ex.code);
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void fetchRedirectsOnce() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.fetchCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<Record> recs = c.fetch("t", 0, 0);
                assertTrue(recs.isEmpty());
            }
            assertEquals(1, follower.fetchCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.fetchCount.get());
        }
    }

    @Test
    void missingLeaderRaises13() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = new Metadata(
                    Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", follower.port)),
                    Collections.singletonList(new Metadata.TopicInfo(
                            "t",
                            1,
                            0,
                            Collections.singletonList(
                                    new Metadata.PartitionInfo(0, 99, 0, Collections.emptyList(), Collections.emptyList(), 0)))));

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(NOT_LEADER, ex.code);
                assertEquals("127.0.0.1:" + follower.port, c.addr());
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void emptyHostRaises13() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = new Metadata(
                    Collections.singletonList(new Metadata.BrokerInfo(2, "", 9092)),
                    Collections.singletonList(new Metadata.TopicInfo(
                            "t",
                            1,
                            0,
                            Collections.singletonList(
                                    new Metadata.PartitionInfo(0, 2, 0, Collections.emptyList(), Collections.emptyList(), 0)))));

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                BrokerException ex = assertThrows(
                        BrokerException.class, () -> c.produce("t", 0, null, "x".getBytes(StandardCharsets.UTF_8)));
                assertEquals(NOT_LEADER, ex.code);
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void idempotentOnInitsThenSequences() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.produce("t", 0, null, "a".getBytes(StandardCharsets.UTF_8));
                c.produce("t", 0, null, "b".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1, srv.initCount.get());
            assertEquals(List.of(""), srv.initTxnIds);
            assertEquals(
                    List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_PRODUCE, Codec.OP_PRODUCE), srv.opcodes);
            assertEquals(2, srv.produceReqs.size());
            Codec.ProduceRequest first = srv.produceReqs.get(0);
            Codec.ProduceRequest second = srv.produceReqs.get(1);
            assertEquals(42L, first.producerId);
            assertEquals(1, first.producerEpoch);
            assertEquals(0, first.baseSequence);
            assertEquals(42L, second.producerId);
            assertEquals(1, second.producerEpoch);
            assertEquals(1, second.baseSequence);
        }
    }

    @Test
    void initProducerIdSendsOpcodeAndReturnsPid() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long pid = c.initProducerId();
                assertEquals(42L, pid);
                assertEquals(42L, c.producerId());
                assertEquals(1, c.producerEpoch());
            }
            assertEquals(1, srv.initCount.get());
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID), srv.opcodes);
        }
    }

    @Test
    void initProducerIdSecondCallIsNoop() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long first = c.initProducerId();
                long second = c.initProducerId();
                assertEquals(42L, first);
                assertEquals(42L, second);
                assertEquals(1, c.producerEpoch());
            }
            assertEquals(1, srv.initCount.get());
            assertEquals(List.of(Codec.OP_INIT_PRODUCER_ID), srv.opcodes);
        }
    }

    @Test
    void idempotentProduceStillInitsOnce() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.produce("t", 0, null, "a".getBytes(StandardCharsets.UTF_8));
                c.produce("t", 0, null, "b".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1, srv.initCount.get());
            assertEquals(
                    List.of(Codec.OP_INIT_PRODUCER_ID, Codec.OP_PRODUCE, Codec.OP_PRODUCE), srv.opcodes);
        }
    }

    @Test
    void idempotentOffDefaultTrailer() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.produce("t", 0, null, "a".getBytes(StandardCharsets.UTF_8));
                c.produce("t", 0, null, "b".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(0, srv.initCount.get());
            assertEquals(List.of(Codec.OP_PRODUCE, Codec.OP_PRODUCE), srv.opcodes);
            for (Codec.ProduceRequest req : srv.produceReqs) {
                assertEquals(0L, req.producerId);
                assertEquals(0, req.producerEpoch);
                assertEquals(-1, req.baseSequence);
            }
        }
    }

    @Test
    void idempotentRedirectKeepsSequence() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
                c.produce("t", 0, null, "again".getBytes(StandardCharsets.UTF_8));
                assertEquals("127.0.0.1:" + leader.port, c.addr());
            }
            assertEquals(1, follower.initCount.get());
            assertEquals(0, leader.initCount.get());
            assertEquals(1, follower.produceCount.get());
            assertEquals(2, leader.produceCount.get());
            assertEquals(0, follower.produceReqs.get(0).baseSequence);
            assertEquals(42L, follower.produceReqs.get(0).producerId);
            assertEquals(0, leader.produceReqs.get(0).baseSequence);
            assertEquals(1, leader.produceReqs.get(1).baseSequence);
            assertEquals(42L, leader.produceReqs.get(0).producerId);
        }
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnInitTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.initCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                assertEquals(0, c.maxRetries());
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.initCount.get());
            assertEquals(0, srv.produceCount.get());
        }
    }

    @Test
    void retriesInitTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.initCodes.add(TIMEOUT);
            srv.initCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                long off = c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
                assertEquals(7L, off);
            }
            assertEquals(2, srv.initCount.get());
            assertEquals(1, srv.produceCount.get());
        }
    }

    @Test
    void initUnknownProducerIdNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.initCodes.add(UNKNOWN_PRODUCER);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(UNKNOWN_PRODUCER, ex.code);
            }
            assertEquals(1, srv.initCount.get());
            assertEquals(0, srv.produceCount.get());
        }
    }

    @Test
    void initExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.initCodes.add(TIMEOUT);
            srv.initCodes.add(TIMEOUT);
            srv.initCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.initCount.get());
            assertEquals(0, srv.produceCount.get());
        }
    }

    @Test
    void fetchOptsSendsKnobs() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.fetch("t", 0, 0, 10, 4096L, 100);
            }
            assertEquals(1, srv.fetchReqs.size());
            Codec.FetchRequest req = srv.fetchReqs.get(0);
            assertEquals(10, req.maxMessages);
            assertEquals(4096, req.maxBytes);
            assertEquals(100, req.maxWaitMs);
        }
    }

    @Test
    void fetchDefaultKnobs() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.fetch("t", 0, 0);
            }
            assertEquals(1, srv.fetchReqs.size());
            Codec.FetchRequest req = srv.fetchReqs.get(0);
            assertEquals(128, req.maxMessages);
            assertEquals(4L * 1024 * 1024, req.maxBytes);
            assertEquals(0, req.maxWaitMs);
        }
    }

    @Test
    void fetchSetClientDefaults() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setFetchMaxMessages(10);
                c.setFetchMaxBytes(4096L);
                c.setFetchMaxWaitMs(100);
                assertEquals(10, c.fetchMaxMessages());
                assertEquals(4096L, c.fetchMaxBytes());
                assertEquals(100, c.fetchMaxWaitMs());
                c.fetch("t", 0, 0);
            }
            assertEquals(1, srv.fetchReqs.size());
            Codec.FetchRequest req = srv.fetchReqs.get(0);
            assertEquals(10, req.maxMessages);
            assertEquals(4096, req.maxBytes);
            assertEquals(100, req.maxWaitMs);
        }
    }

    @Test
    void fetchSixArgIgnoresClientDefaults() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setFetchMaxMessages(10);
                c.setFetchMaxBytes(4096L);
                c.setFetchMaxWaitMs(100);
                c.fetch("t", 0, 0, 20, 8192L, 50);
            }
            assertEquals(1, srv.fetchReqs.size());
            Codec.FetchRequest req = srv.fetchReqs.get(0);
            assertEquals(20, req.maxMessages);
            assertEquals(8192, req.maxBytes);
            assertEquals(50, req.maxWaitMs);
        }
    }

    @Test
    void produceAcksAll() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8), 255);
            }
            assertEquals(1, srv.produceReqs.size());
            assertEquals(255, srv.produceReqs.get(0).acks);
        }
    }

    @Test
    void produceDefaultAcks() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(1, c.acks());
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1, srv.produceReqs.size());
            assertEquals(1, srv.produceReqs.get(0).acks);
        }
    }

    @Test
    void produceSetAcksAll() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setAcks(255);
                assertEquals(255, c.acks());
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1, srv.produceReqs.size());
            assertEquals(255, srv.produceReqs.get(0).acks);
        }
    }

    @Test
    void produceAcksExplicitWins() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setAcks(255);
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8), 1);
            }
            assertEquals(1, srv.produceReqs.size());
            assertEquals(1, srv.produceReqs.get(0).acks);
        }
    }

    @Test
    void fetchOptsRedirectsOnce() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.fetchCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<Record> recs = c.fetch("t", 0, 0, 10, 4096L, 100);
                assertTrue(recs.isEmpty());
            }
            assertEquals(1, follower.fetchCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.fetchCount.get());
            assertEquals(10, follower.fetchReqs.get(0).maxMessages);
            assertEquals(4096, follower.fetchReqs.get(0).maxBytes);
            assertEquals(100, follower.fetchReqs.get(0).maxWaitMs);
            assertEquals(10, leader.fetchReqs.get(0).maxMessages);
            assertEquals(4096, leader.fetchReqs.get(0).maxBytes);
            assertEquals(100, leader.fetchReqs.get(0).maxWaitMs);
        }
    }

    @Test
    void produceAcksRedirectsToLeader() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);
            leader.produceCodes.add(0);

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                long off = c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8), 255);
                assertEquals(7L, off);
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.produceCount.get());
            assertEquals(255, follower.produceReqs.get(0).acks);
            assertEquals(255, leader.produceReqs.get(0).acks);
        }
    }

    private static Metadata leaderMeta(String topic, int partition, int leaderId, String host, int port) {
        List<Metadata.BrokerInfo> brokers = new ArrayList<>();
        brokers.add(new Metadata.BrokerInfo(1, "127.0.0.1", 1));
        brokers.add(new Metadata.BrokerInfo(leaderId, host, port));
        return new Metadata(
                brokers,
                Collections.singletonList(new Metadata.TopicInfo(
                        topic,
                        1,
                        0,
                        Collections.singletonList(new Metadata.PartitionInfo(
                                partition,
                                leaderId,
                                0,
                                List.of(1L, (long) leaderId),
                                Collections.singletonList((long) leaderId),
                                1)))));
    }

    static final class ScriptedBroker implements AutoCloseable {
        final int port;
        final List<Integer> produceCodes = new CopyOnWriteArrayList<>();
        final List<Integer> initCodes = new CopyOnWriteArrayList<>();
        final List<Integer> fetchCodes = new CopyOnWriteArrayList<>();
        volatile long fetchHighWatermark = 0;
        volatile List<Record> fetchRecords = Collections.emptyList();
        final List<Integer> heartbeatCodes = new CopyOnWriteArrayList<>();
        final List<int[]> heartbeatReplies = new CopyOnWriteArrayList<>();
        final List<String> heartbeatMessages = new CopyOnWriteArrayList<>();
        final List<Integer> leaveGroupCodes = new CopyOnWriteArrayList<>();
        final List<int[]> leaveGroupReplies = new CopyOnWriteArrayList<>();
        final List<String> leaveGroupMessages = new CopyOnWriteArrayList<>();
        final List<Integer> offsetCommitCodes = new CopyOnWriteArrayList<>();
        final List<Integer> offsetFetchCodes = new CopyOnWriteArrayList<>();
        final List<Codec.OffsetFetchEntry> offsetFetchEntries = new CopyOnWriteArrayList<>();
        final List<Integer> deleteOffsetsCodes = new CopyOnWriteArrayList<>();
        final List<Integer> listOffsetsCodes = new CopyOnWriteArrayList<>();
        final List<Integer> describeGroupCodes = new CopyOnWriteArrayList<>();
        final List<int[]> describeGroupReplies = new CopyOnWriteArrayList<>();
        final List<String> describeGroupMessages = new CopyOnWriteArrayList<>();
        final List<Integer> listGroupsCodes = new CopyOnWriteArrayList<>();
        final List<int[]> listGroupsReplies = new CopyOnWriteArrayList<>();
        final List<String> listGroupsMessages = new CopyOnWriteArrayList<>();
        final List<Integer> metadataCodes = new CopyOnWriteArrayList<>();
        final List<Integer> listMembersCodes = new CopyOnWriteArrayList<>();
        final List<int[]> listMembersReplies = new CopyOnWriteArrayList<>();
        final List<String> listMembersMessages = new CopyOnWriteArrayList<>();
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final List<Codec.ProduceRequest> produceReqs = new CopyOnWriteArrayList<>();
        final List<Codec.FetchRequest> fetchReqs = new CopyOnWriteArrayList<>();
        final List<Codec.OffsetCommitRequest> offsetCommitReqs = new CopyOnWriteArrayList<>();
        final List<Codec.OffsetFetchRequest> offsetFetchReqs = new CopyOnWriteArrayList<>();
        final List<String> initTxnIds = new CopyOnWriteArrayList<>();
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger produceCount = new AtomicInteger();
        final AtomicInteger fetchCount = new AtomicInteger();
        final AtomicInteger heartbeatCount = new AtomicInteger();
        final AtomicInteger leaveGroupCount = new AtomicInteger();
        final AtomicInteger offsetCommitCount = new AtomicInteger();
        final AtomicInteger offsetFetchCount = new AtomicInteger();
        final AtomicInteger deleteOffsetsCount = new AtomicInteger();
        final AtomicInteger listOffsetsCount = new AtomicInteger();
        final AtomicInteger describeGroupCount = new AtomicInteger();
        final AtomicInteger listGroupsCount = new AtomicInteger();
        final AtomicInteger listMembersCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();
        volatile long initPid = 42L;
        volatile int initEpoch = 1;

        private final ServerSocket listen;
        private final Thread acceptThread;

        static ScriptedBroker start() throws IOException {
            return new ScriptedBroker();
        }

        void queueHeartbeatError(int code, String message) {
            heartbeatReplies.add(new int[] {code, 1});
            heartbeatMessages.add(message);
        }

        void queueDescribeGroupError(int code, String message) {
            describeGroupReplies.add(new int[] {code, 1});
            describeGroupMessages.add(message);
        }

        void queueLeaveGroupError(int code, String message) {
            leaveGroupReplies.add(new int[] {code, 1});
            leaveGroupMessages.add(message);
        }

        private ScriptedBroker() throws IOException {
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            acceptThread = new Thread(
                    () -> {
                        while (!listen.isClosed()) {
                            try {
                                Socket conn = listen.accept();
                                acceptCount.incrementAndGet();
                                Thread t = new Thread(() -> serve(conn), "volant-scripted-conn");
                                t.setDaemon(true);
                                t.start();
                            } catch (IOException e) {
                                return;
                            }
                        }
                    },
                    "volant-scripted-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        private void serve(Socket conn) {
            try {
                conn.setSoTimeout(5_000);
                InputStream in = conn.getInputStream();
                OutputStream out = conn.getOutputStream();
                byte[] buf = new byte[0];
                while (true) {
                    Frame.Decode d = Frame.tryDecode(buf);
                    if (d.frame == null) {
                        byte[] tmp = new byte[4096];
                        int n = in.read(tmp);
                        if (n < 0) {
                            return;
                        }
                        byte[] next = new byte[buf.length + n];
                        System.arraycopy(buf, 0, next, 0, buf.length);
                        System.arraycopy(tmp, 0, next, buf.length, n);
                        buf = next;
                        continue;
                    }
                    buf = d.rest;
                    int[] replyOp = new int[1];
                    byte[] payload = handle(d.frame, replyOp);
                    out.write(Frame.encode(replyOp[0], d.frame.correlationId, payload));
                    out.flush();
                }
            } catch (Exception ignored) {
                // client closed or timeout
            } finally {
                try {
                    conn.close();
                } catch (IOException ignored) {
                    // best-effort
                }
            }
        }

        private byte[] handle(Frame frame, int[] replyOp) {
            opcodes.add(frame.opcode);
            replyOp[0] = frame.opcode;
            if (frame.opcode == Codec.OP_INIT_PRODUCER_ID) {
                initCount.incrementAndGet();
                Codec.InitProducerIdRequest req = Codec.decodeInitProducerIdRequest(frame.payload);
                initTxnIds.add(req.transactionalId);
                int code = 0;
                if (!initCodes.isEmpty()) {
                    code = initCodes.remove(0);
                }
                replyOp[0] = Codec.OP_INIT_PRODUCER_ID_RESPONSE;
                return Codec.encodeInitProducerIdResponse(
                        new Codec.InitProducerIdResponse(initPid, initEpoch, code));
            }
            if (frame.opcode == Codec.OP_PRODUCE) {
                produceCount.incrementAndGet();
                Codec.ProduceRequest req = Codec.decodeProduceRequest(frame.payload);
                produceReqs.add(req);
                int code = 0;
                if (!produceCodes.isEmpty()) {
                    code = produceCodes.remove(0);
                }
                long part = req.partition >= 0 ? req.partition : 0;
                return Codec.encodeProduceResponse(
                        new Codec.ProduceResponse(
                                req.topic, part, code == 0 ? 7 : 0, code == 0 ? req.messages.size() : 0, code));
            }
            if (frame.opcode == Codec.OP_FETCH) {
                fetchCount.incrementAndGet();
                Codec.FetchRequest req = Codec.decodeFetchRequest(frame.payload);
                fetchReqs.add(req);
                int code = 0;
                if (!fetchCodes.isEmpty()) {
                    code = fetchCodes.remove(0);
                }
                return Codec.encodeFetchResponse(
                        new Codec.FetchResponse(req.topic, req.partition, fetchHighWatermark, code, fetchRecords));
            }
            if (frame.opcode == Codec.OP_HEARTBEAT) {
                heartbeatCount.incrementAndGet();
                if (!heartbeatReplies.isEmpty()) {
                    int[] rep = heartbeatReplies.remove(0);
                    String msg = heartbeatMessages.isEmpty() ? "" : heartbeatMessages.remove(0);
                    if (rep.length > 1 && rep[1] == 1) {
                        replyOp[0] = Codec.OP_ERROR;
                        return Codec.encodeErrorResponse(new Codec.ErrorResponse(rep[0], msg));
                    }
                    return Codec.encodeHeartbeatResponse(new Codec.HeartbeatResponse(rep[0]));
                }
                int code = 0;
                if (!heartbeatCodes.isEmpty()) {
                    code = heartbeatCodes.remove(0);
                }
                return Codec.encodeHeartbeatResponse(new Codec.HeartbeatResponse(code));
            }
            if (frame.opcode == Codec.OP_LEAVE_GROUP) {
                leaveGroupCount.incrementAndGet();
                if (!leaveGroupReplies.isEmpty()) {
                    int[] rep = leaveGroupReplies.remove(0);
                    String msg = leaveGroupMessages.isEmpty() ? "" : leaveGroupMessages.remove(0);
                    if (rep.length > 1 && rep[1] == 1) {
                        replyOp[0] = Codec.OP_ERROR;
                        return Codec.encodeErrorResponse(new Codec.ErrorResponse(rep[0], msg));
                    }
                    return Codec.encodeLeaveGroupResponse(new Codec.LeaveGroupResponse(rep[0]));
                }
                int code = 0;
                if (!leaveGroupCodes.isEmpty()) {
                    code = leaveGroupCodes.remove(0);
                }
                return Codec.encodeLeaveGroupResponse(new Codec.LeaveGroupResponse(code));
            }
            if (frame.opcode == Codec.OP_OFFSET_COMMIT) {
                offsetCommitCount.incrementAndGet();
                offsetCommitReqs.add(Codec.decodeOffsetCommitRequest(frame.payload));
                int code = 0;
                if (!offsetCommitCodes.isEmpty()) {
                    code = offsetCommitCodes.remove(0);
                }
                return Codec.encodeOffsetCommitResponse(new Codec.OffsetCommitResponse(code));
            }
            if (frame.opcode == Codec.OP_OFFSET_FETCH) {
                offsetFetchCount.incrementAndGet();
                offsetFetchReqs.add(Codec.decodeOffsetFetchRequest(frame.payload));
                int code = 0;
                if (!offsetFetchCodes.isEmpty()) {
                    code = offsetFetchCodes.remove(0);
                }
                return Codec.encodeOffsetFetchResponse(
                        new Codec.OffsetFetchResponse(code, new ArrayList<>(offsetFetchEntries)));
            }
            if (frame.opcode == Codec.OP_DELETE_OFFSETS) {
                deleteOffsetsCount.incrementAndGet();
                int code = 0;
                if (!deleteOffsetsCodes.isEmpty()) {
                    code = deleteOffsetsCodes.remove(0);
                }
                replyOp[0] = Codec.OP_DELETE_OFFSETS_RESPONSE;
                return Codec.encodeDeleteOffsetsResponse(new Codec.DeleteOffsetsResponse(code, 0));
            }
            if (frame.opcode == Codec.OP_LIST_OFFSETS) {
                listOffsetsCount.incrementAndGet();
                int code = 0;
                if (!listOffsetsCodes.isEmpty()) {
                    code = listOffsetsCodes.remove(0);
                }
                replyOp[0] = Codec.OP_LIST_OFFSETS_RESPONSE;
                return Codec.encodeListOffsetsResponse(
                        new Codec.ListOffsetsResponse(code, "", Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_DESCRIBE_GROUP) {
                describeGroupCount.incrementAndGet();
                if (!describeGroupReplies.isEmpty()) {
                    int[] rep = describeGroupReplies.remove(0);
                    String msg = describeGroupMessages.isEmpty() ? "" : describeGroupMessages.remove(0);
                    if (rep.length > 1 && rep[1] == 1) {
                        replyOp[0] = Codec.OP_ERROR;
                        return Codec.encodeErrorResponse(new Codec.ErrorResponse(rep[0], msg));
                    }
                    replyOp[0] = Codec.OP_DESCRIBE_GROUP_RESPONSE;
                    return Codec.encodeDescribeGroupResponse(
                            new Codec.DescribeGroupResponse(rep[0], "", 0, Collections.emptyList()));
                }
                int code = 0;
                if (!describeGroupCodes.isEmpty()) {
                    code = describeGroupCodes.remove(0);
                }
                replyOp[0] = Codec.OP_DESCRIBE_GROUP_RESPONSE;
                return Codec.encodeDescribeGroupResponse(
                        new Codec.DescribeGroupResponse(code, "", 0, Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_LIST_GROUPS) {
                listGroupsCount.incrementAndGet();
                if (!listGroupsReplies.isEmpty()) {
                    int[] rep = listGroupsReplies.remove(0);
                    String msg = listGroupsMessages.isEmpty() ? "" : listGroupsMessages.remove(0);
                    if (rep.length > 1 && rep[1] == 1) {
                        replyOp[0] = Codec.OP_ERROR;
                        return Codec.encodeErrorResponse(new Codec.ErrorResponse(rep[0], msg));
                    }
                    replyOp[0] = Codec.OP_LIST_GROUPS_RESPONSE;
                    return Codec.encodeListGroupsResponse(
                            new Codec.ListGroupsResponse(rep[0], Collections.emptyList()));
                }
                int code = 0;
                if (!listGroupsCodes.isEmpty()) {
                    code = listGroupsCodes.remove(0);
                }
                replyOp[0] = Codec.OP_LIST_GROUPS_RESPONSE;
                return Codec.encodeListGroupsResponse(
                        new Codec.ListGroupsResponse(code, Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_LIST_MEMBERS) {
                listMembersCount.incrementAndGet();
                int code = 0;
                String message = "";
                boolean asError = false;
                if (!listMembersReplies.isEmpty()) {
                    int[] spec = listMembersReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = listMembersMessages.isEmpty() ? "" : listMembersMessages.remove(0);
                } else if (!listMembersCodes.isEmpty()) {
                    code = listMembersCodes.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_LIST_MEMBERS_RESPONSE;
                return Codec.encodeListMembersResponse(
                        new Codec.ListMembersResponse(
                                code, 0, Collections.emptyList(), Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_METADATA) {
                metadataCount.incrementAndGet();
                int code = 0;
                if (!metadataCodes.isEmpty()) {
                    code = metadataCodes.remove(0);
                }
                if (code != 0) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, ""));
                }
                return Codec.encodeMetadataResponse(meta);
            }
            throw new ProtocolException("unexpected opcode " + frame.opcode);
        }

        @Override
        public void close() {
            try {
                listen.close();
            } catch (IOException ignored) {
                // best-effort
            }
            try {
                acceptThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.produceCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                assertEquals(50, c.retryBackoffMs());
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.produceCount.get());
        }
    }


    @Test
    void retriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                long off = c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
                assertEquals(7L, off);
            }
            assertEquals(3, srv.produceCount.get());
        }
    }


    @Test
    void exhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.produceCount.get());
        }
    }


    @Test
    void error13DoesNotConsumeRetries() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals(NOT_LEADER, ex.code);
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(0, follower.metadataCount.get());
        }
    }


    @Test
    void failedRetriesDoNotIncrementSequence() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.produceCodes.add(0);
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.produce("t", 0, null, "a".getBytes(StandardCharsets.UTF_8));
                assertThrows(
                        BrokerException.class,
                        () -> c.produce("t", 0, null, "b".getBytes(StandardCharsets.UTF_8)));
                c.produce("t", 0, null, "c".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(5, srv.produceReqs.size());
            assertEquals(0, srv.produceReqs.get(0).baseSequence);
            assertEquals(1, srv.produceReqs.get(1).baseSequence);
            assertEquals(1, srv.produceReqs.get(2).baseSequence);
            assertEquals(1, srv.produceReqs.get(3).baseSequence);
            assertEquals(1, srv.produceReqs.get(4).baseSequence);
        }
    }

    @Test
    void fetchDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.fetchCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.fetch("t", 0, 0));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.fetchCount.get());
        }
    }

    @Test
    void fetchRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.fetchCodes.add(TIMEOUT);
            srv.fetchCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                List<Record> recs = c.fetch("t", 0, 0);
                assertTrue(recs.isEmpty());
            }
            assertEquals(2, srv.fetchCount.get());
        }
    }

    @Test
    void fetchError13StillRedirectsNotRetry() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.fetchCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                List<Record> recs = c.fetch("t", 0, 0);
                assertTrue(recs.isEmpty());
            }
            assertEquals(1, follower.fetchCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.fetchCount.get());
        }
    }

    @Test
    void fetchTransportFailThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(1);
                c.setRetryBackoffMs(0);
                c.injectFetchTransportFails = 1;
                List<Record> recs = c.fetch("t", 0, 0);
                assertTrue(recs.isEmpty());
            }
            assertEquals(1, srv.fetchCount.get());
        }
    }

    @Test
    void fetchResultReturnsHighWatermark() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.fetchHighWatermark = 42L;
            srv.fetchRecords = List.of(
                    new Record(7L, -1L, null, "hello".getBytes(StandardCharsets.UTF_8), List.of()));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                FetchResult got = c.fetchResult("t", 0, 0);
                assertEquals("t", got.topic);
                assertEquals(0, got.partition);
                assertEquals(42L, got.highWatermark);
                assertEquals(1, got.records.size());
                assertEquals(7L, got.records.get(0).offset);
                assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), got.records.get(0).value);

                List<Record> recs = c.fetch("t", 0, 0);
                assertEquals(1, recs.size());
                assertEquals(7L, recs.get(0).offset);
                assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), recs.get(0).value);
            }
            assertEquals(2, srv.fetchCount.get());
        }
    }

    @Test
    void fetchExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.fetchCodes.add(TIMEOUT);
            srv.fetchCodes.add(TIMEOUT);
            srv.fetchCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.fetch("t", 0, 0));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.fetchCount.get());
        }
    }

    @Test
    void heartbeatDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.heartbeatCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.heartbeat("g", "m1", 1));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.heartbeatCount.get());
        }
    }

    @Test
    void heartbeatRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.heartbeatCodes.add(TIMEOUT);
            srv.heartbeatCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.heartbeat("g", "m1", 1);
            }
            assertEquals(2, srv.heartbeatCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void heartbeatRebalanceIsNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.heartbeatCodes.add(REBALANCE);
            srv.heartbeatCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.heartbeat("g", "m1", 1));
                assertEquals(REBALANCE, ex.code);
            }
            assertEquals(1, srv.heartbeatCount.get());
        }
    }

    @Test
    void heartbeatTransportFailThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(1);
                c.setRetryBackoffMs(0);
                c.injectHeartbeatTransportFails = 1;
                c.heartbeat("g", "m1", 1);
            }
            assertEquals(1, srv.heartbeatCount.get());
        }
    }

    @Test
    void heartbeatExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.heartbeatCodes.add(TIMEOUT);
            srv.heartbeatCodes.add(TIMEOUT);
            srv.heartbeatCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.heartbeat("g", "m1", 1));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.heartbeatCount.get());
        }
    }

    @Test
    void heartbeatError14RedirectsViaControllerId() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.queueHeartbeatError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.heartbeat("g", "m1", 1);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.heartbeatCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.heartbeatCount.get());
        }
    }

    @Test
    void heartbeatTyped14NoHintThenOk() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.heartbeatCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.heartbeat("g", "m1", 1);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.heartbeatCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.heartbeatCount.get());
        }
    }

    @Test
    void heartbeatMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.queueHeartbeatError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.heartbeat("g", "m1", 1));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.heartbeatCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void leaveGroupDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.leaveGroupCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.leaveGroup("g", "m1"));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.leaveGroupCodes.add(TIMEOUT);
            srv.leaveGroupCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.leaveGroup("g", "m1");
            }
            assertEquals(2, srv.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupUnknownMemberIsSuccess() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.leaveGroupCodes.add(UNKNOWN_MEMBER);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.leaveGroup("g", "m1");
            }
            assertEquals(1, srv.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupRetriesTimeoutThenUnknownMember() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.leaveGroupCodes.add(TIMEOUT);
            srv.leaveGroupCodes.add(UNKNOWN_MEMBER);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.leaveGroup("g", "m1");
            }
            assertEquals(2, srv.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupRebalanceIsNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.leaveGroupCodes.add(REBALANCE);
            srv.leaveGroupCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.leaveGroup("g", "m1"));
                assertEquals(REBALANCE, ex.code);
            }
            assertEquals(1, srv.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupError14RedirectsViaControllerId() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.queueLeaveGroupError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.leaveGroup("g", "m1");
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.leaveGroupCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupTyped14NoHintThenOk() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.leaveGroupCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.leaveGroup("g", "m1");
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.leaveGroupCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.leaveGroupCount.get());
        }
    }

    @Test
    void leaveGroupMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.queueLeaveGroupError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.leaveGroup("g", "m1"));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.leaveGroupCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void offsetCommitDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetCommitCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.offsetCommit("g", "t", 0, 5));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.offsetCommitCount.get());
        }
    }

    @Test
    void offsetCommitRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetCommitCodes.add(TIMEOUT);
            srv.offsetCommitCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.offsetCommit("g", "t", 0, 5);
            }
            assertEquals(2, srv.offsetCommitCount.get());
        }
    }

    @Test
    void offsetFetchRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchCodes.add(TIMEOUT);
            srv.offsetFetchCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                List<Offset> offs = c.offsetFetch("g", "t");
                assertTrue(offs.isEmpty());
            }
            assertEquals(2, srv.offsetFetchCount.get());
        }
    }

    @Test
    void offsetFetchAllTwoTopics() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, ""));
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("u", 1, 9, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetFetchEntry> offs = c.offsetFetchAll("g");
                assertEquals(
                        List.of(new OffsetFetchEntry("t", 0, 5), new OffsetFetchEntry("u", 1, 9)),
                        offs);
            }
            assertEquals(1, srv.offsetFetchCount.get());
        }
    }

    @Test
    void offsetFetchStillFiltersTopic() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, ""));
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("u", 1, 9, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<Offset> offs = c.offsetFetch("g", "t");
                assertEquals(List.of(new Offset(0, 5)), offs);
            }
            assertEquals(1, srv.offsetFetchCount.get());
        }
    }

    @Test
    void fetchOffsetsEncodesSpecificEntries() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetFetchEntry> offs =
                        c.fetchOffsets("g", List.of(new Codec.OffsetEntry("t", 0)));
                assertEquals(List.of(new OffsetFetchEntry("t", 0, 5)), offs);
            }
            assertEquals(1, srv.offsetFetchCount.get());
            assertEquals(1, srv.offsetFetchReqs.size());
            Codec.OffsetFetchRequest req = srv.offsetFetchReqs.get(0);
            assertEquals("g", req.groupId);
            assertEquals(1, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(0, req.entries.get(0).partition);
        }
    }

    @Test
    void fetchOffsetsNullOrEmptySendsAll() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, ""));
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("u", 1, 9, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetFetchEntry> none = c.fetchOffsets("g", null);
                List<OffsetFetchEntry> empty = c.fetchOffsets("g", Collections.emptyList());
                assertEquals(
                        List.of(new OffsetFetchEntry("t", 0, 5), new OffsetFetchEntry("u", 1, 9)),
                        none);
                assertEquals(none, empty);
            }
            assertEquals(2, srv.offsetFetchCount.get());
            assertEquals(2, srv.offsetFetchReqs.size());
            assertTrue(srv.offsetFetchReqs.get(0).entries.isEmpty());
            assertTrue(srv.offsetFetchReqs.get(1).entries.isEmpty());
        }
    }

    @Test
    void offsetFetchStillFiltersTopicRecordsEmptyWire() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, ""));
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("u", 1, 9, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<Offset> offs = c.offsetFetch("g", "t");
                assertEquals(List.of(new Offset(0, 5)), offs);
            }
            assertEquals(1, srv.offsetFetchCount.get());
            assertTrue(srv.offsetFetchReqs.get(0).entries.isEmpty());
        }
    }

    @Test
    void offsetFetchAllStillWorksRecordsEmptyWire() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, ""));
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("u", 1, 9, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetFetchEntry> offs = c.offsetFetchAll("g");
                assertEquals(
                        List.of(new OffsetFetchEntry("t", 0, 5), new OffsetFetchEntry("u", 1, 9)),
                        offs);
            }
            assertEquals(1, srv.offsetFetchCount.get());
            assertTrue(srv.offsetFetchReqs.get(0).entries.isEmpty());
        }
    }

    @Test
    void offsetFetchAllSurfacesMetadata() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, "consumer-1"));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetFetchEntry> offs = c.offsetFetchAll("g");
                assertEquals(List.of(new OffsetFetchEntry("t", 0, 5, "consumer-1")), offs);
                List<OffsetFetchEntry> rows = c.fetchOffsets("g", Collections.emptyList());
                assertEquals("consumer-1", rows.get(0).metadata);
            }
        }
    }

    @Test
    void offsetFetchEntriesFiltersTopicKeepsMetadata() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("t", 0, 5, "consumer-1"));
            srv.offsetFetchEntries.add(new Codec.OffsetFetchEntry("u", 1, 9, ""));
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetFetchEntry> entries = c.offsetFetchEntries("g", "t");
                assertEquals(List.of(new OffsetFetchEntry("t", 0, 5, "consumer-1")), entries);
                List<Offset> offs = c.offsetFetch("g", "t");
                assertEquals(List.of(new Offset(0, 5)), offs);
            }
        }
    }

    @Test
    void offsetFetchEntryThreeArgMetadataEmpty() {
        OffsetFetchEntry e = new OffsetFetchEntry("t", 0, 5);
        assertEquals("t", e.topic);
        assertEquals(0, e.partition);
        assertEquals(5, e.offset);
        assertEquals("", e.metadata);
    }

    @Test
    void deleteOffsetsRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.deleteOffsetsCodes.add(TIMEOUT);
            srv.deleteOffsetsCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                assertEquals(0, c.deleteOffsets("g"));
            }
            assertEquals(2, srv.deleteOffsetsCount.get());
        }
    }

    @Test
    void offsetCommitNotFoundIsNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetCommitCodes.add(NOT_FOUND);
            srv.offsetCommitCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.offsetCommit("g", "t", 0, 5));
                assertEquals(NOT_FOUND, ex.code);
            }
            assertEquals(1, srv.offsetCommitCount.get());
        }
    }

    @Test
    void offsetCommitExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.offsetCommitCodes.add(TIMEOUT);
            srv.offsetCommitCodes.add(TIMEOUT);
            srv.offsetCommitCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.offsetCommit("g", "t", 0, 5));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.offsetCommitCount.get());
        }
    }

    @Test
    void commitOffsetsBatchOfTwoOnTheWire() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.offsetCommit(
                        "g",
                        "",
                        0,
                        List.of(
                                new Codec.OffsetCommitEntry("t", 0, 5L, "m0"),
                                new Codec.OffsetCommitEntry("u", 1, 9L, "m1")));
            }
            assertEquals(1, srv.offsetCommitCount.get());
            assertEquals(1, srv.offsetCommitReqs.size());
            Codec.OffsetCommitRequest req = srv.offsetCommitReqs.get(0);
            assertEquals("g", req.groupId);
            assertEquals("", req.memberId);
            assertEquals(0L, req.generation);
            assertEquals(2, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(0, req.entries.get(0).partition);
            assertEquals(5L, req.entries.get(0).offset);
            assertEquals("m0", req.entries.get(0).metadata);
            assertEquals("u", req.entries.get(1).topic);
            assertEquals(1, req.entries.get(1).partition);
            assertEquals(9L, req.entries.get(1).offset);
            assertEquals("m1", req.entries.get(1).metadata);
        }
    }

    @Test
    void offsetCommitOneEntryStillWorks() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.offsetCommit("g", "t", 0, 5);
            }
            assertEquals(1, srv.offsetCommitCount.get());
            Codec.OffsetCommitRequest req = srv.offsetCommitReqs.get(0);
            assertEquals("g", req.groupId);
            assertEquals("", req.memberId);
            assertEquals(0L, req.generation);
            assertEquals(1, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(0, req.entries.get(0).partition);
            assertEquals(5L, req.entries.get(0).offset);
            assertEquals("", req.entries.get(0).metadata);
        }
    }

    @Test
    void offsetCommitFiveArgEncodesMetadata() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.offsetCommit("g", "t", 0, 5, "consumer-1");
            }
            assertEquals(1, srv.offsetCommitCount.get());
            Codec.OffsetCommitRequest req = srv.offsetCommitReqs.get(0);
            assertEquals("g", req.groupId);
            assertEquals("", req.memberId);
            assertEquals(0L, req.generation);
            assertEquals(1, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(0, req.entries.get(0).partition);
            assertEquals(5L, req.entries.get(0).offset);
            assertEquals("consumer-1", req.entries.get(0).metadata);
        }
    }

    @Test
    void offsetCommitSixArgStillSendsEmptyMetadata() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.offsetCommit("g", "t", 0, 5, "m1", 3);
            }
            Codec.OffsetCommitRequest req = srv.offsetCommitReqs.get(0);
            assertEquals("m1", req.memberId);
            assertEquals(3L, req.generation);
            assertEquals(1, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(5L, req.entries.get(0).offset);
            assertEquals("", req.entries.get(0).metadata);
        }
    }

    @Test
    void offsetCommitSevenArgEncodesMemberGenerationAndMetadata() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.offsetCommit("g", "t", 0, 5, "m1", 3, "consumer-1");
            }
            Codec.OffsetCommitRequest req = srv.offsetCommitReqs.get(0);
            assertEquals("m1", req.memberId);
            assertEquals(3L, req.generation);
            assertEquals(1, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(5L, req.entries.get(0).offset);
            assertEquals("consumer-1", req.entries.get(0).metadata);
        }
    }

    @Test
    void commitOffsetsSendsMemberIdAndGeneration() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.offsetCommit(
                        "g",
                        "m1",
                        3,
                        List.of(new Codec.OffsetCommitEntry("t", 0, 5L, "")));
            }
            Codec.OffsetCommitRequest req = srv.offsetCommitReqs.get(0);
            assertEquals("m1", req.memberId);
            assertEquals(3L, req.generation);
            assertEquals(1, req.entries.size());
            assertEquals("t", req.entries.get(0).topic);
            assertEquals(5L, req.entries.get(0).offset);
            assertEquals("", req.entries.get(0).metadata);
        }
    }

    @Test
    void listOffsetsDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.listOffsetsCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.listOffsets("t"));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.listOffsetsCount.get());
        }
    }

    @Test
    void listOffsetsRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.listOffsetsCodes.add(TIMEOUT);
            srv.listOffsetsCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                List<OffsetListing> got = c.listOffsets("t");
                assertTrue(got.isEmpty());
            }
            assertEquals(2, srv.listOffsetsCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void listOffsetsNotFoundIsNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.listOffsetsCodes.add(NOT_FOUND);
            srv.listOffsetsCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.listOffsets("missing"));
                assertEquals(NOT_FOUND, ex.code);
            }
            assertEquals(1, srv.listOffsetsCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void listOffsetsExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.listOffsetsCodes.add(TIMEOUT);
            srv.listOffsetsCodes.add(TIMEOUT);
            srv.listOffsetsCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.listOffsets("t"));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.listOffsetsCount.get());
        }
    }

    @Test
    void describeGroupDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.describeGroupCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.describeGroup("g"));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.describeGroupCount.get());
        }
    }

    @Test
    void describeGroupRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.describeGroupCodes.add(TIMEOUT);
            srv.describeGroupCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                DescribeGroupResult got = c.describeGroup("g");
                assertEquals("", got.groupId);
                assertTrue(got.members.isEmpty());
            }
            assertEquals(2, srv.describeGroupCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void describeGroupNotFoundIsNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.describeGroupCodes.add(NOT_FOUND);
            srv.describeGroupCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.describeGroup("missing"));
                assertEquals(NOT_FOUND, ex.code);
            }
            assertEquals(1, srv.describeGroupCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void listGroupsRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.listGroupsCodes.add(TIMEOUT);
            srv.listGroupsCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                List<Codec.GroupListing> got = c.listGroups();
                assertTrue(got.isEmpty());
            }
            assertEquals(2, srv.listGroupsCount.get());
        }
    }

    @Test
    void describeGroupExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.describeGroupCodes.add(TIMEOUT);
            srv.describeGroupCodes.add(TIMEOUT);
            srv.describeGroupCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.describeGroup("g"));
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.describeGroupCount.get());
        }
    }

    @Test
    void describeGroupError14RedirectsViaControllerId() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.queueDescribeGroupError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                DescribeGroupResult got = c.describeGroup("g");
                assertEquals("", got.groupId);
                assertTrue(got.members.isEmpty());
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.describeGroupCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.describeGroupCount.get());
        }
    }

    @Test
    void listGroupsTyped14NoHintThenOk() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.listGroupsCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<Codec.GroupListing> got = c.listGroups();
                assertTrue(got.isEmpty());
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.listGroupsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listGroupsCount.get());
        }
    }

    @Test
    void describeGroupMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.queueDescribeGroupError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.describeGroup("g"));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.describeGroupCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void metadataDefaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.metadataCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.metadata());
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(1, srv.metadataCount.get());
        }
    }

    @Test
    void metadataRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.metadataCodes.add(TIMEOUT);
            srv.metadataCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                Metadata got = c.metadata();
                assertTrue(got.brokers.isEmpty());
                assertTrue(got.topics.isEmpty());
            }
            assertEquals(2, srv.metadataCount.get());
        }
    }

    @Test
    void metadataNotFoundIsNotRetried() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.metadataCodes.add(NOT_FOUND);
            srv.metadataCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.metadata());
                assertEquals(NOT_FOUND, ex.code);
            }
            assertEquals(1, srv.metadataCount.get());
        }
    }

    @Test
    void listMembersRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.listMembersCodes.add(TIMEOUT);
            srv.listMembersCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                MembershipList got = c.listMembers();
                assertEquals(0, got.generation);
                assertTrue(got.brokers.isEmpty());
                assertTrue(got.live.isEmpty());
            }
            assertEquals(2, srv.listMembersCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void listMembersError14RedirectsViaControllerId() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.listMembersReplies.add(new int[] {NOT_CONTROLLER, 1});
            follower.listMembersMessages.add("not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                MembershipList got = c.listMembers();
                assertEquals(0, got.generation);
                assertTrue(got.brokers.isEmpty());
                assertTrue(got.live.isEmpty());
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.listMembersCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listMembersCount.get());
            assertEquals(0, leader.metadataCount.get());
        }
    }

    @Test
    void listMembersTyped14NoHintThenOk() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.listMembersCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                MembershipList got = c.listMembers();
                assertEquals(0, got.generation);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.listMembersCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listMembersCount.get());
        }
    }

    @Test
    void listMembersMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (ScriptedBroker follower = ScriptedBroker.start()) {
            follower.listMembersReplies.add(new int[] {NOT_CONTROLLER, 1});
            follower.listMembersMessages.add("not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex = assertThrows(BrokerException.class, c::listMembers);
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.listMembersCount.get());
            assertEquals(0, follower.metadataCount.get());
        }
    }

    @Test
    void metadataExhaustedRetriesRaises() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.metadataCodes.add(TIMEOUT);
            srv.metadataCodes.add(TIMEOUT);
            srv.metadataCodes.add(TIMEOUT);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.metadata());
                assertEquals(TIMEOUT, ex.code);
            }
            assertEquals(3, srv.metadataCount.get());
        }
    }


    private static List<Codec.ProduceMessage> batchMsgs(String... values) {
        List<Codec.ProduceMessage> out = new ArrayList<>();
        for (String v : values) {
            out.add(new Codec.ProduceMessage(null, v.getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    @Test
    void produceBatchThreeMessages() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off = c.produce("t", 0, batchMsgs("a", "b", "c"), 1);
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(3, req.messages.size());
            assertEquals(1, req.acks);
            assertEquals("a", new String(req.messages.get(0).value, StandardCharsets.UTF_8));
            assertEquals("b", new String(req.messages.get(1).value, StandardCharsets.UTF_8));
            assertEquals("c", new String(req.messages.get(2).value, StandardCharsets.UTF_8));
        }
    }

    @Test
    void produceBatchEmpty() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> c.produce("t", 0, Collections.emptyList(), 1));
                assertThrows(IllegalArgumentException.class, () -> c.produce("t", 0, null, 1));
            }
            assertEquals(0, srv.produceCount.get());
        }
    }

    @Test
    void produceBatchDefaultUsesSetAcks() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setAcks(255);
                long off = c.produce("t", 0, batchMsgs("a", "b", "c"));
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(3, req.messages.size());
            assertEquals(255, req.acks);
            assertEquals("a", new String(req.messages.get(0).value, StandardCharsets.UTF_8));
            assertEquals("b", new String(req.messages.get(1).value, StandardCharsets.UTF_8));
            assertEquals("c", new String(req.messages.get(2).value, StandardCharsets.UTF_8));
        }
    }

    @Test
    void produceBatchExplicitAcksWins() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setAcks(255);
                long off = c.produce("t", 0, batchMsgs("a", "b", "c"), 1);
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(3, req.messages.size());
            assertEquals(1, req.acks);
        }
    }

    @Test
    void produceStillOneMessage() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(1, srv.produceReqs.size());
            assertEquals(1, srv.produceReqs.get(0).messages.size());
            assertEquals(
                    "hello",
                    new String(srv.produceReqs.get(0).messages.get(0).value, StandardCharsets.UTF_8));
            assertTrue(srv.produceReqs.get(0).messages.get(0).headers.isEmpty());
            assertEquals(-1L, srv.produceReqs.get(0).messages.get(0).timestampMs);
        }
    }

    @Test
    void produceHeadersEncodes() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off =
                        c.produce(
                                "t",
                                0,
                                null,
                                "hello".getBytes(StandardCharsets.UTF_8),
                                Collections.singletonList(
                                        new Record.Header("h", "hv".getBytes(StandardCharsets.UTF_8))));
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(1, req.acks);
            assertEquals(1, req.messages.size());
            assertEquals(1, req.messages.get(0).headers.size());
            assertEquals("h", req.messages.get(0).headers.get(0).name);
            assertArrayEquals(
                    "hv".getBytes(StandardCharsets.UTF_8), req.messages.get(0).headers.get(0).value);
        }
    }

    @Test
    void produceTimestampEncodes() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off =
                        c.produceTimestamp(
                                "t", 0, null, "hello".getBytes(StandardCharsets.UTF_8), 1_700_000_000_000L);
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(1, req.acks);
            assertEquals(1, req.messages.size());
            assertEquals(1_700_000_000_000L, req.messages.get(0).timestampMs);
            assertTrue(req.messages.get(0).headers.isEmpty());
        }
    }

    @Test
    void produceHeadersUsesSetAcks() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setAcks(255);
                c.produce(
                        "t",
                        0,
                        null,
                        "hello".getBytes(StandardCharsets.UTF_8),
                        Collections.singletonList(
                                new Record.Header("h", "hv".getBytes(StandardCharsets.UTF_8))));
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(255, req.acks);
            assertEquals(1, req.messages.get(0).headers.size());
            assertEquals("h", req.messages.get(0).headers.get(0).name);
            assertArrayEquals(
                    "hv".getBytes(StandardCharsets.UTF_8), req.messages.get(0).headers.get(0).value);
        }
    }

    @Test
    void produceHeadersAcksEncodes() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off =
                        c.produceHeadersAcks(
                                "t",
                                0,
                                null,
                                "hello".getBytes(StandardCharsets.UTF_8),
                                Collections.singletonList(
                                        new Record.Header("h", "hv".getBytes(StandardCharsets.UTF_8))),
                                255);
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(255, req.acks);
            assertEquals(1, req.messages.size());
            assertEquals(1, req.messages.get(0).headers.size());
            assertEquals("h", req.messages.get(0).headers.get(0).name);
            assertArrayEquals(
                    "hv".getBytes(StandardCharsets.UTF_8), req.messages.get(0).headers.get(0).value);
        }
    }

    @Test
    void produceTimestampHeadersEncodes() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off =
                        c.produceTimestampHeaders(
                                "t",
                                0,
                                null,
                                "hello".getBytes(StandardCharsets.UTF_8),
                                1_700_000_000_000L,
                                Collections.singletonList(
                                        new Record.Header("h", "hv".getBytes(StandardCharsets.UTF_8))));
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(1, req.acks);
            assertEquals(1, req.messages.size());
            assertEquals(1_700_000_000_000L, req.messages.get(0).timestampMs);
            assertEquals(1, req.messages.get(0).headers.size());
            assertEquals("h", req.messages.get(0).headers.get(0).name);
            assertArrayEquals(
                    "hv".getBytes(StandardCharsets.UTF_8), req.messages.get(0).headers.get(0).value);
        }
    }

    @Test
    void produceTimestampAcksEncodes() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off =
                        c.produceTimestampAcks(
                                "t", 0, null, "hello".getBytes(StandardCharsets.UTF_8), 1_700_000_000_000L, 255);
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(255, req.acks);
            assertEquals(1, req.messages.size());
            assertEquals(1_700_000_000_000L, req.messages.get(0).timestampMs);
            assertTrue(req.messages.get(0).headers.isEmpty());
        }
    }

    @Test
    void produceTimestampHeadersAcksEncodes() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long off =
                        c.produceTimestampHeadersAcks(
                                "t",
                                0,
                                null,
                                "hello".getBytes(StandardCharsets.UTF_8),
                                1_700_000_000_000L,
                                Collections.singletonList(
                                        new Record.Header("h", "hv".getBytes(StandardCharsets.UTF_8))),
                                255);
                assertEquals(7L, off);
            }
            assertEquals(1, srv.produceReqs.size());
            Codec.ProduceRequest req = srv.produceReqs.get(0);
            assertEquals(255, req.acks);
            assertEquals(1, req.messages.size());
            assertEquals(1_700_000_000_000L, req.messages.get(0).timestampMs);
            assertEquals(1, req.messages.get(0).headers.size());
            assertEquals("h", req.messages.get(0).headers.get(0).name);
            assertArrayEquals(
                    "hv".getBytes(StandardCharsets.UTF_8), req.messages.get(0).headers.get(0).value);
        }
    }

    @Test
    void produceBatchRetriesTimeoutThenOk() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            srv.produceCodes.add(TIMEOUT);
            srv.produceCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                long off = c.produce("t", 0, batchMsgs("a", "b", "c"), 1);
                assertEquals(7L, off);
            }
            assertEquals(2, srv.produceReqs.size());
            for (Codec.ProduceRequest req : srv.produceReqs) {
                assertEquals(3, req.messages.size());
                assertEquals("a", new String(req.messages.get(0).value, StandardCharsets.UTF_8));
                assertEquals("b", new String(req.messages.get(1).value, StandardCharsets.UTF_8));
                assertEquals("c", new String(req.messages.get(2).value, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void produceBatchRedirectsToLeader() throws Exception {
        try (ScriptedBroker leader = ScriptedBroker.start();
                ScriptedBroker follower = ScriptedBroker.start()) {
            follower.produceCodes.add(NOT_LEADER);
            follower.meta = leaderMeta("t", 0, 2, "127.0.0.1", leader.port);
            leader.produceCodes.add(0);

            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                long off = c.produce("t", 0, batchMsgs("a", "b", "c"), 1);
                assertEquals(7L, off);
            }
            assertEquals(1, follower.produceCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.produceCount.get());
            assertEquals(3, follower.produceReqs.get(0).messages.size());
            assertEquals(3, leader.produceReqs.get(0).messages.size());
        }
    }

    @Test
    void produceBatchIncrementsSequenceByCount() throws Exception {
        try (ScriptedBroker srv = ScriptedBroker.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setEnableIdempotence(true);
                c.produce("t", 0, batchMsgs("a", "b", "c"), 1);
                c.produce("t", 0, batchMsgs("d", "e"), 1);
            }
            assertEquals(2, srv.produceReqs.size());
            assertEquals(0, srv.produceReqs.get(0).baseSequence);
            assertEquals(3, srv.produceReqs.get(1).baseSequence);
        }
    }

    private static final int NOT_CONTROLLER = Client.NOT_CONTROLLER;

    private static Metadata controllerMeta(int nodeId, String host, int port) {
        List<Metadata.BrokerInfo> brokers = new ArrayList<>();
        brokers.add(new Metadata.BrokerInfo(1, "127.0.0.1", 1));
        brokers.add(new Metadata.BrokerInfo(nodeId, host, port));
        return new Metadata(brokers, Collections.emptyList());
    }

    private static Metadata otherBrokerMeta(int currentPort, String host, int port) {
        List<Metadata.BrokerInfo> brokers = new ArrayList<>();
        brokers.add(new Metadata.BrokerInfo(1, "127.0.0.1", currentPort));
        brokers.add(new Metadata.BrokerInfo(2, host, port));
        return new Metadata(brokers, Collections.emptyList());
    }

    static final class AdminBroker implements AutoCloseable {
        final int port;
        final List<int[]> createTopicReplies = new CopyOnWriteArrayList<>();
        final List<String> createTopicMessages = new CopyOnWriteArrayList<>();
        final List<Integer> createPartitionsCodes = new CopyOnWriteArrayList<>();
        final List<Integer> createAclsCodes = new CopyOnWriteArrayList<>();
        final List<Integer> reassignCodes = new CopyOnWriteArrayList<>();
        final List<int[]> createScramReplies = new CopyOnWriteArrayList<>();
        final List<String> createScramMessages = new CopyOnWriteArrayList<>();
        final List<Integer> deleteScramCodes = new CopyOnWriteArrayList<>();
        final List<Integer> listScramCodes = new CopyOnWriteArrayList<>();
        final List<Integer> listAclsCodes = new CopyOnWriteArrayList<>();
        final List<int[]> addBrokerReplies = new CopyOnWriteArrayList<>();
        final List<String> addBrokerMessages = new CopyOnWriteArrayList<>();
        final List<Integer> removeBrokerCodes = new CopyOnWriteArrayList<>();
        final List<int[]> describeConfigsReplies = new CopyOnWriteArrayList<>();
        final List<String> describeConfigsMessages = new CopyOnWriteArrayList<>();
        final List<Integer> alterConfigsCodes = new CopyOnWriteArrayList<>();
        final List<int[]> deleteOffsetsReplies = new CopyOnWriteArrayList<>();
        final List<String> deleteOffsetsMessages = new CopyOnWriteArrayList<>();
        final List<Integer> deleteOffsetsCodes = new CopyOnWriteArrayList<>();
        final List<int[]> offsetCommitReplies = new CopyOnWriteArrayList<>();
        final List<String> offsetCommitMessages = new CopyOnWriteArrayList<>();
        final List<Integer> offsetCommitCodes = new CopyOnWriteArrayList<>();
        final List<int[]> offsetFetchReplies = new CopyOnWriteArrayList<>();
        final List<String> offsetFetchMessages = new CopyOnWriteArrayList<>();
        final List<Integer> offsetFetchCodes = new CopyOnWriteArrayList<>();
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        final AtomicInteger createTopicCount = new AtomicInteger();
        final AtomicInteger createPartitionsCount = new AtomicInteger();
        final AtomicInteger createAclsCount = new AtomicInteger();
        final AtomicInteger reassignCount = new AtomicInteger();
        final AtomicInteger createScramCount = new AtomicInteger();
        final AtomicInteger deleteScramCount = new AtomicInteger();
        final AtomicInteger listScramCount = new AtomicInteger();
        final AtomicInteger listAclsCount = new AtomicInteger();
        final AtomicInteger addBrokerCount = new AtomicInteger();
        final AtomicInteger removeBrokerCount = new AtomicInteger();
        final AtomicInteger describeConfigsCount = new AtomicInteger();
        final AtomicInteger alterConfigsCount = new AtomicInteger();
        final AtomicInteger deleteOffsetsCount = new AtomicInteger();
        final AtomicInteger offsetCommitCount = new AtomicInteger();
        final AtomicInteger offsetFetchCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger listMembersCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();
        final List<String[]> lastCreateTopicConfigs = new CopyOnWriteArrayList<>();

        private final ServerSocket listen;
        private final Thread acceptThread;

        static AdminBroker start() throws IOException {
            return new AdminBroker();
        }

        private AdminBroker() throws IOException {
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            acceptThread = new Thread(
                    () -> {
                        while (!listen.isClosed()) {
                            try {
                                Socket conn = listen.accept();
                                acceptCount.incrementAndGet();
                                Thread t = new Thread(() -> serve(conn), "volant-admin-conn");
                                t.setDaemon(true);
                                t.start();
                            } catch (IOException e) {
                                return;
                            }
                        }
                    },
                    "volant-admin-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        void queueCreateTopicError(int code, String message) {
            createTopicReplies.add(new int[] {code, 1});
            createTopicMessages.add(message);
        }

        void queueCreateTopicOk() {
            queueCreateTopicOk(1);
        }

        void queueCreateTopicOk(int topicId) {
            createTopicReplies.add(new int[] {0, 0, topicId});
            createTopicMessages.add("");
        }

        void queueCreateScramError(int code, String message) {
            createScramReplies.add(new int[] {code, 1});
            createScramMessages.add(message);
        }

        void queueCreateScramOk() {
            createScramReplies.add(new int[] {0, 0});
            createScramMessages.add("");
        }

        void queueAddBrokerError(int code, String message) {
            addBrokerReplies.add(new int[] {code, 1});
            addBrokerMessages.add(message);
        }

        void queueAddBrokerOk() {
            addBrokerReplies.add(new int[] {0, 0});
            addBrokerMessages.add("");
        }

        void queueDescribeConfigsError(int code, String message) {
            describeConfigsReplies.add(new int[] {code, 1});
            describeConfigsMessages.add(message);
        }

        void queueDescribeConfigsOk() {
            describeConfigsReplies.add(new int[] {0, 0});
            describeConfigsMessages.add("");
        }

        void queueDeleteOffsetsError(int code, String message) {
            deleteOffsetsReplies.add(new int[] {code, 1});
            deleteOffsetsMessages.add(message);
        }

        void queueDeleteOffsetsOk() {
            deleteOffsetsReplies.add(new int[] {0, 0});
            deleteOffsetsMessages.add("");
        }

        void queueOffsetCommitError(int code, String message) {
            offsetCommitReplies.add(new int[] {code, 1});
            offsetCommitMessages.add(message);
        }

        void queueOffsetCommitOk() {
            offsetCommitReplies.add(new int[] {0, 0});
            offsetCommitMessages.add("");
        }

        void queueOffsetFetchError(int code, String message) {
            offsetFetchReplies.add(new int[] {code, 1});
            offsetFetchMessages.add(message);
        }

        void queueOffsetFetchOk() {
            offsetFetchReplies.add(new int[] {0, 0});
            offsetFetchMessages.add("");
        }

        private void serve(Socket conn) {
            try {
                conn.setSoTimeout(5_000);
                InputStream in = conn.getInputStream();
                OutputStream out = conn.getOutputStream();
                byte[] buf = new byte[0];
                while (true) {
                    Frame.Decode d = Frame.tryDecode(buf);
                    if (d.frame == null) {
                        byte[] tmp = new byte[4096];
                        int n = in.read(tmp);
                        if (n < 0) {
                            return;
                        }
                        byte[] next = new byte[buf.length + n];
                        System.arraycopy(buf, 0, next, 0, buf.length);
                        System.arraycopy(tmp, 0, next, buf.length, n);
                        buf = next;
                        continue;
                    }
                    buf = d.rest;
                    int[] replyOp = new int[1];
                    byte[] payload = handle(d.frame, replyOp);
                    out.write(Frame.encode(replyOp[0], d.frame.correlationId, payload));
                    out.flush();
                }
            } catch (Exception ignored) {
                // client closed or timeout
            } finally {
                try {
                    conn.close();
                } catch (IOException ignored) {
                    // best-effort
                }
            }
        }

        private byte[] handle(Frame frame, int[] replyOp) {
            replyOp[0] = frame.opcode;
            if (frame.opcode == Codec.OP_CREATE_TOPIC) {
                createTopicCount.incrementAndGet();
                Codec.CreateTopicRequest req = Codec.decodeCreateTopicRequest(frame.payload);
                lastCreateTopicConfigs.clear();
                if (req.configs != null) {
                    lastCreateTopicConfigs.addAll(req.configs);
                }
                int code = 0;
                boolean asError = false;
                String message = "";
                int topicId = 1;
                if (!createTopicReplies.isEmpty()) {
                    int[] spec = createTopicReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = createTopicMessages.isEmpty() ? "" : createTopicMessages.remove(0);
                    if (spec.length > 2) {
                        topicId = spec[2];
                    }
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                return Codec.encodeCreateTopicResponse(
                        new Codec.CreateTopicResponse(
                                code == 0 ? topicId : 0, req.name, code == 0 ? req.partitions : 0, code));
            }
            if (frame.opcode == Codec.OP_CREATE_PARTITIONS) {
                createPartitionsCount.incrementAndGet();
                Codec.CreatePartitionsRequest req = Codec.decodeCreatePartitionsRequest(frame.payload);
                int code = createPartitionsCodes.isEmpty() ? 0 : createPartitionsCodes.remove(0);
                replyOp[0] = Codec.OP_CREATE_PARTITIONS_RESPONSE;
                return Codec.encodeCreatePartitionsResponse(
                        new Codec.CreatePartitionsResponse(code, req.topic, code == 0 ? req.totalCount : 0));
            }
            if (frame.opcode == Codec.OP_CREATE_ACLS) {
                createAclsCount.incrementAndGet();
                int code = createAclsCodes.isEmpty() ? 0 : createAclsCodes.remove(0);
                replyOp[0] = Codec.OP_CREATE_ACLS_RESPONSE;
                return Codec.encodeCreateAclsResponse(new Codec.CreateAclsResponse(code));
            }
            if (frame.opcode == Codec.OP_REASSIGN_PARTITIONS) {
                reassignCount.incrementAndGet();
                int code = reassignCodes.isEmpty() ? 0 : reassignCodes.remove(0);
                replyOp[0] = Codec.OP_REASSIGN_PARTITIONS_RESPONSE;
                return Codec.encodeReassignPartitionsResponse(
                        new Codec.ReassignPartitionsResponse(code, code == 0 ? 7 : 0));
            }
            if (frame.opcode == Codec.OP_CREATE_SCRAM_USER) {
                createScramCount.incrementAndGet();
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!createScramReplies.isEmpty()) {
                    int[] spec = createScramReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = createScramMessages.isEmpty() ? "" : createScramMessages.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_CREATE_SCRAM_USER_RESPONSE;
                return Codec.encodeCreateScramUserResponse(new Codec.CreateScramUserResponse(code));
            }
            if (frame.opcode == Codec.OP_DELETE_SCRAM_USER) {
                deleteScramCount.incrementAndGet();
                int code = deleteScramCodes.isEmpty() ? 0 : deleteScramCodes.remove(0);
                replyOp[0] = Codec.OP_DELETE_SCRAM_USER_RESPONSE;
                return Codec.encodeDeleteScramUserResponse(new Codec.DeleteScramUserResponse(code));
            }
            if (frame.opcode == Codec.OP_LIST_SCRAM_USERS) {
                listScramCount.incrementAndGet();
                int code = listScramCodes.isEmpty() ? 0 : listScramCodes.remove(0);
                replyOp[0] = Codec.OP_LIST_SCRAM_USERS_RESPONSE;
                List<String> names = code == 0 ? Collections.singletonList("alice") : Collections.emptyList();
                return Codec.encodeListScramUsersResponse(new Codec.ListScramUsersResponse(code, names));
            }
            if (frame.opcode == Codec.OP_LIST_ACLS) {
                listAclsCount.incrementAndGet();
                int code = listAclsCodes.isEmpty() ? 0 : listAclsCodes.remove(0);
                replyOp[0] = Codec.OP_LIST_ACLS_RESPONSE;
                return Codec.encodeListAclsResponse(
                        new Codec.ListAclsResponse(code, Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_ADD_BROKER) {
                addBrokerCount.incrementAndGet();
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!addBrokerReplies.isEmpty()) {
                    int[] spec = addBrokerReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = addBrokerMessages.isEmpty() ? "" : addBrokerMessages.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_ADD_BROKER_RESPONSE;
                return Codec.encodeAddBrokerResponse(
                        new Codec.AddBrokerResponse(code, code == 0 ? 11L : 0L));
            }
            if (frame.opcode == Codec.OP_REMOVE_BROKER) {
                removeBrokerCount.incrementAndGet();
                int code = removeBrokerCodes.isEmpty() ? 0 : removeBrokerCodes.remove(0);
                replyOp[0] = Codec.OP_REMOVE_BROKER_RESPONSE;
                return Codec.encodeRemoveBrokerResponse(
                        new Codec.RemoveBrokerResponse(code, code == 0 ? 12L : 0L));
            }
            if (frame.opcode == Codec.OP_DESCRIBE_CONFIGS) {
                describeConfigsCount.incrementAndGet();
                Codec.DescribeConfigsRequest req = Codec.decodeDescribeConfigsRequest(frame.payload);
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!describeConfigsReplies.isEmpty()) {
                    int[] spec = describeConfigsReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = describeConfigsMessages.isEmpty() ? "" : describeConfigsMessages.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_DESCRIBE_CONFIGS_RESPONSE;
                List<String[]> cfgs = code == 0
                        ? Collections.singletonList(new String[] {"retention.ms", "86400000"})
                        : Collections.emptyList();
                return Codec.encodeDescribeConfigsResponse(
                        new Codec.DescribeConfigsResponse(
                                code, req.topic, code == 0 ? 1 : 0, code == 0 ? 1 : 0, cfgs));
            }
            if (frame.opcode == Codec.OP_ALTER_CONFIGS) {
                alterConfigsCount.incrementAndGet();
                Codec.AlterConfigsRequest req = Codec.decodeAlterConfigsRequest(frame.payload);
                int code = alterConfigsCodes.isEmpty() ? 0 : alterConfigsCodes.remove(0);
                replyOp[0] = Codec.OP_ALTER_CONFIGS_RESPONSE;
                return Codec.encodeAlterConfigsResponse(new Codec.AlterConfigsResponse(code, req.topic));
            }
            if (frame.opcode == Codec.OP_DELETE_OFFSETS) {
                deleteOffsetsCount.incrementAndGet();
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!deleteOffsetsReplies.isEmpty()) {
                    int[] spec = deleteOffsetsReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = deleteOffsetsMessages.isEmpty() ? "" : deleteOffsetsMessages.remove(0);
                } else if (!deleteOffsetsCodes.isEmpty()) {
                    code = deleteOffsetsCodes.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_DELETE_OFFSETS_RESPONSE;
                return Codec.encodeDeleteOffsetsResponse(
                        new Codec.DeleteOffsetsResponse(code, code == 0 ? 3 : 0));
            }
            if (frame.opcode == Codec.OP_OFFSET_COMMIT) {
                offsetCommitCount.incrementAndGet();
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!offsetCommitReplies.isEmpty()) {
                    int[] spec = offsetCommitReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = offsetCommitMessages.isEmpty() ? "" : offsetCommitMessages.remove(0);
                } else if (!offsetCommitCodes.isEmpty()) {
                    code = offsetCommitCodes.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_OFFSET_COMMIT;
                return Codec.encodeOffsetCommitResponse(new Codec.OffsetCommitResponse(code));
            }
            if (frame.opcode == Codec.OP_OFFSET_FETCH) {
                offsetFetchCount.incrementAndGet();
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!offsetFetchReplies.isEmpty()) {
                    int[] spec = offsetFetchReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = offsetFetchMessages.isEmpty() ? "" : offsetFetchMessages.remove(0);
                } else if (!offsetFetchCodes.isEmpty()) {
                    code = offsetFetchCodes.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                replyOp[0] = Codec.OP_OFFSET_FETCH;
                return Codec.encodeOffsetFetchResponse(
                        new Codec.OffsetFetchResponse(code, Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_METADATA) {
                metadataCount.incrementAndGet();
                return Codec.encodeMetadataResponse(meta);
            }
            if (frame.opcode == Codec.OP_LIST_MEMBERS) {
                listMembersCount.incrementAndGet();
                replyOp[0] = Codec.OP_LIST_MEMBERS_RESPONSE;
                return Codec.encodeListMembersResponse(
                        new Codec.ListMembersResponse(0, 0, Collections.emptyList(), Collections.emptyList()));
            }
            throw new ProtocolException("unexpected opcode " + frame.opcode);
        }

        @Override
        public void close() {
            try {
                listen.close();
            } catch (IOException ignored) {
                // best-effort
            }
            try {
                acceptThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void createTopicError14RedirectsViaControllerId() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueCreateTopicError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueCreateTopicOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int id = c.createTopic("events", 1);
                assertEquals(1, id);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.createTopicCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.createTopicCount.get());
        }
    }

    @Test
    void createPartitionsError14NoHintPicksOtherBroker() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.createPartitionsCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int n = c.createPartitions("events", 4);
                assertEquals(4, n);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.createPartitionsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.createPartitionsCount.get());
        }
    }

    @Test
    void createPartitionsPrefersMetadataControllerId() throws Exception {
        try (AdminBroker controller = AdminBroker.start();
                AdminBroker decoy = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.createPartitionsCodes.add(NOT_CONTROLLER);
            List<Metadata.BrokerInfo> brokers = new ArrayList<>();
            brokers.add(new Metadata.BrokerInfo(1, "127.0.0.1", follower.port));
            brokers.add(new Metadata.BrokerInfo(3, "127.0.0.1", decoy.port));
            brokers.add(new Metadata.BrokerInfo(2, "127.0.0.1", controller.port));
            follower.meta = new Metadata(brokers, Collections.emptyList(), 2);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int n = c.createPartitions("events", 4);
                assertEquals(4, n);
                assertEquals("127.0.0.1:" + controller.port, c.addr());
            }
            assertEquals(1, follower.createPartitionsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, controller.createPartitionsCount.get());
            assertEquals(0, decoy.createPartitionsCount.get());
        }
    }

    @Test
    void createPartitionsMetadataControllerIdZeroPicksOther() throws Exception {
        try (AdminBroker later = AdminBroker.start();
                AdminBroker firstOther = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.createPartitionsCodes.add(NOT_CONTROLLER);
            List<Metadata.BrokerInfo> brokers = new ArrayList<>();
            brokers.add(new Metadata.BrokerInfo(1, "127.0.0.1", follower.port));
            brokers.add(new Metadata.BrokerInfo(3, "127.0.0.1", firstOther.port));
            brokers.add(new Metadata.BrokerInfo(2, "127.0.0.1", later.port));
            follower.meta = new Metadata(brokers, Collections.emptyList(), 0);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int n = c.createPartitions("events", 4);
                assertEquals(4, n);
                assertEquals("127.0.0.1:" + firstOther.port, c.addr());
            }
            assertEquals(1, follower.createPartitionsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, firstOther.createPartitionsCount.get());
            assertEquals(0, later.createPartitionsCount.get());
        }
    }

    @Test
    void createTopicMessageControllerIdWinsOverMetadata() throws Exception {
        try (AdminBroker hinted = AdminBroker.start();
                AdminBroker metaCtrl = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueCreateTopicError(NOT_CONTROLLER, "not controller; controller_id=3");
            List<Metadata.BrokerInfo> brokers = new ArrayList<>();
            brokers.add(new Metadata.BrokerInfo(1, "127.0.0.1", follower.port));
            brokers.add(new Metadata.BrokerInfo(2, "127.0.0.1", metaCtrl.port));
            brokers.add(new Metadata.BrokerInfo(3, "127.0.0.1", hinted.port));
            follower.meta = new Metadata(brokers, Collections.emptyList(), 2);
            hinted.queueCreateTopicOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int id = c.createTopic("events", 1);
                assertEquals(1, id);
                assertEquals("127.0.0.1:" + hinted.port, c.addr());
            }
            assertEquals(1, follower.createTopicCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, hinted.createTopicCount.get());
            assertEquals(0, metaCtrl.createTopicCount.get());
        }
    }

    @Test
    void maxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.queueCreateTopicError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex = assertThrows(BrokerException.class, () -> c.createTopic("events", 1));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.createTopicCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void helperNoOtherBrokerRaises14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.createPartitionsCodes.add(NOT_CONTROLLER);
            follower.meta = new Metadata(
                    Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", follower.port)),
                    Collections.emptyList());
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createPartitions("events", 4));
                assertEquals(NOT_CONTROLLER, ex.code);
                assertEquals("127.0.0.1:" + follower.port, c.addr());
            }
            assertEquals(1, follower.createPartitionsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void helperEmptyHostRaises14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.createPartitionsCodes.add(NOT_CONTROLLER);
            follower.meta = new Metadata(
                    Collections.singletonList(new Metadata.BrokerInfo(2, "", 9092)),
                    Collections.emptyList());
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createPartitions("events", 4));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.createPartitionsCount.get());
            assertEquals(1, follower.metadataCount.get());
        }
    }

    @Test
    void createAclsError14ThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.createAclsCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.createAcls(List.of(new AclBinding("User:alice", 0, "events", 3, 1)));
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.createAclsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.createAclsCount.get());
        }
    }

    @Test
    void reassignPartitionsError14ThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.reassignCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int gen = c.reassignPartitions("events", new int[] {1, 2});
                assertEquals(7, gen);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.reassignCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.reassignCount.get());
        }
    }

    @Test
    void createScramUserError14RedirectsViaControllerId() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueCreateScramError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueCreateScramOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.createScramUser("alice", "s3cret");
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.createScramCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.createScramCount.get());
        }
    }

    @Test
    void listAclsTyped14NoHintThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.listAclsCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<AclBinding> listed = c.listAcls();
                assertEquals(0, listed.size());
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.listAclsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listAclsCount.get());
        }
    }

    @Test
    void deleteScramUserMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.deleteScramCodes.add(NOT_CONTROLLER);
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex = assertThrows(BrokerException.class, () -> c.deleteScramUser("alice"));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.deleteScramCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void listScramUsersError14ThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.listScramCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<String> names = c.listScramUsers();
                assertEquals(Collections.singletonList("alice"), names);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.listScramCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listScramCount.get());
        }
    }

    @Test
    void addBrokerError14RedirectsViaControllerId() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueAddBrokerError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueAddBrokerOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                long gen = c.addBroker(3, "10.0.0.3", 9092);
                assertEquals(11L, gen);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.addBrokerCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.addBrokerCount.get());
        }
    }

    @Test
    void removeBrokerTyped14NoHintThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.removeBrokerCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                long gen = c.removeBroker(3);
                assertEquals(12L, gen);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.removeBrokerCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.removeBrokerCount.get());
        }
    }

    @Test
    void addBrokerMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.queueAddBrokerError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.addBroker(3, "10.0.0.3", 9092));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.addBrokerCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void describeConfigsError14RedirectsViaControllerId() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueDescribeConfigsError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueDescribeConfigsOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                DescribeConfigsResult got = c.describeConfigs("events");
                assertEquals("events", got.topic);
                assertEquals(1L, got.topicId);
                assertEquals(1L, got.partitionCount);
                assertEquals(1, got.configs.size());
                assertEquals("retention.ms", got.configs.get(0)[0]);
                assertEquals("86400000", got.configs.get(0)[1]);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.describeConfigsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.describeConfigsCount.get());
        }
    }

    @Test
    void alterConfigsTyped14NoHintThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.alterConfigsCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.alterConfigs(
                        "events",
                        Collections.singletonList(new String[] {"retention.ms", "86400000"}));
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.alterConfigsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.alterConfigsCount.get());
        }
    }

    @Test
    void describeConfigsMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.queueDescribeConfigsError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.describeConfigs("events"));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.describeConfigsCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void deleteOffsetsError14RedirectsViaControllerId() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueDeleteOffsetsError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueDeleteOffsetsOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int got = c.deleteOffsets("g");
                assertEquals(3, got);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.deleteOffsetsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.deleteOffsetsCount.get());
        }
    }

    @Test
    void deleteOffsetsTyped14NoHintThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.deleteOffsetsCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int got = c.deleteOffsets(
                        "g", Collections.singletonList(new Codec.OffsetEntry("events", 0)));
                assertEquals(3, got);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.deleteOffsetsCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.deleteOffsetsCount.get());
        }
    }

    @Test
    void deleteOffsetsMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.queueDeleteOffsetsError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteOffsets("g"));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.deleteOffsetsCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnCreateTopicTimeout() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.createTopicReplies.add(new int[] {7, 0});
            srv.createTopicMessages.add("");
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createTopic("events", 1));
                assertEquals(7, ex.code);
            }
            assertEquals(1, srv.createTopicCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void createTopicRetriesTimeoutThenOk() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.createTopicReplies.add(new int[] {7, 0});
            srv.createTopicMessages.add("");
            srv.queueCreateTopicOk();
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                assertEquals(1, c.createTopic("events", 1));
            }
            assertEquals(2, srv.createTopicCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void createTopic14RedirectNotCountedAsRetry() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueCreateTopicError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueCreateTopicOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                int id = c.createTopic("events", 1);
                assertEquals(1, id);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.createTopicCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.createTopicCount.get());
        }
    }

    @Test
    void createTopicNotFoundNotRetried() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.createTopicReplies.add(new int[] {2, 0});
            srv.createTopicMessages.add("");
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createTopic("events", 1));
                assertEquals(2, ex.code);
            }
            assertEquals(1, srv.createTopicCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void createTopicSendsEmptyConfigs() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.queueCreateTopicOk();
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(1, c.createTopic("events", 1));
            }
            assertEquals(1, srv.createTopicCount.get());
            assertTrue(srv.lastCreateTopicConfigs.isEmpty());
        }
    }

    @Test
    void createTopicWithConfigsSendsPairsAndReturnsTopicId() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.queueCreateTopicOk(42);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int id = c.createTopic(
                        "events", 1, Collections.singletonList(new String[] {"retention.ms", "1000"}));
                assertEquals(42, id);
            }
            assertEquals(1, srv.createTopicCount.get());
            assertEquals(1, srv.lastCreateTopicConfigs.size());
            assertEquals("retention.ms", srv.lastCreateTopicConfigs.get(0)[0]);
            assertEquals("1000", srv.lastCreateTopicConfigs.get(0)[1]);
        }
    }

    @Test
    void createTopicWithConfigsError14Redirects() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueCreateTopicError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueCreateTopicOk(7);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                int id = c.createTopic(
                        "events", 1, Collections.singletonList(new String[] {"retention.ms", "1000"}));
                assertEquals(7, id);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.createTopicCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.createTopicCount.get());
            assertEquals(1, leader.lastCreateTopicConfigs.size());
            assertEquals("retention.ms", leader.lastCreateTopicConfigs.get(0)[0]);
            assertEquals("1000", leader.lastCreateTopicConfigs.get(0)[1]);
        }
    }

    @Test
    void createTopicExhaustedRetriesRaises() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.createTopicReplies.add(new int[] {7, 0});
            srv.createTopicMessages.add("");
            srv.createTopicReplies.add(new int[] {7, 0});
            srv.createTopicMessages.add("");
            srv.createTopicReplies.add(new int[] {7, 0});
            srv.createTopicMessages.add("");
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createTopic("events", 1));
                assertEquals(7, ex.code);
            }
            assertEquals(3, srv.createTopicCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void createAclsRetriesTimeoutThenOk() throws Exception {
        try (AdminBroker srv = AdminBroker.start()) {
            srv.createAclsCodes.add(7);
            srv.createAclsCodes.add(0);
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                c.createAcls(List.of(new AclBinding("User:alice", 0, "events", 3, 1)));
            }
            assertEquals(2, srv.createAclsCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void offsetCommitError14RedirectsViaControllerId() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.queueOffsetCommitError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", leader.port);
            leader.queueOffsetCommitOk();
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.offsetCommit("g", "t", 0, 5);
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.offsetCommitCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.offsetCommitCount.get());
        }
    }

    @Test
    void offsetFetchTyped14NoHintThenOk() throws Exception {
        try (AdminBroker leader = AdminBroker.start();
                AdminBroker follower = AdminBroker.start()) {
            follower.offsetFetchCodes.add(NOT_CONTROLLER);
            follower.meta = otherBrokerMeta(follower.port, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<Offset> offs = c.offsetFetch("g", "t");
                assertTrue(offs.isEmpty());
                assertEquals(leader.port, Integer.parseInt(c.addr().substring(c.addr().indexOf(':') + 1)));
            }
            assertEquals(1, follower.offsetFetchCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.offsetFetchCount.get());
        }
    }

    @Test
    void offsetCommitMaxRedirectsZeroRaisesOnFirst14() throws Exception {
        try (AdminBroker follower = AdminBroker.start()) {
            follower.queueOffsetCommitError(NOT_CONTROLLER, "not controller; controller_id=2");
            follower.meta = controllerMeta(2, "127.0.0.1", 9);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.offsetCommit("g", "t", 0, 5));
                assertEquals(NOT_CONTROLLER, ex.code);
            }
            assertEquals(1, follower.offsetCommitCount.get());
            assertEquals(0, follower.metadataCount.get());
            assertEquals(1, follower.acceptCount.get());
        }
    }

}
