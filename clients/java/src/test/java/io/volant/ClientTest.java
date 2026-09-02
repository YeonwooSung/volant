package io.volant;

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
    private static final int NOT_FOUND = 2;

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
                c.produce("t", 0, null, "hello".getBytes(StandardCharsets.UTF_8));
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
        final List<Integer> fetchCodes = new CopyOnWriteArrayList<>();
        final List<Integer> heartbeatCodes = new CopyOnWriteArrayList<>();
        final List<Integer> offsetCommitCodes = new CopyOnWriteArrayList<>();
        final List<Integer> offsetFetchCodes = new CopyOnWriteArrayList<>();
        final List<Integer> deleteOffsetsCodes = new CopyOnWriteArrayList<>();
        final List<Integer> listOffsetsCodes = new CopyOnWriteArrayList<>();
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final List<Codec.ProduceRequest> produceReqs = new CopyOnWriteArrayList<>();
        final List<Codec.FetchRequest> fetchReqs = new CopyOnWriteArrayList<>();
        final List<String> initTxnIds = new CopyOnWriteArrayList<>();
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger produceCount = new AtomicInteger();
        final AtomicInteger fetchCount = new AtomicInteger();
        final AtomicInteger heartbeatCount = new AtomicInteger();
        final AtomicInteger offsetCommitCount = new AtomicInteger();
        final AtomicInteger offsetFetchCount = new AtomicInteger();
        final AtomicInteger deleteOffsetsCount = new AtomicInteger();
        final AtomicInteger listOffsetsCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();
        volatile long initPid = 42L;
        volatile int initEpoch = 1;

        private final ServerSocket listen;
        private final Thread acceptThread;

        static ScriptedBroker start() throws IOException {
            return new ScriptedBroker();
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
                replyOp[0] = Codec.OP_INIT_PRODUCER_ID_RESPONSE;
                return Codec.encodeInitProducerIdResponse(
                        new Codec.InitProducerIdResponse(initPid, initEpoch, 0));
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
                        new Codec.FetchResponse(req.topic, req.partition, 0, code, Collections.emptyList()));
            }
            if (frame.opcode == Codec.OP_HEARTBEAT) {
                heartbeatCount.incrementAndGet();
                int code = 0;
                if (!heartbeatCodes.isEmpty()) {
                    code = heartbeatCodes.remove(0);
                }
                return Codec.encodeHeartbeatResponse(new Codec.HeartbeatResponse(code));
            }
            if (frame.opcode == Codec.OP_OFFSET_COMMIT) {
                offsetCommitCount.incrementAndGet();
                int code = 0;
                if (!offsetCommitCodes.isEmpty()) {
                    code = offsetCommitCodes.remove(0);
                }
                return Codec.encodeOffsetCommitResponse(new Codec.OffsetCommitResponse(code));
            }
            if (frame.opcode == Codec.OP_OFFSET_FETCH) {
                offsetFetchCount.incrementAndGet();
                int code = 0;
                if (!offsetFetchCodes.isEmpty()) {
                    code = offsetFetchCodes.remove(0);
                }
                return Codec.encodeOffsetFetchResponse(
                        new Codec.OffsetFetchResponse(code, Collections.emptyList()));
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
            if (frame.opcode == Codec.OP_METADATA) {
                metadataCount.incrementAndGet();
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
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        final AtomicInteger createTopicCount = new AtomicInteger();
        final AtomicInteger createPartitionsCount = new AtomicInteger();
        final AtomicInteger createAclsCount = new AtomicInteger();
        final AtomicInteger reassignCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger listMembersCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();

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
            createTopicReplies.add(new int[] {0, 0});
            createTopicMessages.add("");
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
                int code = 0;
                boolean asError = false;
                String message = "";
                if (!createTopicReplies.isEmpty()) {
                    int[] spec = createTopicReplies.remove(0);
                    code = spec[0];
                    asError = spec[1] != 0;
                    message = createTopicMessages.isEmpty() ? "" : createTopicMessages.remove(0);
                }
                if (asError) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, message));
                }
                return Codec.encodeCreateTopicResponse(
                        new Codec.CreateTopicResponse(code == 0 ? 1 : 0, req.name, code == 0 ? req.partitions : 0, code));
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

}
