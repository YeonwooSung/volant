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
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final List<Codec.ProduceRequest> produceReqs = new CopyOnWriteArrayList<>();
        final List<Codec.FetchRequest> fetchReqs = new CopyOnWriteArrayList<>();
        final List<String> initTxnIds = new CopyOnWriteArrayList<>();
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger produceCount = new AtomicInteger();
        final AtomicInteger fetchCount = new AtomicInteger();
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

}
