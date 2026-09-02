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
        final AtomicInteger produceCount = new AtomicInteger();
        final AtomicInteger fetchCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();

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
                    byte[] payload = handle(d.frame);
                    out.write(Frame.encode(d.frame.opcode, d.frame.correlationId, payload));
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

        private byte[] handle(Frame frame) {
            if (frame.opcode == Codec.OP_PRODUCE) {
                produceCount.incrementAndGet();
                Codec.ProduceRequest req = Codec.decodeProduceRequest(frame.payload);
                int code = 0;
                if (!produceCodes.isEmpty()) {
                    code = produceCodes.remove(0);
                }
                long part = req.partition >= 0 ? req.partition : 0;
                return Codec.encodeProduceResponse(
                        new Codec.ProduceResponse(req.topic, part, code == 0 ? 7 : 0, code == 0 ? 1 : 0, code));
            }
            if (frame.opcode == Codec.OP_FETCH) {
                fetchCount.incrementAndGet();
                Codec.FetchRequest req = Codec.decodeFetchRequest(frame.payload);
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
}
