package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** v0.116: Java Client Metadata topic filter ({@code metadata(List)}). */
class MetadataTopicsTest {
    private static final int TIMEOUT = 7;

    @Test
    void metadataSendsEmptyTopicsList() throws Exception {
        try (MetaTopicsStub srv = MetaTopicsStub.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                Metadata got = c.metadata();
                assertTrue(got.brokers.isEmpty());
                assertTrue(got.topics.isEmpty());
            }
            assertEquals(1, srv.metadataCount.get());
            assertEquals(List.of(List.of()), srv.seenTopics());
        }
    }

    @Test
    void metadataListEncodesNamedFilter() throws Exception {
        try (MetaTopicsStub srv = MetaTopicsStub.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                Metadata got = c.metadata(List.of("events"));
                assertTrue(got.brokers.isEmpty());
            }
            assertEquals(1, srv.metadataCount.get());
            assertEquals(List.of(List.of("events")), srv.seenTopics());
        }
    }

    @Test
    void metadataEmptyListMatchesAllTopics() throws Exception {
        try (MetaTopicsStub srv = MetaTopicsStub.start()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.metadata();
                c.metadata(Collections.emptyList());
                c.metadata(List.of());
            }
            assertEquals(3, srv.metadataCount.get());
            List<List<String>> seen = srv.seenTopics();
            assertEquals(3, seen.size());
            for (List<String> topics : seen) {
                assertTrue(topics.isEmpty());
            }
        }
    }

    @Test
    void metadataStillRetriesTimeout() throws Exception {
        try (MetaTopicsStub srv = MetaTopicsStub.start()) {
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
            assertEquals(List.of(List.of(), List.of()), srv.seenTopics());
        }
    }

    private static final class MetaTopicsStub implements AutoCloseable {
        final int port;
        final List<Integer> metadataCodes = new CopyOnWriteArrayList<>();
        final AtomicInteger metadataCount = new AtomicInteger();
        private final List<List<String>> seenTopics = new CopyOnWriteArrayList<>();
        private final ServerSocket listen;
        private final Thread acceptThread;

        static MetaTopicsStub start() throws IOException {
            return new MetaTopicsStub();
        }

        private MetaTopicsStub() throws IOException {
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            acceptThread = new Thread(
                    () -> {
                        while (!listen.isClosed()) {
                            try {
                                Socket conn = listen.accept();
                                Thread t = new Thread(() -> serve(conn), "volant-meta-topics-conn");
                                t.setDaemon(true);
                                t.start();
                            } catch (IOException e) {
                                return;
                            }
                        }
                    },
                    "volant-meta-topics-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        List<List<String>> seenTopics() {
            return new ArrayList<>(seenTopics);
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
                    if (d.frame.opcode != Codec.OP_METADATA) {
                        throw new ProtocolException("unexpected opcode " + d.frame.opcode);
                    }
                    Codec.MetadataRequest req = Codec.decodeMetadataRequest(d.frame.payload);
                    metadataCount.incrementAndGet();
                    seenTopics.add(new ArrayList<>(req.topics));
                    int code = 0;
                    if (!metadataCodes.isEmpty()) {
                        code = metadataCodes.remove(0);
                    }
                    byte[] payload;
                    int replyOp = Codec.OP_METADATA;
                    if (code != 0) {
                        replyOp = Codec.OP_ERROR;
                        payload = Codec.encodeErrorResponse(new Codec.ErrorResponse(code, ""));
                    } else {
                        payload = Codec.encodeMetadataResponse(
                                new Metadata(Collections.emptyList(), Collections.emptyList()));
                    }
                    out.write(Frame.encode(replyOp, d.frame.correlationId, payload));
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
