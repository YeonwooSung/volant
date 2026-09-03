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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** ListOffsets client tests against a scripted TCP broker (no live server). */
class ListOffsetsTest {
    private static final int NOT_LEADER = Client.NOT_LEADER_FOR_PARTITION;

    @Test
    void emptyPartitionsEncodedAsCountZero() throws Exception {
        List<OffsetListing> entries =
                Arrays.asList(new OffsetListing(0, 0, 10), new OffsetListing(1, 2, 5));
        try (ListOffsetsServer srv = ListOffsetsServer.ok(entries)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetListing> got = c.listOffsets("events");
                assertEquals(entries, got);
            }
            assertEquals("events", srv.topic.get());
            assertTrue(srv.partitions.isEmpty());
        }
    }

    @Test
    void explicitPartitionsRoundtrip() throws Exception {
        List<OffsetListing> entries = Collections.singletonList(new OffsetListing(0, 0, 10));
        try (ListOffsetsServer srv = ListOffsetsServer.ok(entries)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<OffsetListing> got = c.listOffsets("events", 0, 1);
                assertEquals(entries, got);
            }
            assertEquals(Arrays.asList(0, 1), srv.partitions);
        }
    }

    @Test
    void nonzeroErrorCodeRaises() throws Exception {
        try (ListOffsetsServer srv = ListOffsetsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.listOffsets("missing"));
                assertEquals(2, ex.code);
                assertEquals("list_offsets", ex.op);
            }
        }
    }

    @Test
    void error13RedirectsToLeader() throws Exception {
        List<OffsetListing> entries = Collections.singletonList(new OffsetListing(0, 0, 10));
        try (ListOffsetsServer leader = ListOffsetsServer.ok(entries);
                ListOffsetsServer follower = ListOffsetsServer.errorOpcode(13)) {
            follower.meta = leaderMeta("events", 0, 2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<OffsetListing> got = c.listOffsets("events");
                assertEquals(entries, got);
                assertEquals("127.0.0.1:" + leader.port, c.addr());
            }
            assertEquals(1, follower.listCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listCount.get());
            assertEquals("events", leader.topic.get());
            assertTrue(leader.partitions.isEmpty());
        }
    }

    @Test
    void typedError13RedirectsToLeader() throws Exception {
        List<OffsetListing> entries = Collections.singletonList(new OffsetListing(0, 0, 10));
        try (ListOffsetsServer leader = ListOffsetsServer.ok(entries);
                ListOffsetsServer follower = ListOffsetsServer.error(13)) {
            follower.meta = leaderMeta("events", 0, 2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                List<OffsetListing> got = c.listOffsets("events");
                assertEquals(entries, got);
                assertEquals("127.0.0.1:" + leader.port, c.addr());
            }
            assertEquals(1, follower.listCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.listCount.get());
            assertEquals(List.of(Codec.OP_LIST_OFFSETS, Codec.OP_METADATA), follower.opcodes);
        }
    }

    @Test
    void error13MaxRedirectsZeroRaises() throws Exception {
        try (ListOffsetsServer srv = ListOffsetsServer.error(13)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.listOffsets("events"));
                assertEquals(NOT_LEADER, ex.code);
                assertEquals("list_offsets", ex.op);
            }
            assertEquals(List.of(Codec.OP_LIST_OFFSETS), srv.opcodes);
            assertEquals(1, srv.listCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void retriesTimeoutThenOkNoMetadata() throws Exception {
        List<OffsetListing> entries = Collections.singletonList(new OffsetListing(0, 0, 10));
        try (ListOffsetsServer srv = ListOffsetsServer.codes(entries, 7, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                List<OffsetListing> got = c.listOffsets("events");
                assertEquals(entries, got);
            }
            assertEquals(List.of(Codec.OP_LIST_OFFSETS, Codec.OP_LIST_OFFSETS), srv.opcodes);
            assertEquals(2, srv.listCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void notFoundNotRetried() throws Exception {
        try (ListOffsetsServer srv = ListOffsetsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.listOffsets("missing"));
                assertEquals(2, ex.code);
                assertEquals("list_offsets", ex.op);
            }
            assertEquals(List.of(Codec.OP_LIST_OFFSETS), srv.opcodes);
            assertEquals(1, srv.listCount.get());
            assertEquals(0, srv.metadataCount.get());
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

    private static final class ListOffsetsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final List<Integer> partitions = new CopyOnWriteArrayList<>();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final List<Integer> errorCodes = new CopyOnWriteArrayList<>();
        final AtomicInteger listCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        private final List<OffsetListing> entries;
        private final boolean errorAsOpcode;
        private final ServerSocket listen;
        private final Thread acceptThread;

        static ListOffsetsServer ok(List<OffsetListing> entries) throws IOException {
            return new ListOffsetsServer(entries, false, 0);
        }

        static ListOffsetsServer error(int code) throws IOException {
            return new ListOffsetsServer(Collections.emptyList(), false, code);
        }

        static ListOffsetsServer errorOpcode(int code) throws IOException {
            return new ListOffsetsServer(Collections.emptyList(), true, code);
        }

        static ListOffsetsServer codes(List<OffsetListing> entries, int... codes) throws IOException {
            return new ListOffsetsServer(entries, false, codes);
        }

        private ListOffsetsServer(List<OffsetListing> entries, boolean errorAsOpcode, int... codes)
                throws IOException {
            this.entries = entries;
            this.errorAsOpcode = errorAsOpcode;
            if (codes.length == 0) {
                this.errorCodes.add(0);
            } else {
                for (int code : codes) {
                    this.errorCodes.add(code);
                }
            }
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            acceptThread = new Thread(
                    () -> {
                        while (!listen.isClosed()) {
                            try {
                                Socket conn = listen.accept();
                                acceptCount.incrementAndGet();
                                Thread t = new Thread(() -> serve(conn), "volant-list-offsets-conn");
                                t.setDaemon(true);
                                t.start();
                            } catch (IOException e) {
                                return;
                            }
                        }
                    },
                    "volant-list-offsets-accept");
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
            if (frame.opcode == Codec.OP_LIST_OFFSETS) {
                listCount.incrementAndGet();
                Codec.ListOffsetsRequest req = Codec.decodeListOffsetsRequest(frame.payload);
                topic.set(req.topic);
                partitions.clear();
                partitions.addAll(req.partitions);
                int code = 0;
                if (!errorCodes.isEmpty()) {
                    code = errorCodes.remove(0);
                }
                if (errorAsOpcode && code != 0) {
                    replyOp[0] = Codec.OP_ERROR;
                    return Codec.encodeErrorResponse(new Codec.ErrorResponse(code, ""));
                }
                replyOp[0] = Codec.OP_LIST_OFFSETS_RESPONSE;
                return Codec.encodeListOffsetsResponse(
                        new Codec.ListOffsetsResponse(
                                code, req.topic, code == 0 ? entries : Collections.emptyList()));
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
