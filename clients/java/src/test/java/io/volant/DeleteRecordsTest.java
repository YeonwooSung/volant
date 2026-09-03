package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** DeleteRecords client tests against a scripted TCP broker (no live server). */
class DeleteRecordsTest {
    private static final int NOT_LEADER = Client.NOT_LEADER_FOR_PARTITION;

    @Test
    void successReturnsLowWatermark() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.ok(96)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                DeleteRecordsResult got = c.deleteRecords("events", 2, 100);
                assertEquals(new DeleteRecordsResult("events", 2, 96), got);
                DeleteRecordsResult flagged = c.deleteRecords("events", 2, 100, 1);
                assertEquals(96, flagged.lowWatermark);
            }
            assertEquals("events", srv.topic.get());
            assertEquals(2, srv.partition.get());
            assertEquals(100, srv.beforeOffset.get());
            assertEquals(1, srv.waitMajority.get());
            assertEquals(List.of(Codec.OP_DELETE_RECORDS, Codec.OP_DELETE_RECORDS), srv.opcodes);
        }
    }

    @Test
    void defaultWaitMajorityZero() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.ok(96)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.deleteRecordsWait());
                c.deleteRecords("events", 2, 100);
            }
            assertEquals(0, srv.waitMajority.get());
        }
    }

    @Test
    void setDeleteRecordsWait() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.ok(96)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setDeleteRecordsWait(1);
                assertEquals(1, c.deleteRecordsWait());
                c.deleteRecords("events", 2, 100);
            }
            assertEquals(1, srv.waitMajority.get());
        }
    }

    @Test
    void explicitWaitFlagWins() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.ok(96)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setDeleteRecordsWait(1);
                c.deleteRecords("events", 2, 100, 2);
            }
            assertEquals(2, srv.waitMajority.get());
        }
    }

    @Test
    void error13MaxRedirectsZeroRaises() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.error(13)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRedirects(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteRecords("events", 0, 10));
                assertEquals(NOT_LEADER, ex.code);
                assertEquals("delete_records", ex.op);
            }
            assertEquals(List.of(Codec.OP_DELETE_RECORDS), srv.opcodes);
            assertEquals(1, srv.deleteCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void error13RedirectsToLeader() throws Exception {
        try (DeleteRecordsServer leader = DeleteRecordsServer.ok(96);
                DeleteRecordsServer follower = DeleteRecordsServer.error(13)) {
            follower.meta = leaderMeta("events", 2, 2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                DeleteRecordsResult got = c.deleteRecords("events", 2, 100, 1);
                assertEquals(new DeleteRecordsResult("events", 2, 96), got);
                assertEquals("127.0.0.1:" + leader.port, c.addr());
            }
            assertEquals(1, follower.deleteCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.deleteCount.get());
            assertEquals(1, leader.waitMajority.get());
            assertEquals(100, leader.beforeOffset.get());
        }
    }

    @Test
    void error13UnknownTopicRaises() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.error(13)) {
            srv.meta = new Metadata(
                    Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", srv.port)),
                    Collections.emptyList());
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteRecords("events", 0, 10));
                assertEquals(NOT_LEADER, ex.code);
                assertEquals("delete_records", ex.op);
                assertEquals("127.0.0.1:" + srv.port, c.addr());
            }
            assertEquals(1, srv.deleteCount.get());
            assertEquals(1, srv.metadataCount.get());
            assertEquals(1, srv.acceptCount.get());
        }
    }

    @Test
    void defaultMaxRetriesZeroRaisesOnTimeout() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.error(7)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteRecords("events", 0, 10));
                assertEquals(7, ex.code);
                assertEquals("delete_records", ex.op);
            }
            assertEquals(List.of(Codec.OP_DELETE_RECORDS), srv.opcodes);
            assertEquals(1, srv.deleteCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void retriesTimeoutThenOk() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.codes(96, 7, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                DeleteRecordsResult got = c.deleteRecords("events", 2, 100);
                assertEquals(new DeleteRecordsResult("events", 2, 96), got);
            }
            assertEquals(List.of(Codec.OP_DELETE_RECORDS, Codec.OP_DELETE_RECORDS), srv.opcodes);
            assertEquals(2, srv.deleteCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void error13RedirectNotCountedAsRetry() throws Exception {
        try (DeleteRecordsServer leader = DeleteRecordsServer.ok(96);
                DeleteRecordsServer follower = DeleteRecordsServer.error(13)) {
            follower.meta = leaderMeta("events", 2, 2, "127.0.0.1", leader.port);
            try (Client c = Client.connect("127.0.0.1", follower.port, 5_000)) {
                assertEquals(0, c.maxRetries());
                DeleteRecordsResult got = c.deleteRecords("events", 2, 100, 1);
                assertEquals(new DeleteRecordsResult("events", 2, 96), got);
                assertEquals("127.0.0.1:" + leader.port, c.addr());
            }
            assertEquals(1, follower.deleteCount.get());
            assertEquals(1, follower.metadataCount.get());
            assertEquals(1, leader.deleteCount.get());
            assertEquals(1, leader.waitMajority.get());
        }
    }

    @Test
    void notFoundNotRetried() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteRecords("events", 0, 10));
                assertEquals(2, ex.code);
                assertEquals("delete_records", ex.op);
            }
            assertEquals(List.of(Codec.OP_DELETE_RECORDS), srv.opcodes);
            assertEquals(1, srv.deleteCount.get());
            assertEquals(0, srv.metadataCount.get());
        }
    }

    @Test
    void exhaustedRetriesRaises() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.codes(0, 7, 7, 7)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.setMaxRetries(2);
                c.setRetryBackoffMs(0);
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteRecords("events", 0, 10));
                assertEquals(7, ex.code);
                assertEquals("delete_records", ex.op);
            }
            assertEquals(
                    List.of(Codec.OP_DELETE_RECORDS, Codec.OP_DELETE_RECORDS, Codec.OP_DELETE_RECORDS),
                    srv.opcodes);
            assertEquals(3, srv.deleteCount.get());
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

    private static final class DeleteRecordsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final AtomicInteger partition = new AtomicInteger();
        final AtomicLong beforeOffset = new AtomicLong();
        final AtomicInteger waitMajority = new AtomicInteger();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final List<Integer> errorCodes = new CopyOnWriteArrayList<>();
        final AtomicInteger deleteCount = new AtomicInteger();
        final AtomicInteger metadataCount = new AtomicInteger();
        final AtomicInteger acceptCount = new AtomicInteger();
        volatile Metadata meta = new Metadata(Collections.emptyList(), Collections.emptyList());
        private final long lowWatermark;
        private final ServerSocket listen;
        private final Thread acceptThread;

        static DeleteRecordsServer ok(long lowWatermark) throws IOException {
            return new DeleteRecordsServer(lowWatermark, 0);
        }

        static DeleteRecordsServer error(int code) throws IOException {
            return new DeleteRecordsServer(0, code);
        }

        static DeleteRecordsServer codes(long lowWatermark, int... codes) throws IOException {
            return new DeleteRecordsServer(lowWatermark, codes);
        }

        private DeleteRecordsServer(long lowWatermark, int... codes) throws IOException {
            this.lowWatermark = lowWatermark;
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
                                Thread t = new Thread(() -> serve(conn), "volant-delete-records-conn");
                                t.setDaemon(true);
                                t.start();
                            } catch (IOException e) {
                                return;
                            }
                        }
                    },
                    "volant-delete-records-accept");
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
            if (frame.opcode == Codec.OP_DELETE_RECORDS) {
                deleteCount.incrementAndGet();
                Codec.DeleteRecordsRequest req = Codec.decodeDeleteRecordsRequest(frame.payload);
                topic.set(req.topic);
                partition.set((int) req.partition);
                beforeOffset.set(req.beforeOffset);
                waitMajority.set(req.waitMajority);
                int code = 0;
                if (!errorCodes.isEmpty()) {
                    code = errorCodes.remove(0);
                }
                replyOp[0] = Codec.OP_DELETE_RECORDS_RESPONSE;
                return Codec.encodeDeleteRecordsResponse(
                        new Codec.DeleteRecordsResponse(
                                code, req.topic, req.partition, code == 0 ? lowWatermark : 0));
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
