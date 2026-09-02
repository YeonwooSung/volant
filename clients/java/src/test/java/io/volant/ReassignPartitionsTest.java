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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** ReassignPartitions client tests against a scripted TCP broker (no live server). */
class ReassignPartitionsTest {
    @Test
    void successReturnsGeneration() throws Exception {
        try (ReassignPartitionsServer srv = ReassignPartitionsServer.ok(7)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int got = c.reassignPartitions("events", Integer.valueOf(0), new int[] {1, 2});
                assertEquals(7, got);
            }
            assertEquals("events", srv.topic.get());
            assertEquals(0L, srv.partition.get());
            assertEquals(List.of(1L, 2L), srv.replicas.get());
        }
    }

    @Test
    void nullPartitionEncodesAllSentinel() throws Exception {
        try (ReassignPartitionsServer srv = ReassignPartitionsServer.ok(3)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int got = c.reassignPartitions("events");
                assertEquals(3, got);
            }
            assertEquals("events", srv.topic.get());
            assertEquals(Codec.REASSIGN_ALL_PARTITIONS, srv.partition.get());
            assertTrue(srv.replicas.get().isEmpty());
        }
    }

    @Test
    void nonzeroErrorCodeRaises() throws Exception {
        try (ReassignPartitionsServer srv = ReassignPartitionsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.reassignPartitions("missing", new int[] {1, 2}));
                assertEquals(2, ex.code);
                assertEquals("reassign_partitions", ex.op);
            }
        }
    }

    private static final class ReassignPartitionsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final AtomicLong partition = new AtomicLong();
        final AtomicReference<List<Long>> replicas = new AtomicReference<>(List.of());
        private final int errorCode;
        private final long generation;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        static ReassignPartitionsServer ok(long generation) throws IOException {
            return new ReassignPartitionsServer(0, generation);
        }

        static ReassignPartitionsServer error(int code) throws IOException {
            return new ReassignPartitionsServer(code, 0);
        }

        private ReassignPartitionsServer(int errorCode, long generation) throws IOException {
            this.errorCode = errorCode;
            this.generation = generation;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-reassign-partitions");
            thread.setDaemon(true);
            thread.start();
        }

        private void serve() {
            try (Socket conn = listen.accept()) {
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
                    if (d.frame.opcode != Codec.OP_REASSIGN_PARTITIONS) {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    Codec.ReassignPartitionsRequest req =
                            Codec.decodeReassignPartitionsRequest(d.frame.payload);
                    topic.set(req.topic);
                    partition.set(req.partition);
                    replicas.set(new ArrayList<>(req.replicas));
                    long gen = errorCode != 0 ? 0 : generation;
                    byte[] payload = Codec.encodeReassignPartitionsResponse(
                            new Codec.ReassignPartitionsResponse(errorCode, gen));
                    out.write(
                            Frame.encode(
                                    Codec.OP_REASSIGN_PARTITIONS_RESPONSE,
                                    d.frame.correlationId,
                                    payload));
                    out.flush();
                }
            } catch (Exception e) {
                error.set(e);
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
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
