package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** CreatePartitions client tests against a scripted TCP broker (no live server). */
class CreatePartitionsTest {
    @Test
    void successReturnsNewCount() throws Exception {
        try (CreatePartitionsServer srv = CreatePartitionsServer.ok(4)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int got = c.createPartitions("events", 4);
                assertEquals(4, got);
            }
            assertEquals("events", srv.topic.get());
            assertEquals(4, srv.totalCount.get());
        }
    }

    @Test
    void nonzeroErrorCodeRaises() throws Exception {
        try (CreatePartitionsServer srv = CreatePartitionsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createPartitions("missing", 4));
                assertEquals(2, ex.code);
                assertEquals("create_partitions", ex.op);
            }
        }
    }

    private static final class CreatePartitionsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final AtomicInteger totalCount = new AtomicInteger();
        private final int errorCode;
        private final int partitions;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        static CreatePartitionsServer ok(int partitions) throws IOException {
            return new CreatePartitionsServer(0, partitions);
        }

        static CreatePartitionsServer error(int code) throws IOException {
            return new CreatePartitionsServer(code, 0);
        }

        private CreatePartitionsServer(int errorCode, int partitions) throws IOException {
            this.errorCode = errorCode;
            this.partitions = partitions;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-create-partitions");
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
                    if (d.frame.opcode != Codec.OP_CREATE_PARTITIONS) {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    Codec.CreatePartitionsRequest req =
                            Codec.decodeCreatePartitionsRequest(d.frame.payload);
                    topic.set(req.topic);
                    totalCount.set((int) req.totalCount);
                    int newTotal = errorCode != 0 ? 0 : partitions;
                    byte[] payload = Codec.encodeCreatePartitionsResponse(
                            new Codec.CreatePartitionsResponse(errorCode, req.topic, newTotal));
                    out.write(
                            Frame.encode(
                                    Codec.OP_CREATE_PARTITIONS_RESPONSE,
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
