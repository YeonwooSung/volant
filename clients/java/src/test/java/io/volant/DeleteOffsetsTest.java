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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** DeleteOffsets client tests against a scripted TCP broker (no live server). */
class DeleteOffsetsTest {
    @Test
    void emptyEntriesEncodedAsCountZero() throws Exception {
        try (DeleteOffsetsServer srv = DeleteOffsetsServer.ok(3)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int got = c.deleteOffsets("g");
                assertEquals(3, got);
            }
            assertEquals("g", srv.group.get());
            assertTrue(srv.entries.isEmpty());
        }
    }

    @Test
    void explicitEntryRoundtrip() throws Exception {
        try (DeleteOffsetsServer srv = DeleteOffsetsServer.ok(1)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int got = c.deleteOffsets(
                        "g", Collections.singletonList(new Codec.OffsetEntry("events", 0)));
                assertEquals(1, got);
            }
            assertEquals("g", srv.group.get());
            assertEquals(1, srv.entries.size());
            assertEquals("events", srv.entries.get(0).topic);
            assertEquals(0, srv.entries.get(0).partition);
        }
    }

    @Test
    void deleteOffsetEncodesOneEntry() throws Exception {
        try (DeleteOffsetsServer srv = DeleteOffsetsServer.ok(1)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int got = c.deleteOffset("g", "events", 0);
                assertEquals(1, got);
            }
            assertEquals("g", srv.group.get());
            assertEquals(1, srv.entries.size());
            assertEquals("events", srv.entries.get(0).topic);
            assertEquals(0, srv.entries.get(0).partition);
        }
    }

    @Test
    void nonzeroErrorCodeRaises() throws Exception {
        try (DeleteOffsetsServer srv = DeleteOffsetsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.deleteOffsets("missing"));
                assertEquals(2, ex.code);
                assertEquals("delete_offsets", ex.op);
            }
        }
    }

    private static final class DeleteOffsetsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> group = new AtomicReference<>();
        final List<Codec.OffsetEntry> entries = new CopyOnWriteArrayList<>();
        private final int errorCode;
        private final int deletedCount;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        static DeleteOffsetsServer ok(int deletedCount) throws IOException {
            return new DeleteOffsetsServer(0, deletedCount);
        }

        static DeleteOffsetsServer error(int code) throws IOException {
            return new DeleteOffsetsServer(code, 0);
        }

        private DeleteOffsetsServer(int errorCode, int deletedCount) throws IOException {
            this.errorCode = errorCode;
            this.deletedCount = deletedCount;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-delete-offsets");
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
                    if (d.frame.opcode != Codec.OP_DELETE_OFFSETS) {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    Codec.DeleteOffsetsRequest req = Codec.decodeDeleteOffsetsRequest(d.frame.payload);
                    group.set(req.groupId);
                    entries.clear();
                    entries.addAll(req.entries);
                    byte[] payload = Codec.encodeDeleteOffsetsResponse(
                            new Codec.DeleteOffsetsResponse(errorCode, deletedCount));
                    out.write(Frame.encode(Codec.OP_DELETE_OFFSETS_RESPONSE, d.frame.correlationId, payload));
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
