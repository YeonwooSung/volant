package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** DeleteRecords client tests against a scripted TCP broker (no live server). */
class DeleteRecordsTest {
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
    void error13RaisesWithoutRedirect() throws Exception {
        try (DeleteRecordsServer srv = DeleteRecordsServer.error(13)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.deleteRecords("events", 0, 10));
                assertEquals(13, ex.code);
                assertEquals("delete_records", ex.op);
            }
            assertEquals(List.of(Codec.OP_DELETE_RECORDS), srv.opcodes);
        }
    }

    private static final class DeleteRecordsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final AtomicInteger partition = new AtomicInteger();
        final AtomicLong beforeOffset = new AtomicLong();
        final AtomicInteger waitMajority = new AtomicInteger();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        private final int errorCode;
        private final long lowWatermark;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        static DeleteRecordsServer ok(long lowWatermark) throws IOException {
            return new DeleteRecordsServer(0, lowWatermark);
        }

        static DeleteRecordsServer error(int code) throws IOException {
            return new DeleteRecordsServer(code, 0);
        }

        private DeleteRecordsServer(int errorCode, long lowWatermark) throws IOException {
            this.errorCode = errorCode;
            this.lowWatermark = lowWatermark;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-delete-records");
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
                    opcodes.add(d.frame.opcode);
                    if (d.frame.opcode != Codec.OP_DELETE_RECORDS) {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    Codec.DeleteRecordsRequest req = Codec.decodeDeleteRecordsRequest(d.frame.payload);
                    topic.set(req.topic);
                    partition.set((int) req.partition);
                    beforeOffset.set(req.beforeOffset);
                    waitMajority.set(req.waitMajority);
                    byte[] payload = Codec.encodeDeleteRecordsResponse(
                            new Codec.DeleteRecordsResponse(
                                    errorCode, req.topic, req.partition, lowWatermark));
                    out.write(Frame.encode(Codec.OP_DELETE_RECORDS_RESPONSE, d.frame.correlationId, payload));
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
