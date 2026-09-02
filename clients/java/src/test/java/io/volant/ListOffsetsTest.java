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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** ListOffsets client tests against a scripted TCP broker (no live server). */
class ListOffsetsTest {
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

    private static final class ListOffsetsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final List<Integer> partitions = new CopyOnWriteArrayList<>();
        private final int errorCode;
        private final List<OffsetListing> entries;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        static ListOffsetsServer ok(List<OffsetListing> entries) throws IOException {
            return new ListOffsetsServer(0, entries);
        }

        static ListOffsetsServer error(int code) throws IOException {
            return new ListOffsetsServer(code, Collections.emptyList());
        }

        private ListOffsetsServer(int errorCode, List<OffsetListing> entries) throws IOException {
            this.errorCode = errorCode;
            this.entries = entries;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-list-offsets");
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
                    if (d.frame.opcode != Codec.OP_LIST_OFFSETS) {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    Codec.ListOffsetsRequest req = Codec.decodeListOffsetsRequest(d.frame.payload);
                    topic.set(req.topic);
                    partitions.clear();
                    partitions.addAll(req.partitions);
                    byte[] payload = Codec.encodeListOffsetsResponse(
                            new Codec.ListOffsetsResponse(errorCode, req.topic, entries));
                    out.write(Frame.encode(Codec.OP_LIST_OFFSETS_RESPONSE, d.frame.correlationId, payload));
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
