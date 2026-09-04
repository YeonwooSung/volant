package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** SyncGroup client tests against a scripted TCP broker (no live server). */
class SyncGroupTest {
    @Test
    void codecRoundtrip() {
        Codec.SyncGroupRequest req = new Codec.SyncGroupRequest("g1", "m1", 3, new byte[0]);
        byte[] raw = Codec.encodeSyncGroupRequest(req);
        Codec.SyncGroupRequest decoded = Codec.decodeSyncGroupRequest(raw);
        assertEquals("g1", decoded.groupId);
        assertEquals("m1", decoded.memberId);
        assertEquals(3L, decoded.generation);
        assertEquals(0, decoded.assignmentBytes.length);
        Codec.SyncGroupResponse resp = new Codec.SyncGroupResponse(
                0, List.of(new Codec.Assignment("events", 2)));
        byte[] rraw = Codec.encodeSyncGroupResponse(resp);
        Codec.SyncGroupResponse got = Codec.decodeSyncGroupResponse(rraw);
        assertEquals(0, got.errorCode);
        assertEquals(1, got.assignment.size());
        assertEquals("events", got.assignment.get(0).topic);
        assertEquals(2, got.assignment.get(0).partition);
        Object dispatched = Codec.decodeResponse(Codec.OP_SYNC_GROUP_RESPONSE, rraw);
        assertInstanceOf(Codec.SyncGroupResponse.class, dispatched);
    }

    @Test
    void successReturnsAssignment() throws Exception {
        try (SyncGroupServer srv = SyncGroupServer.ok(List.of(new Codec.Assignment("events", 2)))) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<Codec.Assignment> got = c.syncGroup("g1", "m1", 3);
                assertEquals(1, got.size());
                assertEquals("events", got.get(0).topic);
                assertEquals(2, got.get(0).partition);
            }
            assertEquals("g1", srv.groupId.get());
            assertEquals("m1", srv.memberId.get());
            assertEquals(3, srv.generation.get());
            assertEquals(0, srv.assignmentBytesLen.get());
        }
    }

    @Test
    void unknownMemberIs10() throws Exception {
        try (SyncGroupServer srv = SyncGroupServer.error(10)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(
                        BrokerException.class, () -> c.syncGroup("g", "ghost", 1));
                assertEquals(10, ex.code);
                assertEquals("sync_group", ex.op);
            }
        }
    }

    @Test
    void generationMismatchIs9() throws Exception {
        try (SyncGroupServer srv = SyncGroupServer.error(9)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(
                        BrokerException.class, () -> c.syncGroup("g", "m1", 99));
                assertEquals(9, ex.code);
                assertEquals("sync_group", ex.op);
            }
        }
    }

    private static final class SyncGroupServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> groupId = new AtomicReference<>();
        final AtomicReference<String> memberId = new AtomicReference<>();
        final AtomicInteger generation = new AtomicInteger();
        final AtomicInteger assignmentBytesLen = new AtomicInteger();
        private final int errorCode;
        private final List<Codec.Assignment> assignment;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();

        static SyncGroupServer ok(List<Codec.Assignment> assignment) throws IOException {
            return new SyncGroupServer(0, assignment);
        }

        static SyncGroupServer error(int code) throws IOException {
            return new SyncGroupServer(code, List.of());
        }

        private SyncGroupServer(int errorCode, List<Codec.Assignment> assignment) throws IOException {
            this.errorCode = errorCode;
            this.assignment = assignment;
            this.listen = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
            this.port = listen.getLocalPort();
            this.thread = new Thread(this::serve, "sync-group-stub");
            this.thread.setDaemon(true);
            this.thread.start();
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
                    if (d.frame.opcode != Codec.OP_SYNC_GROUP) {
                        error.set(new ProtocolException("unexpected opcode " + d.frame.opcode));
                        return;
                    }
                    Codec.SyncGroupRequest req = Codec.decodeSyncGroupRequest(d.frame.payload);
                    groupId.set(req.groupId);
                    memberId.set(req.memberId);
                    generation.set((int) req.generation);
                    assignmentBytesLen.set(req.assignmentBytes.length);
                    List<Codec.Assignment> asgn = errorCode != 0 ? List.of() : assignment;
                    byte[] payload = Codec.encodeSyncGroupResponse(
                            new Codec.SyncGroupResponse(errorCode, asgn));
                    out.write(Frame.encode(
                            Codec.OP_SYNC_GROUP_RESPONSE, d.frame.correlationId, payload));
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
            }
            try {
                thread.join(2_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            Exception e = error.get();
            if (e != null) {
                throw new RuntimeException(e);
            }
        }
    }
}
