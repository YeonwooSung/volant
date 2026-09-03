package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** v0.131: thin Client JoinGroup rejoin (records decoded member_id). */
class JoinGroupMemberTest {
    @Test
    void joinGroupSendsEmptyMemberId() throws Exception {
        try (JoinGroupStub srv = new JoinGroupStub()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                JoinGroupResult j = c.joinGroup("g", List.of("t"), 10_000);
                assertEquals("m-1", j.memberId);
                assertEquals(1L, j.generation);
            }
            srv.assertOk();
            assertEquals("", srv.memberId.get());
            assertEquals("g", srv.group.get());
            assertEquals(Collections.singletonList(Codec.OP_JOIN_GROUP), srv.opcodes);
        }
    }

    @Test
    void joinGroupMemberEncodesId() throws Exception {
        try (JoinGroupStub srv = new JoinGroupStub()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                JoinGroupResult j = c.joinGroupMember("g", "rejoin-1", List.of("t"), 10_000);
                assertEquals("m-1", j.memberId);
            }
            srv.assertOk();
            assertEquals("rejoin-1", srv.memberId.get());
        }
    }

    @Test
    void joinGroupMemberEmptyMatchesPublicApi() throws Exception {
        try (JoinGroupStub srv = new JoinGroupStub()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.joinGroupMember("g", "", List.of("t"), 10_000);
            }
            srv.assertOk();
            assertEquals("", srv.memberId.get());
        }
    }

    private static final class JoinGroupStub implements AutoCloseable {
        final int port;
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final AtomicReference<String> memberId = new AtomicReference<>();
        final AtomicReference<String> group = new AtomicReference<>();
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        JoinGroupStub() throws IOException {
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress("127.0.0.1", 0));
            ss.setSoTimeout(5_000);
            this.listen = ss;
            this.port = ss.getLocalPort();
            this.thread = new Thread(
                    () -> {
                        try (Socket conn = listen.accept()) {
                            handle(conn);
                        } catch (Exception e) {
                            error.set(e);
                        } finally {
                            done.countDown();
                        }
                    },
                    "volant-join-group-member");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        void assertOk() throws Exception {
            if (!done.await(5, TimeUnit.SECONDS)) {
                throw new IOException("server did not finish");
            }
            Exception e = error.get();
            if (e != null) {
                throw e;
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

        private void handle(Socket conn) throws Exception {
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
                if (d.frame.opcode != Codec.OP_JOIN_GROUP) {
                    throw new IOException("unexpected opcode " + d.frame.opcode);
                }
                Codec.JoinGroupRequest req = Codec.decodeJoinGroupRequest(d.frame.payload);
                group.set(req.groupId);
                memberId.set(req.memberId);
                byte[] payload = Codec.encodeJoinGroupResponse(
                        new Codec.JoinGroupResponse(
                                0, 1L, "m-1", Collections.emptyList(), Collections.emptyList()));
                out.write(Frame.encode(Codec.OP_JOIN_GROUP, d.frame.correlationId, payload));
                out.flush();
            }
        }
    }
}
