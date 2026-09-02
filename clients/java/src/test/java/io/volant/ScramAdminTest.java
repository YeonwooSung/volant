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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Create/Delete/ListScramUsers client tests against a fake native server. */
class ScramAdminTest {
    @Test
    void createOk() throws Exception {
        try (ScramAdminServer srv = new ScramAdminServer(0, 0, 0, Collections.emptyList())) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.createScramUser("alice", "s3cret", 4096);
            }
            assertEquals("alice", srv.createUser.get());
            assertEquals("s3cret", srv.createPass.get());
            assertEquals(4096, srv.createIters.get());
            assertEquals(Collections.singletonList(Codec.OP_CREATE_SCRAM_USER), srv.opcodes);
        }
    }

    @Test
    void deleteNotFoundRaises() throws Exception {
        try (ScramAdminServer srv = new ScramAdminServer(0, 2, 0, Collections.emptyList())) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.deleteScramUser("missing"));
                assertEquals(2, ex.code);
                assertEquals("delete_scram_user", ex.op);
            }
            assertEquals("missing", srv.deleteUser.get());
        }
    }

    @Test
    void listReturnsNames() throws Exception {
        try (ScramAdminServer srv = new ScramAdminServer(0, 0, 0, Arrays.asList("alice", "bob"))) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<String> names = c.listScramUsers();
                assertEquals(Arrays.asList("alice", "bob"), names);
            }
            assertTrue(srv.listPayloadEmpty);
        }
    }

    @Test
    void unauthorizedRaises() throws Exception {
        try (ScramAdminServer srv = new ScramAdminServer(0, 0, 23, Collections.emptyList())) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, c::listScramUsers);
                assertEquals(23, ex.code);
                assertEquals("list_scram_users", ex.op);
            }
        }
    }

    private static final class ScramAdminServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> createUser = new AtomicReference<>();
        final AtomicReference<String> createPass = new AtomicReference<>();
        final AtomicInteger createIters = new AtomicInteger();
        final AtomicReference<String> deleteUser = new AtomicReference<>();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        volatile boolean listPayloadEmpty;
        private final int createError;
        private final int deleteError;
        private final int listError;
        private final List<String> usernames;
        private final ServerSocket listen;
        private final Thread thread;

        ScramAdminServer(int createError, int deleteError, int listError, List<String> usernames)
                throws IOException {
            this.createError = createError;
            this.deleteError = deleteError;
            this.listError = listError;
            this.usernames = usernames;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-scram-admin");
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
                    byte[] payload;
                    int respOp;
                    if (d.frame.opcode == Codec.OP_CREATE_SCRAM_USER) {
                        Codec.CreateScramUserRequest req = Codec.decodeCreateScramUserRequest(d.frame.payload);
                        createUser.set(req.username);
                        createPass.set(req.password);
                        createIters.set(req.iterations);
                        payload = Codec.encodeCreateScramUserResponse(
                                new Codec.CreateScramUserResponse(createError));
                        respOp = Codec.OP_CREATE_SCRAM_USER_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_DELETE_SCRAM_USER) {
                        Codec.DeleteScramUserRequest req = Codec.decodeDeleteScramUserRequest(d.frame.payload);
                        deleteUser.set(req.username);
                        payload = Codec.encodeDeleteScramUserResponse(
                                new Codec.DeleteScramUserResponse(deleteError));
                        respOp = Codec.OP_DELETE_SCRAM_USER_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_LIST_SCRAM_USERS) {
                        listPayloadEmpty = d.frame.payload == null || d.frame.payload.length == 0;
                        payload = Codec.encodeListScramUsersResponse(
                                new Codec.ListScramUsersResponse(listError, usernames));
                        respOp = Codec.OP_LIST_SCRAM_USERS_RESPONSE;
                    } else {
                        return;
                    }
                    out.write(Frame.encode(respOp, d.frame.correlationId, payload));
                    out.flush();
                }
            } catch (Exception ignored) {
                // test assertions cover the client side
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
