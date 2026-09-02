package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

/** Create/Delete/ListAcls client tests against a fake native server. */
class AclsTest {
    private static AclBinding sample() {
        return new AclBinding("User:alice", 0, "events", 3, 1);
    }

    @Test
    void createOk() throws Exception {
        AclBinding entry = sample();
        try (AclServer srv = new AclServer(0, 0, 0, 1, Collections.singletonList(entry))) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.createAcls(Collections.singletonList(entry));
            }
            assertEquals(Collections.singletonList(entry), srv.create.get());
            assertEquals(Collections.singletonList(Codec.OP_CREATE_ACLS), srv.opcodes);
        }
    }

    @Test
    void deleteReturnsRemoved() throws Exception {
        AclBinding entry = sample();
        try (AclServer srv = new AclServer(0, 0, 0, 1, Collections.emptyList())) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                int n = c.deleteAcls(Collections.singletonList(entry));
                assertEquals(1, n);
            }
            assertEquals(Collections.singletonList(entry), srv.delete.get());
        }
    }

    @Test
    void listReturnsBindings() throws Exception {
        AclBinding entry = sample();
        try (AclServer srv = new AclServer(0, 0, 0, 0, Collections.singletonList(entry))) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<AclBinding> listed = c.listAcls();
                assertEquals(Collections.singletonList(entry), listed);
            }
            Codec.ListAclsRequest req = srv.listReq.get();
            assertEquals("", req.principal);
            assertEquals(255, req.resourceType);
            assertEquals("", req.resource);
        }
    }

    @Test
    void unauthorizedRaises() throws Exception {
        try (AclServer srv = new AclServer(23, 0, 0, 0, Collections.emptyList())) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex =
                        assertThrows(BrokerException.class, () -> c.createAcls(Collections.singletonList(sample())));
                assertEquals(23, ex.code);
                assertEquals("create_acls", ex.op);
            }
        }
    }

    private static final class AclServer implements AutoCloseable {
        final int port;
        final AtomicReference<List<AclBinding>> create = new AtomicReference<>();
        final AtomicReference<List<AclBinding>> delete = new AtomicReference<>();
        final AtomicReference<Codec.ListAclsRequest> listReq = new AtomicReference<>();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        private final int createError;
        private final int deleteError;
        private final int listError;
        private final int removed;
        private final List<AclBinding> entries;
        private final ServerSocket listen;
        private final Thread thread;

        AclServer(int createError, int deleteError, int listError, int removed, List<AclBinding> entries)
                throws IOException {
            this.createError = createError;
            this.deleteError = deleteError;
            this.listError = listError;
            this.removed = removed;
            this.entries = entries;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-acls");
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
                    if (d.frame.opcode == Codec.OP_CREATE_ACLS) {
                        Codec.CreateAclsRequest req = Codec.decodeCreateAclsRequest(d.frame.payload);
                        create.set(req.entries);
                        payload = Codec.encodeCreateAclsResponse(new Codec.CreateAclsResponse(createError));
                        respOp = Codec.OP_CREATE_ACLS_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_DELETE_ACLS) {
                        Codec.DeleteAclsRequest req = Codec.decodeDeleteAclsRequest(d.frame.payload);
                        delete.set(req.entries);
                        payload = Codec.encodeDeleteAclsResponse(
                                new Codec.DeleteAclsResponse(deleteError, removed));
                        respOp = Codec.OP_DELETE_ACLS_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_LIST_ACLS) {
                        listReq.set(Codec.decodeListAclsRequest(d.frame.payload));
                        payload = Codec.encodeListAclsResponse(new Codec.ListAclsResponse(listError, entries));
                        respOp = Codec.OP_LIST_ACLS_RESPONSE;
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
