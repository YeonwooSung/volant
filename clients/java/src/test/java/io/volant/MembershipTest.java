package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

/** AddBroker / RemoveBroker / ListMembers client tests against a fake native server. */
class MembershipTest {
    @Test
    void addReturnsGeneration() throws Exception {
        try (MembershipServer srv = new MembershipServer(0, 0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long gen = c.addBroker(2, "10.0.0.2", 9092, "r1");
                assertEquals(5L, gen);
            }
            assertEquals(2, srv.addId.get());
            assertEquals("10.0.0.2", srv.addHost.get());
            assertEquals(9092, srv.addPort.get());
            assertEquals("r1", srv.addRack.get());
            assertEquals(Collections.singletonList(Codec.OP_ADD_BROKER), srv.opcodes);
        }
    }

    @Test
    void removeReturnsGeneration() throws Exception {
        try (MembershipServer srv = new MembershipServer(0, 0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                long gen = c.removeBroker(2);
                assertEquals(6L, gen);
            }
            assertEquals(2, srv.removeId.get());
        }
    }

    @Test
    void listParsesBrokersAndLive() throws Exception {
        try (MembershipServer srv = new MembershipServer(0, 0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                MembershipList members = c.listMembers();
                assertEquals(4L, members.generation);
                assertEquals(2, members.brokers.size());
                assertEquals(1, members.brokers.get(0).id);
                assertEquals("10.0.0.1", members.brokers.get(0).host);
                assertEquals(9092, members.brokers.get(0).port);
                assertNull(members.brokers.get(0).rack);
                assertEquals(2, members.brokers.get(1).id);
                assertEquals("r1", members.brokers.get(1).rack);
                assertEquals(Arrays.asList(1, 2), members.live);
            }
            assertTrue(srv.listPayloadEmpty);
        }
    }

    @Test
    void addErrorRaises() throws Exception {
        try (MembershipServer srv = new MembershipServer(3, 0, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.addBroker(2, "10.0.0.2", 9092));
                assertEquals(3, ex.code);
                assertEquals("add_broker", ex.op);
            }
        }
    }

    @Test
    void removeErrorRaises() throws Exception {
        try (MembershipServer srv = new MembershipServer(0, 2, 0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.removeBroker(2));
                assertEquals(2, ex.code);
                assertEquals("remove_broker", ex.op);
            }
        }
    }

    @Test
    void listErrorRaises() throws Exception {
        try (MembershipServer srv = new MembershipServer(0, 0, 23)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, c::listMembers);
                assertEquals(23, ex.code);
                assertEquals("list_members", ex.op);
            }
        }
    }

    private static final class MembershipServer implements AutoCloseable {
        final int port;
        final AtomicInteger addId = new AtomicInteger();
        final AtomicReference<String> addHost = new AtomicReference<>();
        final AtomicInteger addPort = new AtomicInteger();
        final AtomicReference<String> addRack = new AtomicReference<>();
        final AtomicInteger removeId = new AtomicInteger();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        volatile boolean listPayloadEmpty;
        private final int addError;
        private final int removeError;
        private final int listError;
        private final ServerSocket listen;
        private final Thread thread;

        MembershipServer(int addError, int removeError, int listError) throws IOException {
            this.addError = addError;
            this.removeError = removeError;
            this.listError = listError;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-membership");
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
                    if (d.frame.opcode == Codec.OP_ADD_BROKER) {
                        Codec.AddBrokerRequest req = Codec.decodeAddBrokerRequest(d.frame.payload);
                        addId.set(req.id);
                        addHost.set(req.host);
                        addPort.set(req.port);
                        addRack.set(req.rack);
                        payload = Codec.encodeAddBrokerResponse(
                                new Codec.AddBrokerResponse(addError, addError == 0 ? 5L : 0L));
                        respOp = Codec.OP_ADD_BROKER_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_REMOVE_BROKER) {
                        Codec.RemoveBrokerRequest req = Codec.decodeRemoveBrokerRequest(d.frame.payload);
                        removeId.set(req.id);
                        payload = Codec.encodeRemoveBrokerResponse(
                                new Codec.RemoveBrokerResponse(removeError, removeError == 0 ? 6L : 0L));
                        respOp = Codec.OP_REMOVE_BROKER_RESPONSE;
                    } else if (d.frame.opcode == Codec.OP_LIST_MEMBERS) {
                        listPayloadEmpty = d.frame.payload == null || d.frame.payload.length == 0;
                        payload = Codec.encodeListMembersResponse(
                                new Codec.ListMembersResponse(
                                        listError,
                                        listError == 0 ? 4L : 0L,
                                        Arrays.asList(
                                                new MembershipBroker(1, "10.0.0.1", 9092, null),
                                                new MembershipBroker(2, "10.0.0.2", 9092, "r1")),
                                        Arrays.asList(1, 2)));
                        respOp = Codec.OP_LIST_MEMBERS_RESPONSE;
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
