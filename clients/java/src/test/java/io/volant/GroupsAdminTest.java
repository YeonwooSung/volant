package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** ListGroups / DescribeGroup client tests against a fake native server. */
class GroupsAdminTest {
    @Test
    void listGroupsEmptyAndStable() throws Exception {
        try (GroupAdminServer srv = new GroupAdminServer(0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                List<Codec.GroupListing> groups = c.listGroups();
                assertEquals(2, groups.size());
                Map<String, Codec.GroupListing> byId = new HashMap<>();
                for (Codec.GroupListing g : groups) {
                    byId.put(g.groupId, g);
                }
                assertEquals(Codec.GROUP_STATE_EMPTY, byId.get("g2").state);
                assertEquals(0, byId.get("g2").memberCount);
                assertEquals(0, byId.get("g2").generation);
                assertEquals(Codec.GROUP_STATE_STABLE, byId.get("g1").state);
                assertEquals(2, byId.get("g1").memberCount);
                assertEquals(5, byId.get("g1").generation);
            }
            srv.assertOk();
            assertEquals(Collections.singletonList(Codec.OP_LIST_GROUPS), srv.opcodes);
        }
    }

    @Test
    void describeGroupMembersAndAssignment() throws Exception {
        try (GroupAdminServer srv = new GroupAdminServer(0)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                DescribeGroupResult desc = c.describeGroup("cg-1");
                assertEquals("cg-1", desc.groupId);
                assertEquals(3, desc.generation);
                assertEquals(1, desc.members.size());
                Codec.GroupMemberInfo m = desc.members.get(0);
                assertEquals("m-a", m.memberId);
                assertEquals(Collections.singletonList("events"), m.topics);
                assertEquals(2, m.assignment.size());
                assertEquals("events", m.assignment.get(0).topic);
                assertEquals(0, m.assignment.get(0).partition);
                assertEquals(2, m.assignment.get(1).partition);
            }
            srv.assertOk();
            assertEquals("cg-1", srv.described.get());
        }
    }

    @Test
    void describeGroupNotFoundRaises() throws Exception {
        try (GroupAdminServer srv = new GroupAdminServer(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.describeGroup("missing"));
                assertEquals(2, ex.code);
                assertEquals("describe_group", ex.op);
            }
            srv.assertOk();
            assertEquals("missing", srv.described.get());
        }
    }

    private static final class GroupAdminServer implements AutoCloseable {
        final int port;
        final List<Integer> opcodes = Collections.synchronizedList(new java.util.ArrayList<>());
        final AtomicReference<String> described = new AtomicReference<>();
        private final int describeError;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        GroupAdminServer(int describeError) throws IOException {
            this.describeError = describeError;
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
                    "volant-groups");
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
                if (d.frame.opcode == Codec.OP_LIST_GROUPS) {
                    byte[] payload = Codec.encodeListGroupsResponse(
                            new Codec.ListGroupsResponse(
                                    0,
                                    Arrays.asList(
                                            new Codec.GroupListing("g2", Codec.GROUP_STATE_EMPTY, 0, 0),
                                            new Codec.GroupListing("g1", Codec.GROUP_STATE_STABLE, 2, 5))));
                    out.write(Frame.encode(Codec.OP_LIST_GROUPS_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    continue;
                }
                if (d.frame.opcode == Codec.OP_DESCRIBE_GROUP) {
                    String groupId = Codec.decodeDescribeGroupRequest(d.frame.payload).groupId;
                    described.set(groupId);
                    Codec.DescribeGroupResponse resp;
                    if (describeError != 0) {
                        resp = new Codec.DescribeGroupResponse(describeError, groupId, 0, Collections.emptyList());
                    } else {
                        resp = new Codec.DescribeGroupResponse(
                                0,
                                groupId,
                                3,
                                Collections.singletonList(
                                        new Codec.GroupMemberInfo(
                                                "m-a",
                                                Collections.singletonList("events"),
                                                Arrays.asList(
                                                        new Codec.Assignment("events", 0),
                                                        new Codec.Assignment("events", 2)))));
                    }
                    byte[] payload = Codec.encodeDescribeGroupResponse(resp);
                    out.write(Frame.encode(Codec.OP_DESCRIBE_GROUP_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    if (describeError != 0) {
                        return;
                    }
                    continue;
                }
                return;
            }
        }
    }
}
