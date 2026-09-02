package io.volant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

/** DescribeConfigs / AlterConfigs client tests against a scripted TCP broker. */
class DescribeAlterConfigsTest {
    @Test
    void describeReturnsPairs() throws Exception {
        try (ConfigsServer srv = ConfigsServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                DescribeConfigsResult got = c.describeConfigs("events");
                assertEquals("events", got.topic);
                assertEquals(1, got.topicId);
                assertEquals(1, got.partitionCount);
                assertEquals(1, got.configs.size());
                assertEquals("retention.ms", got.configs.get(0)[0]);
                assertEquals("86400000", got.configs.get(0)[1]);
            }
            assertEquals("events", srv.topic.get());
            assertEquals(Collections.singletonList(Codec.OP_DESCRIBE_CONFIGS), srv.opcodes);
        }
    }

    @Test
    void alterOkEmptyValueClear() throws Exception {
        try (ConfigsServer srv = ConfigsServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                c.alterConfigs("events", Collections.singletonList(new String[] {"retention.ms", ""}));
            }
            assertEquals("events", srv.topic.get());
            assertEquals(1, srv.alter.size());
            assertArrayEquals(new String[] {"retention.ms", ""}, srv.alter.get(0));
            assertEquals(Collections.singletonList(Codec.OP_ALTER_CONFIGS), srv.opcodes);
        }
    }

    @Test
    void describeErrorCodeRaises() throws Exception {
        try (ConfigsServer srv = ConfigsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(BrokerException.class, () -> c.describeConfigs("missing"));
                assertEquals(2, ex.code);
                assertEquals("describe_configs", ex.op);
            }
        }
    }

    @Test
    void alterErrorCodeRaises() throws Exception {
        try (ConfigsServer srv = ConfigsServer.error(2)) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                BrokerException ex = assertThrows(
                        BrokerException.class,
                        () -> c.alterConfigs(
                                "missing", Collections.singletonList(new String[] {"retention.ms", "1"})));
                assertEquals(2, ex.code);
                assertEquals("alter_configs", ex.op);
            }
        }
    }

    private static final class ConfigsServer implements AutoCloseable {
        final int port;
        final AtomicReference<String> topic = new AtomicReference<>();
        final List<Integer> opcodes = new CopyOnWriteArrayList<>();
        final List<String[]> alter = new CopyOnWriteArrayList<>();
        private final int errorCode;
        private final ServerSocket listen;
        private final Thread thread;

        static ConfigsServer ok() throws IOException {
            return new ConfigsServer(0);
        }

        static ConfigsServer error(int code) throws IOException {
            return new ConfigsServer(code);
        }

        private ConfigsServer(int errorCode) throws IOException {
            this.errorCode = errorCode;
            listen = new ServerSocket(0, 8, InetAddress.getByName("127.0.0.1"));
            listen.setSoTimeout(8_000);
            port = listen.getLocalPort();
            thread = new Thread(this::serve, "volant-configs");
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
                    if (d.frame.opcode == Codec.OP_DESCRIBE_CONFIGS) {
                        Codec.DescribeConfigsRequest req = Codec.decodeDescribeConfigsRequest(d.frame.payload);
                        topic.set(req.topic);
                        byte[] payload = Codec.encodeDescribeConfigsResponse(
                                new Codec.DescribeConfigsResponse(
                                        errorCode,
                                        req.topic,
                                        1,
                                        1,
                                        Collections.singletonList(new String[] {"retention.ms", "86400000"})));
                        out.write(Frame.encode(
                                Codec.OP_DESCRIBE_CONFIGS_RESPONSE, d.frame.correlationId, payload));
                        out.flush();
                    } else if (d.frame.opcode == Codec.OP_ALTER_CONFIGS) {
                        Codec.AlterConfigsRequest req = Codec.decodeAlterConfigsRequest(d.frame.payload);
                        topic.set(req.topic);
                        alter.clear();
                        alter.addAll(req.configs);
                        byte[] payload = Codec.encodeAlterConfigsResponse(
                                new Codec.AlterConfigsResponse(errorCode, req.topic));
                        out.write(Frame.encode(
                                Codec.OP_ALTER_CONFIGS_RESPONSE, d.frame.correlationId, payload));
                        out.flush();
                    } else {
                        return;
                    }
                }
            } catch (Exception ignored) {
                // connection close ends the script
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
