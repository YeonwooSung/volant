package io.volant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Shared-token Auth constructor tests against a fake native server. */
class AuthTest {
    @Test
    void connectWithTokenSendsAuth() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000, "s3cret")) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Codec.OP_AUTH, srv.firstOpcode.get());
            assertEquals("s3cret", srv.token.get());
        }
    }

    @Test
    void rejectedTokenRaises() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.reject(17)) {
            BrokerException ex = assertThrows(
                    BrokerException.class, () -> Client.connect("127.0.0.1", srv.port, 5_000, "nope"));
            assertEquals(17, ex.code);
            assertEquals("auth", ex.op);
            srv.assertOk();
            assertEquals("nope", srv.token.get());
        }
    }

    @Test
    void noTokenSendsNoAuth() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000)) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Codec.OP_METADATA, srv.firstOpcode.get());
            assertNull(srv.token.get());
        }
    }

    @Test
    void emptyTokenSkipsAuth() throws Exception {
        try (OneShotAuthServer srv = OneShotAuthServer.ok()) {
            try (Client c = Client.connect("127.0.0.1", srv.port, 5_000, "")) {
                assertEquals(1, c.metadata().brokers.size());
            }
            srv.assertOk();
            assertEquals(Codec.OP_METADATA, srv.firstOpcode.get());
        }
    }

    private static final class OneShotAuthServer implements AutoCloseable {
        final int port;
        final AtomicInteger firstOpcode = new AtomicInteger(-1);
        final AtomicReference<String> token = new AtomicReference<>();
        private final int authError;
        private final ServerSocket listen;
        private final Thread thread;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private final CountDownLatch done = new CountDownLatch(1);

        static OneShotAuthServer ok() throws Exception {
            return new OneShotAuthServer(0);
        }

        static OneShotAuthServer reject(int code) throws Exception {
            return new OneShotAuthServer(code);
        }

        private OneShotAuthServer(int authError) throws IOException {
            this.authError = authError;
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
                    "volant-auth");
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
                if (firstOpcode.get() < 0) {
                    firstOpcode.set(d.frame.opcode);
                }
                if (d.frame.opcode == Codec.OP_AUTH) {
                    token.set(Codec.decodeAuthRequest(d.frame.payload).token);
                    byte[] payload = Codec.encodeAuthResponse(new Codec.AuthResponse(authError));
                    out.write(Frame.encode(Codec.OP_AUTH_RESPONSE, d.frame.correlationId, payload));
                    out.flush();
                    if (authError != 0) {
                        return;
                    }
                    continue;
                }
                if (d.frame.opcode == Codec.OP_METADATA) {
                    byte[] payload = Codec.encodeMetadataResponse(
                            new Metadata(
                                    Collections.singletonList(new Metadata.BrokerInfo(1, "127.0.0.1", 1)),
                                    Collections.emptyList()));
                    out.write(Frame.encode(Codec.OP_METADATA, d.frame.correlationId, payload));
                    out.flush();
                    return;
                }
                return;
            }
        }
    }
}