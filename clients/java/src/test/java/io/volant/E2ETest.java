package io.volant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Optional live broker e2e. Skipped unless {@code VOLANT_E2E=1}.
 *
 * <pre>
 * cargo build -p volant-server
 * VOLANT_E2E=1 mvn -q test
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "VOLANT_E2E", matches = "1")
class E2ETest {
    private static Process proc;
    private static Path dataDir;
    private static String host = "127.0.0.1";
    private static int port;

    @BeforeAll
    static void startBroker() throws Exception {
        assumeTrue("1".equals(System.getenv("VOLANT_E2E")), "set VOLANT_E2E=1 to run live broker e2e");
        String existing = System.getenv("VOLANT_BROKER");
        if (existing != null && !existing.isEmpty()) {
            String[] parts = splitHostPort(existing);
            host = parts[0];
            port = Integer.parseInt(parts[1]);
            waitPort(host, port, 5_000);
            return;
        }
        Path binary = ensureServerBin();
        assumeTrue(binary != null, "volant-server not found; build with `cargo build -p volant-server` "
                + "or set VOLANT_SERVER / VOLANT_BROKER");
        dataDir = Files.createTempDirectory("volant-java-e2e-");
        port = freePort();
        proc = new ProcessBuilder(
                        binary.toString(),
                        "--listen",
                        host + ":" + port,
                        "--data-dir",
                        dataDir.toString())
                .directory(repoRoot().toFile())
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        try {
            waitPort(host, port, 15_000);
        } catch (Exception e) {
            stopBroker();
            throw e;
        }
    }

    @AfterAll
    static void stopBroker() {
        if (proc != null) {
            proc.destroy();
            try {
                if (!proc.waitFor(5, TimeUnit.SECONDS)) {
                    proc.destroyForcibly();
                    proc.waitFor(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                proc.destroyForcibly();
            }
            proc = null;
        }
        if (dataDir != null) {
            try {
                deleteRecursively(dataDir);
            } catch (IOException ignored) {
                // best-effort
            }
        }
    }

    @Test
    void createProduceFetchMetadata() {
        String topic = "java-e2e-" + ProcessHandle.current().pid() + "-" + System.nanoTime();
        try (Client c = Client.connect(host, port, 5_000)) {
            int topicId = c.createTopic(topic, 1);
            assertTrue(topicId >= 1, "topic id=" + topicId);

            long off = c.produce(topic, 0, null, "hello".getBytes(StandardCharsets.UTF_8));
            assertEquals(0L, off);

            List<Record> recs = c.fetch(topic, 0, 0);
            assertEquals(1, recs.size());
            assertEquals(0L, recs.get(0).offset);
            assertNull(recs.get(0).key);
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), recs.get(0).value);

            Metadata meta = c.metadata();
            assertFalse(meta.brokers.isEmpty());
            assertTrue(meta.topics.stream().anyMatch(t -> topic.equals(t.name)), "topic missing from metadata");

            c.deleteTopic(topic);
            Metadata meta2 = c.metadata();
            assertTrue(meta2.topics.stream().noneMatch(t -> topic.equals(t.name)), "topic still present after delete");
        }
    }

    @Test
    void offsetCommitFetch() {
        String topic = "java-off-" + ProcessHandle.current().pid() + "-" + System.nanoTime();
        String group = "java-og-" + ProcessHandle.current().pid();
        try (Client c = Client.connect(host, port, 5_000)) {
            c.createTopic(topic, 1);
            long produced = c.produce(topic, 0, null, "hello".getBytes(StandardCharsets.UTF_8));
            assertEquals(0L, produced);
            c.offsetCommit(group, topic, 0, 5);
            List<Offset> offs = c.offsetFetch(group, topic);
            assertEquals(1, offs.size());
            assertEquals(0, offs.get(0).partition);
            assertEquals(5L, offs.get(0).offset);
            c.deleteTopic(topic);
        }
    }

    @Test
    void groupConsumerPollCommitResume() {
        String topic = "java-gc-" + ProcessHandle.current().pid() + "-" + System.nanoTime();
        String group = "java-gcg-" + ProcessHandle.current().pid();
        try (Client c = Client.connect(host, port, 5_000)) {
            c.createTopic(topic, 1);
            c.produce(topic, 0, null, "hello".getBytes(StandardCharsets.UTF_8));

            try (GroupConsumer g = GroupConsumer.join(c, group, List.of(topic), 10_000)) {
                assertFalse(g.memberId().isEmpty());
                assertTrue(g.generation() >= 1);
                assertEquals(1, g.assignment().size());
                assertEquals(topic, g.assignment().get(0).topic);
                List<Record> recs = new ArrayList<>();
                for (int i = 0; i < 8 && recs.isEmpty(); i++) {
                    recs.addAll(g.poll(200));
                }
                assertEquals(1, recs.size(), "expected the produced record");
                assertEquals(0L, recs.get(0).offset);
                g.commit();
            }

            List<Offset> offs = c.offsetFetch(group, topic);
            assertEquals(1, offs.size());
            assertEquals(1L, offs.get(0).offset);

            c.produce(topic, 0, null, "world".getBytes(StandardCharsets.UTF_8));
            try (GroupConsumer g = GroupConsumer.join(c, group, List.of(topic), 10_000)) {
                List<Record> recs = new ArrayList<>();
                for (int i = 0; i < 8 && recs.isEmpty(); i++) {
                    recs.addAll(g.poll(200));
                }
                assertEquals(1, recs.size(), "resume should see only the new record");
                assertEquals(1L, recs.get(0).offset);
                assertArrayEquals("world".getBytes(StandardCharsets.UTF_8), recs.get(0).value);
                g.commit();
            }
            c.deleteTopic(topic);
        }
    }

    @Test
    void groupConsumerStaticMembership() {
        String topic = "java-static-" + ProcessHandle.current().pid() + "-" + System.nanoTime();
        String group = "java-staticg-" + ProcessHandle.current().pid();
        try (Client c = Client.connect(host, port, 5_000)) {
            c.createTopic(topic, 1);
            try (GroupConsumer g = GroupConsumer.joinStatic(c, group, List.of(topic), 10_000, "inst-1")) {
                assertEquals("inst-1", g.groupInstanceId());
                assertEquals("static:inst-1", g.memberId());
            }
            c.deleteTopic(topic);
        }
    }

    @Test
    void joinHeartbeatLeave() {
        String topic = "java-grp-" + ProcessHandle.current().pid() + "-" + System.nanoTime();
        String group = "java-cg-" + ProcessHandle.current().pid();
        try (Client c = Client.connect(host, port, 5_000)) {
            c.createTopic(topic, 1);
            JoinGroupResult j = c.joinGroup(group, List.of(topic), 10000);
            assertFalse(j.memberId.isEmpty(), "expected broker-assigned member id");
            assertTrue(j.generation >= 1, "generation=" + j.generation);
            assertEquals(1, j.assignment.size());
            assertEquals(topic, j.assignment.get(0).topic);
            assertEquals(0, j.assignment.get(0).partition);
            c.heartbeat(group, j.memberId, j.generation);
            c.leaveGroup(group, j.memberId);
            c.deleteTopic(topic);
        }
    }

    private static Path repoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        for (int i = 0; i < 8 && dir != null; i++) {
            if (Files.isRegularFile(dir.resolve("Cargo.toml"))
                    && Files.isDirectory(dir.resolve("clients"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        // src/test/java/io/volant → 6 parents = clients/java
        Path fromCwd = Paths.get("").toAbsolutePath();
        Path guess = fromCwd.resolve("../..").normalize();
        return Files.isRegularFile(guess.resolve("Cargo.toml")) ? guess : fromCwd;
    }

    private static Path findServerBin() {
        String env = System.getenv("VOLANT_SERVER");
        if (env != null && !env.isEmpty()) {
            Path p = Paths.get(env);
            if (Files.isRegularFile(p)) {
                return p;
            }
        }
        Path root = repoRoot();
        Path debug = root.resolve("target").resolve("debug").resolve("volant-server");
        if (Files.isRegularFile(debug)) {
            return debug;
        }
        Path release = root.resolve("target").resolve("release").resolve("volant-server");
        if (Files.isRegularFile(release)) {
            return release;
        }
        return null;
    }

    private static Path ensureServerBin() {
        Path found = findServerBin();
        if (found != null) {
            return found;
        }
        Path cargo = which("cargo");
        if (cargo == null) {
            return null;
        }
        try {
            Process build = new ProcessBuilder(cargo.toString(), "build", "-p", "volant-server")
                    .directory(repoRoot().toFile())
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start();
            if (!build.waitFor(300, TimeUnit.SECONDS) || build.exitValue() != 0) {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
        return findServerBin();
    }

    private static Path which(String name) {
        String path = System.getenv("PATH");
        if (path == null) {
            return null;
        }
        for (String dir : path.split(":")) {
            Path p = Paths.get(dir, name);
            if (Files.isRegularFile(p) && Files.isExecutable(p)) {
                return p;
            }
        }
        return null;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket()) {
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress("127.0.0.1", 0));
            return s.getLocalPort();
        }
    }

    private static void waitPort(String host, int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), 250);
                return;
            } catch (IOException e) {
                last = e;
                Thread.sleep(50);
            }
        }
        throw new IOException("broker did not listen on " + host + ":" + port + ": " + last);
    }

    private static String[] splitHostPort(String addr) {
        int idx = addr.lastIndexOf(':');
        if (idx <= 0) {
            throw new IllegalArgumentException("invalid VOLANT_BROKER: " + addr);
        }
        return new String[] {addr.substring(0, idx), addr.substring(idx + 1)};
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walk(root)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
    }
}
