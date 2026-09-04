package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * High-level consumer that joins a group, polls assigned partitions, and commits.
 *
 * <p>Same semantics as the Rust {@code GroupConsumer}: join, OffsetFetch
 * assigned partitions, heartbeat on {@link #poll(int)}, commit with
 * member+generation, rejoin on heartbeat error 9 (and 10/11, matching Rust).
 *
 * <p>v0.37 starts a background heartbeat executor after a successful join
 * (interval {@code sessionTimeoutMs / 3}, clamped to 100–3000 ms). Pass
 * {@code heartbeat=false} to keep the v0.33 poll-only loop.
 *
 * <p>{@link #poll(int)} / {@link #commit()} share an internal lock with that
 * loop (join state + GroupConsumer RPCs) but are <strong>not</strong> a fully
 * concurrent API: do not call them from multiple threads, and do not use the
 * same {@link Client} for other RPCs while the consumer is open.
 *
 * <pre>
 * GroupConsumer g = GroupConsumer.join(c, "g", List.of("t"), 10_000);
 * GroupConsumer s = GroupConsumer.joinStatic(c, "g", List.of("t"), 10_000, "inst-1");
 * GroupConsumer a = GroupConsumer.joinWithAutoCommit(c, "g", List.of("t"), 10_000, 5000);
 * GroupConsumer r = GroupConsumer.joinWithOffsetReset(c, "g", List.of("t"), 10_000, "latest");
 * g.setFetchMaxMessages(10); // poll fetch size (v0.75); default 100 / 4MiB
 * List&lt;Record&gt; recs = g.poll(500);
 * g.commit();
 * g.close();
 * </pre>
 */
public final class GroupConsumer implements AutoCloseable {
    /** Wire sentinel: unknown / not-committed offset ({@code u64::MAX}). */
    static final long OFFSET_UNKNOWN = 0xFFFFFFFFFFFFFFFFL;

    /** Native rebalance / stale-membership codes (Rust {@code needs_rebalance}). */
    static final int ERR_REBALANCE = 9;
    static final int ERR_UNKNOWN_MEMBER = 10;
    static final int ERR_ILLEGAL_GENERATION = 11;

    private static final int POLL_MAX_MESSAGES = 100;
    private static final long POLL_MAX_BYTES = 4L * 1024 * 1024;
    private static final long HB_INTERVAL_MIN_MS = 100L;
    private static final long HB_INTERVAL_MAX_MS = 3000L;
    private static final long DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000L;
    static final String ASSIGNOR_BROKER = "broker";
    static final String ASSIGNOR_RANGE = "range";
    static final String RESET_EARLIEST = "earliest";
    static final String RESET_LATEST = "latest";
    static final String RESET_NONE = "none";

    private final Backend backend;
    private final String groupId;
    private final List<String> topics;
    private final int sessionTimeoutMs;
    /** Phase 12 static membership; empty = dynamic. */
    private final String groupInstanceId;
    private final boolean backgroundHeartbeat;
    private final String assignor;
    private final boolean autoCommit;
    private final long autoCommitIntervalMs;
    private final String autoOffsetReset;
    private int fetchMaxMessages = POLL_MAX_MESSAGES;
    private long fetchMaxBytes = POLL_MAX_BYTES;
    private long lastAutoCommitNanos;
    private boolean dirty;
    private final ReentrantLock lock = new ReentrantLock();
    private String memberId = "";
    private long generation;
    private List<Codec.Assignment> assignment = Collections.emptyList();
    private List<Codec.Assignment> lastRevoked = Collections.emptyList();
    private final Map<Tp, Long> positions = new LinkedHashMap<>();
    private boolean closed;
    /** Heartbeat RPCs issued by {@link #poll} + background (not JoinGroup). */
    private long heartbeatCount;
    private ScheduledExecutorService hbExecutor;
    private ScheduledFuture<?> hbFuture;

    GroupConsumer(Backend backend, String groupId, List<String> topics, int sessionTimeoutMs) {
        this(backend, groupId, topics, sessionTimeoutMs, "", true, ASSIGNOR_BROKER);
    }

    GroupConsumer(
            Backend backend, String groupId, List<String> topics, int sessionTimeoutMs, String groupInstanceId) {
        this(backend, groupId, topics, sessionTimeoutMs, groupInstanceId, true, ASSIGNOR_BROKER);
    }

    GroupConsumer(
            Backend backend,
            String groupId,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat) {
        this(backend, groupId, topics, sessionTimeoutMs, groupInstanceId, heartbeat, ASSIGNOR_BROKER);
    }

    GroupConsumer(
            Backend backend,
            String groupId,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat,
            String assignor) {
        this(backend, groupId, topics, sessionTimeoutMs, groupInstanceId, heartbeat, assignor, false,
                DEFAULT_AUTO_COMMIT_INTERVAL_MS);
    }

    GroupConsumer(
            Backend backend,
            String groupId,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat,
            String assignor,
            boolean autoCommit,
            long autoCommitIntervalMs) {
        this(backend, groupId, topics, sessionTimeoutMs, groupInstanceId, heartbeat, assignor, autoCommit,
                autoCommitIntervalMs, RESET_EARLIEST);
    }

    GroupConsumer(
            Backend backend,
            String groupId,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat,
            String assignor,
            boolean autoCommit,
            long autoCommitIntervalMs,
            String autoOffsetReset) {
        this.backend = backend;
        this.groupId = groupId;
        this.topics = topics == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(topics));
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.groupInstanceId = groupInstanceId == null ? "" : groupInstanceId;
        this.backgroundHeartbeat = heartbeat;
        this.assignor = normalizeAssignor(assignor);
        this.autoCommit = autoCommit;
        this.autoCommitIntervalMs = autoCommitIntervalMs < 0 ? 0L : autoCommitIntervalMs;
        this.autoOffsetReset = normalizeAutoOffsetReset(autoOffsetReset);
    }

    /** Background heartbeat period: {@code sessionTimeoutMs / 3}, clamped to 100–3000 ms. */
    static long heartbeatIntervalMs(int sessionTimeoutMs) {
        long interval = sessionTimeoutMs / 3L;
        if (interval < HB_INTERVAL_MIN_MS) {
            return HB_INTERVAL_MIN_MS;
        }
        if (interval > HB_INTERVAL_MAX_MS) {
            return HB_INTERVAL_MAX_MS;
        }
        return interval;
    }

    /** Join a consumer group on the given topics. {@code sessionTimeoutMs} 0 defaults to 10000. */
    public static GroupConsumer join(Client client, String group, List<String> topics, int sessionTimeoutMs) {
        return joinStatic(client, group, topics, sessionTimeoutMs, "");
    }

    /**
     * Join a consumer group. {@code heartbeat} starts the v0.37 background loop
     * (default {@code true} on {@link #join(Client, String, List, int)}).
     * {@code false} keeps v0.33 poll-only heartbeats.
     */
    public static GroupConsumer join(
            Client client, String group, List<String> topics, int sessionTimeoutMs, boolean heartbeat) {
        return join(new ClientBackend(client), group, topics, sessionTimeoutMs, "", heartbeat, ASSIGNOR_BROKER);
    }

    /**
     * Join a consumer group with an explicit assignor.
     *
     * <p>{@code assignor} is {@code "broker"} (default: honor JoinGroup) or
     * {@code "range"} (replace the fetch set with a local range over
     * DescribeGroup members; still no SyncGroup). Empty / {@code null} is
     * {@code "broker"}. Unknown values throw {@link IllegalArgumentException}.
     */
    public static GroupConsumer join(
            Client client, String group, List<String> topics, int sessionTimeoutMs, String assignor) {
        return join(new ClientBackend(client), group, topics, sessionTimeoutMs, "", true, assignor);
    }

    /**
     * Join with Phase 12 static membership. Empty {@code groupInstanceId} is
     * dynamic (same as {@link #join}). Re-join after error 9/10/11 resends the
     * same instance id.
     */
    public static GroupConsumer joinStatic(
            Client client, String group, List<String> topics, int sessionTimeoutMs, String groupInstanceId) {
        return join(new ClientBackend(client), group, topics, sessionTimeoutMs, groupInstanceId, true, ASSIGNOR_BROKER);
    }

    /**
     * Join with opt-in auto-commit (v0.48). Default {@link #join} stays
     * explicit-commit only.
     *
     * <p>{@code intervalMs} 0 commits after every successful {@link #poll}
     * that returned records. {@code intervalMs > 0} commits on the first
     * such poll, then when at least {@code intervalMs} has elapsed since
     * the last auto or explicit {@link #commit()}. Not Kafka
     * {@code enable.auto.commit} (no background commit thread).
     *
     * <p>Named method (not a {@code boolean} overload) so it does not
     * collide with {@link #join(Client, String, List, int, boolean)}
     * heartbeat or {@link #join(Client, String, List, int, String)}
     * assignor.
     */
    public static GroupConsumer joinWithAutoCommit(
            Client client, String group, List<String> topics, int sessionTimeoutMs, long intervalMs) {
        return join(
                new ClientBackend(client),
                group,
                topics,
                sessionTimeoutMs,
                "",
                true,
                ASSIGNOR_BROKER,
                true,
                intervalMs);
    }

    /**
     * Join with an explicit {@code auto_offset_reset} (v0.62/v0.70). Default
     * {@link #join} stays {@code earliest} (native ListOffsets earliest).
     *
     * <p>{@code autoOffsetReset} is {@code "earliest"} (ListOffsets earliest),
     * {@code "latest"} (ListOffsets latest / LEO), or {@code "none"} (raise if
     * OffsetFetch is missing / {@link #OFFSET_UNKNOWN}). Empty / {@code null}
     * is {@code earliest}. Unknown values throw
     * {@link IllegalArgumentException} before JoinGroup. Not Kafka
     * {@code auto.offset.reset} (no timestamp).
     *
     * <p>Auto-commit stays off (use {@link #joinWithAutoCommit} for that).
     * Named method so it does not collide with
     * {@link #join(Client, String, List, int, String)} assignor or
     * {@link #joinStatic} instance id.
     */
    public static GroupConsumer joinWithOffsetReset(
            Client client, String group, List<String> topics, int sessionTimeoutMs, String autoOffsetReset) {
        return join(
                new ClientBackend(client),
                group,
                topics,
                sessionTimeoutMs,
                "",
                true,
                ASSIGNOR_BROKER,
                false,
                DEFAULT_AUTO_COMMIT_INTERVAL_MS,
                autoOffsetReset);
    }

    static GroupConsumer join(Backend backend, String group, List<String> topics, int sessionTimeoutMs) {
        return join(backend, group, topics, sessionTimeoutMs, "", true, ASSIGNOR_BROKER);
    }

    static GroupConsumer join(
            Backend backend, String group, List<String> topics, int sessionTimeoutMs, String groupInstanceId) {
        return join(backend, group, topics, sessionTimeoutMs, groupInstanceId, true, ASSIGNOR_BROKER);
    }

    static GroupConsumer join(
            Backend backend, String group, List<String> topics, int sessionTimeoutMs, boolean heartbeat) {
        return join(backend, group, topics, sessionTimeoutMs, "", heartbeat, ASSIGNOR_BROKER);
    }

    static GroupConsumer joinWithAssignor(
            Backend backend, String group, List<String> topics, int sessionTimeoutMs, String assignor) {
        return join(backend, group, topics, sessionTimeoutMs, "", true, assignor);
    }

    static GroupConsumer join(
            Backend backend,
            String group,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat) {
        return join(backend, group, topics, sessionTimeoutMs, groupInstanceId, heartbeat, ASSIGNOR_BROKER);
    }

    static GroupConsumer join(
            Backend backend,
            String group,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat,
            String assignor) {
        return join(backend, group, topics, sessionTimeoutMs, groupInstanceId, heartbeat, assignor, false,
                DEFAULT_AUTO_COMMIT_INTERVAL_MS);
    }

    /** Package-visible: unit tests keep {@code heartbeat=false}. */
    static GroupConsumer joinWithAutoCommit(
            Backend backend, String group, List<String> topics, int sessionTimeoutMs, long intervalMs) {
        return join(backend, group, topics, sessionTimeoutMs, "", false, ASSIGNOR_BROKER, true, intervalMs);
    }

    static GroupConsumer join(
            Backend backend,
            String group,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat,
            String assignor,
            boolean autoCommit,
            long autoCommitIntervalMs) {
        return join(
                backend,
                group,
                topics,
                sessionTimeoutMs,
                groupInstanceId,
                heartbeat,
                assignor,
                autoCommit,
                autoCommitIntervalMs,
                RESET_EARLIEST);
    }

    /** Package-visible: unit tests keep {@code heartbeat=false}. */
    static GroupConsumer joinWithOffsetReset(
            Backend backend, String group, List<String> topics, int sessionTimeoutMs, String autoOffsetReset) {
        return join(
                backend,
                group,
                topics,
                sessionTimeoutMs,
                "",
                false,
                ASSIGNOR_BROKER,
                false,
                DEFAULT_AUTO_COMMIT_INTERVAL_MS,
                autoOffsetReset);
    }

    static GroupConsumer join(
            Backend backend,
            String group,
            List<String> topics,
            int sessionTimeoutMs,
            String groupInstanceId,
            boolean heartbeat,
            String assignor,
            boolean autoCommit,
            long autoCommitIntervalMs,
            String autoOffsetReset) {
        int timeout = sessionTimeoutMs == 0 ? 10_000 : sessionTimeoutMs;
        GroupConsumer g = new GroupConsumer(
                backend,
                group,
                topics,
                timeout,
                groupInstanceId,
                heartbeat,
                assignor,
                autoCommit,
                autoCommitIntervalMs,
                autoOffsetReset);
        g.doJoin();
        g.startHeartbeat();
        return g;
    }

    private void doJoin() {
        List<Codec.Assignment> previous = new ArrayList<>(assignment);
        JoinGroupResult result = backend.joinGroup(groupId, memberId, topics, sessionTimeoutMs, groupInstanceId);
        memberId = result.memberId;
        generation = result.generation;
        List<Codec.Assignment> newAssignment = new ArrayList<>(result.assignment);
        if (ASSIGNOR_RANGE.equals(assignor)) {
            newAssignment = localRangeAssignment();
        }

        Set<Tp> oldSet = toSet(previous);
        Set<Tp> newSet = toSet(newAssignment);

        List<Tp> revoked = new ArrayList<>();
        for (Tp tp : oldSet) {
            if (!newSet.contains(tp)) {
                revoked.add(tp);
            }
        }
        if (!ASSIGNOR_RANGE.equals(assignor)) {
            for (Codec.Assignment a : result.revoked) {
                Tp tp = new Tp(a.topic, a.partition);
                if (!revoked.contains(tp)) {
                    revoked.add(tp);
                }
            }
        }
        Collections.sort(revoked);

        List<Tp> added = new ArrayList<>();
        for (Tp tp : newSet) {
            if (!oldSet.contains(tp)) {
                added.add(tp);
            }
        }

        for (Tp tp : revoked) {
            positions.remove(tp);
        }

        assignment = Collections.unmodifiableList(newAssignment);
        List<Codec.Assignment> revokedAssign = new ArrayList<>();
        for (Tp tp : revoked) {
            revokedAssign.add(new Codec.Assignment(tp.topic, tp.partition));
        }
        lastRevoked = Collections.unmodifiableList(revokedAssign);

        if (!added.isEmpty() || (positions.isEmpty() && !assignment.isEmpty())) {
            List<Tp> toFetch = previous.isEmpty() ? toList(newSet) : added;
            fetchPositionsFor(toFetch);
        }

        List<Tp> missing = new ArrayList<>();
        for (Codec.Assignment a : assignment) {
            Tp tp = new Tp(a.topic, a.partition);
            if (!positions.containsKey(tp)) {
                missing.add(tp);
            }
        }
        applyReset(missing);
    }

    private void fetchPositionsFor(List<Tp> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        List<Codec.OffsetEntry> entries = new ArrayList<>();
        for (Tp tp : partitions) {
            entries.add(new Codec.OffsetEntry(tp.topic, tp.partition));
        }
        List<Codec.OffsetFetchEntry> fetched = backend.fetchOffsets(groupId, entries);
        Map<Tp, Long> found = new HashMap<>();
        for (Codec.OffsetFetchEntry e : fetched) {
            found.put(new Tp(e.topic, e.partition), e.offset);
        }
        List<Tp> unknown = new ArrayList<>();
        for (Tp tp : partitions) {
            Long off = found.get(tp);
            if (off == null || off == OFFSET_UNKNOWN) {
                unknown.add(tp);
                continue;
            }
            positions.put(tp, off);
        }
        applyReset(unknown);
    }

    private void applyReset(List<Tp> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        if (RESET_NONE.equals(autoOffsetReset)) {
            Tp tp = partitions.get(0);
            throw new IllegalStateException(
                    "no committed offset for " + tp.topic + "-" + tp.partition
                            + " and auto_offset_reset=" + autoOffsetReset);
        }
        boolean useEarliest = RESET_EARLIEST.equals(autoOffsetReset);
        Map<String, List<Integer>> byTopic = new LinkedHashMap<>();
        for (Tp tp : partitions) {
            byTopic.computeIfAbsent(tp.topic, k -> new ArrayList<>()).add(tp.partition);
        }
        for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
            List<Integer> wanted = e.getValue();
            int[] parts = new int[wanted.size()];
            for (int i = 0; i < wanted.size(); i++) {
                parts[i] = wanted.get(i);
            }
            List<OffsetListing> listings = backend.listOffsets(e.getKey(), parts);
            Map<Integer, Long> got = new HashMap<>();
            if (listings != null) {
                for (OffsetListing listing : listings) {
                    got.put(listing.partition, useEarliest ? listing.earliest : listing.latest);
                }
            }
            for (int part : wanted) {
                Long off = got.get(part);
                if (off == null) {
                    throw new IllegalStateException(
                            "list_offsets missing partition " + e.getKey() + "-" + part);
                }
                positions.put(new Tp(e.getKey(), part), off);
            }
        }
    }

    private List<Codec.Assignment> localRangeAssignment() {
        Metadata meta = backend.metadata();
        Map<String, Integer> counts = new HashMap<>();
        if (meta != null) {
            for (Metadata.TopicInfo topic : meta.topics) {
                counts.put(topic.name, topic.partitions.size());
            }
        }
        List<String> memberIds = new ArrayList<>();
        List<List<String>> memberTopics = new ArrayList<>();
        if (!collectRangeMembers(memberIds, memberTopics)) {
            memberIds = Collections.singletonList(memberId);
            memberTopics = Collections.singletonList(topics);
        }
        List<List<Codec.Assignment>> assigned = RangeAssignor.rangeAssignMulti(
                memberIds, memberTopics, counts);
        if (assigned.isEmpty()) {
            return Collections.emptyList();
        }
        int idx = memberIds.indexOf(memberId);
        if (idx < 0 || idx >= assigned.size()) {
            assigned = RangeAssignor.rangeAssignMulti(
                    Collections.singletonList(memberId),
                    Collections.singletonList(topics),
                    counts);
            if (assigned.isEmpty()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(assigned.get(0));
        }
        return new ArrayList<>(assigned.get(idx));
    }

    /** DescribeGroup members for local range. False means solo fallback. */
    private boolean collectRangeMembers(List<String> ids, List<List<String>> topicsOut) {
        DescribeGroupResult desc;
        try {
            desc = backend.describeGroup(groupId);
        } catch (RuntimeException ignored) {
            return false;
        }
        if (desc == null || desc.members == null) {
            return false;
        }
        boolean seen = false;
        for (Codec.GroupMemberInfo m : desc.members) {
            String id = m.memberId == null ? "" : m.memberId;
            List<String> subscribed = m.topics == null
                    ? Collections.emptyList()
                    : new ArrayList<>(m.topics);
            ids.add(id);
            topicsOut.add(subscribed);
            if (id.equals(memberId)) {
                seen = true;
            }
        }
        if (!seen) {
            ids.add(memberId);
            topicsOut.add(new ArrayList<>(topics));
        }
        return !ids.isEmpty() && ids.contains(memberId);
    }

    /**
     * Heartbeat, then fetch from all assigned partitions.
     *
     * <p>{@code timeoutMs} is Fetch {@code max_wait_ms} on the first assigned
     * partition (0 = non-blocking). Rejoins on heartbeat error 9/10/11.
     */
    public List<Record> poll(int timeoutMs) {
        lock.lock();
        try {
            ensureOpen();
            try {
                heartbeatCount++;
                backend.heartbeat(groupId, memberId, generation);
            } catch (BrokerException e) {
                if (needsRebalance(e.code)) {
                    doJoin();
                } else {
                    throw e;
                }
            }

            List<Record> out = new ArrayList<>();
            List<Codec.Assignment> assigned = new ArrayList<>(assignment);
            boolean waited = false;
            long wait = timeoutMs < 0 ? 0 : timeoutMs;
            int maxMessages = fetchMaxMessages <= 0 ? POLL_MAX_MESSAGES : fetchMaxMessages;
            long maxBytes = fetchMaxBytes <= 0 ? POLL_MAX_BYTES : fetchMaxBytes;
            for (Codec.Assignment a : assigned) {
                long from = positions.getOrDefault(new Tp(a.topic, a.partition), 0L);
                long maxWait = 0;
                if (!waited && wait > 0) {
                    maxWait = wait;
                    waited = true;
                }
                List<Record> recs = backend.fetch(a.topic, a.partition, from, maxMessages, maxBytes, maxWait);
                for (Record r : recs) {
                    long next = r.offset == Long.MAX_VALUE ? Long.MAX_VALUE : r.offset + 1;
                    positions.put(new Tp(a.topic, a.partition), next);
                    out.add(r);
                }
            }
            if (!out.isEmpty()) {
                dirty = true;
                maybeAutoCommit();
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** Commit last+1 positions for all assigned partitions (member + generation). */
    public void commit() {
        lock.lock();
        try {
            ensureOpen();
            doCommit();
        } finally {
            lock.unlock();
        }
    }

    private void doCommit() {
        if (!positions.isEmpty()) {
            List<Codec.OffsetCommitEntry> entries = new ArrayList<>();
            for (Map.Entry<Tp, Long> e : positions.entrySet()) {
                entries.add(new Codec.OffsetCommitEntry(e.getKey().topic, e.getKey().partition, e.getValue(), ""));
            }
            backend.commitOffsets(groupId, memberId, generation, entries);
        }
        lastAutoCommitNanos = System.nanoTime();
        dirty = false;
    }

    private void maybeAutoCommit() {
        if (!autoCommit) {
            return;
        }
        long now = System.nanoTime();
        if (autoCommitIntervalMs > 0 && lastAutoCommitNanos != 0) {
            long elapsedMs = (now - lastAutoCommitNanos) / 1_000_000L;
            if (elapsedMs < autoCommitIntervalMs) {
                return;
            }
        }
        doCommit();
    }

    /**
     * Stop the heartbeat executor (if any), then LeaveGroup. Does not close
     * the underlying {@link Client}. Idempotent. Auto-commit on +
     * uncommitted positions: best-effort commit once (errors ignored),
     * then leave.
     */
    @Override
    public void close() {
        stopHeartbeat();
        lock.lock();
        try {
            if (closed) {
                return;
            }
            if (autoCommit && dirty) {
                try {
                    doCommit();
                } catch (RuntimeException ignored) {
                    // best-effort
                }
            }
            closed = true;
            if (memberId != null && !memberId.isEmpty()) {
                backend.leaveGroup(groupId, memberId);
            }
        } finally {
            lock.unlock();
        }
    }

    private void startHeartbeat() {
        if (!backgroundHeartbeat) {
            return;
        }
        long interval = heartbeatIntervalMs(sessionTimeoutMs);
        hbExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "volant-group-heartbeat");
            t.setDaemon(true);
            return t;
        });
        hbFuture = hbExecutor.scheduleWithFixedDelay(this::heartbeatOnce, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeat() {
        ScheduledExecutorService exec;
        lock.lock();
        try {
            if (hbFuture != null) {
                hbFuture.cancel(false);
                hbFuture = null;
            }
            exec = hbExecutor;
            hbExecutor = null;
        } finally {
            lock.unlock();
        }
        if (exec != null) {
            exec.shutdown();
            try {
                if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                    exec.shutdownNow();
                    exec.awaitTermination(1, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                exec.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void heartbeatOnce() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            try {
                heartbeatCount++;
                backend.heartbeat(groupId, memberId, generation);
            } catch (BrokerException e) {
                if (needsRebalance(e.code)) {
                    doJoin();
                }
            }
        } catch (RuntimeException ignored) {
            // keep the loop alive
        } finally {
            lock.unlock();
        }
    }

    public String groupId() {
        return groupId;
    }

    /** Phase 12 static membership id (empty = dynamic). */
    public String groupInstanceId() {
        return groupInstanceId;
    }

    public String memberId() {
        lock.lock();
        try {
            return memberId;
        } finally {
            lock.unlock();
        }
    }

    public long generation() {
        lock.lock();
        try {
            return generation;
        } finally {
            lock.unlock();
        }
    }

    public List<Codec.Assignment> assignment() {
        lock.lock();
        try {
            return assignment;
        } finally {
            lock.unlock();
        }
    }

    /** Join-time reset policy ({@code earliest} / {@code latest} / {@code none}). */
    public String autoOffsetReset() {
        return autoOffsetReset;
    }

    /** Join-time assignor ({@code broker} or {@code range}). */
    public String assignor() {
        return assignor;
    }

    /**
     * Bound each assigned {@code fetch} inside {@link #poll} ({@code max_messages}).
     * Default 100. Values {@code <= 0} clamp to 100. Not Kafka
     * {@code max.poll.records}.
     */
    public void setFetchMaxMessages(int maxMessages) {
        lock.lock();
        try {
            this.fetchMaxMessages = maxMessages <= 0 ? POLL_MAX_MESSAGES : maxMessages;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Bound each assigned {@code fetch} inside {@link #poll} ({@code max_bytes}).
     * Default 4MiB. Values {@code <= 0} clamp to 4MiB.
     */
    public void setFetchMaxBytes(long maxBytes) {
        lock.lock();
        try {
            this.fetchMaxBytes = maxBytes <= 0 ? POLL_MAX_BYTES : maxBytes;
        } finally {
            lock.unlock();
        }
    }

    /** Poll fetch {@code max_messages} (default 100). */
    public int fetchMaxMessages() {
        lock.lock();
        try {
            return fetchMaxMessages;
        } finally {
            lock.unlock();
        }
    }

    /** Poll fetch {@code max_bytes} (default 4MiB). */
    public long fetchMaxBytes() {
        lock.lock();
        try {
            return fetchMaxBytes;
        } finally {
            lock.unlock();
        }
    }

    /** Heartbeat RPCs issued by {@link #poll} + background (not JoinGroup). */
    public long heartbeatCount() {
        lock.lock();
        try {
            return heartbeatCount;
        } finally {
            lock.unlock();
        }
    }

    public List<Codec.Assignment> lastRevoked() {
        lock.lock();
        try {
            return lastRevoked;
        } finally {
            lock.unlock();
        }
    }

    /** Current next-read positions as an unmodifiable snapshot. */
    public Map<Codec.Assignment, Long> positions() {
        lock.lock();
        try {
            Map<Codec.Assignment, Long> out = new LinkedHashMap<>();
            for (Map.Entry<Tp, Long> e : positions.entrySet()) {
                out.put(new Codec.Assignment(e.getKey().topic, e.getKey().partition), e.getValue());
            }
            return Collections.unmodifiableMap(out);
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new ProtocolException("consumer closed");
        }
    }

    static boolean needsRebalance(int code) {
        return code == ERR_REBALANCE || code == ERR_UNKNOWN_MEMBER || code == ERR_ILLEGAL_GENERATION;
    }

    static String normalizeAssignor(String name) {
        if (name == null || name.isEmpty() || ASSIGNOR_BROKER.equals(name)) {
            return ASSIGNOR_BROKER;
        }
        if (ASSIGNOR_RANGE.equals(name)) {
            return ASSIGNOR_RANGE;
        }
        throw new IllegalArgumentException("unknown assignor: " + name);
    }

    static String normalizeAutoOffsetReset(String name) {
        if (name == null || name.isEmpty() || RESET_EARLIEST.equals(name)) {
            return RESET_EARLIEST;
        }
        if (RESET_LATEST.equals(name)) {
            return RESET_LATEST;
        }
        if (RESET_NONE.equals(name)) {
            return RESET_NONE;
        }
        throw new IllegalArgumentException("unknown auto_offset_reset: " + name);
    }

    private static Set<Tp> toSet(List<Codec.Assignment> items) {
        Set<Tp> out = new HashSet<>();
        for (Codec.Assignment a : items) {
            out.add(new Tp(a.topic, a.partition));
        }
        return out;
    }

    private static List<Tp> toList(Set<Tp> items) {
        return new ArrayList<>(items);
    }

    static final class Tp implements Comparable<Tp> {
        final String topic;
        final int partition;

        Tp(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Tp)) {
                return false;
            }
            Tp other = (Tp) o;
            return partition == other.partition && Objects.equals(topic, other.topic);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hashCode(topic) + partition;
        }

        @Override
        public int compareTo(Tp o) {
            int c = topic.compareTo(o.topic);
            if (c != 0) {
                return c;
            }
            return Integer.compare(partition, o.partition);
        }
    }

    /** Package-visible so unit tests can inject a fake broker. */
    interface Backend {
        JoinGroupResult joinGroup(
                String group, String memberId, List<String> topics, int sessionTimeoutMs, String groupInstanceId);

        void heartbeat(String group, String memberId, long generation);

        void leaveGroup(String group, String memberId);

        List<Record> fetch(
                String topic, int partition, long offset, int maxMessages, long maxBytes, long maxWaitMs);

        void commitOffsets(String group, String memberId, long generation, List<Codec.OffsetCommitEntry> entries);

        List<Codec.OffsetFetchEntry> fetchOffsets(String group, List<Codec.OffsetEntry> entries);

        List<OffsetListing> listOffsets(String topic, int... partitions);

        Metadata metadata();

        DescribeGroupResult describeGroup(String group);
    }

    static final class ClientBackend implements Backend {
        private final Client client;

        ClientBackend(Client client) {
            this.client = client;
        }

        @Override
        public JoinGroupResult joinGroup(
                String group, String memberId, List<String> topics, int sessionTimeoutMs, String groupInstanceId) {
            return client.joinGroup(group, memberId, topics, sessionTimeoutMs, groupInstanceId);
        }

        @Override
        public void heartbeat(String group, String memberId, long generation) {
            client.heartbeat(group, memberId, generation);
        }

        @Override
        public void leaveGroup(String group, String memberId) {
            client.leaveGroup(group, memberId);
        }

        @Override
        public List<Record> fetch(
                String topic, int partition, long offset, int maxMessages, long maxBytes, long maxWaitMs) {
            return client.fetch(topic, partition, offset, maxMessages, maxBytes, maxWaitMs);
        }

        @Override
        public void commitOffsets(
                String group, String memberId, long generation, List<Codec.OffsetCommitEntry> entries) {
            client.offsetCommit(group, memberId, generation, entries);
        }

        @Override
        public List<Codec.OffsetFetchEntry> fetchOffsets(String group, List<Codec.OffsetEntry> entries) {
            return client.offsetFetchEntries(group, entries);
        }

        @Override
        public List<OffsetListing> listOffsets(String topic, int... partitions) {
            return client.listOffsets(topic, partitions);
        }

        @Override
        public Metadata metadata() {
            return client.metadata();
        }

        @Override
        public DescribeGroupResult describeGroup(String group) {
            return client.describeGroup(group);
        }
    }
}
