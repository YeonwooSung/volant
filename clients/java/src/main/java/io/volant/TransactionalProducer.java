package io.volant;

import java.util.ArrayList;
import java.util.List;

/**
 * Thin wrapper around {@link Client#beginTransaction()} / EndTxn (v0.63).
 *
 * <p>Matches {@code crates/volant-client/src/txn.rs}. Native opcodes 50–53
 * only; not Kafka transactions. {@link #addOffsets} queues locally; nothing
 * is sent until {@link #commit()}. Produce is write-through.
 */
public final class TransactionalProducer {
    private final Client client;
    private final List<TxnOffsetCommit> pending = new ArrayList<>();
    private boolean open;

    private TransactionalProducer(Client client) {
        this.client = client;
    }

    /**
     * Wrap an existing client that has {@code transactional_id} configured
     * (same check as {@link Client#beginTransaction()}).
     */
    public static TransactionalProducer from(Client client) {
        if (client == null
                || client.transactionalId() == null
                || client.transactionalId().isEmpty()) {
            throw new IllegalStateException("transactional_id not configured");
        }
        return new TransactionalProducer(client);
    }

    public Client client() {
        return client;
    }

    /** Open a native transaction. Double-begin while open is an error. */
    public void begin() {
        if (open) {
            throw new IllegalStateException("transaction already open");
        }
        client.beginTransaction();
        pending.clear();
        open = true;
    }

    /** Delegates to {@link Client#produce} (write-through). */
    public long produce(String topic, int partition, byte[] key, byte[] value) {
        return client.produce(topic, partition, key, value);
    }

    /** Queue one group offset. Nothing is sent until {@link #commit()}. */
    public void addOffsets(String groupId, String topic, int partition, long offset) {
        pending.add(new TxnOffsetCommit(groupId, topic, partition, offset, ""));
    }

    /**
     * Queue group offsets from existing {@link TxnOffsetCommit} rows. The
     * {@code groupId} argument is applied to each queued row; topic /
     * partition / offset / metadata are taken from {@code offsets}.
     */
    public void addOffsets(String groupId, List<TxnOffsetCommit> offsets) {
        if (offsets == null) {
            return;
        }
        for (TxnOffsetCommit o : offsets) {
            pending.add(new TxnOffsetCommit(
                    groupId, o.topic, o.partition, o.offset, o.metadata));
        }
    }

    /** EndTxn committed=1 with the queued offsets. */
    public List<TxnProduceResult> commit() {
        if (!open) {
            throw new IllegalStateException("transaction is not open");
        }
        List<TxnOffsetCommit> offsets = new ArrayList<>(pending);
        pending.clear();
        List<TxnProduceResult> results = client.commitTransaction(offsets);
        open = false;
        return results;
    }

    /** Clear the offset queue and send EndTxn committed=0. */
    public void abort() {
        if (!open) {
            throw new IllegalStateException("transaction is not open");
        }
        pending.clear();
        client.abortTransaction();
        open = false;
    }

    /** Whether a transaction is open locally. */
    public boolean isOpen() {
        return open;
    }
}
