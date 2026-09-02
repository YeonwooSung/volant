package io.volant;

/** One flushed produce batch from EndTxn commit (Phase 18 / v0.57). */
public final class TxnProduceResult {
    public final String topic;
    public final int partition;
    public final long baseOffset;
    public final int count;

    public TxnProduceResult(String topic, int partition, long baseOffset, int count) {
        this.topic = topic == null ? "" : topic;
        this.partition = partition;
        this.baseOffset = baseOffset;
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TxnProduceResult)) {
            return false;
        }
        TxnProduceResult other = (TxnProduceResult) o;
        return partition == other.partition
                && baseOffset == other.baseOffset
                && count == other.count
                && topic.equals(other.topic);
    }

    @Override
    public int hashCode() {
        int h = topic.hashCode();
        h = 31 * h + partition;
        h = 31 * h + Long.hashCode(baseOffset);
        return 31 * h + count;
    }

    @Override
    public String toString() {
        return "TxnProduceResult{topic="
                + topic
                + ", partition="
                + partition
                + ", baseOffset="
                + baseOffset
                + ", count="
                + count
                + "}";
    }
}
