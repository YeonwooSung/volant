package io.volant;

/** One deferred offset commit inside EndTxn (Phase 18 / v0.57). */
public final class TxnOffsetCommit {
    public final String groupId;
    public final String topic;
    public final int partition;
    public final long offset;
    public final String metadata;

    public TxnOffsetCommit(String groupId, String topic, int partition, long offset, String metadata) {
        this.groupId = groupId == null ? "" : groupId;
        this.topic = topic == null ? "" : topic;
        this.partition = partition;
        this.offset = offset;
        this.metadata = metadata == null ? "" : metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TxnOffsetCommit)) {
            return false;
        }
        TxnOffsetCommit other = (TxnOffsetCommit) o;
        return partition == other.partition
                && offset == other.offset
                && groupId.equals(other.groupId)
                && topic.equals(other.topic)
                && metadata.equals(other.metadata);
    }

    @Override
    public int hashCode() {
        int h = groupId.hashCode();
        h = 31 * h + topic.hashCode();
        h = 31 * h + partition;
        h = 31 * h + Long.hashCode(offset);
        return 31 * h + metadata.hashCode();
    }

    @Override
    public String toString() {
        return "TxnOffsetCommit{groupId="
                + groupId
                + ", topic="
                + topic
                + ", partition="
                + partition
                + ", offset="
                + offset
                + ", metadata="
                + metadata
                + "}";
    }
}
