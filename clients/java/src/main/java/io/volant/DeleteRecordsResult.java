package io.volant;

/** Result of DeleteRecords (Phase 14 / v0.52). */
public final class DeleteRecordsResult {
    public final String topic;
    public final int partition;
    public final long lowWatermark;

    public DeleteRecordsResult(String topic, int partition, long lowWatermark) {
        this.topic = topic;
        this.partition = partition;
        this.lowWatermark = lowWatermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeleteRecordsResult)) {
            return false;
        }
        DeleteRecordsResult other = (DeleteRecordsResult) o;
        return partition == other.partition
                && lowWatermark == other.lowWatermark
                && (topic == null ? other.topic == null : topic.equals(other.topic));
    }

    @Override
    public int hashCode() {
        int h = topic == null ? 0 : topic.hashCode();
        h = 31 * h + partition;
        return 31 * h + Long.hashCode(lowWatermark);
    }

    @Override
    public String toString() {
        return "DeleteRecordsResult{topic=" + topic + ", partition=" + partition
                + ", lowWatermark=" + lowWatermark + "}";
    }
}
