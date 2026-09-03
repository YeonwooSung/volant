package io.volant;

import java.util.Objects;

/** One committed (topic, partition, offset) from OffsetFetchAll. */
public final class OffsetFetchEntry {
    public final String topic;
    public final int partition;
    public final long offset;

    public OffsetFetchEntry(String topic, int partition, long offset) {
        this.topic = topic == null ? "" : topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OffsetFetchEntry)) {
            return false;
        }
        OffsetFetchEntry other = (OffsetFetchEntry) o;
        return partition == other.partition
                && offset == other.offset
                && Objects.equals(topic, other.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
        return "OffsetFetchEntry{topic=" + topic + ", partition=" + partition + ", offset=" + offset + "}";
    }
}
