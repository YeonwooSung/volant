package io.volant;

import java.util.Objects;

/** One committed (topic, partition, offset, metadata) from OffsetFetchAll. */
public final class OffsetFetchEntry {
    public final String topic;
    public final int partition;
    public final long offset;
    public final String metadata;

    public OffsetFetchEntry(String topic, int partition, long offset) {
        this(topic, partition, offset, "");
    }

    public OffsetFetchEntry(String topic, int partition, long offset, String metadata) {
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
        if (!(o instanceof OffsetFetchEntry)) {
            return false;
        }
        OffsetFetchEntry other = (OffsetFetchEntry) o;
        return partition == other.partition
                && offset == other.offset
                && Objects.equals(topic, other.topic)
                && Objects.equals(metadata, other.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, metadata);
    }

    @Override
    public String toString() {
        return "OffsetFetchEntry{topic="
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
