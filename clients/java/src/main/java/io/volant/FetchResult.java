package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Fetched batch: records plus the already-decoded high watermark. */
public final class FetchResult {
    public final String topic;
    public final int partition;
    public final long highWatermark;
    public final List<Record> records;

    public FetchResult(String topic, int partition, long highWatermark, List<Record> records) {
        this.topic = topic == null ? "" : topic;
        this.partition = partition;
        this.highWatermark = highWatermark;
        this.records = records == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(records));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FetchResult)) {
            return false;
        }
        FetchResult other = (FetchResult) o;
        return partition == other.partition
                && highWatermark == other.highWatermark
                && Objects.equals(topic, other.topic)
                && Objects.equals(records, other.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, highWatermark, records);
    }

    @Override
    public String toString() {
        return "FetchResult{topic="
                + topic
                + ", partition="
                + partition
                + ", highWatermark="
                + highWatermark
                + ", records="
                + records
                + "}";
    }
}
