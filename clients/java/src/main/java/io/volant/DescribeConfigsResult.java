package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of a successful DescribeConfigs (Phase 13 / v0.53). */
public final class DescribeConfigsResult {
    public final String topic;
    public final long topicId;
    public final long partitionCount;
    public final List<String[]> configs;

    public DescribeConfigsResult(String topic, long topicId, long partitionCount, List<String[]> configs) {
        this.topic = topic == null ? "" : topic;
        this.topicId = topicId;
        this.partitionCount = partitionCount;
        this.configs = configs == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(configs));
    }
}
