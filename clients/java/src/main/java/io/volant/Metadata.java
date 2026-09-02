package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Cluster brokers and topics from the Metadata opcode. */
public final class Metadata {
    public final List<BrokerInfo> brokers;
    public final List<TopicInfo> topics;

    public Metadata(List<BrokerInfo> brokers, List<TopicInfo> topics) {
        this.brokers = copy(brokers);
        this.topics = copy(topics);
    }

    public static final class BrokerInfo {
        public final long nodeId;
        public final String host;
        public final int port;

        public BrokerInfo(long nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host == null ? "" : host;
            this.port = port;
        }
    }

    public static final class PartitionInfo {
        public final long partitionId;
        public final long leader;
        public final long hwm;
        public final List<Long> replicas;
        public final List<Long> isr;
        public final long leaderEpoch;

        public PartitionInfo(
                long partitionId,
                long leader,
                long hwm,
                List<Long> replicas,
                List<Long> isr,
                long leaderEpoch) {
            this.partitionId = partitionId;
            this.leader = leader;
            this.hwm = hwm;
            this.replicas = copy(replicas);
            this.isr = copy(isr);
            this.leaderEpoch = leaderEpoch;
        }
    }

    public static final class TopicInfo {
        public final String name;
        public final long topicId;
        public final int errorCode;
        public final List<PartitionInfo> partitions;

        public TopicInfo(String name, long topicId, int errorCode, List<PartitionInfo> partitions) {
            this.name = name == null ? "" : name;
            this.topicId = topicId;
            this.errorCode = errorCode;
            this.partitions = copy(partitions);
        }
    }

    private static <T> List<T> copy(List<T> in) {
        if (in == null || in.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(in));
    }
}
