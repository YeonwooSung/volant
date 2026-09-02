package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of a successful DescribeGroup (Phase 11 / v0.49). */
public final class DescribeGroupResult {
    public final String groupId;
    public final long generation;
    public final List<Codec.GroupMemberInfo> members;

    public DescribeGroupResult(String groupId, long generation, List<Codec.GroupMemberInfo> members) {
        this.groupId = groupId == null ? "" : groupId;
        this.generation = generation;
        this.members = members == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(members));
    }
}
