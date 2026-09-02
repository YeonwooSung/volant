package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of a successful JoinGroup (Rust client field names). */
public final class JoinGroupResult {
    public final String memberId;
    public final long generation;
    public final List<Codec.Assignment> assignment;
    public final List<Codec.Assignment> revoked;

    public JoinGroupResult(
            String memberId,
            long generation,
            List<Codec.Assignment> assignment,
            List<Codec.Assignment> revoked) {
        this.memberId = memberId;
        this.generation = generation;
        this.assignment = assignment == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(assignment));
        this.revoked = revoked == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(revoked));
    }
}
