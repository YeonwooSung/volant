package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Configured + live membership (v0.10 / v0.58). */
public final class MembershipList {
    public final long generation;
    public final List<MembershipBroker> brokers;
    public final List<Integer> live;

    public MembershipList(long generation, List<MembershipBroker> brokers, List<Integer> live) {
        this.generation = generation;
        this.brokers = brokers == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(brokers));
        this.live = live == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(live));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MembershipList)) {
            return false;
        }
        MembershipList other = (MembershipList) o;
        return generation == other.generation
                && Objects.equals(brokers, other.brokers)
                && Objects.equals(live, other.live);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generation, brokers, live);
    }

    @Override
    public String toString() {
        return "MembershipList{generation=" + generation + ", brokers=" + brokers + ", live=" + live + "}";
    }
}
