package io.volant;

/** One partition earliest/latest pair from ListOffsets (Phase 15 / v0.50). */
public final class OffsetListing {
    public final int partition;
    public final long earliest;
    public final long latest;

    public OffsetListing(int partition, long earliest, long latest) {
        this.partition = partition;
        this.earliest = earliest;
        this.latest = latest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OffsetListing)) {
            return false;
        }
        OffsetListing other = (OffsetListing) o;
        return partition == other.partition && earliest == other.earliest && latest == other.latest;
    }

    @Override
    public int hashCode() {
        int h = 31 * partition + Long.hashCode(earliest);
        return 31 * h + Long.hashCode(latest);
    }

    @Override
    public String toString() {
        return "OffsetListing{partition=" + partition + ", earliest=" + earliest + ", latest=" + latest + "}";
    }
}
