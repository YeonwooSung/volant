package io.volant;

/** One committed (partition, offset) pair from OffsetFetch. */
public final class Offset {
    public final int partition;
    public final long offset;

    public Offset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Offset)) {
            return false;
        }
        Offset other = (Offset) o;
        return partition == other.partition && offset == other.offset;
    }

    @Override
    public int hashCode() {
        return 31 * partition + Long.hashCode(offset);
    }

    @Override
    public String toString() {
        return "Offset{partition=" + partition + ", offset=" + offset + "}";
    }
}
