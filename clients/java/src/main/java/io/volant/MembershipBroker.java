package io.volant;

import java.util.Objects;

/** One overlay broker endpoint (v0.10 / v0.58). {@code rack == null} is absent. */
public final class MembershipBroker {
    public final int id;
    public final String host;
    public final int port;
    public final String rack;

    public MembershipBroker(int id, String host, int port) {
        this(id, host, port, null);
    }

    public MembershipBroker(int id, String host, int port, String rack) {
        this.id = id;
        this.host = host == null ? "" : host;
        this.port = port;
        this.rack = rack;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MembershipBroker)) {
            return false;
        }
        MembershipBroker other = (MembershipBroker) o;
        return id == other.id
                && port == other.port
                && Objects.equals(host, other.host)
                && Objects.equals(rack, other.rack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port, rack);
    }

    @Override
    public String toString() {
        return "MembershipBroker{id=" + id + ", host=" + host + ", port=" + port + ", rack=" + rack + "}";
    }
}
