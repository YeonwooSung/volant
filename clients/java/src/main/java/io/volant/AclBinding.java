package io.volant;

import java.util.Objects;

/** One ACL binding on the wire (Phase 20 / v0.56). */
public final class AclBinding {
    /** Principal name, or {@code *}. */
    public final String principal;
    /** 0=Topic, 1=Group, 2=Cluster. */
    public final int resourceType;
    /** Resource name, or {@code *}. */
    public final String resource;
    /** 0=All … 7=ClusterAction. */
    public final int operation;
    /** 0=Deny, 1=Allow. */
    public final int permission;

    public AclBinding(String principal, int resourceType, String resource, int operation, int permission) {
        this.principal = principal == null ? "" : principal;
        this.resourceType = resourceType;
        this.resource = resource == null ? "" : resource;
        this.operation = operation;
        this.permission = permission;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AclBinding)) {
            return false;
        }
        AclBinding other = (AclBinding) o;
        return resourceType == other.resourceType
                && operation == other.operation
                && permission == other.permission
                && Objects.equals(principal, other.principal)
                && Objects.equals(resource, other.resource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, resourceType, resource, operation, permission);
    }

    @Override
    public String toString() {
        return "AclBinding{principal="
                + principal
                + ", resourceType="
                + resourceType
                + ", resource="
                + resource
                + ", operation="
                + operation
                + ", permission="
                + permission
                + "}";
    }
}
