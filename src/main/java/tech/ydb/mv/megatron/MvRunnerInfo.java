package tech.ydb.mv.megatron;

import java.io.Serializable;
import java.time.Instant;

/**
 * Information about a runner in the mv_runners table.
 *
 * @author zinal
 */
public class MvRunnerInfo implements Serializable {
    private static final long serialVersionUID = 20250113001L;

    private final String runnerId;
    private final String identity;
    private final Instant updatedAt;

    public MvRunnerInfo(String runnerId, String identity, Instant updatedAt) {
        if (identity==null) {
            identity = makeIdentity();
        }
        if (updatedAt == null) {
            updatedAt = Instant.now();
        }
        this.runnerId = runnerId;
        this.identity = identity;
        this.updatedAt = updatedAt;
    }

    public MvRunnerInfo(String runnerId) {
        this(runnerId, null, null);
    }

    public String getRunnerId() {
        return runnerId;
    }

    public String getIdentity() {
        return identity;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    @Override
    public String toString() {
        return "MvRunnerInfo{" +
                "runnerId='" + runnerId + '\'' +
                ", runnerIdentity='" + identity + '\'' +
                ", updatedAt=" + updatedAt +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MvRunnerInfo that = (MvRunnerInfo) o;
        return java.util.Objects.equals(runnerId, that.runnerId) &&
                java.util.Objects.equals(identity, that.identity) &&
                java.util.Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(runnerId, identity, updatedAt);
    }

    private String makeIdentity() {
        try {
            // Get host name
            String hostName = java.net.InetAddress.getLocalHost().getHostName();

            // Get process identifier
            long pid = ProcessHandle.current().pid();

            // Combine the information
            StringBuilder sb = new StringBuilder();
            sb.append(hostName).append(" / ");
            sb.append(pid);
            return sb.toString();
        } catch (Exception e) {
            // Fallback to a basic identifier if anything fails
            return "unknown-" + System.currentTimeMillis();
        }
    }
}