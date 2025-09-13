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
    private final String runnerIdentity;
    private final Instant updatedAt;

    public MvRunnerInfo(String runnerId, String runnerIdentity, Instant updatedAt) {
        this.runnerId = runnerId;
        this.runnerIdentity = runnerIdentity;
        this.updatedAt = updatedAt;
    }

    public String getRunnerId() {
        return runnerId;
    }

    public String getRunnerIdentity() {
        return runnerIdentity;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    @Override
    public String toString() {
        return "MvRunnerInfo{" +
                "runnerId='" + runnerId + '\'' +
                ", runnerIdentity='" + runnerIdentity + '\'' +
                ", updatedAt=" + updatedAt +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MvRunnerInfo that = (MvRunnerInfo) o;
        return java.util.Objects.equals(runnerId, that.runnerId) &&
                java.util.Objects.equals(runnerIdentity, that.runnerIdentity) &&
                java.util.Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(runnerId, runnerIdentity, updatedAt);
    }
}