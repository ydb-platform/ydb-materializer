package tech.ydb.mv.mgt;

import java.io.Serializable;
import java.time.Instant;

/**
 * Information about a runner job in the mv_runner_jobs table.
 *
 * @author zinal
 */
public class MvRunnerJobInfo implements Serializable {
    private static final long serialVersionUID = 20250113001L;

    private final String runnerId;
    private final String jobName;
    private final String jobSettings;
    private final Instant startedAt;

    public MvRunnerJobInfo(String runnerId, String jobName, String jobSettings, Instant startedAt) {
        this.runnerId = runnerId;
        this.jobName = jobName;
        this.jobSettings = jobSettings;
        this.startedAt = startedAt;
    }

    public String getRunnerId() {
        return runnerId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobSettings() {
        return jobSettings;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    @Override
    public String toString() {
        return "MvRunnerJobInfo{" +
                "runnerId='" + runnerId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", startedAt=" + startedAt +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MvRunnerJobInfo that = (MvRunnerJobInfo) o;
        return java.util.Objects.equals(runnerId, that.runnerId) &&
                java.util.Objects.equals(jobName, that.jobName) &&
                java.util.Objects.equals(jobSettings, that.jobSettings) &&
                java.util.Objects.equals(startedAt, that.startedAt);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(runnerId, jobName, jobSettings, startedAt);
    }
}