package tech.ydb.mv.mgt;

import java.io.Serializable;

/**
 * Information about a job in the mv_jobs table.
 *
 * @author zinal
 */
public class MvJobInfo implements Serializable {
    private static final long serialVersionUID = 20250113001L;

    private final String jobName;
    private final String jobSettings;
    private final boolean shouldRun;
    private final String runnerId;

    public MvJobInfo(String jobName, String jobSettings, boolean shouldRun, String runnerId) {
        if (jobName==null || runnerId==null) {
            throw new IllegalArgumentException();
        }
        this.jobName = jobName;
        this.jobSettings = jobSettings;
        this.shouldRun = shouldRun;
        this.runnerId = runnerId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobSettings() {
        return jobSettings;
    }

    public boolean isShouldRun() {
        return shouldRun;
    }

    public String getRunnerId() {
        return runnerId;
    }

    @Override
    public String toString() {
        return "MvJobInfo{" +
                "jobName='" + jobName + '\'' +
                ", shouldRun=" + shouldRun +
                ", runnerId='" + runnerId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MvJobInfo mvJobInfo = (MvJobInfo) o;
        return shouldRun == mvJobInfo.shouldRun &&
                java.util.Objects.equals(jobName, mvJobInfo.jobName) &&
                java.util.Objects.equals(jobSettings, mvJobInfo.jobSettings) &&
                java.util.Objects.equals(runnerId, mvJobInfo.runnerId);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(jobName, jobSettings, shouldRun, runnerId);
    }
}