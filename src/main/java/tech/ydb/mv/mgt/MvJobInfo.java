package tech.ydb.mv.mgt;

import java.io.Serializable;

import tech.ydb.mv.MvConfig;

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

    public MvJobInfo(String jobName, String jobSettings, boolean shouldRun) {
        if (jobName == null) {
            throw new IllegalArgumentException();
        }
        this.jobName = jobName;
        this.jobSettings = jobSettings;
        this.shouldRun = shouldRun;
    }

    public boolean isRegularJob() {
        return jobName != null && !jobName.equals(MvConfig.HANDLER_COORDINATOR);
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

    @Override
    public String toString() {
        return "MvJobInfo{"
                + "jobName='" + jobName + '\''
                + ", shouldRun=" + shouldRun
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MvJobInfo other = (MvJobInfo) o;
        return shouldRun == other.shouldRun
                && java.util.Objects.equals(jobName, other.jobName)
                && java.util.Objects.equals(jobSettings, other.jobSettings);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(jobName, jobSettings, shouldRun);
    }
}
