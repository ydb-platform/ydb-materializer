package tech.ydb.mv.mgt;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import tech.ydb.mv.MvConfig;

/**
 * Information about a job scan in the mv_job_scans table.
 *
 * @author zinal
 * @author Kirill Kurdyukov
 */
public class MvJobScanInfo implements Serializable {

    private static final long serialVersionUID = 20250921001L;

    private final String jobName;
    private final String targetName;
    private final String scanSettings;
    private final Instant requestedAt;
    private Instant acceptedAt;
    private String runnerId;
    private Long commandNo;

    public MvJobScanInfo(String jobName, String targetName, String scanSettings, Instant requestedAt) {
        this.jobName = jobName;
        this.targetName = targetName;
        this.scanSettings = scanSettings;
        this.requestedAt = requestedAt;
    }

    public boolean isRegularJob() {
        return jobName != null && !jobName.equals(MvConfig.HANDLER_COORDINATOR);
    }

    public String getJobName() {
        return jobName;
    }

    public String getTargetName() {
        return targetName;
    }

    public String getScanSettings() {
        return scanSettings;
    }

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public Instant getAcceptedAt() {
        return acceptedAt;
    }

    public void setAcceptedAt(Instant acceptedAt) {
        this.acceptedAt = acceptedAt;
    }

    public String getRunnerId() {
        return runnerId;
    }

    public void setRunnerId(String runnerId) {
        this.runnerId = runnerId;
    }

    public Long getCommandNo() {
        return commandNo;
    }

    public void setCommandNo(Long commandNo) {
        this.commandNo = commandNo;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.jobName);
        hash = 97 * hash + Objects.hashCode(this.targetName);
        hash = 97 * hash + Objects.hashCode(this.scanSettings);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MvJobScanInfo other = (MvJobScanInfo) obj;
        if (!Objects.equals(this.jobName, other.jobName)) {
            return false;
        }
        if (!Objects.equals(this.targetName, other.targetName)) {
            return false;
        }
        return Objects.equals(this.scanSettings, other.scanSettings);
    }

    @Override
    public String toString() {
        return "MvJobScanInfo{" + "jobName=" + jobName
                + ", target=" + targetName
                + ", settings=" + scanSettings + '}';
    }

}
