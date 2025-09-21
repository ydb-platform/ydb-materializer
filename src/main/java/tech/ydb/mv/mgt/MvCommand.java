package tech.ydb.mv.mgt;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Information about a command in the mv_commands table.
 *
 * @author zinal
 */
public class MvCommand implements Serializable {

    private static final long serialVersionUID = 20250921001L;

    public static final String TYPE_START = "START";
    public static final String TYPE_STOP = "STOP";
    public static final String TYPE_SCAN = "SCAN";
    public static final String TYPE_NOSCAN = "NOSCAN";

    public static final String STATUS_CREATED = "CREATED";
    public static final String STATUS_TAKEN = "TAKEN";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_ERROR = "ERROR";

    private final String runnerId;
    private final long commandNo;
    private final Instant createdAt;
    private final String commandType;
    private final String jobName;
    private final String targetName;
    private final String jobSettings;
    private final String commandStatus;
    private final String commandDiag;

    public MvCommand(String runnerId, long commandNo, Instant createdAt, String commandType,
            String jobName, String jobSettings, String commandStatus, String commandDiag) {
        this(runnerId, commandNo, createdAt, commandType, jobName,
                null, jobSettings, commandStatus, commandDiag);
    }

    public MvCommand(String runnerId, long commandNo, Instant createdAt, String commandType,
            String jobName, String targetName, String jobSettings,
            String commandStatus, String commandDiag) {
        this.runnerId = runnerId;
        this.commandNo = commandNo;
        this.createdAt = createdAt;
        this.commandType = commandType;
        this.jobName = jobName;
        this.targetName = targetName;
        this.jobSettings = jobSettings;
        this.commandStatus = commandStatus;
        this.commandDiag = commandDiag;
    }

    public String getRunnerId() {
        return runnerId;
    }

    public long getCommandNo() {
        return commandNo;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public String getCommandType() {
        return commandType;
    }

    public String getJobName() {
        return jobName;
    }

    public String getTargetName() {
        return targetName;
    }

    public String getJobSettings() {
        return jobSettings;
    }

    public String getCommandStatus() {
        return commandStatus;
    }

    public String getCommandDiag() {
        return commandDiag;
    }

    public boolean isStartCommand() {
        return TYPE_START.equals(commandType);
    }

    public boolean isStopCommand() {
        return TYPE_STOP.equals(commandType);
    }

    public boolean isScanCommand() {
        return TYPE_SCAN.equals(commandType);
    }

    public boolean isNoScanCommand() {
        return TYPE_NOSCAN.equals(commandType);
    }

    public boolean isCreated() {
        return STATUS_CREATED.equals(commandStatus);
    }

    public boolean isTaken() {
        return STATUS_TAKEN.equals(commandStatus);
    }

    public boolean isSuccess() {
        return STATUS_SUCCESS.equals(commandStatus);
    }

    public boolean isError() {
        return STATUS_ERROR.equals(commandStatus);
    }

    @Override
    public String toString() {
        return "MvCommandInfo{"
                + "runnerId='" + runnerId + '\''
                + ", no=" + commandNo
                + ", type='" + commandType + '\''
                + ", name='" + jobName + '\''
                + ", target='" + targetName + '\''
                + ", status='" + commandStatus + '\''
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
        MvCommand that = (MvCommand) o;
        return commandNo == that.commandNo
                && Objects.equals(runnerId, that.runnerId)
                && Objects.equals(createdAt, that.createdAt)
                && Objects.equals(commandType, that.commandType)
                && Objects.equals(jobName, that.jobName)
                && Objects.equals(targetName, that.targetName)
                && Objects.equals(jobSettings, that.jobSettings)
                && Objects.equals(commandStatus, that.commandStatus)
                && Objects.equals(commandDiag, that.commandDiag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runnerId, commandNo);
    }
}
