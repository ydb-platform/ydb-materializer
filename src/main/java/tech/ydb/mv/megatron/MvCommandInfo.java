package tech.ydb.mv.megatron;

import java.io.Serializable;
import java.time.Instant;

/**
 * Information about a command in the mv_commands table.
 *
 * @author zinal
 */
public class MvCommandInfo implements Serializable {
    private static final long serialVersionUID = 20250113001L;

    public static final String COMMAND_TYPE_START = "START";
    public static final String COMMAND_TYPE_STOP = "STOP";
    
    public static final String COMMAND_STATUS_CREATED = "CREATED";
    public static final String COMMAND_STATUS_TAKEN = "TAKEN";
    public static final String COMMAND_STATUS_SUCCESS = "SUCCESS";
    public static final String COMMAND_STATUS_ERROR = "ERROR";

    private final String runnerId;
    private final long commandNo;
    private final Instant createdAt;
    private final String commandType;
    private final String jobName;
    private final String jobSettings;
    private final String commandStatus;
    private final String commandDiag;

    public MvCommandInfo(String runnerId, long commandNo, Instant createdAt, String commandType, 
                        String jobName, String jobSettings, String commandStatus, String commandDiag) {
        this.runnerId = runnerId;
        this.commandNo = commandNo;
        this.createdAt = createdAt;
        this.commandType = commandType;
        this.jobName = jobName;
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
        return COMMAND_TYPE_START.equals(commandType);
    }

    public boolean isStopCommand() {
        return COMMAND_TYPE_STOP.equals(commandType);
    }

    public boolean isCreated() {
        return COMMAND_STATUS_CREATED.equals(commandStatus);
    }

    public boolean isTaken() {
        return COMMAND_STATUS_TAKEN.equals(commandStatus);
    }

    public boolean isSuccess() {
        return COMMAND_STATUS_SUCCESS.equals(commandStatus);
    }

    public boolean isError() {
        return COMMAND_STATUS_ERROR.equals(commandStatus);
    }

    @Override
    public String toString() {
        return "MvCommandInfo{" +
                "runnerId='" + runnerId + '\'' +
                ", commandNo=" + commandNo +
                ", commandType='" + commandType + '\'' +
                ", jobName='" + jobName + '\'' +
                ", commandStatus='" + commandStatus + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MvCommandInfo that = (MvCommandInfo) o;
        return commandNo == that.commandNo &&
                java.util.Objects.equals(runnerId, that.runnerId) &&
                java.util.Objects.equals(createdAt, that.createdAt) &&
                java.util.Objects.equals(commandType, that.commandType) &&
                java.util.Objects.equals(jobName, that.jobName) &&
                java.util.Objects.equals(jobSettings, that.jobSettings) &&
                java.util.Objects.equals(commandStatus, that.commandStatus) &&
                java.util.Objects.equals(commandDiag, that.commandDiag);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(runnerId, commandNo, createdAt, commandType, jobName, jobSettings, commandStatus, commandDiag);
    }
}