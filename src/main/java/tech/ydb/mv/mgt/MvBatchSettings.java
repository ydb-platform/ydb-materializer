package tech.ydb.mv.mgt;

import java.io.Serializable;
import java.util.Properties;

/**
 * Configuration settings for the distributed job management system.
 * Provides customizable table names and database scan/reporting periods.
 *
 * @author zinal
 */
public class MvBatchSettings implements Serializable {
    private static final long serialVersionUID = 20250113001L;

    // Configuration property keys
    public static final String CONF_MV_JOBS_TABLE = "megatron.mv.jobs.table";
    public static final String CONF_MV_RUNNERS_TABLE = "megatron.mv.runners.table";
    public static final String CONF_MV_RUNNER_JOBS_TABLE = "megatron.mv.runner.jobs.table";
    public static final String CONF_MV_COMMANDS_TABLE = "megatron.mv.commands.table";
    public static final String CONF_SCAN_PERIOD_MS = "megatron.scan.period.ms";
    public static final String CONF_REPORT_PERIOD_MS = "megatron.report.period.ms";
    public static final String CONF_RUNNER_TIMEOUT_MS = "megatron.runner.timeout.ms";
    public static final String CONF_COORDINATOR_TIMEOUT_MS = "megatron.coordinator.timeout.ms";

    // Default values
    public static final String DEF_MV_JOBS_TABLE = "mv_jobs";
    public static final String DEF_MV_RUNNERS_TABLE = "mv_runners";
    public static final String DEF_MV_RUNNER_JOBS_TABLE = "mv_runner_jobs";
    public static final String DEF_MV_COMMANDS_TABLE = "mv_commands";
    public static final long DEF_SCAN_PERIOD_MS = 5000L; // 5 seconds
    public static final long DEF_REPORT_PERIOD_MS = 10000L; // 10 seconds
    public static final long DEF_RUNNER_TIMEOUT_MS = 30000L; // 30 seconds
    public static final long DEF_COORDINATOR_TIMEOUT_MS = 60000L; // 60 seconds

    // Table names
    private String mvJobsTable = DEF_MV_JOBS_TABLE;
    private String mvRunnersTable = DEF_MV_RUNNERS_TABLE;
    private String mvRunnerJobsTable = DEF_MV_RUNNER_JOBS_TABLE;
    private String mvCommandsTable = DEF_MV_COMMANDS_TABLE;

    // Timing settings
    private long scanPeriodMs = DEF_SCAN_PERIOD_MS;
    private long reportPeriodMs = DEF_REPORT_PERIOD_MS;
    private long runnerTimeoutMs = DEF_RUNNER_TIMEOUT_MS;
    private long coordinatorTimeoutMs = DEF_COORDINATOR_TIMEOUT_MS;

    public MvBatchSettings() {
    }

    public MvBatchSettings(MvBatchSettings src) {
        this.mvJobsTable = src.mvJobsTable;
        this.mvRunnersTable = src.mvRunnersTable;
        this.mvRunnerJobsTable = src.mvRunnerJobsTable;
        this.mvCommandsTable = src.mvCommandsTable;
        this.scanPeriodMs = src.scanPeriodMs;
        this.reportPeriodMs = src.reportPeriodMs;
        this.runnerTimeoutMs = src.runnerTimeoutMs;
        this.coordinatorTimeoutMs = src.coordinatorTimeoutMs;
    }

    public MvBatchSettings(Properties props) {
        loadFromProperties(props);
    }

    private void loadFromProperties(Properties props) {
        this.mvJobsTable = props.getProperty(CONF_MV_JOBS_TABLE, DEF_MV_JOBS_TABLE);
        this.mvRunnersTable = props.getProperty(CONF_MV_RUNNERS_TABLE, DEF_MV_RUNNERS_TABLE);
        this.mvRunnerJobsTable = props.getProperty(CONF_MV_RUNNER_JOBS_TABLE, DEF_MV_RUNNER_JOBS_TABLE);
        this.mvCommandsTable = props.getProperty(CONF_MV_COMMANDS_TABLE, DEF_MV_COMMANDS_TABLE);

        String v = props.getProperty(CONF_SCAN_PERIOD_MS, String.valueOf(DEF_SCAN_PERIOD_MS));
        this.scanPeriodMs = Long.parseLong(v);

        v = props.getProperty(CONF_REPORT_PERIOD_MS, String.valueOf(DEF_REPORT_PERIOD_MS));
        this.reportPeriodMs = Long.parseLong(v);

        v = props.getProperty(CONF_RUNNER_TIMEOUT_MS, String.valueOf(DEF_RUNNER_TIMEOUT_MS));
        this.runnerTimeoutMs = Long.parseLong(v);

        v = props.getProperty(CONF_COORDINATOR_TIMEOUT_MS, String.valueOf(DEF_COORDINATOR_TIMEOUT_MS));
        this.coordinatorTimeoutMs = Long.parseLong(v);
    }

    // Getters and setters for table names
    public String getMvJobsTable() {
        return mvJobsTable;
    }

    public void setMvJobsTable(String mvJobsTable) {
        this.mvJobsTable = mvJobsTable;
    }

    public String getMvRunnersTable() {
        return mvRunnersTable;
    }

    public void setMvRunnersTable(String mvRunnersTable) {
        this.mvRunnersTable = mvRunnersTable;
    }

    public String getMvRunnerJobsTable() {
        return mvRunnerJobsTable;
    }

    public void setMvRunnerJobsTable(String mvRunnerJobsTable) {
        this.mvRunnerJobsTable = mvRunnerJobsTable;
    }

    public String getMvCommandsTable() {
        return mvCommandsTable;
    }

    public void setMvCommandsTable(String mvCommandsTable) {
        this.mvCommandsTable = mvCommandsTable;
    }

    // Getters and setters for timing settings
    public long getScanPeriodMs() {
        return scanPeriodMs;
    }

    public void setScanPeriodMs(long scanPeriodMs) {
        this.scanPeriodMs = scanPeriodMs;
    }

    public long getReportPeriodMs() {
        return reportPeriodMs;
    }

    public void setReportPeriodMs(long reportPeriodMs) {
        this.reportPeriodMs = reportPeriodMs;
    }

    public long getRunnerTimeoutMs() {
        return runnerTimeoutMs;
    }

    public void setRunnerTimeoutMs(long runnerTimeoutMs) {
        this.runnerTimeoutMs = runnerTimeoutMs;
    }

    public long getCoordinatorTimeoutMs() {
        return coordinatorTimeoutMs;
    }

    public void setCoordinatorTimeoutMs(long coordinatorTimeoutMs) {
        this.coordinatorTimeoutMs = coordinatorTimeoutMs;
    }

    /**
     * Get the full table name with database prefix.
     */
    public String getFullTableName(String tableName, String database) {
        if (tableName.startsWith("/")) {
            return tableName;
        }
        return database + "/" + tableName;
    }

    /**
     * Get the full table name for mv_jobs table.
     */
    public String getFullMvJobsTable(String database) {
        return getFullTableName(mvJobsTable, database);
    }

    /**
     * Get the full table name for mv_runners table.
     */
    public String getFullMvRunnersTable(String database) {
        return getFullTableName(mvRunnersTable, database);
    }

    /**
     * Get the full table name for mv_runner_jobs table.
     */
    public String getFullMvRunnerJobsTable(String database) {
        return getFullTableName(mvRunnerJobsTable, database);
    }

    /**
     * Get the full table name for mv_commands table.
     */
    public String getFullMvCommandsTable(String database) {
        return getFullTableName(mvCommandsTable, database);
    }
}