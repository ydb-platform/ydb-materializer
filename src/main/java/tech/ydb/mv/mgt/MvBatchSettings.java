package tech.ydb.mv.mgt;

import java.io.Serializable;
import java.util.Properties;

/**
 * Configuration settings for the distributed job management system. Provides
 * customizable table names and database scan/reporting periods.
 *
 * @author zinal
 */
public class MvBatchSettings implements Serializable {

    private static final long serialVersionUID = 20250113001L;

    // Configuration property keys
    public static final String CONF_TABLE_JOBS = "megatron.mv.jobs.table";
    public static final String CONF_TABLE_RUNNERS = "megatron.mv.runners.table";
    public static final String CONF_TABLE_RUNNER_JOBS = "megatron.mv.runner.jobs.table";
    public static final String CONF_TABLE_COMMANDS = "megatron.mv.commands.table";
    public static final String CONF_SCAN_PERIOD_MS = "megatron.scan.period.ms";
    public static final String CONF_REPORT_PERIOD_MS = "megatron.report.period.ms";
    public static final String CONF_RUNNER_TIMEOUT_MS = "megatron.runner.timeout.ms";
    public static final String CONF_COORDINATOR_TIMEOUT_MS = "megatron.coordinator.timeout.ms";

    // Default values
    public static final String DEF_TABLE_JOBS = "mv_jobs";
    public static final String DEF_TABLE_RUNNERS = "mv_runners";
    public static final String DEF_TABLE_RUNNER_JOBS = "mv_runner_jobs";
    public static final String DEF_TABLE_COMMANDS = "mv_commands";
    public static final long DEF_SCAN_PERIOD_MS = 5000L; // 5 seconds
    public static final long DEF_REPORT_PERIOD_MS = 10000L; // 10 seconds
    public static final long DEF_RUNNER_TIMEOUT_MS = 30000L; // 30 seconds
    public static final long DEF_COORDINATOR_TIMEOUT_MS = 60000L; // 60 seconds

    // Table names
    private String tableJobs = DEF_TABLE_JOBS;
    private String tableRunners = DEF_TABLE_RUNNERS;
    private String tableRunnerJobs = DEF_TABLE_RUNNER_JOBS;
    private String tableCommands = DEF_TABLE_COMMANDS;

    // Timing settings
    private long scanPeriodMs = DEF_SCAN_PERIOD_MS;
    private long reportPeriodMs = DEF_REPORT_PERIOD_MS;
    private long runnerTimeoutMs = DEF_RUNNER_TIMEOUT_MS;
    private long coordinatorTimeoutMs = DEF_COORDINATOR_TIMEOUT_MS;

    public MvBatchSettings() {
    }

    public MvBatchSettings(MvBatchSettings src) {
        this.tableJobs = src.tableJobs;
        this.tableRunners = src.tableRunners;
        this.tableRunnerJobs = src.tableRunnerJobs;
        this.tableCommands = src.tableCommands;
        this.scanPeriodMs = src.scanPeriodMs;
        this.reportPeriodMs = src.reportPeriodMs;
        this.runnerTimeoutMs = src.runnerTimeoutMs;
        this.coordinatorTimeoutMs = src.coordinatorTimeoutMs;
    }

    public MvBatchSettings(Properties props) {
        loadFromProperties(props);
    }

    private void loadFromProperties(Properties props) {
        this.tableJobs = props.getProperty(CONF_TABLE_JOBS, DEF_TABLE_JOBS);
        this.tableRunners = props.getProperty(CONF_TABLE_RUNNERS, DEF_TABLE_RUNNERS);
        this.tableRunnerJobs = props.getProperty(CONF_TABLE_RUNNER_JOBS, DEF_TABLE_RUNNER_JOBS);
        this.tableCommands = props.getProperty(CONF_TABLE_COMMANDS, DEF_TABLE_COMMANDS);

        String v = props.getProperty(CONF_SCAN_PERIOD_MS, String.valueOf(DEF_SCAN_PERIOD_MS));
        this.scanPeriodMs = Long.parseLong(v);

        v = props.getProperty(CONF_REPORT_PERIOD_MS, String.valueOf(DEF_REPORT_PERIOD_MS));
        this.reportPeriodMs = Long.parseLong(v);

        v = props.getProperty(CONF_RUNNER_TIMEOUT_MS, String.valueOf(DEF_RUNNER_TIMEOUT_MS));
        this.runnerTimeoutMs = Long.parseLong(v);

        v = props.getProperty(CONF_COORDINATOR_TIMEOUT_MS, String.valueOf(DEF_COORDINATOR_TIMEOUT_MS));
        this.coordinatorTimeoutMs = Long.parseLong(v);
    }

    public String getTableJobs() {
        return tableJobs;
    }

    public void setTableJobs(String tableJobs) {
        this.tableJobs = tableJobs;
    }

    public String getTableRunners() {
        return tableRunners;
    }

    public void setTableRunners(String tableRunners) {
        this.tableRunners = tableRunners;
    }

    public String getTableRunnerJobs() {
        return tableRunnerJobs;
    }

    public void setTableRunnerJobs(String tableRunnerJobs) {
        this.tableRunnerJobs = tableRunnerJobs;
    }

    public String getTableCommands() {
        return tableCommands;
    }

    public void setTableCommands(String tableCommands) {
        this.tableCommands = tableCommands;
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
}
