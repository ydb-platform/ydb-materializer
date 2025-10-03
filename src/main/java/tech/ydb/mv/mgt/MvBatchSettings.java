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

    private static final long serialVersionUID = 20251003001L;

    // Configuration property keys
    public static final String CONF_TABLE_JOBS = "mv.jobs.table";
    public static final String CONF_TABLE_SCANS = "mv.scans.table";
    public static final String CONF_TABLE_RUNNERS = "mv.runners.table";
    public static final String CONF_TABLE_RUNNER_JOBS = "mv.runner.jobs.table";
    public static final String CONF_TABLE_COMMANDS = "mv.commands.table";
    public static final String CONF_SCAN_PERIOD_MS = "mv.scan.period.ms";
    public static final String CONF_REPORT_PERIOD_MS = "mv.report.period.ms";
    public static final String CONF_RUNNER_TIMEOUT_MS = "mv.runner.timeout.ms";
    public static final String CONF_COORD_STARTUP_MS = "mv.coord.startup.ms";
    public static final String CONF_COORD_RUNNERS_COUNT = "mv.coord.runners.count";

    // Default values
    public static final String DEF_TABLE_JOBS = "mv_jobs";
    public static final String DEF_TABLE_SCANS = "mv_job_scans";
    public static final String DEF_TABLE_RUNNERS = "mv_runners";
    public static final String DEF_TABLE_RUNNER_JOBS = "mv_runner_jobs";
    public static final String DEF_TABLE_COMMANDS = "mv_commands";

    public static final long DEF_SCAN_PERIOD_MS = 5000L; // 5 seconds
    public static final long DEF_REPORT_PERIOD_MS = 10000L; // 10 seconds
    public static final long DEF_RUNNER_TIMEOUT_MS = 30000L; // 30 seconds
    public static final long DEF_COORD_STARTUP_MS = 90000L; // 90 seconds

    // Table names
    private String tableJobs = DEF_TABLE_JOBS;
    private String tableScans = DEF_TABLE_SCANS;
    private String tableRunners = DEF_TABLE_RUNNERS;
    private String tableRunnerJobs = DEF_TABLE_RUNNER_JOBS;
    private String tableCommands = DEF_TABLE_COMMANDS;

    // Timing settings
    private long scanPeriodMs = DEF_SCAN_PERIOD_MS;
    private long reportPeriodMs = DEF_REPORT_PERIOD_MS;
    private long runnerTimeoutMs = DEF_RUNNER_TIMEOUT_MS;
    private long coordStartupMs = DEF_COORD_STARTUP_MS;

    // Extra settings
    private int runnersCount = 0;

    public MvBatchSettings() {
    }

    public MvBatchSettings(MvBatchSettings src) {
        this.tableJobs = src.tableJobs;
        this.tableScans = src.tableScans;
        this.tableRunners = src.tableRunners;
        this.tableRunnerJobs = src.tableRunnerJobs;
        this.tableCommands = src.tableCommands;
        this.scanPeriodMs = src.scanPeriodMs;
        this.reportPeriodMs = src.reportPeriodMs;
        this.runnerTimeoutMs = src.runnerTimeoutMs;
        this.coordStartupMs = src.coordStartupMs;
        this.runnersCount = src.runnersCount;
    }

    public MvBatchSettings(Properties props) {
        loadFromProperties(props);
    }

    private void loadFromProperties(Properties props) {
        this.tableJobs = props.getProperty(CONF_TABLE_JOBS, DEF_TABLE_JOBS);
        this.tableScans = props.getProperty(CONF_TABLE_SCANS, DEF_TABLE_SCANS);
        this.tableRunners = props.getProperty(CONF_TABLE_RUNNERS, DEF_TABLE_RUNNERS);
        this.tableRunnerJobs = props.getProperty(CONF_TABLE_RUNNER_JOBS, DEF_TABLE_RUNNER_JOBS);
        this.tableCommands = props.getProperty(CONF_TABLE_COMMANDS, DEF_TABLE_COMMANDS);

        String v = props.getProperty(CONF_SCAN_PERIOD_MS, String.valueOf(DEF_SCAN_PERIOD_MS));
        this.scanPeriodMs = Long.parseLong(v);

        v = props.getProperty(CONF_REPORT_PERIOD_MS, String.valueOf(DEF_REPORT_PERIOD_MS));
        this.reportPeriodMs = Long.parseLong(v);

        v = props.getProperty(CONF_RUNNER_TIMEOUT_MS, String.valueOf(DEF_RUNNER_TIMEOUT_MS));
        this.runnerTimeoutMs = Long.parseLong(v);

        v = props.getProperty(CONF_COORD_STARTUP_MS, String.valueOf(DEF_COORD_STARTUP_MS));
        this.coordStartupMs = Long.parseLong(v);

        v = props.getProperty(CONF_COORD_RUNNERS_COUNT, "0");
        this.runnersCount = Integer.parseInt(v);
    }

    public String getTableJobs() {
        return tableJobs;
    }

    public void setTableJobs(String tableJobs) {
        this.tableJobs = tableJobs;
    }

    public String getTableScans() {
        return tableScans;
    }

    public void setTableScans(String tableScans) {
        this.tableScans = tableScans;
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

    public long getCoordStartupMs() {
        return coordStartupMs;
    }

    public void setCoordStartupMs(long coordStartupMs) {
        this.coordStartupMs = coordStartupMs;
    }

    public int getRunnersCount() {
        return runnersCount;
    }

    public void setRunnersCount(int runnersCount) {
        this.runnersCount = runnersCount;
    }
}
