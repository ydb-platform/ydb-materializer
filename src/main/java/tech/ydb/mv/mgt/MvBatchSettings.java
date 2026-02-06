package tech.ydb.mv.mgt;

import java.io.Serializable;
import java.util.Properties;

import tech.ydb.mv.MvConfig;

/**
 * Configuration settings for the distributed job management system. Provides
 * customizable table names and database scan/reporting periods.
 *
 * @author zinal
 */
public class MvBatchSettings implements Serializable {

    private static final long serialVersionUID = 20251003001L;

    /**
     * Custom MV_JOBS table name.
     */
    public static final String CONF_TABLE_JOBS = "mv.jobs.table";
    /**
     * Custom MV_JOB_SCANS table name.
     */
    public static final String CONF_TABLE_SCANS = "mv.scans.table";
    /**
     * Custom MV_RUNNERS table name.
     */
    public static final String CONF_TABLE_RUNNERS = "mv.runners.table";
    /**
     * Custom MV_RUNNER_JOBS table name.
     */
    public static final String CONF_TABLE_RUNNER_JOBS = "mv.runner.jobs.table";
    /**
     * Custom MV_COMMANDS table name.
     */
    public static final String CONF_TABLE_COMMANDS = "mv.commands.table";
    /**
     * Runner and Coordinator re-scan period, in milliseconds.
     */
    public static final String CONF_SCAN_PERIOD_MS = "mv.scan.period.ms";
    /**
     * Runner status report period, in milliseconds.
     *
     * Should be less than the missing timeout period configured.
     */
    public static final String CONF_REPORT_PERIOD_MS = "mv.report.period.ms";
    /**
     * Runner and Coordinator missing timeout period, in milliseconds.
     */
    public static final String CONF_RUNNER_TIMEOUT_MS = "mv.runner.timeout.ms";
    /**
     * The delay between the Coordinator startup and job distribution
     * activation, milliseconds.
     */
    public static final String CONF_COORD_STARTUP_MS = "mv.coord.startup.ms";
    /**
     * The minimal number of Runners for job distribution.
     */
    public static final String CONF_COORD_RUNNERS_COUNT = "mv.coord.runners.count";

    /**
     * The default name for MV_JOBS table.
     */
    public static final String DEF_TABLE_JOBS = "mv/jobs";
    /**
     * The default name for MV_JOB_SCANS table.
     */
    public static final String DEF_TABLE_SCANS = "mv/job_scans";
    /**
     * The default name for MV_RUNNERS table.
     */
    public static final String DEF_TABLE_RUNNERS = "mv/runners";
    /**
     * The default name for MV_RUNNER_JOBS table.
     */
    public static final String DEF_TABLE_RUNNER_JOBS = "mv/runner_jobs";
    /**
     * The default name for MV_COMMANDS table.
     */
    public static final String DEF_TABLE_COMMANDS = "mv/commands";

    /**
     * The default value for mv.scan.period.ms.
     */
    public static final long DEF_SCAN_PERIOD_MS = 5000L; // 5 seconds
    /**
     * The default value for mv.report.period.ms.
     */
    public static final long DEF_REPORT_PERIOD_MS = 10000L; // 10 seconds
    /**
     * The default value for mv.runner.timeout.ms.
     */
    public static final long DEF_RUNNER_TIMEOUT_MS = 30000L; // 30 seconds
    /**
     * The default value for mv.coord.startup.ms.
     */
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

    /**
     * Create the settings with the default values.
     */
    public MvBatchSettings() {
    }

    /**
     * Create the copy of the settings object.
     *
     * @param src Original settings to be copied.
     */
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

    /**
     * Create the settings by applying the properties.
     *
     * @param props Properties to be applied.
     */
    public MvBatchSettings(Properties props) {
        loadFromProperties(props);
    }

    private void loadFromProperties(Properties props) {
        this.tableJobs = props.getProperty(CONF_TABLE_JOBS, DEF_TABLE_JOBS);
        this.tableScans = props.getProperty(CONF_TABLE_SCANS, DEF_TABLE_SCANS);
        this.tableRunners = props.getProperty(CONF_TABLE_RUNNERS, DEF_TABLE_RUNNERS);
        this.tableRunnerJobs = props.getProperty(CONF_TABLE_RUNNER_JOBS, DEF_TABLE_RUNNER_JOBS);
        this.tableCommands = props.getProperty(CONF_TABLE_COMMANDS, DEF_TABLE_COMMANDS);
        this.scanPeriodMs = MvConfig.parseLong(props, CONF_SCAN_PERIOD_MS, DEF_SCAN_PERIOD_MS);
        this.reportPeriodMs = MvConfig.parseLong(props, CONF_REPORT_PERIOD_MS, DEF_REPORT_PERIOD_MS);
        this.runnerTimeoutMs = MvConfig.parseLong(props, CONF_RUNNER_TIMEOUT_MS, DEF_RUNNER_TIMEOUT_MS);
        this.coordStartupMs = MvConfig.parseLong(props, CONF_COORD_STARTUP_MS, DEF_COORD_STARTUP_MS);
        this.runnersCount = MvConfig.parseInt(props, CONF_COORD_RUNNERS_COUNT, 0);
    }

    /**
     * Get MV_JOBS table name.
     *
     * @return MV_JOBS table name to be used.
     */
    public String getTableJobs() {
        return tableJobs;
    }

    /**
     * Set MV_JOBS table name.
     *
     * @param tableJobs MV_JOBS table name to be used.
     */
    public void setTableJobs(String tableJobs) {
        this.tableJobs = tableJobs;
    }

    /**
     * Get MV_JOB_SCANS table name.
     *
     * @return MV_JOB_SCANS table name to be used.
     */
    public String getTableScans() {
        return tableScans;
    }

    /**
     * Set MV_JOB_SCANS table name.
     *
     * @param tableScans MV_JOB_SCANS table name to be used.
     */
    public void setTableScans(String tableScans) {
        this.tableScans = tableScans;
    }

    /**
     * Get MV_RUNNERS table name.
     *
     * @return MV_RUNNERS table name to be used.
     */
    public String getTableRunners() {
        return tableRunners;
    }

    /**
     * Set MV_RUNNERS table name.
     *
     * @param tableRunners MV_RUNNERS table name to be used.
     */
    public void setTableRunners(String tableRunners) {
        this.tableRunners = tableRunners;
    }

    /**
     * Get MV_RUNNER_JOBS table name.
     *
     * @return MV_RUNNER_JOBS table name to be used.
     */
    public String getTableRunnerJobs() {
        return tableRunnerJobs;
    }

    /**
     * Set MV_RUNNER_JOBS table name.
     *
     * @param tableRunnerJobs MV_RUNNER_JOBS table name to be used.
     */
    public void setTableRunnerJobs(String tableRunnerJobs) {
        this.tableRunnerJobs = tableRunnerJobs;
    }

    /**
     * Get MV_COMMANDS table name.
     *
     * @return MV_COMMANDS table name to be used.
     */
    public String getTableCommands() {
        return tableCommands;
    }

    /**
     * Set MV_COMMANDS table name.
     *
     * @param tableCommands MV_COMMANDS table name to be used.
     */
    public void setTableCommands(String tableCommands) {
        this.tableCommands = tableCommands;
    }

    // Getters and setters for timing settings
    /**
     * Get scan period.
     *
     * @return Period (ms) for runners/coordinator to re-scan state.
     */
    public long getScanPeriodMs() {
        return scanPeriodMs;
    }

    /**
     * Set scan period.
     *
     * @param scanPeriodMs Period (ms) for runners/coordinator to re-scan state.
     */
    public void setScanPeriodMs(long scanPeriodMs) {
        this.scanPeriodMs = scanPeriodMs;
    }

    /**
     * Get runner report period.
     *
     * @return Period (ms) for runner status reports.
     */
    public long getReportPeriodMs() {
        return reportPeriodMs;
    }

    /**
     * Set runner report period.
     *
     * @param reportPeriodMs Period (ms) for runner status reports.
     */
    public void setReportPeriodMs(long reportPeriodMs) {
        this.reportPeriodMs = reportPeriodMs;
    }

    /**
     * Get runner timeout.
     *
     * @return Timeout (ms) after which a runner is considered missing.
     */
    public long getRunnerTimeoutMs() {
        return runnerTimeoutMs;
    }

    /**
     * Set runner timeout.
     *
     * @param runnerTimeoutMs Timeout (ms) after which a runner is considered
     * missing.
     */
    public void setRunnerTimeoutMs(long runnerTimeoutMs) {
        this.runnerTimeoutMs = runnerTimeoutMs;
    }

    /**
     * Get coordinator startup delay.
     *
     * @return Delay (ms) between coordinator startup and activation.
     */
    public long getCoordStartupMs() {
        return coordStartupMs;
    }

    /**
     * Set coordinator startup delay.
     *
     * @param coordStartupMs Delay (ms) between coordinator startup and
     * activation.
     */
    public void setCoordStartupMs(long coordStartupMs) {
        this.coordStartupMs = coordStartupMs;
    }

    /**
     * Get minimal number of runners required for distribution.
     *
     * @return Minimal number of runners required for job distribution (0 means
     * "no minimum").
     */
    public int getRunnersCount() {
        return runnersCount;
    }

    /**
     * Set minimal number of runners required for distribution.
     *
     * @param runnersCount Minimal number of runners required for job
     * distribution.
     */
    public void setRunnersCount(int runnersCount) {
        this.runnersCount = runnersCount;
    }
}
