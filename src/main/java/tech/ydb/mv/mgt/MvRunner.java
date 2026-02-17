package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.MvApi;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.support.YdbMisc;

/**
 * MvRunner manages local task execution for the distributed job management
 * system. Each process has its own MvRunner instance that reports status and
 * executes commands.
 *
 * @author zinal
 * @author Kirill Kurdyukov
 */
public class MvRunner implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvRunner.class);

    private final MvApi api;
    private final MvBatchSettings settings;
    private final MvJobDao tableOps;
    private final String runnerId;
    private final String runnerIdentity;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean failing = new AtomicBoolean(false);
    private final Map<String, MvRunnerJobInfo> localJobs = new HashMap<>();
    private volatile Thread runnerThread = null;

    public MvRunner(YdbConnector ydb, MvApi api, MvBatchSettings settings, String runnerId) {
        this.api = api;
        this.settings = settings;
        this.tableOps = new MvJobDao(ydb.getConnMgt(), settings);
        this.runnerId = (runnerId == null) ? MvApi.generateId() : runnerId;
        this.runnerIdentity = generateRunnerIdentity();
    }

    public MvRunner(YdbConnector ydb, MvApi api, MvBatchSettings settings) {
        this(ydb, api, settings, api.getIdentification());
    }

    public MvRunner(YdbConnector ydb, MvApi api, String runnerId) {
        this(ydb, api, new MvBatchSettings(), runnerId);
    }

    public MvRunner(YdbConnector ydb, MvApi api) {
        this(ydb, api, new MvBatchSettings(), api.getIdentification());
    }

    public int getJobsCount() {
        synchronized (localJobs) {
            return localJobs.size();
        }
    }

    /**
     * Start the runner.
     *
     * @return true, if the runner has been just started, and false, if it was
     * already running
     */
    public boolean start() {
        if (running.getAndSet(true)) {
            LOG.info("[{}] MvRunner already started, ignored start attempt", runnerId);
            return false;
        }

        LOG.info("[{}] Starting MvRunner", runnerId);

        // Start the runner thread
        runnerThread = new Thread(this::run, "mv-runner-" + runnerId);
        runnerThread.setDaemon(true);
        runnerThread.start();

        return true;
    }

    /**
     * Stop the runner.
     *
     * @return true, if the runner has been just stopped, and false if it was
     * stopped already
     */
    public boolean stop() {
        if (!running.getAndSet(false)) {
            LOG.info("[{}] MvRunner already stopped, ignored stop attempt", runnerId);
            return false;
        }

        LOG.info("[{}] Stopping MvRunner, {} jobs still running", runnerId, getJobsCount());

        // Wait for runner thread to finish
        if (runnerThread != null) {
            try {
                runnerThread.join(20000L); // Wait up to 20 seconds
                runnerThread = null;
            } catch (InterruptedException ex) {
                LOG.warn("[{}] Interrupted while waiting for runner thread to stop", runnerId);
            }
        }

        LOG.info("[{}] MvRunner stopped", runnerId);
        return true;
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Stop all local jobs.
     */
    private void stopAllJobs() {
        // Use the fast algorithm
        api.shutdown();
        synchronized (localJobs) {
            localJobs.clear();
        }
        LOG.info("[{}] All jobs were stopped", runnerId);
    }

    /**
     * Main runner loop.
     */
    private void run() {
        LOG.debug("[{}] Worker thread started", runnerId);

        long lastReportTime = 0;
        long lastCommandCheckTime = 0;
        boolean registered = false;

        failing.set(false);

        while (running.get()) {
            long currentTime = System.currentTimeMillis();
            try {
                if (!registered) {
                    registerRunner();
                    lastReportTime = currentTime;
                    registered = true;
                    continue;
                }

                // Check and report status periodically
                if (currentTime - lastReportTime >= settings.getReportPeriodMs()) {
                    registered = checkAndReport();
                    lastReportTime = currentTime;
                }

                if (!registered) {
                    // lost runner, should stop all jobs and re-register
                    LOG.warn("[{}] MvRunner has been lost, stopping jobs and re-registering", runnerId);
                    stopAllJobs();
                    continue;
                }

                // Check for commands periodically
                if (currentTime - lastCommandCheckTime >= settings.getScanPeriodMs()) {
                    checkCommands();
                    lastCommandCheckTime = currentTime;
                }

                failing.set(false);

                // Sleep for a short time to avoid busy waiting
                YdbMisc.sleep(200L);

            } catch (Exception ex) {
                failing.set(true);
                LOG.error("[{}] Error in MvRunner main loop", runnerId, ex);
                YdbMisc.sleep(5000); // Sleep longer on error
            }
        }

        stopAllJobs();
        unregisterRunner();

        LOG.debug("[{}] Worker thread finished", runnerId);
    }

    /**
     * Registration of the runner to the mv_runners table.
     */
    private void registerRunner() {
        MvRunnerInfo runnerInfo = new MvRunnerInfo(runnerId, runnerIdentity, Instant.now());
        tableOps.upsertRunner(runnerInfo);
        LOG.info("[{}] Registered runner", runnerId);
    }

    private void unregisterRunner() {
        try {
            tableOps.deleteRunnerJobs(runnerId);
            tableOps.deleteRunner(runnerId);
            LOG.info("[{}] Unregistered runner", runnerId);
        } catch (Exception ex) {
            LOG.error("[{}] Failed to unregister runner", runnerId, ex);
        }
    }

    /**
     * Report current status to the mv_runners table.
     *
     * If the registration was lost, this means that the coordinator has dropped
     * it. In that case all the jobs must be stopped immediately.
     */
    private boolean checkAndReport() {
        MvRunnerInfo runnerInfo = new MvRunnerInfo(runnerId, runnerIdentity, Instant.now());
        boolean retval = tableOps.checkRunner(runnerInfo);
        LOG.debug("[{}] Checked status: {}", runnerId, retval);
        return retval;
    }

    /**
     * Check for new commands and execute them.
     */
    private void checkCommands() {
        List<MvCommand> commands = tableOps.getCommandsForRunner(runnerId);
        for (MvCommand command : commands) {
            if (!running.get()) {
                return;
            }
            if (command.isCreated()) {
                executeCommand(command);
            }
        }
    }

    /**
     * Execute a command.
     */
    private void executeCommand(MvCommand command) {
        LOG.info("[{}] Executing command: {} for job: {}", runnerId,
                command.getCommandType(), command.getJobName());

        try {
            tableOps.updateCommandStatus(command.getRunnerId(), command.getCommandNo(),
                    MvCommand.STATUS_TAKEN, null);

            if (command.isStartCommand()) {
                startHandler(command.getJobName(), command.getJobSettings());
            } else if (command.isStopCommand()) {
                stopHandler(command.getJobName());
            } else if (command.isScanCommand()) {
                startScan(command.getJobName(), command.getTargetName(), command.getJobSettings());
            } else if (command.isNoScanCommand()) {
                stopScan(command.getJobName(), command.getTargetName());
            } else {
                throw new IllegalArgumentException("Unknown command type: " + command.getCommandType());
            }

            LOG.info("[{}] Command executed successfully: {} for job: {}",
                    runnerId, command.getCommandType(), command.getJobName());

            tableOps.updateCommandStatus(command.getRunnerId(), command.getCommandNo(),
                    MvCommand.STATUS_SUCCESS, null);

        } catch (Exception ex) {
            LOG.error("[{}] Exception during command execution: {} for job: {}",
                    runnerId, command.getCommandType(), command.getJobName(), ex);
            tableOps.updateCommandStatus(command.getRunnerId(), command.getCommandNo(),
                    MvCommand.STATUS_ERROR, YdbMisc.getStackTrace(ex));
        }
    }

    /**
     * Start a handler with the given name and settings.
     */
    private void startHandler(String jobName, String settingsJson) {
        // Start the handler
        boolean started;
        if (MvConfig.HANDLER_DICTIONARY.equalsIgnoreCase(jobName)) {
            var oldSettings = api.getDictionarySettings();
            try {
                if (settingsJson != null && settingsJson.length() > 2) {
                    var newSettings = MvConfig.GSON.fromJson(settingsJson, oldSettings.getClass());
                    api.setDictionarySettings(newSettings);
                }
                started = api.startHandler(jobName);
            } finally {
                api.setDictionarySettings(oldSettings);
            }
        } else {
            var oldSettings = api.getHandlerSettings();
            try {
                if (settingsJson != null && settingsJson.length() > 2) {
                    var newSettings = MvConfig.GSON.fromJson(settingsJson, oldSettings.getClass());
                    api.setHandlerSettings(newSettings);
                }
                started = api.startHandler(jobName);
            } finally {
                api.setHandlerSettings(oldSettings);
            }
        }

        if (started) {
            // Record the job in local tracking and database
            MvRunnerJobInfo runnerJob = new MvRunnerJobInfo(
                    runnerId, jobName, settingsJson, Instant.now()
            );

            synchronized (localJobs) {
                localJobs.put(jobName, runnerJob);
            }

            tableOps.upsertRunnerJob(runnerJob);

            LOG.info("[{}] Started job `{}`", runnerId, jobName);
        } else {
            throw new RuntimeException("Start request rejected for job `"
                    + jobName + "` - already running.");
        }
    }

    /**
     * Stop a handler with the given name.
     */
    private void stopHandler(String jobName) {
        api.stopHandler(jobName);

        // Remove from local tracking and database
        synchronized (localJobs) {
            localJobs.remove(jobName);
        }

        tableOps.deleteRunnerJob(runnerId, jobName);

        LOG.info("[{}] Stopped job `{}`", runnerId, jobName);
    }

    private void startScan(String jobName, String targetName, String settingsJson) {
        var oldSettings = api.getScanSettings();
        try {
            if (settingsJson != null && settingsJson.length() > 0) {
                var newSettings = MvConfig.GSON.fromJson(settingsJson, oldSettings.getClass());
                api.setScanSettings(newSettings);
            }
            if (!api.startScan(jobName, targetName)) {
                throw new IllegalStateException("Scan was not started for job `"
                        + jobName + "`, table `" + targetName + "`");
            }
        } finally {
            api.setScanSettings(oldSettings);
        }

        LOG.info("[{}] Started scan, job `{}`, table `{}`", runnerId, jobName, targetName);
    }

    private void stopScan(String jobName, String targetName) {
        if (!api.stopScan(jobName, targetName)) {
            throw new IllegalStateException("Scan was not stopped for job `"
                    + jobName + "`, table `" + targetName + "`");
        }
        LOG.info("[{}] Stopped scan, job `{}`, table `{}`", runnerId, jobName, targetName);
    }

    /**
     * Generate runner identity information.
     */
    public String generateRunnerIdentity() {
        return "host:" + getHostname()
                + ",pid:" + getProcessId()
                + ",start:" + System.currentTimeMillis();
    }

    public static String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            return "unknown";
        }
    }

    public static long getProcessId() {
        try {
            return ProcessHandle.current().pid();
        } catch (Exception ex) {
            return -1;
        }
    }

    /**
     * Get the runner ID.
     */
    public String getRunnerId() {
        return runnerId;
    }

    /**
     * Get the runner identity.
     */
    public String getRunnerIdentity() {
        return runnerIdentity;
    }

    /**
     * Check if the runner is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Check if the runner is failing.
     */
    public boolean isFailing() {
        return failing.get();
    }

    /**
     * Get the list of currently running jobs.
     */
    public Map<String, MvRunnerJobInfo> getLocalJobs() {
        synchronized (localJobs) {
            return new HashMap<>(localJobs);
        }
    }
}
