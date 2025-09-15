package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.mv.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvHandlerSettings;
import tech.ydb.mv.support.YdbMisc;

/**
 * MvRunner manages local task execution for the distributed job management
 * system. Each process has its own MvRunner instance that reports status and
 * executes commands.
 *
 * @author zinal
 */
public class MvRunner implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvRunner.class);

    private final MvService mvService;
    private final MvBatchSettings settings;
    private final MvJobDao tableOps;
    private final String runnerId;
    private final String runnerIdentity;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, MvRunnerJobInfo> localJobs = new HashMap<>();
    private volatile Thread runnerThread = null;

    public MvRunner(YdbConnector ydb, MvService mvService, MvBatchSettings settings) {
        this.mvService = mvService;
        this.settings = settings;
        this.tableOps = new MvJobDao(ydb, settings);
        this.runnerId = generateRunnerId();
        this.runnerIdentity = generateRunnerIdentity();
    }

    public MvRunner(YdbConnector ydb, MvService mvService) {
        this(ydb, mvService, new MvBatchSettings());
    }

    /**
     * Start the runner.
     */
    public synchronized void start() {
        if (running.get()) {
            LOG.warn("MvRunner is already running");
            return;
        }

        LOG.info("Starting MvRunner with ID: {}", runnerId);

        // Start the runner thread
        running.set(true);
        runnerThread = new Thread(this::run, "mv-runner-" + runnerId);
        runnerThread.setDaemon(true);
        runnerThread.start();

        LOG.info("MvRunner started successfully");
    }

    /**
     * Stop the runner.
     */
    public synchronized void stop() {
        if (!running.get()) {
            LOG.warn("MvRunner is not running");
            return;
        }

        LOG.info("Stopping MvRunner with ID: {}", runnerId);

        running.set(false);

        // Stop all local jobs
        synchronized (localJobs) {
            for (MvRunnerJobInfo job : localJobs.values()) {
                try {
                    stopHandler(job.getJobName());
                } catch (Exception ex) {
                    LOG.error("Failed to stop job {} during shutdown", job.getJobName(), ex);
                }
            }
            localJobs.clear();
        }

        // Wait for runner thread to finish
        if (runnerThread != null) {
            try {
                runnerThread.join(5000); // Wait up to 5 seconds
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for runner thread to stop");
            }
        }

        LOG.info("MvRunner stopped");
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Main runner loop.
     */
    private void run() {
        LOG.info("MvRunner thread started for runner: {}", runnerId);

        long lastReportTime = 0;
        long lastCommandCheckTime = 0;

        while (running.get()) {
            try {
                long currentTime = System.currentTimeMillis();

                // Report status periodically
                if (currentTime - lastReportTime >= settings.getReportPeriodMs()) {
                    reportStatus();
                    lastReportTime = currentTime;
                }

                // Check for commands periodically
                if (currentTime - lastCommandCheckTime >= settings.getScanPeriodMs()) {
                    checkCommands();
                    lastCommandCheckTime = currentTime;
                }

                // Sleep for a short time to avoid busy waiting
                YdbMisc.sleep(1000);

            } catch (Exception ex) {
                LOG.error("Error in MvRunner main loop", ex);
                YdbMisc.sleep(5000); // Sleep longer on error
            }
        }

        LOG.info("MvRunner thread finished for runner: {}", runnerId);
    }

    /**
     * Report current status to the mv_runners table.
     */
    private void reportStatus() {
        try {
            MvRunnerInfo runnerInfo = new MvRunnerInfo(runnerId, runnerIdentity, Instant.now());
            tableOps.upsertRunner(runnerInfo);
            LOG.debug("Reported status for runner: {}", runnerId);
        } catch (Exception ex) {
            LOG.error("Failed to report status for runner: {}", runnerId, ex);
        }
    }

    /**
     * Check for new commands and execute them.
     */
    private void checkCommands() {
        try {
            List<MvCommand> commands = tableOps.getCommandsForRunner(runnerId);

            for (MvCommand command : commands) {
                if (command.isCreated()) {
                    executeCommand(command);
                }
            }
        } catch (Exception ex) {
            LOG.error("Failed to check commands for runner: {}", runnerId, ex);
        }
    }

    /**
     * Execute a command.
     */
    private void executeCommand(MvCommand command) {
        LOG.info("Executing command: {} for job: {}", command.getCommandType(), command.getJobName());

        try {
            tableOps.updateCommandStatus(command.getRunnerId(), command.getCommandNo(),
                    MvCommand.STATUS_TAKEN, null);

            if (command.isStartCommand()) {
                startHandler(command.getJobName(), command.getJobSettings());
            } else if (command.isStopCommand()) {
                stopHandler(command.getJobName());
            } else {
                throw new IllegalArgumentException("Unknown command type: " + command.getCommandType());
            }

            LOG.info("Command executed successfully: {} for job: {}",
                    command.getCommandType(), command.getJobName());

            tableOps.updateCommandStatus(command.getRunnerId(), command.getCommandNo(),
                    MvCommand.STATUS_SUCCESS, null);

        } catch (Exception ex) {
            LOG.error("Exception during command execution: {} for job: {}",
                    command.getCommandType(), command.getJobName(), ex);
            tableOps.updateCommandStatus(command.getRunnerId(), command.getCommandNo(),
                    MvCommand.STATUS_ERROR, YdbMisc.getStackTrace(ex));
        }
    }

    /**
     * Start a handler with the given name and settings.
     */
    private void startHandler(String jobName, String jobSettingsJson) {
        // Handle special system handlers
        if ("sys$dictionary".equals(jobName)) {
            mvService.startDictionaryHandler();
            synchronized (localJobs) {
                localJobs.put(jobName, new MvRunnerJobInfo(
                        runnerId, jobName, jobSettingsJson, Instant.now()
                ));
            }
            return;
        }

        MvHandlerSettings settings = mvService.getHandlerSettings();

        // Start the handler
        boolean started = mvService.startHandler(jobName, settings);

        if (started) {
            // Record the job in local tracking and database
            MvRunnerJobInfo runnerJob = new MvRunnerJobInfo(
                    runnerId, jobName, jobSettingsJson, Instant.now()
            );

            synchronized (localJobs) {
                localJobs.put(jobName, runnerJob);
            }

            tableOps.upsertRunnerJob(runnerJob);
            LOG.info("Started handler: {}", jobName);
        }
    }

    /**
     * Stop a handler with the given name.
     */
    private void stopHandler(String jobName) {
        // Handle special system handlers
        if ("sys$dictionary".equals(jobName)) {
            mvService.stopDictionaryHandler();
        } else {
            mvService.stopHandler(jobName);
            // Remove from local tracking and database
            synchronized (localJobs) {
                localJobs.remove(jobName);
            }
            tableOps.deleteRunnerJob(runnerId, jobName);
            LOG.info("Stopped handler: {}", jobName);
        }
    }

    /**
     * Generate a unique runner ID.
     */
    private String generateRunnerId() {
        return "runner-" + UUID.randomUUID().toString().substring(0, 8)
                + "-" + System.currentTimeMillis();
    }

    /**
     * Generate runner identity information.
     */
    private String generateRunnerIdentity() {
        return "host:" + getHostname() +
                ",pid:" + getProcessId() +
                ",start:" + System.currentTimeMillis();
    }

    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception ex) {
            return "unknown";
        }
    }

    private long getProcessId() {
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
     * Get the list of currently running jobs.
     */
    public Map<String, MvRunnerJobInfo> getLocalJobs() {
        synchronized (localJobs) {
            return new HashMap<>(localJobs);
        }
    }
}
