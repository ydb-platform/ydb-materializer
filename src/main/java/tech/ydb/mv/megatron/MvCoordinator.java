package tech.ydb.mv.megatron;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.support.YdbMisc;

/**
 * MvCoordinator manages distributed job assignment and coordination.
 * Only one instance should be active system-wide, with leader election.
 *
 * @author zinal
 */
public class MvCoordinator implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinator.class);

    private final MvBatchSettings settings;
    private final MvTableOperations tableOps;

    private volatile boolean running = false;
    private volatile Thread coordinatorThread = null;
    private volatile Object coordinationSession = null;
    private volatile boolean isLeader = false;

    public MvCoordinator(YdbConnector ydb, MvBatchSettings settings) {
        this.settings = settings;
        this.tableOps = new MvTableOperations(ydb, settings);
    }

    public MvCoordinator(YdbConnector ydb) {
        this(ydb, new MvBatchSettings());
    }

    /**
     * Start the coordinator.
     */
    public synchronized void start() {
        if (running) {
            LOG.warn("MvCoordinator is already running");
            return;
        }

        LOG.info("Starting MvCoordinator");

        // Start the coordinator thread
        running = true;
        coordinatorThread = new Thread(this::run, "mv-coordinator");
        coordinatorThread.setDaemon(true);
        coordinatorThread.start();

        LOG.info("MvCoordinator started successfully");
    }

    /**
     * Stop the coordinator.
     */
    public synchronized void stop() {
        if (!running) {
            LOG.warn("MvCoordinator is not running");
            return;
        }

        LOG.info("Stopping MvCoordinator");

        running = false;
        isLeader = false;

        // Close coordination session
        if (coordinationSession != null) {
            try {
                // For now, just set to null - proper session management would be implemented here
                coordinationSession = null;
            } catch (Exception ex) {
                LOG.warn("Error closing coordination session", ex);
            }
        }

        // Wait for coordinator thread to finish
        if (coordinatorThread != null) {
            try {
                coordinatorThread.join(10000); // Wait up to 10 seconds
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for coordinator thread to stop");
            }
        }

        LOG.info("MvCoordinator stopped");
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Main coordinator loop.
     */
    private void run() {
        LOG.info("MvCoordinator thread started");

        long lastCheckTime = 0;

        while (running) {
            try {
                long currentTime = System.currentTimeMillis();

                // Check for leadership and perform coordination tasks
                if (currentTime - lastCheckTime >= settings.getScanPeriodMs()) {
                    if (tryAcquireLeadership()) {
                        if (!isLeader) {
                            LOG.info("Acquired leadership, starting coordination tasks");
                            isLeader = true;
                        }
                        performCoordinationTasks();
                    } else {
                        if (isLeader) {
                            LOG.info("Lost leadership, stopping coordination tasks");
                            isLeader = false;
                        }
                    }
                    lastCheckTime = currentTime;
                }

                // Sleep for a short time to avoid busy waiting
                YdbMisc.sleep(1000);

            } catch (Exception ex) {
                LOG.error("Error in MvCoordinator main loop", ex);
                YdbMisc.sleep(5000); // Sleep longer on error
            }
        }

        LOG.info("MvCoordinator thread finished");
    }

    /**
     * Try to acquire leadership using coordination service.
     */
    private boolean tryAcquireLeadership() {
        try {
            // Simplified leadership acquisition - in practice, you'd use proper distributed locking
            // For now, assume we can always acquire leadership
            return true;

        } catch (Exception ex) {
            LOG.error("Failed to acquire leadership", ex);
            return false;
        }
    }

    /**
     * Perform coordination tasks.
     */
    private void performCoordinationTasks() {
        try {
            // Clean up inactive runners
            cleanupInactiveRunners();

            // Balance jobs - ensure running jobs match mv_jobs table
            balanceJobs();

        } catch (Exception ex) {
            LOG.error("Error during coordination tasks", ex);
        }
    }

    /**
     * Clean up inactive runners and their associated records.
     */
    private void cleanupInactiveRunners() {
        try {
            List<MvRunnerInfo> allRunners = tableOps.getAllRunners();
            Instant cutoffTime = Instant.now().minusMillis(settings.getRunnerTimeoutMs());

            List<MvRunnerInfo> inactiveRunners = allRunners.stream()
                .filter(runner -> runner.getUpdatedAt().isBefore(cutoffTime))
                .collect(Collectors.toList());

            for (MvRunnerInfo inactiveRunner : inactiveRunners) {
                LOG.info("Cleaning up inactive runner: {}", inactiveRunner.getRunnerId());

                // Delete runner jobs
                tableOps.deleteRunnerJobs(inactiveRunner.getRunnerId());

                // Delete runner
                tableOps.deleteRunner(inactiveRunner.getRunnerId());

                LOG.info("Cleaned up inactive runner: {}", inactiveRunner.getRunnerId());
            }

        } catch (Exception ex) {
            LOG.error("Failed to cleanup inactive runners", ex);
        }
    }

    /**
     * Balance jobs - ensure running jobs match the mv_jobs table.
     */
    private void balanceJobs() {
        try {
            // Get all jobs that should be running
            List<MvJobInfo> allJobs = tableOps.getAllJobs();
            Map<String, MvJobInfo> jobsToRun = allJobs.stream()
                .filter(MvJobInfo::isShouldRun)
                .collect(Collectors.toMap(MvJobInfo::getJobName, job -> job));

            // Get all currently running jobs
            List<MvRunnerJobInfo> allRunnerJobs = new ArrayList<>();
            List<MvRunnerInfo> allRunners = tableOps.getAllRunners();
            for (MvRunnerInfo runner : allRunners) {
                allRunnerJobs.addAll(tableOps.getRunnerJobs(runner.getRunnerId()));
            }
            Set<String> runningJobNames = allRunnerJobs.stream()
                .map(MvRunnerJobInfo::getJobName)
                .collect(Collectors.toSet());

            // Find extra jobs (running but not in mv_jobs)
            List<String> extraJobs = runningJobNames.stream()
                .filter(jobName -> !jobsToRun.containsKey(jobName))
                .collect(Collectors.toList());

            // Find missing jobs (in mv_jobs but not running)
            List<MvJobInfo> missingJobs = jobsToRun.values().stream()
                .filter(job -> !runningJobNames.contains(job.getJobName()))
                .collect(Collectors.toList());

            // Create commands to stop extra jobs
            for (String extraJob : extraJobs) {
                createStopCommand(extraJob);
            }

            // Create commands to start missing jobs
            for (MvJobInfo missingJob : missingJobs) {
                createStartCommand(missingJob);
            }

            if (!extraJobs.isEmpty() || !missingJobs.isEmpty()) {
                LOG.info("Balanced jobs - stopped {} extra, started {} missing",
                        extraJobs.size(), missingJobs.size());
            }

        } catch (Exception ex) {
            LOG.error("Failed to balance jobs", ex);
        }
    }

    /**
     * Create a command to start a job.
     */
    private void createStartCommand(MvJobInfo job) {
        try {
            // Find an available runner (simple round-robin for now)
            List<MvRunnerInfo> runners = tableOps.getAllRunners();
            if (runners.isEmpty()) {
                LOG.warn("No runners available to start job: {}", job.getJobName());
                return;
            }

            // Use the first available runner
            MvRunnerInfo runner = runners.get(0);

            MvCommandInfo command = new MvCommandInfo(
                runner.getRunnerId(),
                tableOps.generateCommandNo(),
                Instant.now(),
                MvCommandInfo.COMMAND_TYPE_START,
                job.getJobName(),
                job.getJobSettings(),
                MvCommandInfo.COMMAND_STATUS_CREATED,
                null
            );

            tableOps.createCommand(command);
            LOG.info("Created START command for job: {} on runner: {}",
                    job.getJobName(), runner.getRunnerId());

        } catch (Exception ex) {
            LOG.error("Failed to create START command for job: {}", job.getJobName(), ex);
        }
    }

    /**
     * Create a command to stop a job.
     */
    private void createStopCommand(String jobName) {
        try {
            // Find the runner that has this job
            List<MvRunnerInfo> runners = tableOps.getAllRunners();
            MvRunnerInfo targetRunner = null;

            for (MvRunnerInfo runner : runners) {
                List<MvRunnerJobInfo> runnerJobs = tableOps.getRunnerJobs(runner.getRunnerId());
                if (runnerJobs.stream().anyMatch(job -> jobName.equals(job.getJobName()))) {
                    targetRunner = runner;
                    break;
                }
            }

            if (targetRunner == null) {
                LOG.warn("No runner found for job: {}", jobName);
                return;
            }

            MvCommandInfo command = new MvCommandInfo(
                targetRunner.getRunnerId(),
                tableOps.generateCommandNo(),
                Instant.now(),
                MvCommandInfo.COMMAND_TYPE_STOP,
                jobName,
                null,
                MvCommandInfo.COMMAND_STATUS_CREATED,
                null
            );

            tableOps.createCommand(command);
            LOG.info("Created STOP command for job: {} on runner: {}",
                    jobName, targetRunner.getRunnerId());

        } catch (Exception ex) {
            LOG.error("Failed to create STOP command for job: {}", jobName, ex);
        }
    }

    /**
     * Check if the coordinator is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Check if this coordinator instance is the current leader.
     */
    public boolean isLeader() {
        return isLeader;
    }
}
