package tech.ydb.mv.mgt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Kirill Kurdyukov
 */
public class MvCoordinatorJobImpl implements MvCoordinatorJob {
    private static final Logger LOG = LoggerFactory.getLogger(MvCoordinatorJobImpl.class);

    private final MvJobDao mvJobDao;
    private final MvBatchSettings settings;

    public MvCoordinatorJobImpl(MvJobDao mvJobDao, MvBatchSettings mvBatchSettings) {
        this.mvJobDao = mvJobDao;
        this.settings = mvBatchSettings;
    }

    @Override
    public void performCoordinationTask() {
        cleanupInactiveRunners();
        balanceJobs();
    }

    @Override
    public void stopJobs() {
    }

    /**
     * Clean up inactive runners and their associated records.
     */
    private void cleanupInactiveRunners() {
        try {
            List<MvRunnerInfo> allRunners = mvJobDao.getAllRunners();
            Instant cutoffTime = Instant.now().minusMillis(settings.getRunnerTimeoutMs());

            List<MvRunnerInfo> inactiveRunners = allRunners.stream()
                    .filter(runner -> runner.getUpdatedAt().isBefore(cutoffTime))
                    .toList();

            for (MvRunnerInfo inactiveRunner : inactiveRunners) {
                LOG.info("Cleaning up inactive runner: {}", inactiveRunner.getRunnerId());

                // Delete runner jobs
                mvJobDao.deleteRunnerJobs(inactiveRunner.getRunnerId());
                // Delete runner
                mvJobDao.deleteRunner(inactiveRunner.getRunnerId());

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
            Map<String, MvJobInfo> jobsToRun = mvJobDao.getAllJobs().stream()
                    .filter(MvJobInfo::isShouldRun)
                    .collect(Collectors.toMap(MvJobInfo::getJobName, job -> job));

            // Get all currently running jobs
            List<MvRunnerJobInfo> allRunnerJobs = new ArrayList<>();
            List<MvRunnerInfo> allRunners = mvJobDao.getAllRunners();
            for (MvRunnerInfo runner : allRunners) {
                allRunnerJobs.addAll(mvJobDao.getRunnerJobs(runner.getRunnerId()));
            }
            Set<String> runningJobNames = allRunnerJobs.stream()
                    .map(MvRunnerJobInfo::getJobName)
                    .collect(Collectors.toSet());

            // Find extra jobs (running but not in mv_jobs)
            List<String> extraJobs = runningJobNames.stream()
                    .filter(jobName -> !jobsToRun.containsKey(jobName))
                    .toList();

            // Find missing jobs (in mv_jobs but not running)
            List<MvJobInfo> missingJobs = jobsToRun.values().stream()
                    .filter(job -> !runningJobNames.contains(job.getJobName()))
                    .toList();

            // Create commands to stop extra jobs
            for (String extraJob : extraJobs) {
                createStopCommand(extraJob);
            }

            // Create commands to start missing jobs
            for (MvJobInfo missingJob : missingJobs) {
                createStartCommand(missingJob);
            }

            if (!extraJobs.isEmpty() || !missingJobs.isEmpty()) {
                LOG.info("Balanced jobs - stopped {} extra, started {} missing", extraJobs.size(), missingJobs.size());
            }

        } catch (Exception ex) {
            LOG.error("Failed to balance jobs", ex);
        }
    }

    /**
     * Create a command to stop a job.
     */
    private void createStopCommand(String jobName) {
        try {
            // Find the runner that has this job
            List<MvRunnerInfo> runners = mvJobDao.getAllRunners();
            MvRunnerInfo targetRunner = null;

            for (MvRunnerInfo runner : runners) {
                List<MvRunnerJobInfo> runnerJobs = mvJobDao.getRunnerJobs(runner.getRunnerId());
                if (runnerJobs.stream().anyMatch(job -> jobName.equals(job.getJobName()))) {
                    targetRunner = runner;
                    break;
                }
            }

            if (targetRunner == null) {
                LOG.warn("No runner found for job: {}", jobName);
                return;
            }

            MvCommand command = new MvCommand(
                    targetRunner.getRunnerId(),
                    mvJobDao.generateCommandNo(),
                    Instant.now(),
                    MvCommand.TYPE_STOP,
                    jobName,
                    null,
                    MvCommand.STATUS_CREATED,
                    null
            );

            mvJobDao.createCommand(command);
            LOG.info("Created STOP command for job: {} on runner: {}",
                    jobName, targetRunner.getRunnerId());

        } catch (Exception ex) {
            LOG.error("Failed to create STOP command for job: {}", jobName, ex);
        }
    }

    /**
     * Create a command to start a job.
     */
    private void createStartCommand(MvJobInfo job) {
        try {
            // Find an available runner (simple round-robin for now)
            List<MvRunnerInfo> runners = mvJobDao.getAllRunners();
            if (runners.isEmpty()) {
                LOG.warn("No runners available to start job: {}", job.getJobName());
                return;
            }

            // Use the first available runner
            MvRunnerInfo runner = runners.get(0);

            MvCommand command = new MvCommand(
                    runner.getRunnerId(),
                    mvJobDao.generateCommandNo(),
                    Instant.now(),
                    MvCommand.TYPE_START,
                    job.getJobName(),
                    job.getJobSettings(),
                    MvCommand.STATUS_CREATED,
                    null
            );

            mvJobDao.createCommand(command);
            LOG.info("Created START command for job: {} on runner: {}",
                    job.getJobName(), runner.getRunnerId());

        } catch (Exception ex) {
            LOG.error("Failed to create START command for job: {}", job.getJobName(), ex);
        }
    }

}
