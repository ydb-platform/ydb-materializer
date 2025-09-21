package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Kirill Kurdyukov
 */
class MvCoordinatorImpl implements MvCoordinatorActions {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinatorImpl.class);

    private final AtomicLong commandNo = new AtomicLong();
    private final MvJobDao mvJobDao;
    private final MvBatchSettings settings;

    public MvCoordinatorImpl(MvJobDao mvJobDao, MvBatchSettings mvBatchSettings) {
        this.mvJobDao = mvJobDao;
        this.settings = mvBatchSettings;
    }

    @Override
    public void onStart() {
        commandNo.set(mvJobDao.getMaxCommandNo());
    }

    @Override
    public void onTick() {
        cleanupInactiveRunners();
        balanceJobs();
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

                mvJobDao.deleteRunnerJobs(inactiveRunner.getRunnerId());
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
            Map<String, MvJobInfo> jobsToRun = mvJobDao.getAllJobs().stream()
                    .filter(mvJobInfo -> mvJobInfo.isRegularJob() && mvJobInfo.isShouldRun())
                    .collect(Collectors.toMap(MvJobInfo::getJobName, job -> job));
            List<MvRunnerInfo> allRunners = mvJobDao.getAllRunners();

            if (allRunners.isEmpty()) {
                LOG.warn("No runners available to start jobs [{}]", String.join(", ", jobsToRun.keySet()));
                return;
            }

            var runnerJobs = mvJobDao.getAllRunnerJobs();
            var runnerNameJobs = runnerJobs.stream()
                    .map(MvRunnerJobInfo::getJobName)
                    .collect(Collectors.toSet());

            List<MvRunnerJobInfo> jobsForRemoval = runnerJobs.stream()
                    .filter(runnerJob -> !jobsToRun.containsKey(runnerJob.getJobName()))
                    .toList();

            List<MvJobInfo> newJobs = jobsToRun.values().stream()
                    .filter(job -> !runnerNameJobs.contains(job.getJobName()))
                    .toList();

            // Create commands to stop extra jobs
            for (var extraJob : jobsForRemoval) {
                createStopCommand(extraJob);
            }

            // Create commands to start missing jobs
            runnerJobs = new ArrayList<>(runnerJobs); // repack
            for (var missingJob : newJobs) {
                createStartCommand(missingJob, allRunners, runnerJobs);
            }

            if (!jobsForRemoval.isEmpty() || !newJobs.isEmpty()) {
                LOG.info("Balanced jobs - stopped {} extra, started {} missing", jobsForRemoval.size(), newJobs.size());
            }

        } catch (Exception ex) {
            LOG.error("Failed to balance jobs", ex);
        }
    }

    /**
     * Create a command to stop a job.
     */
    private void createStopCommand(MvRunnerJobInfo job) {
        try {
            MvCommand command = new MvCommand(
                    job.getRunnerId(),
                    commandNo.incrementAndGet(),
                    Instant.now(),
                    MvCommand.TYPE_STOP,
                    job.getJobName(),
                    null,
                    MvCommand.STATUS_CREATED,
                    null
            );

            mvJobDao.createCommand(command);
            LOG.info("Created STOP command for job: {} on runner: {}",
                    job.getJobName(), job.getRunnerId());
        } catch (Exception ex) {
            LOG.error("Failed to create STOP command for job: {}", job.getJobName(), ex);
        }
    }

    /**
     * Create a command to start a job.
     */
    private void createStartCommand(MvJobInfo job, List<MvRunnerInfo> runners, List<MvRunnerJobInfo> jobs) {
        try {
            var cmdCountByRunner = jobs.stream()
                    .collect(Collectors.groupingBy(MvRunnerJobInfo::getRunnerId, Collectors.counting()));

            MvRunnerInfo runner = runners.stream().min(Comparator.comparing(
                    (MvRunnerInfo r) -> cmdCountByRunner.getOrDefault(r.getRunnerId(), 0L)
            ).thenComparing(MvRunnerInfo::getRunnerId)).get();

            MvCommand command = new MvCommand(
                    runner.getRunnerId(),
                    commandNo.incrementAndGet(),
                    Instant.now(),
                    MvCommand.TYPE_START,
                    job.getJobName(),
                    job.getJobSettings(),
                    MvCommand.STATUS_CREATED,
                    null
            );
            // Need to add one so that the new job will be accounted for when planning
            jobs.add(new MvRunnerJobInfo(runner.getRunnerId(), job.getJobName(), job.getJobSettings(), Instant.now()));

            mvJobDao.createCommand(command);
            LOG.info("Created START command for job: {} on runner: {}", job.getJobName(), runner.getRunnerId());
        } catch (Exception ex) {
            LOG.error("Failed to create START command for job: {}", job.getJobName(), ex);
        }
    }

}
