package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import tech.ydb.mv.MvConfig;

/**
 * Job balancing logic, part of the coordinator.
 *
 * @author zinal
 * @author Kirill Kurdyukov
 */
class MvBalancer {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvBalancer.class);

    final MvJobDao jobDao;
    final AtomicLong commandNo;
    final int runnersCount;
    final List<MvRunnerInfo> allRunners = new ArrayList<>();
    // jobName -> job
    final Map<String, MvJobInfo> requiredJobs = new HashMap<>();
    // runnerId -> jobs
    final Map<String, List<MvRunnerJobInfo>> runningJobs = new HashMap<>();
    final Map<String, MvCommand> pendingJobs = new HashMap<>();

    MvBalancer(MvJobDao jobDao, AtomicLong commandNo, int runnersCount) {
        this.jobDao = jobDao;
        this.commandNo = commandNo;
        this.runnersCount = runnersCount;
        allRunners.addAll(jobDao.getAllRunners());
        jobDao.getAllJobs().forEach(
                job -> requiredJobs.put(job.getJobName(), job));
        for (var job : jobDao.getAllRunnerJobs()) {
            addJob(runningJobs, job);
        }
        for (var cmd : jobDao.getPendingCommands()) {
            pendingJobs.put(cmd.getJobName(), cmd);
        }
    }

    static void addJob(Map<String, List<MvRunnerJobInfo>> jobs, MvRunnerJobInfo job) {
        var x = jobs.get(job.getRunnerId());
        if (x == null) {
            x = new ArrayList<>();
            jobs.put(job.getRunnerId(), x);
        }
        x.add(job);
    }

    HashMap<String, MvRunnerJobInfo> collectRunning() {
        var ret = new HashMap<String, MvRunnerJobInfo>();
        for (var runnerJobs : runningJobs.values()) {
            for (var job : runnerJobs) {
                ret.put(job.getJobName(), job);
            }
        }
        return ret;
    }

    void balanceJobs() {
        var allRunning = collectRunning();
        Map<String, MvJobInfo> jobsToRun = requiredJobs.values().stream()
                .filter(job -> job.isRegularJob())
                .filter(job -> !allRunning.containsKey(job.getJobName()))
                .filter(job -> !pendingJobs.containsKey(job.getJobName()))
                .collect(Collectors.toMap(MvJobInfo::getJobName, job -> job));

        if (allRunners.isEmpty()) {
            LOG.warn("No runners available to start jobs [{}]",
                    String.join(", ", jobsToRun.keySet()));
            return;
        }
        if (runnersCount > allRunners.size()) {
            LOG.warn("Insufficient runners ({} vs {}) available to start jobs [{}]",
                    allRunners.size(), runnersCount, String.join(", ", jobsToRun.keySet()));
            return;
        }

        var jobsForRemoval = runningJobs.values().stream()
                .flatMap(v -> v.stream())
                .filter(runnerJob -> runnerJob.isRegularJob())
                .filter(runnerJob -> !requiredJobs.containsKey(runnerJob.getJobName()))
                .toList();

        // Create commands to stop extra jobs
        for (var extraJob : jobsForRemoval) {
            createStopCommand(extraJob);
        }

        // Create commands to start missing jobs
        for (var missingJob : jobsToRun.values()) {
            createStartCommand(missingJob);
        }

        if (!jobsForRemoval.isEmpty() || !jobsToRun.isEmpty()) {
            LOG.info("Balanced jobs - stopped {} extra, started {} missing",
                    jobsForRemoval.size(), jobsToRun.size());
        }
    }

    /**
     * Create a command to stop a job.
     */
    private void createStopCommand(MvRunnerJobInfo job) {
        MvCommand command = new MvCommand(
                job.getRunnerId(),
                commandNo.incrementAndGet(),
                Instant.now(),
                MvCommand.TYPE_STOP,
                job.getJobName(),
                null, // job settings are not needed for STOP
                MvCommand.STATUS_CREATED,
                null // diagnostics should not be set
        );

        jobDao.createCommand(command);
        LOG.info("Created STOP command for job: {} on runner: {}",
                job.getJobName(), job.getRunnerId());
    }

    private int countJobs(List<MvRunnerJobInfo> jobs) {
        int count = 0;
        for (var job : jobs) {
            if (!MvConfig.HANDLER_COORDINATOR.equals(job.getJobName())) {
                ++count;
            }
        }
        return count;
    }

    /**
     * Create a command to start a job.
     */
    private void createStartCommand(MvJobInfo job) {
        MvRunnerInfo runner;
        try {
            var cmdCountByRunner = runningJobs.entrySet().stream()
                    .collect(Collectors.toMap(me -> me.getKey(), me -> countJobs(me.getValue())));
            var comparator = Comparator.comparing(
                    (MvRunnerInfo r) -> cmdCountByRunner.getOrDefault(r.getRunnerId(), 0))
                    .thenComparing(r -> r.getRunnerId());
            runner = allRunners.stream()
                    .sorted(comparator)
                    .findFirst()
                    .get();
        } catch (NoSuchElementException nsee) {
            throw new RuntimeException("Cannot obtain the runner, "
                    + "failed to start job " + job.getJobName(), nsee);
        }

        var command = new MvCommand(
                runner.getRunnerId(),
                commandNo.incrementAndGet(),
                Instant.now(),
                MvCommand.TYPE_START,
                job.getJobName(),
                job.getJobSettings(),
                MvCommand.STATUS_CREATED,
                null // diagnostics should not be set
        );
        jobDao.createCommand(command);

        // Need to add one so that the new job will be accounted for when planning
        var runnerJob = new MvRunnerJobInfo(
                runner.getRunnerId(),
                job.getJobName(),
                job.getJobSettings(),
                command.getCreatedAt());
        addJob(runningJobs, runnerJob);

        LOG.info("Created START command for job: {} on runner: {}",
                job.getJobName(), runner.getRunnerId());
    }

}
