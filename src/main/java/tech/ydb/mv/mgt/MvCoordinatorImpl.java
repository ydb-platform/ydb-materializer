package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Kirill Kurdyukov
 */
class MvCoordinatorImpl implements MvCoordinatorActions {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinatorImpl.class);

    private final AtomicLong commandNo = new AtomicLong();
    private final MvJobDao jobDao;
    private final MvBatchSettings settings;
    private final Instant startupTv;
    private volatile boolean balancing;

    public MvCoordinatorImpl(MvJobDao jobDao, MvBatchSettings settings) {
        this.jobDao = jobDao;
        this.settings = settings;
        this.startupTv = Instant.now().plusMillis(settings.getCoordStartupMs());
        this.balancing = false;
    }

    @Override
    public void onStart() {
        commandNo.set(jobDao.getMaxCommandNo());
    }

    @Override
    public void onTick() {
        cleanupInactiveRunners();
        balanceJobs();
        acceptScans();
    }

    /**
     * Clean up inactive runners and their associated records.
     */
    private void cleanupInactiveRunners() {
        try {
            List<MvRunnerInfo> allRunners = jobDao.getAllRunners();
            Instant cutoffTime = Instant.now().minusMillis(settings.getRunnerTimeoutMs());

            List<MvRunnerInfo> inactiveRunners = allRunners.stream()
                    .filter(runner -> runner.getUpdatedAt().isBefore(cutoffTime))
                    .toList();

            for (MvRunnerInfo inactiveRunner : inactiveRunners) {
                jobDao.deletePendingCommands(inactiveRunner.getRunnerId());
                jobDao.deleteRunnerJobs(inactiveRunner.getRunnerId());
                jobDao.deleteRunner(inactiveRunner.getRunnerId());

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
        if (Instant.now().isBefore(startupTv)) {
            return;
        }
        if (!balancing) {
            balancing = true;
            LOG.info("Started job balancing...");
        }
        try {
            new MvBalancer(jobDao, commandNo, settings.getRunnersCount())
                    .balanceJobs();
        } catch (Exception ex) {
            LOG.error("Failed to balance jobs", ex);
        }
    }

    private void acceptScans() {
        try {
            doAcceptScans();
        } catch (Exception ex) {
            LOG.error("Failed to process scans requests", ex);
        }
    }

    private void doAcceptScans() {
        var scans = jobDao.getAllScans();
        if (scans.isEmpty()) {
            return;
        }

        for (var scan : scans) {
            createScanCommand(scan);
        }
    }

    private void createScanCommand(MvJobScanInfo scan) {
        if (scan.getAcceptedAt() != null) {
            return;
        }
        var runners = jobDao.getJobRunners(scan.getJobName());
        if (runners.size() != 1) {
            LOG.info("Cannot start the requested scan "
                    + "for handler `{}`, target `{}` - runner was not found.",
                    scan.getJobName(), scan.getTargetName());
            return;
        }
        var runner = runners.get(0);

        var command = new MvCommand(
                runner.getRunnerId(),
                commandNo.incrementAndGet(),
                Instant.now(),
                MvCommand.TYPE_SCAN,
                scan.getJobName(),
                scan.getTargetName(),
                scan.getScanSettings(),
                MvCommand.STATUS_CREATED,
                null
        );
        jobDao.createCommand(command);

        scan.setAcceptedAt(command.getCreatedAt());
        scan.setRunnerId(command.getRunnerId());
        scan.setCommandNo(command.getCommandNo());
        jobDao.updateScan(scan);
    }

}
