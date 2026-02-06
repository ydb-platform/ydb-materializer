package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Kirill Kurdyukov
 */
class MvCoordinatorImpl implements MvCoordinatorActions {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinatorImpl.class);

    private final String runnerId;
    private final AtomicLong commandNo = new AtomicLong();
    private final MvJobDao jobDao;
    private final MvBatchSettings settings;
    private final Instant startupTv;
    private volatile boolean balancing;
    private volatile boolean selfCleanupDetected;

    public MvCoordinatorImpl(String runnerId, MvJobDao jobDao, MvBatchSettings settings) {
        this.runnerId = runnerId;
        this.jobDao = jobDao;
        this.settings = settings;
        this.startupTv = Instant.now().plusMillis(settings.getCoordStartupMs());
        this.balancing = false;
        this.selfCleanupDetected = false;
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

            boolean hasSelfCleanup = inactiveRunners.stream()
                    .map(ir -> ir.getRunnerId())
                    .filter(v -> runnerId.equals(v))
                    .count() > 0L;
            if (hasSelfCleanup) {
                if (!selfCleanupDetected) {
                    selfCleanupDetected = true;
                    LOG.warn("[{}] Detected inactivity for self-runner, cleanup DELAYED.", runnerId);
                }
                return;
            } else {
                if (selfCleanupDetected) {
                    selfCleanupDetected = false;
                    LOG.info("[{}] Resumed activity reporting for self-runner, cleanup RE-ACTIVATED.", runnerId);
                }
            }

            for (MvRunnerInfo ir : inactiveRunners) {
                jobDao.deletePendingCommands(ir.getRunnerId());
                jobDao.deleteRunnerJobs(ir.getRunnerId());
                jobDao.deleteRunner(ir.getRunnerId());

                LOG.info("[{}] Cleaned up inactive runner {}, which was last active at {} as {}",
                        runnerId, ir.getRunnerId(), ir.getUpdatedAt(), ir.getIdentity());
            }

        } catch (Exception ex) {
            LOG.error("[{}] Failed to cleanup inactive runners", runnerId, ex);
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
            LOG.info("[{}] Started job balancing...", runnerId);
        }
        try {
            new MvBalancer(jobDao, commandNo, settings.getRunnersCount())
                    .balanceJobs();
        } catch (Exception ex) {
            LOG.error("[{}] Failed to balance jobs", runnerId, ex);
        }
    }

    private void acceptScans() {
        try {
            doAcceptScans();
        } catch (Exception ex) {
            LOG.error("[{}] Failed to process scans requests", runnerId, ex);
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
            LOG.info("[{}] Cannot start the requested scan "
                    + "for handler `{}`, target `{}` - runner was not found.",
                    runnerId, scan.getJobName(), scan.getTargetName());
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
