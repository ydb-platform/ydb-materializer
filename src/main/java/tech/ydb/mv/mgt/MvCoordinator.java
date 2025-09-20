package tech.ydb.mv.mgt;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;

/**
 * Coordinator controls and manages the actual state of the jobs. In case the
 * desired and the actual state differs, it transitions from the actual to the
 * desired state by generating the commands to the runners.
 *
 * @author Kirill Kurdyukov
 */
public class MvCoordinator {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinator.class);

    private final MvLocker locker;
    private final MvJobDao jobDao;
    private final ScheduledExecutorService scheduler;
    private final MvBatchSettings settings;
    private final MvCoordinatorActions job;
    private final String runnerId;

    private final AtomicReference<ScheduledFuture<?>> leaderFuture = new AtomicReference<>();

    public MvCoordinator(
            YdbConnector ydb,
            MvBatchSettings settings,
            String runnerId,
            MvCoordinatorActions job
    ) {
        this.locker = new MvLocker(ydb);
        this.jobDao = new MvJobDao(ydb, settings);
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.settings = settings;
        this.runnerId = runnerId;
        this.job = job;
    }

    public MvCoordinator(
            YdbConnector ydb,
            MvBatchSettings settings,
            String runnerId
    ) {
        this.locker = new MvLocker(ydb);
        this.jobDao = new MvJobDao(ydb, settings);
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.settings = settings;
        this.runnerId = runnerId;
        this.job = new MvCoordinatorImpl(jobDao, settings);
    }

    public void start() {
        scheduleAttempt();
    }

    private void scheduleAttempt() {
        long delayMsec = settings.getScanPeriodMs();
        LOG.debug("Scheduling leadership attempt in {}ms, instanceId={}", delayMsec, runnerId);
        scheduler.schedule(this::attemptLeadership, delayMsec, TimeUnit.MILLISECONDS);
    }

    private void attemptLeadership() {
        try {
            doAttemptLeadership();
        } catch (Exception ex) {
            LOG.error("Error during attemptLeadership, instanceId={}", runnerId, ex);
            scheduleAttempt();
        }
    }

    private void doAttemptLeadership() {
        LOG.trace("Attempting leadership, instanceId={}", runnerId);
        if (leaderFuture.get() != null) {
            LOG.trace("Leader future is already set, instanceId={}", runnerId);
            return;
        }

        boolean acquired = locker.lock(MvConfig.HANDLER_COORDINATOR, Duration.ofSeconds(1));
        if (!acquired) {
            scheduleAttempt();
            return;
        }

        LOG.info("Semaphore '{}' acquired, instanceId={}",
                MvConfig.HANDLER_COORDINATOR, runnerId);

        MvJobInfo info = new MvJobInfo(MvConfig.HANDLER_COORDINATOR, null, true, runnerId);
        jobDao.upsertJob(info);

        startLeaderLoop();
    }

    private void startLeaderLoop() {
        LOG.info("Becoming leader, starting leader loop, tick={}ms, instanceId={}",
                settings.getScanPeriodMs(), runnerId);

        job.onStart();

        ScheduledFuture<?> f = scheduler.scheduleWithFixedDelay(
                this::leaderTick,
                0,
                settings.getScanPeriodMs(),
                TimeUnit.MILLISECONDS
        );
        ScheduledFuture<?> prev = leaderFuture.getAndSet(f);
        if (prev != null) {
            LOG.debug("Cancelling previous leader loop future, instanceId={}", runnerId);
            prev.cancel(true);
        }
    }

    private void leaderTick() {
        try {
            if (!stillActive()) {
                LOG.info("Lost ownership or runner_id mismatch, demoting, instanceId={}", runnerId);
                demote();
                return;
            }

            job.onUpdate();
        } catch (Throwable t) {
            demote();
        }
    }

    private void demote() {
        cancelLeader();
        safeRelease();
        scheduleAttempt();
    }

    private void safeRelease() {
        try {
            LOG.debug("Releasing semaphore '{}', instanceId={}", MvConfig.HANDLER_COORDINATOR, runnerId);
            locker.release(MvConfig.HANDLER_COORDINATOR);
        } catch (Exception e) {
            LOG.trace("Fail release", e);
        }
    }

    private void cancelLeader() {
        ScheduledFuture<?> f = leaderFuture.getAndSet(null);
        if (f != null) {
            f.cancel(true);
        }
    }

    private boolean stillActive() {
        MvJobInfo info = jobDao.getJob(MvConfig.HANDLER_COORDINATOR);
        if (info == null) {
            return false;
        }
        return runnerId.equals(info.getRunnerId());
    }

}
