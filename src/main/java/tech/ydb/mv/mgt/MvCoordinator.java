package tech.ydb.mv.mgt;

import tech.ydb.mv.svc.MvLocker;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
public class MvCoordinator implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvCoordinator.class);

    private final MvLocker locker;
    private final MvJobDao jobDao;
    private final ScheduledExecutorService scheduler;
    private final MvBatchSettings settings;
    private final MvCoordinatorActions job;
    private final String runnerId;
    private final AtomicReference<ScheduledFuture<?>> attemptFuture = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> leaderFuture = new AtomicReference<>();

    public MvCoordinator(
            MvLocker locker,
            MvJobDao jobDao,
            ScheduledExecutorService scheduler,
            MvBatchSettings settings,
            MvCoordinatorActions job,
            String runnerId
    ) {
        this.locker = locker;
        this.jobDao = jobDao;
        this.scheduler = scheduler;
        this.settings = settings;
        this.job = job;
        this.runnerId = runnerId;
    }

    public static MvCoordinator newInstance(YdbConnector ydb,
            MvBatchSettings settings, String runnerId) {
        return newInstance(ydb, settings, runnerId, null, null);
    }

    public static MvCoordinator newInstance(YdbConnector ydb,
            MvBatchSettings settings, String runnerId,
            ScheduledExecutorService scheduler) {
        return newInstance(ydb, settings, runnerId, scheduler, null);
    }

    public static MvCoordinator newInstance(YdbConnector ydb,
            MvBatchSettings settings, String runnerId,
            ScheduledExecutorService scheduler,
            MvCoordinatorActions job) {
        MvLocker locker = new MvLocker(ydb);
        MvJobDao jobDao = new MvJobDao(ydb, settings);
        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }
        if (job == null) {
            job = new MvCoordinatorImpl(runnerId, jobDao, settings);
        }
        return new MvCoordinator(locker, jobDao, scheduler, settings, job, runnerId);
    }

    public String getRunnerId() {
        return runnerId;
    }

    public synchronized boolean start() {
        if (isRunning()) {
            return false;
        }
        LOG.info("[{}] Coordinator starting", runnerId);
        scheduleAttempt();
        return true;
    }

    public boolean stop() {
        if (!isRunning()) {
            return false;
        }
        LOG.info("[{}] Coordinator stopping", runnerId);
        demote();
        cancelLeader();
        cancelAttempt();
        return true;
    }

    @Override
    public void close() {
        LOG.info("[{}] Coordinator closing", runnerId);
        stop();
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(2L, TimeUnit.SECONDS);
        } catch (InterruptedException ix) {
            Thread.currentThread().interrupt();
        }
        locker.close();
    }

    public boolean isRunning() {
        return isLeader() || (attemptFuture.get() != null);
    }

    public boolean isLeader() {
        return leaderFuture.get() != null;
    }

    private void scheduleAttempt() {
        long delayMsec = settings.getScanPeriodMs();
        LOG.debug("[{}] Scheduling leadership attempt in {}ms", runnerId, delayMsec);
        ScheduledFuture<?> f;
        try {
            f = scheduler.schedule(
                    this::attemptLeadership,
                    delayMsec,
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException ex) {
            if (scheduler.isShutdown()) {
                // To avoid useless error traces in shutdown.
                return;
            } else {
                throw ex;
            }
        }
        f = attemptFuture.getAndSet(f);
        if (f != null) {
            f.cancel(false);
        }
    }

    private void attemptLeadership() {
        try {
            doAttemptLeadership();
        } catch (Exception ex) {
            LOG.error("[{}] Error on leadership attempt", runnerId, ex);
            demote();
            scheduleAttempt();
        }
    }

    private void doAttemptLeadership() {
        LOG.trace("[{}] Attempting leadership", runnerId);
        if (leaderFuture.get() != null) {
            LOG.trace("[{}] Leader future is already set", runnerId);
            return;
        }

        boolean acquired = locker.lock(MvConfig.HANDLER_COORDINATOR, Duration.ofSeconds(1));
        if (!acquired) {
            scheduleAttempt();
            return;
        }
        LOG.info("[{}] Leadership acquired", runnerId);

        startLeaderLoop();
    }

    private void startLeaderLoop() {
        LOG.info("[{}] Became leader. Tick={}ms, minRunners={}, stabilization={}ms",
                runnerId, settings.getScanPeriodMs(), settings.getRunnersCount(),
                settings.getCoordStartupMs());

        ScheduledFuture<?> f;
        try {
            var runners = jobDao.getJobRunners(MvConfig.HANDLER_COORDINATOR);
            for (var runner : runners) {
                LOG.info("[{}] Removing demoted coordinator record, which was started at {}",
                        runner.getRunnerId(), runner.getStartedAt());
                jobDao.deleteRunnerJob(runner.getRunnerId(), MvConfig.HANDLER_COORDINATOR);
            }

            jobDao.upsertRunnerJob(new MvRunnerJobInfo(
                    runnerId, MvConfig.HANDLER_COORDINATOR, null, Instant.now()));

            job.onStart();

            f = scheduler.scheduleWithFixedDelay(
                    this::leaderTick,
                    0,
                    settings.getScanPeriodMs(),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException ex) {
            if (scheduler.isShutdown() || !jobDao.isConnectionOpen()) {
                LOG.info("[{}] Shutdown in progress, demoting leadership", runnerId);
                demote();
                return;
            } else {
                throw ex;
            }
        }

        f = leaderFuture.getAndSet(f);
        if (f != null) {
            LOG.debug("[{}] Cancelling previous leader loop future", runnerId);
            f.cancel(true);
        }

        cancelAttempt();

        LOG.info("[{}] Leader loop started", runnerId);
    }

    private void leaderTick() {
        if (!isRunning()) {
            return;
        }
        try {
            if (!stillActive()) {
                LOG.info("[{}] Coordinator lost ownership, demoting...", runnerId);
                demote();
                if (isRunning()) {
                    scheduleAttempt();
                }
                return;
            }

            job.onTick();
        } catch (RejectedExecutionException ree) {
            // shutdown is being performed
            LOG.error("[{}] Detected shutdown on tick action in the coordinator - demoting", runnerId);
            demote();
        } catch (Exception ex) {
            LOG.error("[{}] Failed tick action in the coordinator - demoting", runnerId, ex);
            demote();
            scheduleAttempt();
        }
    }

    private boolean stillActive() {
        if (!isRunning()) {
            return false;
        }
        var runners = jobDao.getJobRunners(MvConfig.HANDLER_COORDINATOR);
        if (runners.size() != 1) {
            return false;
        }
        return runnerId.equals(runners.get(0).getRunnerId());
    }

    private synchronized void demote() {
        if (leaderFuture.get() != null) {
            LOG.info("[{}] Demoting leadership", runnerId);
            cancelLeader();
            if (jobDao.isConnectionOpen()) {
                deleteJobRun();
            }
            safeRelease();
            runStopHandler();
        }
    }

    private void cancelLeader() {
        var f = leaderFuture.getAndSet(null);
        if (f != null) {
            f.cancel(true);
        }
    }

    private void cancelAttempt() {
        var f = attemptFuture.getAndSet(null);
        if (f != null) {
            f.cancel(false);
        }
    }

    private void deleteJobRun() {
        try {
            LOG.debug("[{}] Dropping job run '{}'",
                    runnerId, MvConfig.HANDLER_COORDINATOR);
            jobDao.deleteRunnerJob(runnerId, MvConfig.HANDLER_COORDINATOR);
        } catch (Exception e) {
            LOG.warn("[{}] Failed to delete the job run '{}', instanceId={}",
                    runnerId, MvConfig.HANDLER_COORDINATOR, e);
        }
    }

    private void safeRelease() {
        try {
            LOG.debug("[{}] Releasing semaphore '{}'",
                    runnerId, MvConfig.HANDLER_COORDINATOR);
            locker.release(MvConfig.HANDLER_COORDINATOR);
        } catch (Exception e) {
            LOG.warn("[{}] Failed to release the semaphore '{}'",
                    runnerId, MvConfig.HANDLER_COORDINATOR, e);
        }
    }

    private boolean runStopHandler() {
        try {
            job.onStop();
            return true;
        } catch (Exception ex) {
            LOG.warn("[{}] Stop action failed", runnerId, ex);
            return false;
        }
    }

}
