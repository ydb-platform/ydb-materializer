package tech.ydb.mv.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.mv.support.MvLocker;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CREATE TABLE mv_jobs ( -- в девичестве desired_state
 * job_name Text NOT NULL, -- MvHandler.getName()
 * job_settings JsonDocument, -- сериализованный MvHandlerSettings / MvDictionarySettings
 * should_run Boolean, -- должен ли работать
 * runner_id Text,
 * PRIMARY KEY(job_name)
 * );
 *
 * @author Kirill Kurdyukov
 */
public class MvCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(MvCoordinator.class);

    private static final String SEMAPHORE_NAME = "mv-coordinator-semaphore";

    private final MvLocker mvLocker;
    private final ScheduledExecutorService scheduler;
    private final SessionRetryContext sessionRetryContext;
    private final MvCoordinatorSettings settings;
    private final String instanceId;
    private final Runnable coordinatorJob;

    private final AtomicReference<ScheduledFuture<?>> leaderFuture = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<?>> watchFuture = new AtomicReference<>();

    public MvCoordinator(
            MvLocker mvLocker,
            ScheduledExecutorService scheduler,
            SessionRetryContext sessionRetryContext,
            MvCoordinatorSettings settings,
            String instanceId,
            Runnable coordinatorJob
    ) {
        this.mvLocker = mvLocker;
        this.scheduler = scheduler;
        this.sessionRetryContext = sessionRetryContext;
        this.settings = settings;
        this.instanceId = instanceId;
        this.coordinatorJob = coordinatorJob;
    }

    public void start() {
        scheduleAttempt(0);
    }

    private void scheduleAttempt(long delaySec) {
        LOG.debug("Scheduling leadership attempt in {}s, instanceId={}", delaySec, instanceId);

        scheduler.schedule(this::attemptLeadership, delaySec, TimeUnit.SECONDS);
    }

    private void attemptLeadership() {
        try {
            LOG.trace("Attempting leadership, instanceId={}", instanceId);

            if (leaderFuture.get() != null) {
                return;
            }

            boolean acquired = mvLocker.lock(SEMAPHORE_NAME);
            if (!acquired) {
                scheduleAttempt(settings.getWatchStateDelaySeconds());
                return;
            }

            LOG.info("Semaphore '{}' acquired, instanceId={} — candidate for leader", SEMAPHORE_NAME, instanceId);

            if (IsStoppedRun()) {
                LOG.warn("Coordinator desired state = STOPPED (should_run=false), instanceId={}. Not starting leader loop",
                        instanceId);
                safeRelease();
                scheduleAttempt(settings.getWatchStateDelaySeconds());
                return;
            }

            sessionRetryContext.supplyResult(session -> session.createQuery(
                    """
                            DECLARE $runner_id AS Text;
                                                       
                            UPDATE mv_jobs SET runner_id = $runner_id WHERE job_name = 'sys$coordinator'
                            """,
                    TxMode.SERIALIZABLE_RW, Params.of("$runner_id", PrimitiveValue.newText(instanceId))).execute()
            ).join().getStatus().expectSuccess();

            startLeaderLoop();
        } catch (Exception ex) {
            LOG.error("Error during attemptLeadership, instanceId={}", instanceId, ex);

            scheduleAttempt(settings.getWatchStateDelaySeconds());
        }
    }

    private void startLeaderLoop() {
        LOG.info("Becoming leader, starting leader loop, tick={}s, instanceId={}",
                settings.getWatchStateDelaySeconds(), instanceId);

        ScheduledFuture<?> f = scheduler.scheduleWithFixedDelay(
                this::leaderTick,
                0,
                settings.getWatchStateDelaySeconds(),
                TimeUnit.SECONDS
        );
        ScheduledFuture<?> prev = leaderFuture.getAndSet(f);
        if (prev != null) {
            LOG.debug("Cancelling previous leader loop future, instanceId={}", instanceId);

            prev.cancel(true);
        }

        coordinatorJob.run();
    }

    private void leaderTick() {
        try {
            if (!stillOwnsSemaphore()) {
                LOG.info("Lost semaphore ownership or runner_id mismatch, demoting, instanceId={}", instanceId);

                demote();
                return;
            }

            if (IsStoppedRun()) {
                LOG.info("Coordinator received state is STOP, demoting leader, instanceId={}", instanceId);

                demote();
            }
        } catch (Throwable t) {
            demote();
        }
    }

    private void demote() {
        cancelLeader();
        stopJobs();
        safeRelease();
        // после демоушена снова в режим фолловера — ждем событий и иногда пробуем стать лидером
        scheduleAttempt(settings.getWatchStateDelaySeconds());
    }

    private void safeRelease() {
        try {
            LOG.debug("Releasing semaphore '{}', instanceId={}", SEMAPHORE_NAME, instanceId);

            mvLocker.release(SEMAPHORE_NAME);
        } catch (Exception e) {
            LOG.trace("Fail release", e);
        }
    }

    private void cancelLeader() {
        ScheduledFuture<?> f = leaderFuture.getAndSet(null);
        if (f != null) f.cancel(true);
    }

    private boolean stillOwnsSemaphore() {
        var resultSet = sessionRetryContext.supplyResult(session -> QueryReader.readFrom(session.createQuery(
                """
                        DECLARE $runner_id AS Text;
                        SELECT * FROM mv_jobs WHERE job_name = 'sys$coordinator' AND runner_id = $runner_id;
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of("$runner_id", PrimitiveValue.newText(instanceId))))
        ).join().getValue().getResultSet(0);

        return resultSet.next();
    }


    // корректная остановка задач при выключении координатора
    private void stopJobs() {
        sessionRetryContext.supplyResult(session -> session.createQuery(
                "UPDATE mv_commands SET command_type = 'STOP' WHERE 1 = 1", TxMode.SERIALIZABLE_RW
        ).execute()).join().getStatus().expectSuccess();
    }

    private boolean IsStoppedRun() {
        var resultSet = sessionRetryContext.supplyResult(session -> QueryReader.readFrom(session.createQuery(
                "SELECT should_run FROM mv_jobs WHERE job_name = 'sys$coordinator'", TxMode.SERIALIZABLE_RW)
        )).join().getValue().getResultSet(0);
        resultSet.next();

        return !resultSet.getColumn(0).getBool();
    }
}
