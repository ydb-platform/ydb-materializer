package tech.ydb.mv.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.coordination.settings.DescribeSemaphoreMode;
import tech.ydb.coordination.settings.WatchSemaphoreMode;
import tech.ydb.mv.support.MvLocker;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveValue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CREATE TABLE mv_jobs ( -- в девичестве desired_state
 *     job_name Text NOT NULL, -- MvHandler.getName()
 *     job_settings JsonDocument, -- сериализованный MvHandlerSettings / MvDictionarySettings
 *     should_run Boolean, -- должен ли работать
 *     runner_id Text,
 *     PRIMARY KEY(job_name)
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

    private final AtomicReference<ScheduledFuture<?>> leaderFuture = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<?>> watchFuture = new AtomicReference<>();

    public MvCoordinator(
            MvLocker mvLocker,
            ScheduledExecutorService scheduler,
            SessionRetryContext sessionRetryContext,
            MvCoordinatorSettings settings,
            String instanceId
    ) {
        this.mvLocker = mvLocker;
        this.scheduler = scheduler;
        this.sessionRetryContext = sessionRetryContext;
        this.settings = settings;
        this.instanceId = instanceId;
    }

    public void start() {
        scheduleAttempt(0);
        ensureWatchArmed();
    }

    private void scheduleAttempt(long delaySec) {
        scheduler.schedule(this::attemptLeadership, delaySec, TimeUnit.SECONDS);
    }

    private void attemptLeadership() {
        try {
            if (leaderFuture.get() != null) {
                ensureWatchArmed();
                return;
            }

            boolean acquired = mvLocker.lock(SEMAPHORE_NAME);
            if (!acquired) {
                ensureWatchArmed();
                scheduleAttempt(settings.getWatchStateDelaySeconds());
                return;
            }

            if (IsStoppedRun()) {
                ensureWatchArmed();
                scheduleAttempt(settings.getWatchStateDelaySeconds());
                return;
            }

            startLeaderLoop();
        } catch (Exception ex) {
            LOG.error("", ex);

            ensureWatchArmed();
            scheduleAttempt(settings.getWatchStateDelaySeconds());
        }
    }

    private void startLeaderLoop() {
        ScheduledFuture<?> f = scheduler.scheduleWithFixedDelay(
                this::leaderTick,
                0,
                settings.getWatchStateDelaySeconds(),
                TimeUnit.SECONDS
        );
        ScheduledFuture<?> prev = leaderFuture.getAndSet(f);
        if (prev != null) prev.cancel(true);

        ensureWatchArmed();
        startOrMaintainJobs();
    }

    private void startOrMaintainJobs() {

    }

    private void leaderTick() {
        try {
            if (!stillOwnsSemaphore()) {
                demote();
                return;
            }

            if (IsStoppedRun()) {
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
        scheduleAttempt(0);
        ensureWatchArmed();
    }

    private void safeRelease() {
        try {
            mvLocker.release(SEMAPHORE_NAME);
        } catch (Exception ignore) { }
    }

    private void cancelLeader() {
        ScheduledFuture<?> f = leaderFuture.getAndSet(null);
        if (f != null) f.cancel(true);
    }

    private boolean stillOwnsSemaphore() {
        var resultSet = sessionRetryContext.supplyResult(session -> session.executeDataQuery(
                """
                        DECLARE $runner_id AS Text;
                                                  
                        SELECT * FROM mv_jobs WHERE job_name = 'sys$coordinator' AND runner_id = $runner_id;
                        """,
                TxControl.serializableRw(),
                Params.of("$runner_id", PrimitiveValue.newText(instanceId)))
        ).join().getValue().getResultSet(0);

        return resultSet.next();
    }


    private void ensureWatchArmed() {
        if (watchFuture.get() != null) return;

        CompletableFuture<Void> wf = mvLocker.getSession()
                .watchSemaphore(SEMAPHORE_NAME,
                        DescribeSemaphoreMode.WITH_OWNERS,
                        WatchSemaphoreMode.WATCH_OWNERS)
                .thenCompose(r -> r.getValue().getChangedFuture())
                .thenRun(() -> {
                    // Любое изменение владельцев — переоценка состояния
                    watchFuture.set(null);
                    scheduleAttempt(0);
                })
                .exceptionally(ex -> {
                    watchFuture.set(null);
                    scheduleAttempt(settings.getWatchStateDelaySeconds());
                    return null;
                });

        watchFuture.set(wf);
    }


    // корректная остановка задач при выключении координатора
    private void stopJobs() {
        sessionRetryContext.supplyResult(session -> session.executeDataQuery(
                "UPDATE mv_commands SET command_type = 'STOP' WHERE 1 = 1", TxControl.serializableRw())
        ).join().getStatus().expectSuccess();
    }

    private boolean IsStoppedRun() {
        var resultSet = sessionRetryContext.supplyResult(session -> session.executeDataQuery(
                "SELECT should_run FROM mv_jobs WHERE job_name = 'sys$coordinator'", TxControl.serializableRw())
        ).join().getValue().getResultSet(0);
        resultSet.next();

        return !resultSet.getColumn(0).getBool();
    }
}
