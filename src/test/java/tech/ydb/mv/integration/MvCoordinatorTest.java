package tech.ydb.mv.integration;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import tech.ydb.common.transaction.TxMode;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.mgt.MvCoordinator;
import tech.ydb.mv.mgt.MvCoordinatorJob;
import tech.ydb.mv.mgt.MvCoordinatorSettings;
import tech.ydb.mv.mgt.MvLocker;

/**
 * @author Kirill Kurdyukov
 */
@Disabled
public class MvCoordinatorTest extends AbstractIntegrationBase {

    @Test
    public void mvCoordinationOneThread() {
        pause(10000);

        final var queue = new ConcurrentLinkedQueue<>();
        final var deque = new ConcurrentLinkedDeque<>();
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "classpath:/config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            pause(10000);
            runDdl(conn,
                    """
                            CREATE TABLE mv_jobs (
                                job_name Text NOT NULL,
                                job_settings JsonDocument,
                                should_run Bool,
                                runner_id Text,
                                PRIMARY KEY(job_name)
                            );
                            """);
            runDdl(conn, "INSERT INTO mv_jobs(job_name, should_run) VALUES ('sys$coordinator', true)");
            var scheduler = Executors.newScheduledThreadPool(3);
            new MvCoordinator(new MvLocker(conn), scheduler, conn.getQueryRetryCtx(), new MvCoordinatorSettings(1), "instance",
                    new MvCoordinatorJob() {
                        private final AtomicReference<String> tick = new AtomicReference<>(UUID.randomUUID().toString());

                        @Override
                        public void start() {
                            queue.add(1);
                        }

                        @Override
                        public void performCoordinationTask() {
                            var peekLast = deque.peekLast();
                            if (peekLast == null) {
                                deque.addLast(tick.get());
                                return;
                            }

                            if (!tick.get().equals(peekLast)) {
                                deque.addLast(tick.get());
                            }
                        }

                        @Override
                        public void stop() {
                            tick.set(UUID.randomUUID().toString());
                        }
                    }).start();

            for (int i = 1; i <= 10; i++) {
                pause(5_000);
                Assertions.assertEquals(i, queue.size());
                Assertions.assertEquals(i, deque.size());

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = false WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();

                pause(5_000);

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = true WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();
                pause(5_000);
            }

            runDdl(conn, "DROP TABLE mv_jobs;");
        }
    }

    @Test
    public void mvCoordinationMultiThread() {
        pause(10000);

        final var queue = new ConcurrentLinkedQueue<>();
        final var deque = new ConcurrentLinkedDeque<String>();
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "classpath:/config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            pause(10000);
            runDdl(conn, """
                    CREATE TABLE mv_jobs (
                        job_name Text NOT NULL,
                        job_settings JsonDocument,
                        should_run Bool,
                        runner_id Text,
                        PRIMARY KEY(job_name)
                    );
                    """);
            runDdl(conn, "INSERT INTO mv_jobs(job_name, should_run) VALUES ('sys$coordinator', true)");
            var scheduler = Executors.newScheduledThreadPool(20);

            for (int i = 0; i < 20; i++) {
                new MvCoordinator(new MvLocker(conn), scheduler, conn.getQueryRetryCtx(),
                        new MvCoordinatorSettings(1), "instance_" + i,
                        new MvCoordinatorJob() {
                            private final AtomicReference<String> tick = new AtomicReference<>(UUID.randomUUID().toString());

                            @Override
                            public void start() {
                                queue.add(1);
                            }

                            @Override
                            public void performCoordinationTask() {
                                var peekLast = deque.peekLast();

                                if (peekLast == null) {
                                    deque.addLast(tick.get());
                                    return;
                                }

                                if (!tick.get().equals(peekLast)) {
                                    deque.addLast(tick.get());
                                }
                            }

                            @Override
                            public void stop() {
                                tick.set(UUID.randomUUID().toString());
                            }
                        }).start();
            }

            for (int i = 1; i <= 10; i++) {
                pause(15_000);
                Assertions.assertEquals(i, queue.size());
                Assertions.assertEquals(i, deque.size());

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = false WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();

                pause(10_000);

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = true WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();
                pause(15_000);
            }

            runDdl(conn, "DROP TABLE mv_jobs;");
        }
    }
}
