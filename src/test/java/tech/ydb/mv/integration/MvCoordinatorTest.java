package tech.ydb.mv.integration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvCoordinator;
import tech.ydb.mv.model.MvCoordinatorSettings;
import tech.ydb.mv.support.MvLocker;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;


/**
 * @author Kirill Kurdyukov
 */
public class MvCoordinatorTest extends AbstractIntegrationBase {

    @Test
    public void mvCoordinationOneThread() {
        pause(10000);

        final var concurrentQueue = new ConcurrentLinkedQueue<Integer>();
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
                                                        
                            CREATE TABLE mv_commands (
                                runner_id Text NOT NULL,
                                command_no Uint64 NOT NULL,
                                created_at Timestamp,
                                command_type Text, -- START / STOP
                                job_name Text,
                                job_settings JsonDocument,
                                command_status Text, -- CREATED / TAKEN / SUCCESS / ERROR
                                command_diag Text,
                                PRIMARY KEY(runner_id, command_no)
                            );
                            """);
            runDdl(conn, "INSERT INTO mv_jobs(job_name, should_run) VALUES ('sys$coordinator', true)");
            var scheduler = Executors.newScheduledThreadPool(3);
            new MvCoordinator(new MvLocker(conn), scheduler, conn.getQueryRetryCtx(), new MvCoordinatorSettings(1),
                    "instance", () -> concurrentQueue.add(1)).start();

            for (int i = 1; i <= 10; i++) {
                pause(5000);
                Assertions.assertEquals(i, concurrentQueue.size());

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = false WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();

                pause(5000);

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = true WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();
                pause(5_000);
            }
        }
    }

    @Test
    public void mvCoordinationMultiThread() {
        pause(10000);

        final var concurrentQueue = new ConcurrentLinkedQueue<Integer>();
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

                            CREATE TABLE mv_commands (
                                runner_id Text NOT NULL,
                                command_no Uint64 NOT NULL,
                                created_at Timestamp,
                                command_type Text, -- START / STOP
                                job_name Text,
                                job_settings JsonDocument,
                                command_status Text, -- CREATED / TAKEN / SUCCESS / ERROR
                                command_diag Text,
                                PRIMARY KEY(runner_id, command_no)
                            );
                            """);
            runDdl(conn, "INSERT INTO mv_jobs(job_name, should_run) VALUES ('sys$coordinator', true)");
            var scheduler = Executors.newScheduledThreadPool(20);

            for (int i = 0; i < 20; i++)
                new MvCoordinator(new MvLocker(conn), scheduler, conn.getQueryRetryCtx(), new MvCoordinatorSettings(1),
                        "instance_" + i, () -> concurrentQueue.add(1)).start();

            for (int i = 1; i <= 10; i++) {
                pause(15_000);
                Assertions.assertEquals(i, concurrentQueue.size());

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = false WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();

                pause(10_000);

                conn.getQueryRetryCtx().supplyResult(session -> session
                        .createQuery("UPDATE mv_jobs SET should_run = true WHERE 1 = 1", TxMode.NONE).execute()
                ).join().getStatus().expectSuccess();
                pause(15_000);
            }
        }
    }
}
