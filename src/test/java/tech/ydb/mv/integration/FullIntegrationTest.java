package tech.ydb.mv.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvApi;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.mgt.MvBatchSettings;
import tech.ydb.mv.mgt.MvCoordinator;
import tech.ydb.mv.mgt.MvRunner;

/**
 * @author Kirill Kurdyukov
 */
public class FullIntegrationTest extends AbstractIntegrationBase {

    @BeforeAll
    public static void init() {
        prepareDb();
    }

    @AfterAll
    public static void cleanup() {
        clearDb();
    }

    @Test
    public void concurrencyIntegrationTest() {
        System.err.println("[AAA] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        var batchSettings = new MvBatchSettings(cfg.getProperties());
        cfg.getProperties().setProperty(MvConfig.CONF_COORD_TIMEOUT, "5");
        var instance1 = "instance_1";
        var instance2 = "instance_2";

        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, """
            CREATE TABLE `mv_jobs` (
                job_name Text NOT NULL,
                job_settings JsonDocument,
                should_run Bool,
                PRIMARY KEY(job_name)
            );

            CREATE TABLE `mv_runners` (
                runner_id Text NOT NULL,
                runner_identity Text,
                updated_at Timestamp,
                PRIMARY KEY(runner_id)
            );

            CREATE TABLE `mv_runner_jobs` (
                runner_id Text NOT NULL,
                job_name Text NOT NULL,
                job_settings JsonDocument,
                started_at Timestamp,
                INDEX ix_job_name GLOBAL SYNC ON (job_name),
                PRIMARY KEY(runner_id, job_name)
            );

            CREATE TABLE `mv_commands` (
                runner_id Text NOT NULL,
                command_no Uint64 NOT NULL,
                created_at Timestamp,
                command_type Text,
                job_name Text,
                job_settings JsonDocument,
                command_status Text,
                command_diag Text,
                INDEX ix_command_no GLOBAL SYNC ON (command_no),
                PRIMARY KEY(runner_id, command_no)
            );
                    """);
        }

        Thread t1 = new Thread(() -> handler(cfg, instance1, batchSettings));
        Thread t2 = new Thread(() -> handler(cfg, instance2, batchSettings));
        Thread t1dup = new Thread(() -> handler(cfg, instance1, batchSettings));

        t1.start();
        t2.start();
        pause(100L);
        t1dup.start();

        pause(10_000);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, """
                    INSERT INTO `mv_jobs` (job_name, should_run) VALUES
                        ('handler1', true),
                        ('handler2', true);
                        """);
        }
        pause(20_000);
    }

    private void handler(YdbConnector.Config cfg, String instanceName, MvBatchSettings batchSettings) {
        try (var conn = new YdbConnector(cfg); var api = MvApi.newInstance(conn); var runner = new MvRunner(conn, api)) {
            api.applyDefaults(conn.getConfig().getProperties());
            MvCoordinator.newInstance(conn, batchSettings, instanceName)
                    .start();
            runner.start();

            pause(40_000);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }
}
