package tech.ydb.mv.integration;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

    @BeforeEach
    public void init() {
        System.err.println("[FFF] Core initialization ...");
        prepareDb();
        createExtraTables();
        System.err.println("[FFF] Core initialization completed!");
    }

    @AfterEach
    public void cleanup() {
        System.err.println("[FFF] Core cleanup ...");
        dropExtraTables();
        clearDb();
        System.err.println("[FFF] Core initialization completed!");
    }

    private void createExtraTables() {
        try (YdbConnector conn = new YdbConnector(getConfig())) {
            runDdl(conn, """
            CREATE TABLE `mv/jobs` (
                job_name Text NOT NULL,
                job_settings JsonDocument,
                should_run Bool,
                PRIMARY KEY(job_name)
            );

            CREATE TABLE `mv/job_scans` (
                job_name Text NOT NULL,
                target_name Text NOT NULL,
                scan_settings JsonDocument,
                requested_at Timestamp,
                accepted_at Timestamp,
                runner_id Text,
                command_no Uint64,
                PRIMARY KEY(job_name, target_name)
            );

            CREATE TABLE `mv/runners` (
                runner_id Text NOT NULL,
                runner_identity Text,
                updated_at Timestamp,
                PRIMARY KEY(runner_id)
            );

            CREATE TABLE `mv/runner_jobs` (
                runner_id Text NOT NULL,
                job_name Text NOT NULL,
                job_settings JsonDocument,
                started_at Timestamp,
                INDEX ix_job_name GLOBAL SYNC ON (job_name),
                PRIMARY KEY(runner_id, job_name)
            );

            CREATE TABLE `mv/commands` (
                runner_id Text NOT NULL,
                command_no Uint64 NOT NULL,
                created_at Timestamp,
                command_type Text,
                job_name Text,
                target_name Text,
                job_settings JsonDocument,
                command_status Text,
                command_diag Text,
                INDEX ix_command_no GLOBAL SYNC ON (command_no),
                INDEX ix_command_status GLOBAL SYNC ON (command_status, runner_id),
                PRIMARY KEY(runner_id, command_no)
            );
                    """);
            System.err.println("[FFF] Exta tables created!");
        }
    }

    private void dropExtraTables() {
        System.err.println("[FFF] Database cleanup phase 2...");
        try (YdbConnector conn = new YdbConnector(getConfig())) {
            try {
                runDdl(conn, "DROP TABLE mv_jobs;");
            } catch (Exception ex) {
                System.err.println("[FFF] Cannot drop MV_JOBS: " + ex.toString());
            }
            try {
                runDdl(conn, "DROP TABLE mv_job_scans;");
            } catch (Exception ex) {
                System.err.println("[FFF] Cannot drop MV_JOB_SCANS: " + ex.toString());
            }
            try {
                runDdl(conn, "DROP TABLE mv_runners;");
            } catch (Exception ex) {
                System.err.println("[FFF] Cannot drop MV_RUNNERS: " + ex.toString());
            }
            try {
                runDdl(conn, "DROP TABLE mv_runner_jobs;");
            } catch (Exception ex) {
                System.err.println("[FFF] Cannot drop MV_RUNNER_JOBS: " + ex.toString());
            }
            try {
                runDdl(conn, "DROP TABLE mv_commands;");
            } catch (Exception ex) {
                System.err.println("[FFF] Cannot drop MV_COMMANDS: " + ex.toString());
            }
            System.err.println("[FFF] Exta tables dropped!");
        }
    }

    @Override
    protected MvConfig getNewConfig() {
        var ret = super.getNewConfig();
        ret.getProperties().setProperty(MvConfig.CONF_COORD_TIMEOUT, "5");
        return ret;
    }

    @Test
    public void concurrencyIntegrationTest() {
        System.err.println("[FFF] Starting up...");

        var cfg = getConfig();
        var instance1 = "instance_1";
        var instance2 = "instance_2";

        AtomicInteger successCounter = new AtomicInteger(0);
        Thread t1 = new Thread(() -> handler(cfg, instance1, successCounter));
        Thread t2 = new Thread(() -> handler(cfg, instance2, successCounter));
        Thread t1dup = new Thread(() -> handler(cfg, instance1, new AtomicInteger(0)));

        t1.start();
        t2.start();
        pause(100L);
        t1dup.start();

        System.err.println("[FFF] Threads started, initializing...");

        pause(10_000);

        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, """
                    INSERT INTO `mv_jobs` (job_name, should_run) VALUES
                        ('handler1', true),
                        ('handler2', true);
                        """);
        }

        System.err.println("[FFF] Job added, waiting for thread completion...");

        try {
            t1.join();
        } catch (InterruptedException ix) {
        }
        try {
            t2.join();
        } catch (InterruptedException ix) {
        }
        try {
            t1dup.join();
        } catch (InterruptedException ix) {
        }

        System.err.println("[FFF] All done!");

        Assertions.assertEquals(2, successCounter.get());
    }

    private void handler(MvConfig cfg, String name, AtomicInteger successCounter) {
        var batchSettings = new MvBatchSettings(cfg.getProperties());
        try (var conn = new YdbConnector(cfg, true); var api = MvApi.newInstance(conn)) {
            try (var runner = new MvRunner(conn, api, name)) {
                api.applyDefaults(conn.getConfig().getProperties());
                try (var coord = MvCoordinator.newInstance(conn, batchSettings, name)) {
                    System.err.println("[FFF] Instance starting: " + name);
                    runner.start();
                    pause(1_000);
                    coord.start();
                    pause(40_000);
                }
            }
            successCounter.incrementAndGet();
            System.err.println("[FFF] Instance succeeded: " + name);
        } catch (Exception ex) {
            System.err.println("[FFF] Instance failed: " + name);
            ex.printStackTrace(System.err);
        }
    }
}
