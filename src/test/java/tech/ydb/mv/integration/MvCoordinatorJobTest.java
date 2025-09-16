package tech.ydb.mv.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.mgt.MvBatchSettings;
import tech.ydb.mv.mgt.MvCoordinatorJobImpl;
import tech.ydb.mv.mgt.MvJobDao;

import java.util.List;
import java.util.Properties;

/**
 * @author Kirill Kurdyukov
 */
public class MvCoordinatorJobTest extends AbstractIntegrationBase {

    private static final String CREATE_MEGATRON_TABLES = """
            CREATE TABLE `test1/mv_jobs` (
                job_name Text NOT NULL,
                job_settings JsonDocument,
                should_run Bool,
                runner_id Text,
                PRIMARY KEY(job_name)
            );
                    
            CREATE TABLE `test1/mv_runners` (
                runner_id Text NOT NULL,
                runner_identity Text,
                updated_at Timestamp,
                PRIMARY KEY(runner_id)
            );

            CREATE TABLE `test1/mv_runner_jobs` (
                runner_id Text NOT NULL,
                job_name Text NOT NULL,
                job_settings JsonDocument,
                started_at Timestamp,
                PRIMARY KEY(runner_id, job_name)
            );

            CREATE TABLE `test1/mv_commands` (
                runner_id Text NOT NULL,
                command_no Uint64 NOT NULL,
                created_at Timestamp,
                command_type Text,
                job_name Text,
                job_settings JsonDocument,
                command_status Text,
                command_diag Text,
                PRIMARY KEY(runner_id, command_no)
            );
            """;

    private static final String DROP_MEGATRON_TABLES = """
            DROP TABLE `test1/mv_runners`;
            DROP TABLE `test1/mv_runner_jobs`;
            DROP TABLE `test1/mv_commands`;
            """;

    private static YdbConnector ydbConnector;

    private MvJobDao mvJobDao = null;
    private MvCoordinatorJobImpl mvCoordinatorJob = null;

    @BeforeAll
    public static void setup() {
        pause(10_000);
        prepareMegatronDb();
    }

    @AfterAll
    public static void cleanup() {
        pause(1_000);
        clearMegatronDb();
        if (ydbConnector != null) {
            ydbConnector.close();
        }
    }

    private static byte[] getMegatronConfig() {
        Properties props = new Properties();
        props.setProperty("ydb.url", getConnectionUrl());
        props.setProperty("ydb.auth.mode", "NONE");
        props.setProperty(MvBatchSettings.CONF_MV_JOBS_TABLE, "test1/mv_jobs");
        props.setProperty(MvBatchSettings.CONF_MV_RUNNERS_TABLE, "test1/mv_runners");
        props.setProperty(MvBatchSettings.CONF_MV_RUNNER_JOBS_TABLE, "test1/mv_runner_jobs");
        props.setProperty(MvBatchSettings.CONF_MV_COMMANDS_TABLE, "test1/mv_commands");

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            props.storeToXML(baos, "Test props", java.nio.charset.StandardCharsets.UTF_8);
            return baos.toByteArray();
        } catch (java.io.IOException ix) {
            throw new RuntimeException(ix);
        }
    }

    private static void prepareMegatronDb() {
        System.err.println("[MvTableOperationsTest] Setting up megatron tables...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getMegatronConfig(), "config.xml", null);
        ydbConnector = new YdbConnector(cfg);
        runDdl(ydbConnector, CREATE_MEGATRON_TABLES);
    }

    private static void clearMegatronDb() {
        System.err.println("[MvTableOperationsTest] Cleaning up megatron tables...");
        if (ydbConnector != null) {
            runDdl(ydbConnector, DROP_MEGATRON_TABLES);
        }
    }

    @BeforeEach
    public void prepareEach() {
        // Clear all tables before each test
        runDdl(ydbConnector, "DELETE FROM `test1/mv_runners`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_runner_jobs`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_commands`");

        var settings = new MvBatchSettings(ydbConnector.getConfig().getProperties());
        mvJobDao = new MvJobDao(ydbConnector, settings);
        mvCoordinatorJob = new MvCoordinatorJobImpl(mvJobDao, settings);
    }

    @Test
    public void cleanupInactiveRunnersTest() {
        runDdl(ydbConnector, """       
                INSERT INTO `test1/mv_runners`(runner_id, runner_identity, updated_at) VALUES
                    ('1', 'instance_1', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('2', 'instance_2', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('3', 'instance_3', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('4', 'instance_4', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('5', 'instance_5', CurrentUtcTimestamp() - Interval('PT500S'));
                    
                INSERT INTO `test1/mv_runner_jobs`(runner_id, job_name, started_at) VALUES
                    ('1', 'job_1_1', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('1', 'job_1_2', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('1', 'job_1_3', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('2', 'job_2_1', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('2', 'job_2_2', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('4', 'job_3_1', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('4', 'job_3_2', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('4', 'job_3_3', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('4', 'job_3_4', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('4', 'job_3_5', CurrentUtcTimestamp() - Interval('PT500S')),
                    ('5', 'job', CurrentUtcTimestamp() - Interval('PT500S'));
                """);
        mvCoordinatorJob.performCoordinationTask();
        Assertions.assertEquals(0, mvJobDao.getAllRunners().size());
        Assertions.assertEquals(0, mvJobDao.getRunnerJobs("instance_1").size());
        Assertions.assertEquals(0, mvJobDao.getRunnerJobs("instance_2").size());
        Assertions.assertEquals(0, mvJobDao.getRunnerJobs("instance_3").size());
        Assertions.assertEquals(0, mvJobDao.getRunnerJobs("instance_4").size());
        Assertions.assertEquals(0, mvJobDao.getRunnerJobs("instance_5").size());
    }

    @Test
    /*
     * 2025-09-16 14:20:09 WARN      1  MvCoordinatorJobImpl:133  No runner found for job: not_found_job
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_1 on runner: 2
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_3 on runner: 3
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_2 on runner: 4
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_5 on runner: 5
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_4 on runner: 1
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_7 on runner: 2
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:189  Created START command for job: job_6 on runner: 3
     * 2025-09-16 14:20:09 INFO      1  MvCoordinatorJobImpl:107  Balanced jobs - stopped 1 extra, started 7 missing
     */
    public void balancingTest1() {
        runDdl(ydbConnector, """
                INSERT INTO `test1/mv_jobs` (job_name, should_run) VALUES
                    ('sys$coordinator', true),
                    ('job_1', true),
                    ('job_2', true),
                    ('job_3', true),
                    ('job_4', true),
                    ('job_5', true),
                    ('job_6', true),
                    ('job_7', true);
                INSERT INTO `test1/mv_runners` (runner_id, runner_identity, updated_at) VALUES
                    ('1', 'instance_1', CurrentUtcTimestamp()),
                    ('2', 'instance_2', CurrentUtcTimestamp()),
                    ('3', 'instance_3', CurrentUtcTimestamp()),
                    ('4', 'instance_4', CurrentUtcTimestamp()),
                    ('5', 'instance_5', CurrentUtcTimestamp());
                INSERT INTO `test1/mv_commands`(runner_id, command_no, job_name, command_status) VALUES
                    ('1', 3, 'not_found_job', 'TAKEN');
                """);
        mvCoordinatorJob.performCoordinationTask();
        for (var instance : List.of("1", "2", "3", "4", "5"))
            Assertions.assertTrue(mvJobDao.getCommandsForRunner(instance).size() <= 2);
    }

    @Test
    public void balancingTest2() {
        runDdl(ydbConnector, """
                INSERT INTO `test1/mv_jobs` (job_name, should_run) VALUES
                    ('sys$coordinator', true),
                    ('job_1', true),
                    ('job_2', true),
                    ('job_3', true),
                    ('job_4', true),
                    ('job_5', true),
                    ('job_6', true),
                    ('job_7', true);
                INSERT INTO `test1/mv_runners` (runner_id, runner_identity, updated_at) VALUES
                    ('1', 'instance_1', CurrentUtcTimestamp()),
                    ('2', 'instance_2', CurrentUtcTimestamp());
                INSERT INTO `test1/mv_commands`(runner_id, command_no, job_name, command_status) VALUES
                    ('1', 1, 'job_1', 'TAKEN'),
                    ('1', 2, 'job_2', 'TAKEN'),
                    ('1', 3, 'job_3', 'TAKEN'),
                    ('1', 4, 'job_4', 'TAKEN');
                """);
        Assertions.assertEquals(0, mvJobDao.getCommandsForRunner("2").size());
        mvCoordinatorJob.performCoordinationTask();
        Assertions.assertEquals(3, mvJobDao.getCommandsForRunner("2").size());
        Assertions.assertEquals(4, mvJobDao.getCommandsForRunner("1").size());
    }
}
