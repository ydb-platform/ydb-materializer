package tech.ydb.mv.integration;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.mgt.MvBatchSettings;
import tech.ydb.mv.mgt.MvCommand;
import tech.ydb.mv.mgt.MvJobDao;
import tech.ydb.mv.mgt.MvJobInfo;
import tech.ydb.mv.mgt.MvRunnerInfo;
import tech.ydb.mv.mgt.MvRunnerJobInfo;

/**
 * Comprehensive integration test for MvTableOperations class.
 * Tests all public methods using YDB testcontainer support.
 *
 * @author zinal
 */
public class MvJobDaoTest extends AbstractIntegrationBase {

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
        DROP TABLE `test1/mv_jobs`;
        DROP TABLE `test1/mv_runners`;
        DROP TABLE `test1/mv_runner_jobs`;
        DROP TABLE `test1/mv_commands`;
        """;

    private static YdbConnector ydbConnector;
    private MvJobDao jobDao = null;

    @BeforeAll
    public static void setup() {
        prepareMegatronDb();
    }

    @AfterAll
    public static void cleanup() {
        clearMegatronDb();
        if (ydbConnector != null) {
            ydbConnector.close();
        }
    }

    @BeforeEach
    public void prepareEach() {
        // Clear all tables before each test
        runDdl(ydbConnector, "DELETE FROM `test1/mv_jobs`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_runners`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_runner_jobs`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_commands`");

        jobDao = new MvJobDao(ydbConnector,
                new MvBatchSettings(ydbConnector.getConfig().getProperties()));
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

    // ========== MV_JOBS Operations Tests ==========

    @Test
    public void testGetAllJobsEmpty() {
        List<MvJobInfo> jobs = jobDao.getAllJobs();
        Assertions.assertNotNull(jobs);
        Assertions.assertTrue(jobs.isEmpty());
    }

    @Test
    public void testUpsertAndGetJob() {
        MvJobInfo job = new MvJobInfo("test-job", "{\"key\":\"value\"}", true, "runner-1");

        // Upsert job
        jobDao.upsertJob(job);

        // Get specific job
        MvJobInfo retrieved = jobDao.getJob("test-job");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals(job.getJobName(), retrieved.getJobName());
        Assertions.assertEquals(job.getJobSettings(), retrieved.getJobSettings());
        Assertions.assertEquals(job.isShouldRun(), retrieved.isShouldRun());
        Assertions.assertEquals(job.getRunnerId(), retrieved.getRunnerId());
    }

    @Test
    public void testGetJobNotFound() {
        MvJobInfo retrieved = jobDao.getJob("non-existent-job");
        Assertions.assertNull(retrieved);
    }

    @Test
    public void testUpsertJobWithNullValues() {
        MvJobInfo job = new MvJobInfo("test-job-null", null, false, "runner-99");

        jobDao.upsertJob(job);

        MvJobInfo retrieved = jobDao.getJob("test-job-null");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("test-job-null", retrieved.getJobName());
        // YDB returns empty strings or "null" string instead of actual null for optional fields
        Assertions.assertTrue(retrieved.getJobSettings() == null);
        Assertions.assertFalse(retrieved.isShouldRun());
        Assertions.assertEquals(job.getRunnerId(), retrieved.getRunnerId());
    }

    @Test
    public void testGetAllJobsWithMultipleJobs() {
        MvJobInfo job1 = new MvJobInfo("job1", "{\"key1\":\"value1\"}", true, "runner-1");
        MvJobInfo job2 = new MvJobInfo("job2", "{\"key2\":\"value2\"}", false, "runner-2");
        MvJobInfo job3 = new MvJobInfo("job3", null, true, "runner-1");

        jobDao.upsertJob(job1);
        jobDao.upsertJob(job2);
        jobDao.upsertJob(job3);

        List<MvJobInfo> jobs = jobDao.getAllJobs();
        Assertions.assertNotNull(jobs);
        Assertions.assertEquals(3, jobs.size());

        // Verify all jobs are present
        Assertions.assertTrue(jobs.stream().anyMatch(j -> "job1".equals(j.getJobName())));
        Assertions.assertTrue(jobs.stream().anyMatch(j -> "job2".equals(j.getJobName())));
        Assertions.assertTrue(jobs.stream().anyMatch(j -> "job3".equals(j.getJobName())));
    }

    @Test
    public void testUpdateJob() {
        MvJobInfo job = new MvJobInfo("update-job", "{\"key\":\"value\"}", true, "runner-1");
        jobDao.upsertJob(job);

        // Update the job
        MvJobInfo updatedJob = new MvJobInfo("update-job", "{\"key\":\"updated\"}", false, "runner-2");
        jobDao.upsertJob(updatedJob);

        MvJobInfo retrieved = jobDao.getJob("update-job");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("{\"key\":\"updated\"}", retrieved.getJobSettings());
        Assertions.assertFalse(retrieved.isShouldRun());
        Assertions.assertEquals("runner-2", retrieved.getRunnerId());
    }

    // ========== MV_RUNNERS Operations Tests ==========

    @Test
    public void testGetAllRunnersEmpty() {
        List<MvRunnerInfo> runners = jobDao.getAllRunners();
        Assertions.assertNotNull(runners);
        Assertions.assertTrue(runners.isEmpty());
    }

    @Test
    public void testUpsertAndGetRunner() {
        Instant now = Instant.now();
        MvRunnerInfo runner = new MvRunnerInfo("runner-1", "host-1", now);

        jobDao.upsertRunner(runner);

        List<MvRunnerInfo> runners = jobDao.getAllRunners();
        Assertions.assertNotNull(runners);
        Assertions.assertEquals(1, runners.size());

        MvRunnerInfo retrieved = runners.get(0);
        Assertions.assertEquals(runner.getRunnerId(), retrieved.getRunnerId());
        Assertions.assertEquals(runner.getIdentity(), retrieved.getIdentity());
        // Compare timestamps with some tolerance for precision differences
        Assertions.assertTrue(Math.abs(runner.getUpdatedAt().toEpochMilli() - retrieved.getUpdatedAt().toEpochMilli()) < 1000);
    }

    @Test
    public void testUpsertRunnerWithNullValues() {
        Instant now = Instant.now();
        MvRunnerInfo runner = new MvRunnerInfo("runner-null", null, now);

        jobDao.upsertRunner(runner);

        List<MvRunnerInfo> runners = jobDao.getAllRunners();
        Assertions.assertEquals(1, runners.size());
        Assertions.assertEquals("runner-null", runners.get(0).getRunnerId());
        Assertions.assertEquals(runner.getIdentity(), runners.get(0).getIdentity());
    }

    @Test
    public void testGetAllRunnersWithMultipleRunners() {
        Instant now = Instant.now();
        MvRunnerInfo runner1 = new MvRunnerInfo("runner-1", "host-1", now);
        MvRunnerInfo runner2 = new MvRunnerInfo("runner-2", "host-2", now.plusSeconds(1));
        MvRunnerInfo runner3 = new MvRunnerInfo("runner-3", "host-1", now.plusSeconds(2));

        jobDao.upsertRunner(runner1);
        jobDao.upsertRunner(runner2);
        jobDao.upsertRunner(runner3);

        List<MvRunnerInfo> runners = jobDao.getAllRunners();
        Assertions.assertEquals(3, runners.size());

        Assertions.assertTrue(runners.stream().anyMatch(r -> "runner-1".equals(r.getRunnerId())));
        Assertions.assertTrue(runners.stream().anyMatch(r -> "runner-2".equals(r.getRunnerId())));
        Assertions.assertTrue(runners.stream().anyMatch(r -> "runner-3".equals(r.getRunnerId())));
    }

    @Test
    public void testDeleteRunner() {
        Instant now = Instant.now();
        MvRunnerInfo runner = new MvRunnerInfo("runner-to-delete", "host-1", now);

        jobDao.upsertRunner(runner);
        Assertions.assertEquals(1, jobDao.getAllRunners().size());

        jobDao.deleteRunner("runner-to-delete");
        Assertions.assertEquals(0, jobDao.getAllRunners().size());
    }

    @Test
    public void testDeleteNonExistentRunner() {
        Instant now = Instant.now();
        MvRunnerInfo runner = new MvRunnerInfo("runner-to-delete", "host-1", now);

        jobDao.upsertRunner(runner);
        Assertions.assertEquals(1, jobDao.getAllRunners().size());

        // Should not throw exception
        jobDao.deleteRunner("non-existent-runner");
        Assertions.assertEquals(1, jobDao.getAllRunners().size());
    }

    // ========== MV_RUNNER_JOBS Operations Tests ==========

    @Test
    public void testGetRunnerJobsEmpty() {
        List<MvRunnerJobInfo> jobs = jobDao.getRunnerJobs("runner-1");
        Assertions.assertNotNull(jobs);
        Assertions.assertTrue(jobs.isEmpty());
    }

    @Test
    public void testUpsertAndGetRunnerJob() {
        Instant now = Instant.now();
        MvRunnerJobInfo job = new MvRunnerJobInfo("runner-1", "job-1", "{\"key\":\"value\"}", now);

        jobDao.upsertRunnerJob(job);

        List<MvRunnerJobInfo> jobs = jobDao.getRunnerJobs("runner-1");
        Assertions.assertEquals(1, jobs.size());

        MvRunnerJobInfo retrieved = jobs.get(0);
        Assertions.assertEquals(job.getRunnerId(), retrieved.getRunnerId());
        Assertions.assertEquals(job.getJobName(), retrieved.getJobName());
        Assertions.assertEquals(job.getJobSettings(), retrieved.getJobSettings());
        // Compare timestamps with tolerance for precision differences
        Assertions.assertTrue(Math.abs(job.getStartedAt().toEpochMilli() - retrieved.getStartedAt().toEpochMilli()) < 1000);
    }

    @Test
    public void testUpsertRunnerJobWithNullValues() {
        Instant now = Instant.now();
        MvRunnerJobInfo job = new MvRunnerJobInfo("runner-1", "job-null", null, now);

        jobDao.upsertRunnerJob(job);

        List<MvRunnerJobInfo> jobs = jobDao.getRunnerJobs("runner-1");
        Assertions.assertEquals(1, jobs.size());
        // YDB returns empty strings or "null" string instead of null for optional fields
        Assertions.assertTrue(jobs.get(0).getJobSettings() == null || jobs.get(0).getJobSettings().isEmpty() || "null".equals(jobs.get(0).getJobSettings()));
    }

    @Test
    public void testGetRunnerJobsWithMultipleJobs() {
        Instant now = Instant.now();
        MvRunnerJobInfo job1 = new MvRunnerJobInfo("runner-1", "job-1", "{\"key1\":\"value1\"}", now);
        MvRunnerJobInfo job2 = new MvRunnerJobInfo("runner-1", "job-2", "{\"key2\":\"value2\"}", now.plusSeconds(1));
        MvRunnerJobInfo job3 = new MvRunnerJobInfo("runner-2", "job-3", "{\"key3\":\"value3\"}", now.plusSeconds(2));

        jobDao.upsertRunnerJob(job1);
        jobDao.upsertRunnerJob(job2);
        jobDao.upsertRunnerJob(job3);

        List<MvRunnerJobInfo> runner1Jobs = jobDao.getRunnerJobs("runner-1");
        Assertions.assertEquals(2, runner1Jobs.size());

        List<MvRunnerJobInfo> runner2Jobs = jobDao.getRunnerJobs("runner-2");
        Assertions.assertEquals(1, runner2Jobs.size());
    }

    @Test
    public void testDeleteRunnerJob() {
        Instant now = Instant.now();
        MvRunnerJobInfo job = new MvRunnerJobInfo("runner-1", "job-to-delete", "{\"key\":\"value\"}", now);

        jobDao.upsertRunnerJob(job);
        Assertions.assertEquals(1, jobDao.getRunnerJobs("runner-1").size());

        jobDao.deleteRunnerJob("runner-1", "job-to-delete");
        Assertions.assertEquals(0, jobDao.getRunnerJobs("runner-1").size());
    }

    @Test
    public void testDeleteRunnerJobs() {
        Instant now = Instant.now();
        MvRunnerJobInfo job1 = new MvRunnerJobInfo("runner-1", "job-1", "{\"key1\":\"value1\"}", now);
        MvRunnerJobInfo job2 = new MvRunnerJobInfo("runner-1", "job-2", "{\"key2\":\"value2\"}", now.plusSeconds(1));
        MvRunnerJobInfo job3 = new MvRunnerJobInfo("runner-2", "job-3", "{\"key3\":\"value3\"}", now.plusSeconds(2));

        jobDao.upsertRunnerJob(job1);
        jobDao.upsertRunnerJob(job2);
        jobDao.upsertRunnerJob(job3);

        Assertions.assertEquals(2, jobDao.getRunnerJobs("runner-1").size());
        Assertions.assertEquals(1, jobDao.getRunnerJobs("runner-2").size());

        jobDao.deleteRunnerJobs("runner-1");

        Assertions.assertEquals(0, jobDao.getRunnerJobs("runner-1").size());
        Assertions.assertEquals(1, jobDao.getRunnerJobs("runner-2").size());
    }

    // ========== MV_COMMANDS Operations Tests ==========

    @Test
    public void testGetCommandsForRunnerEmpty() {
        List<MvCommand> commands = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertNotNull(commands);
        Assertions.assertTrue(commands.isEmpty());
    }

    @Test
    public void testCreateAndGetCommand() {
        Instant now = Instant.now();
        MvCommand command = new MvCommand("runner-1", 123L, now, MvCommand.TYPE_START,
                "job-1", "{\"key\":\"value\"}", MvCommand.STATUS_CREATED, "test diag");

        jobDao.createCommand(command);

        List<MvCommand> commands = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(1, commands.size());

        MvCommand retrieved = commands.get(0);
        Assertions.assertEquals(command.getRunnerId(), retrieved.getRunnerId());
        Assertions.assertEquals(command.getCommandNo(), retrieved.getCommandNo());
        // Compare timestamps with tolerance for precision differences
        Assertions.assertTrue(Math.abs(command.getCreatedAt().toEpochMilli() - retrieved.getCreatedAt().toEpochMilli()) < 1000);
        Assertions.assertEquals(command.getCommandType(), retrieved.getCommandType());
        Assertions.assertEquals(command.getJobName(), retrieved.getJobName());
        Assertions.assertEquals(command.getJobSettings(), retrieved.getJobSettings());
        Assertions.assertEquals(command.getCommandStatus(), retrieved.getCommandStatus());
        Assertions.assertEquals(command.getCommandDiag(), retrieved.getCommandDiag());
    }

    @Test
    public void testCreateCommandWithNullValues() {
        Instant now = Instant.now();
        MvCommand command = new MvCommand("runner-1", 123L, now, MvCommand.TYPE_STOP,
                null, null, MvCommand.STATUS_CREATED, null);

        jobDao.createCommand(command);

        List<MvCommand> commands = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(1, commands.size());

        MvCommand retrieved = commands.get(0);
        Assertions.assertEquals(MvCommand.TYPE_STOP, retrieved.getCommandType());
        // YDB returns empty strings or "null" string instead of actual null for optional fields
        Assertions.assertTrue(retrieved.getJobName() == null || retrieved.getJobName().isEmpty());
        Assertions.assertTrue(retrieved.getJobSettings() == null || retrieved.getJobSettings().isEmpty() || "null".equals(retrieved.getJobSettings()));
        Assertions.assertTrue(retrieved.getCommandDiag() == null || retrieved.getCommandDiag().isEmpty());
    }

    @Test
    public void testGetCommandsForRunnerWithMultipleCommands() {
        Instant now = Instant.now();
        MvCommand cmd1 = new MvCommand("runner-1", 1L, now, MvCommand.TYPE_START,
                "job-1", "{\"key1\":\"value1\"}", MvCommand.STATUS_CREATED, "diag1");
        MvCommand cmd2 = new MvCommand("runner-1", 2L, now.plusSeconds(1), MvCommand.TYPE_STOP,
                "job-1", null, MvCommand.STATUS_TAKEN, "diag2");
        MvCommand cmd3 = new MvCommand("runner-2", 3L, now.plusSeconds(2), MvCommand.TYPE_START,
                "job-2", "{\"key2\":\"value2\"}", MvCommand.STATUS_SUCCESS, "diag3");

        jobDao.createCommand(cmd1);
        jobDao.createCommand(cmd2);
        jobDao.createCommand(cmd3);

        List<MvCommand> runner1Commands = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(2, runner1Commands.size());

        List<MvCommand> runner2Commands = jobDao.getCommandsForRunner("runner-2");
        // runner2Commands should be 0 because cmd3 has STATUS_SUCCESS which is not returned by getCommandsForRunner
        Assertions.assertEquals(0, runner2Commands.size());

        // Verify commands are ordered by command_no
        Assertions.assertEquals(1L, runner1Commands.get(0).getCommandNo());
        Assertions.assertEquals(2L, runner1Commands.get(1).getCommandNo());
    }

    @Test
    public void testUpdateCommandStatus() {
        Instant now = Instant.now();
        MvCommand command = new MvCommand("runner-1", 123L, now, MvCommand.TYPE_START,
                "job-1", "{\"key\":\"value\"}", MvCommand.STATUS_CREATED, "initial diag");

        jobDao.createCommand(command);

        // Verify command exists before update
        List<MvCommand> commandsBefore = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(1, commandsBefore.size());
        Assertions.assertEquals(MvCommand.STATUS_CREATED, commandsBefore.get(0).getCommandStatus());

        // Update status
        jobDao.updateCommandStatus("runner-1", 123L, MvCommand.STATUS_SUCCESS, "success diag");

        // After update, command should not be returned by getCommandsForRunner
        // because it only returns CREATED and TAKEN commands
        List<MvCommand> commandsAfter = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(0, commandsAfter.size());
    }

    @Test
    public void testUpdateCommandStatusWithNullDiag() {
        Instant now = Instant.now();
        MvCommand command = new MvCommand("runner-1", 123L, now, MvCommand.TYPE_START,
                "job-1", "{\"key\":\"value\"}", MvCommand.STATUS_CREATED, "initial diag");

        jobDao.createCommand(command);

        // Verify command exists before update
        List<MvCommand> commandsBefore = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(1, commandsBefore.size());
        Assertions.assertEquals(MvCommand.STATUS_CREATED, commandsBefore.get(0).getCommandStatus());

        // Update status with null diag
        jobDao.updateCommandStatus("runner-1", 123L, MvCommand.STATUS_ERROR, null);

        // After update, command should not be returned by getCommandsForRunner
        // because it only returns CREATED and TAKEN commands
        List<MvCommand> commandsAfter = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(0, commandsAfter.size());
    }

    @Test
    public void testGetCommandsForRunnerOnlyReturnsPendingCommands() {
        Instant now = Instant.now();
        MvCommand cmd1 = new MvCommand("runner-1", 1L, now, MvCommand.TYPE_START,
                "job-1", "{\"key1\":\"value1\"}", MvCommand.STATUS_CREATED, "diag1");
        MvCommand cmd2 = new MvCommand("runner-1", 2L, now.plusSeconds(1), MvCommand.TYPE_STOP,
                "job-1", null, MvCommand.STATUS_TAKEN, "diag2");
        MvCommand cmd3 = new MvCommand("runner-1", 3L, now.plusSeconds(2), MvCommand.TYPE_START,
                "job-2", "{\"key2\":\"value2\"}", MvCommand.STATUS_SUCCESS, "diag3");
        MvCommand cmd4 = new MvCommand("runner-1", 4L, now.plusSeconds(3), MvCommand.TYPE_STOP,
                "job-2", null, MvCommand.STATUS_ERROR, "diag4");

        jobDao.createCommand(cmd1);
        jobDao.createCommand(cmd2);
        jobDao.createCommand(cmd3);
        jobDao.createCommand(cmd4);

        List<MvCommand> commands = jobDao.getCommandsForRunner("runner-1");
        // Only CREATED and TAKEN commands should be returned
        Assertions.assertEquals(2, commands.size());
        Assertions.assertTrue(commands.stream().anyMatch(c -> c.getCommandNo() == 1L));
        Assertions.assertTrue(commands.stream().anyMatch(c -> c.getCommandNo() == 2L));
        Assertions.assertFalse(commands.stream().anyMatch(c -> c.getCommandNo() == 3L));
        Assertions.assertFalse(commands.stream().anyMatch(c -> c.getCommandNo() == 4L));
    }

    // ========== Error Handling Tests ==========

    @Test
    public void testOperationsWithInvalidData() {
        // Test with empty job name - this should work as empty string is valid
        MvJobInfo job1 = new MvJobInfo("", "{\"key\":\"value\"}", true, "runner-1");
        jobDao.upsertJob(job1);
        MvJobInfo retrieved1 = jobDao.getJob("");
        Assertions.assertNotNull(retrieved1);
        Assertions.assertEquals("", retrieved1.getJobName());

        // Test with null job name - this should throw an exception
        Assertions.assertThrows(RuntimeException.class, () -> {
            MvJobInfo job = new MvJobInfo(null, "{\"key\":\"value\"}", true, "runner-1");
            jobDao.upsertJob(job);
        });
    }

    @Test
    public void testCommandConstants() {
        // Test command type constants
        Assertions.assertEquals("START", MvCommand.TYPE_START);
        Assertions.assertEquals("STOP", MvCommand.TYPE_STOP);

        // Test command status constants
        Assertions.assertEquals("CREATED", MvCommand.STATUS_CREATED);
        Assertions.assertEquals("TAKEN", MvCommand.STATUS_TAKEN);
        Assertions.assertEquals("SUCCESS", MvCommand.STATUS_SUCCESS);
        Assertions.assertEquals("ERROR", MvCommand.STATUS_ERROR);

        // Test command helper methods
        MvCommand startCmd = new MvCommand("runner-1", 1L, Instant.now(), MvCommand.TYPE_START,
                "job-1", "{}", MvCommand.STATUS_CREATED, "diag");
        MvCommand stopCmd = new MvCommand("runner-1", 2L, Instant.now(), MvCommand.TYPE_STOP,
                "job-1", "{}", MvCommand.STATUS_SUCCESS, "diag");

        Assertions.assertTrue(startCmd.isStartCommand());
        Assertions.assertFalse(startCmd.isStopCommand());
        Assertions.assertTrue(startCmd.isCreated());
        Assertions.assertFalse(startCmd.isSuccess());

        Assertions.assertFalse(stopCmd.isStartCommand());
        Assertions.assertTrue(stopCmd.isStopCommand());
        Assertions.assertFalse(stopCmd.isCreated());
        Assertions.assertTrue(stopCmd.isSuccess());
    }

    @Test
    public void testBatchSettingsIntegration() {
        // Test that MvBatchSettings works correctly with MvTableOperations
        Properties props = new Properties();
        props.setProperty(MvBatchSettings.CONF_MV_JOBS_TABLE, "custom_mv_jobs");
        props.setProperty(MvBatchSettings.CONF_MV_RUNNERS_TABLE, "custom_mv_runners");

        MvBatchSettings customSettings = new MvBatchSettings(props);
        Assertions.assertEquals("custom_mv_jobs", customSettings.getMvJobsTable());
        Assertions.assertEquals("custom_mv_runners", customSettings.getMvRunnersTable());

        // Test full table name generation
        String fullJobsTable = customSettings.getFullMvJobsTable("/test1");
        Assertions.assertEquals("/test1/custom_mv_jobs", fullJobsTable);

        String fullRunnersTable = customSettings.getFullMvRunnersTable("/test1");
        Assertions.assertEquals("/test1/custom_mv_runners", fullRunnersTable);
    }

    @Test
    public void testConcurrentOperations() throws InterruptedException {
        // Test concurrent upserts to the same job
        MvJobInfo job1 = new MvJobInfo("concurrent-job", "{\"key1\":\"value1\"}", true, "runner-1");
        MvJobInfo job2 = new MvJobInfo("concurrent-job", "{\"key2\":\"value2\"}", false, "runner-2");

        Thread thread1 = new Thread(() -> jobDao.upsertJob(job1));
        Thread thread2 = new Thread(() -> jobDao.upsertJob(job2));

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // Verify the job was updated (last write wins)
        MvJobInfo retrieved = jobDao.getJob("concurrent-job");
        Assertions.assertNotNull(retrieved);
        // The exact values depend on which thread finished last, but the job should exist
        Assertions.assertEquals("concurrent-job", retrieved.getJobName());
    }

}
