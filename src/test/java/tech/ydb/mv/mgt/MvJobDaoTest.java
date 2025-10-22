package tech.ydb.mv.mgt;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for MvJobDao class.
 *
 * @author zinal
 */
public class MvJobDaoTest extends MgmtTestBase {

    @BeforeAll
    public static void setup() {
        prepareMgtDb();
    }

    @AfterAll
    public static void cleanup() {
        clearMgtDb();
    }

    @BeforeEach
    public void prepareEach() {
        refreshBeforeRun();
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
        MvJobInfo job = new MvJobInfo("test-job", "{\"key\":\"value\"}", true);

        // Upsert job
        jobDao.upsertJob(job);

        // Get specific job
        MvJobInfo retrieved = jobDao.getJob("test-job");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals(job.getJobName(), retrieved.getJobName());
        Assertions.assertEquals(job.getJobSettings(), retrieved.getJobSettings());
        Assertions.assertEquals(job.isShouldRun(), retrieved.isShouldRun());
    }

    @Test
    public void testGetJobNotFound() {
        MvJobInfo retrieved = jobDao.getJob("non-existent-job");
        Assertions.assertNull(retrieved);
    }

    @Test
    public void testUpsertJobWithNullValues() {
        MvJobInfo job = new MvJobInfo("test-job-null", null, false);

        jobDao.upsertJob(job);

        MvJobInfo retrieved = jobDao.getJob("test-job-null");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("test-job-null", retrieved.getJobName());
        // YDB returns empty strings or "null" string instead of actual null for optional fields
        Assertions.assertTrue(retrieved.getJobSettings() == null);
        Assertions.assertFalse(retrieved.isShouldRun());
    }

    @Test
    public void testGetAllJobsWithMultipleJobs() {
        MvJobInfo job1 = new MvJobInfo("job1", "{\"key1\":\"value1\"}", true);
        MvJobInfo job2 = new MvJobInfo("job2", "{\"key2\":\"value2\"}", false);
        MvJobInfo job3 = new MvJobInfo("job3", null, true);

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
        MvJobInfo job = new MvJobInfo("update-job", "{\"key\":\"value\"}", true);
        jobDao.upsertJob(job);

        // Update the job
        MvJobInfo updatedJob = new MvJobInfo("update-job", "{\"key\":\"updated\"}", false);
        jobDao.upsertJob(updatedJob);

        MvJobInfo retrieved = jobDao.getJob("update-job");
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("{\"key\":\"updated\"}", retrieved.getJobSettings());
        Assertions.assertFalse(retrieved.isShouldRun());
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
                "job-1", "{\"key1\":\"value1\"}", MvCommand.STATUS_CREATED, null);
        MvCommand cmd2 = new MvCommand("runner-1", 2L, now.plusSeconds(1), MvCommand.TYPE_STOP,
                "job-1", null, MvCommand.STATUS_TAKEN, null);
        MvCommand cmd3 = new MvCommand("runner-1", 3L, now.plusSeconds(1), MvCommand.TYPE_STOP,
                "job-2", null, MvCommand.STATUS_CREATED, null);
        MvCommand cmd4 = new MvCommand("runner-2", 4L, now.plusSeconds(2), MvCommand.TYPE_START,
                "job-3", "{\"key2\":\"value2\"}", MvCommand.STATUS_SUCCESS, "diag3");

        jobDao.createCommand(cmd1);
        jobDao.createCommand(cmd2);
        jobDao.createCommand(cmd3);
        jobDao.createCommand(cmd4);

        List<MvCommand> runner1Commands = jobDao.getCommandsForRunner("runner-1");
        Assertions.assertEquals(2, runner1Commands.size());

        List<MvCommand> runner2Commands = jobDao.getCommandsForRunner("runner-2");
        // runner2Commands should be 0 because cmd3 has STATUS_SUCCESS which is not returned by getCommandsForRunner
        Assertions.assertEquals(0, runner2Commands.size());

        // Verify commands are ordered by command_no
        Assertions.assertEquals(1L, runner1Commands.get(0).getCommandNo());
        Assertions.assertEquals(3L, runner1Commands.get(1).getCommandNo());
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
        long maxNo = jobDao.getMaxCommandNo();
        Assertions.assertEquals(0L, maxNo);

        Instant now = Instant.now();
        MvCommand cmd1 = new MvCommand("runner-1", 1L, now, MvCommand.TYPE_START,
                "job-1", "{\"key1\":\"value1\"}", MvCommand.STATUS_CREATED, "diag1");
        MvCommand cmd2 = new MvCommand("runner-1", 2L, now.plusSeconds(1), MvCommand.TYPE_STOP,
                "job-1", null, MvCommand.STATUS_TAKEN, "diag2");
        MvCommand cmd3 = new MvCommand("runner-1", 3L, now.plusSeconds(2), MvCommand.TYPE_START,
                "job-2", "{\"key2\":\"value2\"}", MvCommand.STATUS_CREATED, "diag3");
        MvCommand cmd4 = new MvCommand("runner-1", 4L, now.plusSeconds(3), MvCommand.TYPE_STOP,
                "job-2", null, MvCommand.STATUS_ERROR, "diag4");

        jobDao.createCommand(cmd1);
        jobDao.createCommand(cmd2);
        jobDao.createCommand(cmd3);
        jobDao.createCommand(cmd4);

        List<MvCommand> commands = jobDao.getCommandsForRunner("runner-1");
        // Only CREATED commands should be returned
        Assertions.assertEquals(2, commands.size());
        Assertions.assertTrue(commands.stream().anyMatch(c -> c.getCommandNo() == 1L));
        Assertions.assertTrue(commands.stream().anyMatch(c -> c.getCommandNo() == 3L));
        Assertions.assertFalse(commands.stream().anyMatch(c -> c.getCommandNo() == 2L));
        Assertions.assertFalse(commands.stream().anyMatch(c -> c.getCommandNo() == 4L));

        maxNo = jobDao.getMaxCommandNo();
        Assertions.assertEquals(4L, maxNo);
    }

    // ========== Error Handling Tests ==========
    @Test
    public void testOperationsWithInvalidData() {
        // Test with empty job name - this should work as empty string is valid
        MvJobInfo job1 = new MvJobInfo("", "{\"key\":\"value\"}", true);
        jobDao.upsertJob(job1);
        MvJobInfo retrieved1 = jobDao.getJob("");
        Assertions.assertNotNull(retrieved1);
        Assertions.assertEquals("", retrieved1.getJobName());

        // Test with null job name - this should throw an exception
        Assertions.assertThrows(RuntimeException.class, () -> {
            MvJobInfo job = new MvJobInfo(null, "{\"key\":\"value\"}", true);
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
        props.setProperty(MvBatchSettings.CONF_TABLE_JOBS, "custom_mv_jobs");
        props.setProperty(MvBatchSettings.CONF_TABLE_RUNNERS, "custom_mv_runners");
        props.setProperty(MvBatchSettings.CONF_TABLE_RUNNER_JOBS, "custom_mv_runner_jobs");
        props.setProperty(MvBatchSettings.CONF_TABLE_COMMANDS, "custom_mv_commands");

        MvBatchSettings customSettings = new MvBatchSettings(props);
        Assertions.assertEquals("custom_mv_jobs", customSettings.getTableJobs());
        Assertions.assertEquals("custom_mv_runners", customSettings.getTableRunners());
        Assertions.assertEquals("custom_mv_runner_jobs", customSettings.getTableRunnerJobs());
        Assertions.assertEquals("custom_mv_commands", customSettings.getTableCommands());
    }

}
