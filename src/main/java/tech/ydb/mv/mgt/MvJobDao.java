package tech.ydb.mv.mgt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

import tech.ydb.mv.YdbConnector;

/**
 * Database operations for the distributed job management system. Handles all
 * YDB table interactions for mv_jobs, mv_runners, mv_runner_jobs, and
 * mv_commands tables.
 *
 * @author zinal
 */
public class MvJobDao {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvJobDao.class);

    private final YdbConnector ydb;
    private final AtomicLong commandNo = new AtomicLong(System.currentTimeMillis());

    // Pre-generated SQL statements
    private final String sqlGetAllJobs;
    private final String sqlGetJob;
    private final String sqlUpsertJob;
    private final String sqlUpsertRunner;
    private final String sqlGetAllRunners;
    private final String sqlDeleteRunner;
    private final String sqlUpsertRunnerJob;
    private final String sqlGetRunnerJobs;
    private final String sqlDeleteRunnerJob;
    private final String sqlDeleteRunnerJobs;
    private final String sqlCreateCommand;
    private final String sqlGetCommandsForRunner;
    private final String sqlUpdateCommandStatus;

    public MvJobDao(YdbConnector ydb, MvBatchSettings settings) {
        this.ydb = ydb;

        // Generate all SQL statements in constructor
        String mvJobsTable = settings.getFullMvJobsTable(ydb.getDatabase());
        String mvRunnersTable = settings.getFullMvRunnersTable(ydb.getDatabase());
        String mvRunnerJobsTable = settings.getFullMvRunnerJobsTable(ydb.getDatabase());
        String mvCommandsTable = settings.getFullMvCommandsTable(ydb.getDatabase());

        // MV_JOBS SQL statements
        this.sqlGetAllJobs = String.format("SELECT * FROM `%s`", mvJobsTable);
        this.sqlGetJob = String.format(
                "DECLARE $job_name AS Text;\n"
                + "SELECT * FROM `%s` WHERE job_name = $job_name", mvJobsTable
        );
        this.sqlUpsertJob = String.format(
                "DECLARE $job_name AS Text;\n"
                + "DECLARE $job_settings AS JsonDocument?;\n"
                + "DECLARE $should_run AS Bool;\n"
                + "DECLARE $runner_id AS Text;\n"
                + "UPSERT INTO `%s` (job_name, job_settings, should_run, runner_id) "
                + "VALUES ($job_name, $job_settings, $should_run, $runner_id)",
                mvJobsTable
        );

        // MV_RUNNERS SQL statements
        this.sqlUpsertRunner = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "DECLARE $runner_identity AS Text;\n"
                + "DECLARE $updated_at AS Timestamp;\n"
                + "UPSERT INTO `%s` (runner_id, runner_identity, updated_at) "
                + "VALUES ($runner_id, $runner_identity, $updated_at)",
                mvRunnersTable
        );
        this.sqlGetAllRunners = String.format("SELECT * FROM `%s`", mvRunnersTable);
        this.sqlDeleteRunner = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "DELETE FROM `%s` WHERE runner_id = $runner_id", mvRunnersTable
        );

        // MV_RUNNER_JOBS SQL statements
        this.sqlUpsertRunnerJob = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "DECLARE $job_name AS Text;\n"
                + "DECLARE $job_settings AS JsonDocument?;\n"
                + "DECLARE $started_at AS Timestamp;\n"
                + "UPSERT INTO `%s` (runner_id, job_name, job_settings, started_at) "
                + "VALUES ($runner_id, $job_name, $job_settings, $started_at)",
                mvRunnerJobsTable
        );
        this.sqlGetRunnerJobs = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "SELECT * FROM `%s` WHERE runner_id = $runner_id", mvRunnerJobsTable
        );
        this.sqlDeleteRunnerJob = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "DECLARE $job_name AS Text;\n"
                + "DELETE FROM `%s` "
                + "WHERE runner_id = $runner_id "
                + "AND job_name = $job_name", mvRunnerJobsTable
        );
        this.sqlDeleteRunnerJobs = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "DELETE FROM `%s` WHERE runner_id = $runner_id", mvRunnerJobsTable
        );

        // MV_COMMANDS SQL statements
        this.sqlCreateCommand = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "DECLARE $command_no AS Uint64;\n"
                + "DECLARE $created_at AS Timestamp;\n"
                + "DECLARE $command_type AS Text;\n"
                + "DECLARE $job_name AS Text;\n"
                + "DECLARE $job_settings AS JsonDocument?;\n"
                + "DECLARE $command_status AS Text;\n"
                + "DECLARE $command_diag AS Text?;\n"
                + "INSERT INTO `%s` (runner_id, command_no, created_at, command_type, "
                + "job_name, job_settings, command_status, command_diag) "
                + "VALUES ($runner_id, $command_no, $created_at, $command_type, "
                + "$job_name, $job_settings, $command_status, $command_diag)",
                mvCommandsTable
        );
        this.sqlGetCommandsForRunner = String.format(
                "DECLARE $runner_id AS Text;\n"
                + "SELECT * FROM `%s` WHERE runner_id = $runner_id AND command_status "
                + "IN ('CREATED'u, 'TAKEN'u) ORDER BY command_no",
                mvCommandsTable
        );
        this.sqlUpdateCommandStatus = String.format(
                "DECLARE $command_status AS Text;\n"
                + "DECLARE $command_diag AS Text?;\n"
                + "DECLARE $runner_id AS Text;\n"
                + "DECLARE $command_no AS Uint64;\n"
                + "UPDATE `%s` SET command_status = $command_status, "
                + "command_diag = $command_diag "
                + "WHERE runner_id = $runner_id AND command_no = $command_no",
                mvCommandsTable
        );
    }

    // MV_JOBS operations
    public List<MvJobInfo> getAllJobs() {
        try {
            QueryReader reader = ydb.sqlRead(sqlGetAllJobs, Params.empty());
            ResultSetReader resultSet = reader.getResultSet(0);
            List<MvJobInfo> jobs = new ArrayList<>();
            while (resultSet.next()) {
                jobs.add(parseJobInfo(resultSet));
            }
            return jobs;
        } catch (Exception ex) {
            LOG.error("Failed to get all jobs", ex);
            throw new RuntimeException("Failed to get all jobs", ex);
        }
    }

    public MvJobInfo getJob(String jobName) {
        try {
            QueryReader reader = ydb.sqlRead(sqlGetJob, Params.of("$job_name", PrimitiveValue.newText(jobName)));
            ResultSetReader resultSet = reader.getResultSet(0);
            if (resultSet.next()) {
                return parseJobInfo(resultSet);
            }
            return null;
        } catch (Exception ex) {
            LOG.error("Failed to get job {}", jobName, ex);
            throw new RuntimeException("Failed to get job " + jobName, ex);
        }
    }

    public void upsertJob(MvJobInfo job) {
        try {
            ydb.sqlWrite(sqlUpsertJob, Params.of(
                    "$job_name", PrimitiveValue.newText(job.getJobName()),
                    "$job_settings", job.getJobSettings() != null
                    ? PrimitiveValue.newJsonDocument(job.getJobSettings()).makeOptional()
                    : PrimitiveType.JsonDocument.makeOptional().emptyValue(),
                    "$should_run", PrimitiveValue.newBool(job.isShouldRun()),
                    "$runner_id", PrimitiveValue.newText(job.getRunnerId())
            ));
        } catch (Exception ex) {
            LOG.error("Failed to upsert job {}", job.getJobName(), ex);
            throw new RuntimeException("Failed to upsert job " + job.getJobName(), ex);
        }
    }

    // MV_RUNNERS operations
    public void upsertRunner(MvRunnerInfo runner) {
        try {
            ydb.sqlWrite(sqlUpsertRunner, Params.of(
                    "$runner_id", PrimitiveValue.newText(runner.getRunnerId()),
                    "$runner_identity", PrimitiveValue.newText(runner.getIdentity()),
                    "$updated_at", PrimitiveValue.newTimestamp(runner.getUpdatedAt())
            ));
        } catch (Exception ex) {
            LOG.error("Failed to upsert runner {}", runner.getRunnerId(), ex);
            throw new RuntimeException("Failed to upsert runner " + runner.getRunnerId(), ex);
        }
    }

    public List<MvRunnerInfo> getAllRunners() {
        try {
            QueryReader reader = ydb.sqlRead(sqlGetAllRunners, Params.empty());
            ResultSetReader resultSet = reader.getResultSet(0);
            List<MvRunnerInfo> runners = new ArrayList<>();
            while (resultSet.next()) {
                runners.add(parseRunnerInfo(resultSet));
            }
            return runners;
        } catch (Exception ex) {
            LOG.error("Failed to get all runners", ex);
            throw new RuntimeException("Failed to get all runners", ex);
        }
    }

    public void deleteRunner(String runnerId) {
        try {
            ydb.sqlWrite(sqlDeleteRunner, Params.of("$runner_id", PrimitiveValue.newText(runnerId)));
        } catch (Exception ex) {
            LOG.error("Failed to delete runner {}", runnerId, ex);
            throw new RuntimeException("Failed to delete runner " + runnerId, ex);
        }
    }

    // MV_RUNNER_JOBS operations
    public void upsertRunnerJob(MvRunnerJobInfo jobInfo) {
        try {
            ydb.sqlWrite(sqlUpsertRunnerJob, Params.of(
                    "$runner_id", PrimitiveValue.newText(jobInfo.getRunnerId()),
                    "$job_name", PrimitiveValue.newText(jobInfo.getJobName()),
                    "$job_settings", jobInfo.getJobSettings() != null
                    ? PrimitiveValue.newJsonDocument(jobInfo.getJobSettings()).makeOptional()
                    : PrimitiveType.JsonDocument.makeOptional().emptyValue(),
                    "$started_at", PrimitiveValue.newTimestamp(jobInfo.getStartedAt())
            ));
        } catch (Exception ex) {
            LOG.error("Failed to upsert runner job {}/{}", jobInfo.getRunnerId(), jobInfo.getJobName(), ex);
            throw new RuntimeException("Failed to upsert runner job", ex);
        }
    }

    public List<MvRunnerJobInfo> getRunnerJobs(String runnerId) {
        try {
            QueryReader reader = ydb.sqlRead(sqlGetRunnerJobs, Params.of(
                    "$runner_id", PrimitiveValue.newText(runnerId)));
            ResultSetReader resultSet = reader.getResultSet(0);
            List<MvRunnerJobInfo> jobs = new ArrayList<>();
            while (resultSet.next()) {
                jobs.add(parseRunnerJobInfo(resultSet));
            }
            return jobs;
        } catch (Exception ex) {
            LOG.error("Failed to get runner jobs for {}", runnerId, ex);
            throw new RuntimeException("Failed to get runner jobs for " + runnerId, ex);
        }
    }

    public void deleteRunnerJob(String runnerId, String jobName) {
        try {
            ydb.sqlWrite(sqlDeleteRunnerJob, Params.of(
                    "$runner_id", PrimitiveValue.newText(runnerId),
                    "$job_name", PrimitiveValue.newText(jobName)
            ));
        } catch (Exception ex) {
            LOG.error("Failed to delete runner job {}/{}", runnerId, jobName, ex);
            throw new RuntimeException("Failed to delete runner job", ex);
        }
    }

    public void deleteRunnerJobs(String runnerId) {
        try {
            ydb.sqlWrite(sqlDeleteRunnerJobs, Params.of("$runner_id", PrimitiveValue.newText(runnerId)));
        } catch (Exception ex) {
            LOG.error("Failed to delete runner jobs for {}", runnerId, ex);
            throw new RuntimeException("Failed to delete runner jobs for " + runnerId, ex);
        }
    }

    // MV_COMMANDS operations
    public void createCommand(MvCommand command) {
        try {
            ydb.sqlWrite(sqlCreateCommand, Params.of(
                    "$runner_id", PrimitiveValue.newText(command.getRunnerId()),
                    "$command_no", PrimitiveValue.newUint64(command.getCommandNo()),
                    "$created_at", PrimitiveValue.newTimestamp(command.getCreatedAt()),
                    "$command_type", PrimitiveValue.newText(command.getCommandType()),
                    "$job_name", command.getJobName() != null
                    ? PrimitiveValue.newText(command.getJobName())
                    : PrimitiveValue.newText(""),
                    "$job_settings", command.getJobSettings() != null
                    ? PrimitiveValue.newJsonDocument(command.getJobSettings()).makeOptional()
                    : PrimitiveType.JsonDocument.makeOptional().emptyValue(),
                    "$command_status", PrimitiveValue.newText(command.getCommandStatus()),
                    "$command_diag", command.getCommandDiag() != null
                    ? PrimitiveValue.newText(command.getCommandDiag()).makeOptional()
                    : PrimitiveType.Text.makeOptional().emptyValue()
            ));
        } catch (Exception ex) {
            LOG.error("Failed to create command {}/{}", command.getRunnerId(), command.getCommandNo(), ex);
            throw new RuntimeException("Failed to create command", ex);
        }
    }

    public List<MvCommand> getCommandsForRunner(String runnerId) {
        try {
            QueryReader reader = ydb.sqlRead(sqlGetCommandsForRunner,
                    Params.of("$runner_id", PrimitiveValue.newText(runnerId)));
            ResultSetReader resultSet = reader.getResultSet(0);
            List<MvCommand> commands = new ArrayList<>();
            while (resultSet.next()) {
                commands.add(parseCommandInfo(resultSet));
            }
            return commands;
        } catch (Exception ex) {
            LOG.error("Failed to get commands for runner {}", runnerId, ex);
            throw new RuntimeException("Failed to get commands for runner " + runnerId, ex);
        }
    }

    public void updateCommandStatus(String runnerId, long commandNo, String status, String diag) {
        try {
            ydb.sqlWrite(sqlUpdateCommandStatus, Params.of(
                    "$command_status", PrimitiveValue.newText(status),
                    "$command_diag", diag != null ?
                            PrimitiveValue.newText(diag).makeOptional() :
                            PrimitiveType.Text.makeOptional().emptyValue(),
                    "$runner_id", PrimitiveValue.newText(runnerId),
                    "$command_no", PrimitiveValue.newUint64(commandNo)
            ));
        } catch (Exception ex) {
            LOG.error("Failed to update command status {}/{}", runnerId, commandNo, ex);
            throw new RuntimeException("Failed to update command status", ex);
        }
    }

    // Helper methods for parsing query results
    private MvJobInfo parseJobInfo(ResultSetReader reader) {
        return new MvJobInfo(
                reader.getColumn("job_name").getText(),
                getJsonDocument(reader, "job_settings"),
                reader.getColumn("should_run").getBool(),
                reader.getColumn("runner_id").getText()
        );
    }

    private MvRunnerInfo parseRunnerInfo(ResultSetReader reader) {
        return new MvRunnerInfo(
                reader.getColumn("runner_id").getText(),
                reader.getColumn("runner_identity").getText(),
                reader.getColumn("updated_at").getTimestamp()
        );
    }

    private MvRunnerJobInfo parseRunnerJobInfo(ResultSetReader reader) {
        return new MvRunnerJobInfo(
                reader.getColumn("runner_id").getText(),
                reader.getColumn("job_name").getText(),
                getJsonDocument(reader, "job_settings"),
                reader.getColumn("started_at").getTimestamp()
        );
    }

    private MvCommand parseCommandInfo(ResultSetReader reader) {
        return new MvCommand(
                reader.getColumn("runner_id").getText(),
                reader.getColumn("command_no").getUint64(),
                reader.getColumn("created_at").getTimestamp(),
                reader.getColumn("command_type").getText(),
                reader.getColumn("job_name").getText(),
                getJsonDocument(reader, "job_settings"),
                reader.getColumn("command_status").getText(),
                getText(reader, "command_diag")
        );
    }

    private static String getText(ResultSetReader reader, String column) {
        var c = reader.getColumn(column);
        if (c.isOptionalItemPresent()) {
            return c.getText();
        }
        return null;
    }

    private static String getJsonDocument(ResultSetReader reader, String column) {
        var c = reader.getColumn(column);
        if (c.isOptionalItemPresent()) {
            return c.getJsonDocument();
        }
        return null;
    }

    /**
     * Generate a unique command number for a runner.
     */
    public long generateCommandNo() {
        return commandNo.incrementAndGet();
    }
}
