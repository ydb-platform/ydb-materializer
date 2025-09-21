package tech.ydb.mv.mgt;

import java.util.ArrayList;
import java.util.List;

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

    // Pre-generated SQL statements
    private final String sqlGetAllJobs;
    private final String sqlGetJob;
    private final String sqlUpsertJob;
    private final String sqlUpsertRunner;
    private final String sqlGetAllRunners;
    private final String sqlDeleteRunner;
    private final String sqlUpsertRunnerJob;
    private final String sqlGetRunnerJobs;
    private final String sqlGetJobRunners;
    private final String sqlGetAllRunnerJobs;
    private final String sqlDeleteRunnerJob;
    private final String sqlDeleteRunnerJobs;
    private final String sqlCreateCommand;
    private final String sqlGetCommandsForRunner;
    private final String sqlUpdateCommandStatus;
    private final String sqlMaxCommandNo;

    public MvJobDao(YdbConnector ydb, MvBatchSettings settings) {
        this.ydb = ydb;

        String tabJobs = settings.getTableJobs();
        String tabRunners = settings.getTableRunners();
        String tabRunnerJobs = settings.getTableRunnerJobs();
        String tabCommands = settings.getTableCommands();

        // MV_JOBS SQL statements
        this.sqlGetAllJobs = String.format("""
            SELECT job_name, job_settings, should_run FROM `%s`;
            """, tabJobs);
        this.sqlGetJob = String.format("""
            DECLARE $job_name AS Text;
            SELECT job_name, job_settings, should_run FROM `%s`
                WHERE job_name = $job_name;
            """, tabJobs
        );
        this.sqlUpsertJob = String.format("""
            DECLARE $job_name AS Text;
            DECLARE $job_settings AS JsonDocument?;
            DECLARE $should_run AS Bool;
            UPSERT INTO `%s` (job_name, job_settings, should_run)
            VALUES ($job_name, $job_settings, $should_run);
            """, tabJobs
        );

        // MV_RUNNERS SQL statements
        this.sqlUpsertRunner = String.format("""
            DECLARE $runner_id AS Text;
            DECLARE $runner_identity AS Text;
            DECLARE $updated_at AS Timestamp;
            UPSERT INTO `%s` (runner_id, runner_identity, updated_at)
            VALUES ($runner_id, $runner_identity, $updated_at);
            """, tabRunners
        );
        this.sqlGetAllRunners = String.format("""
            SELECT runner_id, runner_identity, updated_at FROM `%s`
            """, tabRunners);
        this.sqlDeleteRunner = String.format("""
            DECLARE $runner_id AS Text;
            DELETE FROM `%s` WHERE runner_id = $runner_id;
            """, tabRunners
        );

        // MV_RUNNER_JOBS SQL statements
        this.sqlUpsertRunnerJob = String.format("""
            DECLARE $runner_id AS Text;
            DECLARE $job_name AS Text;
            DECLARE $job_settings AS JsonDocument?;
            DECLARE $started_at AS Timestamp;
            UPSERT INTO `%s` (runner_id, job_name, job_settings, started_at)
            VALUES ($runner_id, $job_name, $job_settings, $started_at);
            """, tabRunnerJobs
        );
        this.sqlGetRunnerJobs = String.format("""
            DECLARE $runner_id AS Text;
            SELECT runner_id, job_name, job_settings, started_at FROM `%s`
                WHERE runner_id = $runner_id;
            """, tabRunnerJobs
        );
        this.sqlGetJobRunners = String.format("""
            DECLARE $job_name AS Text;
            SELECT runner_id, job_name, job_settings, started_at
            FROM `%s` VIEW ix_job_name
            WHERE job_name = $job_name;
            """, tabRunnerJobs
        );
        this.sqlGetAllRunnerJobs = String.format("""
            DECLARE $runner_id AS Text;
            SELECT runner_id, job_name, job_settings, started_at FROM `%s`;
            """, tabRunnerJobs
        );
        this.sqlDeleteRunnerJob = String.format("""
            DECLARE $runner_id AS Text;
            DECLARE $job_name AS Text;
            DELETE FROM `%s` WHERE runner_id = $runner_id AND job_name = $job_name;
            """, tabRunnerJobs
        );
        this.sqlDeleteRunnerJobs = String.format("""
            DECLARE $runner_id AS Text;
            DELETE FROM `%s` WHERE runner_id = $runner_id
            """, tabRunnerJobs
        );

        // MV_COMMANDS SQL statements
        this.sqlCreateCommand = String.format("""
            DECLARE $runner_id AS Text;
            DECLARE $command_no AS Uint64;
            DECLARE $created_at AS Timestamp;
            DECLARE $command_type AS Text;
            DECLARE $job_name AS Text;
            DECLARE $job_settings AS JsonDocument?;
            DECLARE $command_status AS Text;
            DECLARE $command_diag AS Text?;
            INSERT INTO `%s` (runner_id, command_no, created_at, command_type,
                job_name, job_settings, command_status, command_diag)
            VALUES ($runner_id, $command_no, $created_at, $command_type,
                $job_name, $job_settings, $command_status, $command_diag);
            """, tabCommands
        );
        this.sqlGetCommandsForRunner = String.format("""
            DECLARE $runner_id AS Text;
            SELECT runner_id, command_no, created_at, command_type,
                   job_name, job_settings, command_status, command_diag
            FROM `%s`
            WHERE runner_id = $runner_id AND command_status = 'CREATED'u
            ORDER BY runner_id, command_no;
            """, tabCommands
        );
        this.sqlUpdateCommandStatus = String.format("""
            DECLARE $command_status AS Text;
            DECLARE $command_diag AS Text?;
            DECLARE $runner_id AS Text;
            DECLARE $command_no AS Uint64;
            UPDATE `%s`
                SET command_status = $command_status, command_diag = $command_diag
                WHERE runner_id = $runner_id AND command_no = $command_no;
            """, tabCommands
        );
        this.sqlMaxCommandNo = String.format("""
            SELECT command_no FROM `%s` VIEW ix_command_no
            ORDER BY command_no DESC LIMIT 1;
            """, tabCommands);
    }

    public long getMaxCommandNo() {
        var rs = ydb.sqlRead(sqlMaxCommandNo, Params.empty()).getResultSet(0);
        return rs.next() ? rs.getColumn(0).getUint64() : 0L;
    }

    // MV_JOBS operations
    public List<MvJobInfo> getAllJobs() {
        var rs = ydb.sqlRead(sqlGetAllJobs, Params.empty()).getResultSet(0);
        List<MvJobInfo> jobs = new ArrayList<>();
        while (rs.next()) {
            jobs.add(parseJobInfo(rs));
        }
        return jobs;
    }

    public MvJobInfo getJob(String jobName) {
        var rs = ydb.sqlRead(sqlGetJob, Params.of(
                "$job_name", PrimitiveValue.newText(jobName))
        ).getResultSet(0);
        if (rs.next()) {
            return parseJobInfo(rs);
        }
        return null;
    }

    public void upsertJob(MvJobInfo job) {
        ydb.sqlWrite(sqlUpsertJob, Params.of(
                "$job_name", PrimitiveValue.newText(job.getJobName()),
                "$job_settings", job.getJobSettings() != null
                ? PrimitiveValue.newJsonDocument(job.getJobSettings()).makeOptional()
                : PrimitiveType.JsonDocument.makeOptional().emptyValue(),
                "$should_run", PrimitiveValue.newBool(job.isShouldRun())
        ));
    }

    // MV_RUNNERS operations
    public void upsertRunner(MvRunnerInfo runner) {
        ydb.sqlWrite(sqlUpsertRunner, Params.of(
                "$runner_id", PrimitiveValue.newText(runner.getRunnerId()),
                "$runner_identity", PrimitiveValue.newText(runner.getIdentity()),
                "$updated_at", PrimitiveValue.newTimestamp(runner.getUpdatedAt())
        ));
    }

    public List<MvRunnerInfo> getAllRunners() {
        var rs = ydb.sqlRead(sqlGetAllRunners, Params.empty()).getResultSet(0);
        List<MvRunnerInfo> runners = new ArrayList<>();
        while (rs.next()) {
            runners.add(parseRunnerInfo(rs));
        }
        return runners;
    }

    public void deleteRunner(String runnerId) {
        ydb.sqlWrite(sqlDeleteRunner, Params.of(
                "$runner_id", PrimitiveValue.newText(runnerId)
        ));
    }

    // MV_RUNNER_JOBS operations
    public void upsertRunnerJob(MvRunnerJobInfo jobInfo) {
        ydb.sqlWrite(sqlUpsertRunnerJob, Params.of(
                "$runner_id", PrimitiveValue.newText(jobInfo.getRunnerId()),
                "$job_name", PrimitiveValue.newText(jobInfo.getJobName()),
                "$job_settings", jobInfo.getJobSettings() != null
                ? PrimitiveValue.newJsonDocument(jobInfo.getJobSettings()).makeOptional()
                : PrimitiveType.JsonDocument.makeOptional().emptyValue(),
                "$started_at", PrimitiveValue.newTimestamp(jobInfo.getStartedAt())
        ));
    }

    public List<MvRunnerJobInfo> getRunnerJobs(String runnerId) {
        var rs = ydb.sqlRead(sqlGetRunnerJobs, Params.of(
                "$runner_id", PrimitiveValue.newText(runnerId)
        )).getResultSet(0);
        List<MvRunnerJobInfo> jobs = new ArrayList<>();
        while (rs.next()) {
            jobs.add(parseRunnerJobInfo(rs));
        }
        return jobs;
    }

    public List<MvRunnerJobInfo> getJobRunners(String jobName) {
        var rs = ydb.sqlRead(sqlGetJobRunners, Params.of(
                "$job_name", PrimitiveValue.newText(jobName)
        )).getResultSet(0);
        List<MvRunnerJobInfo> jobs = new ArrayList<>();
        while (rs.next()) {
            jobs.add(parseRunnerJobInfo(rs));
        }
        return jobs;
    }

    public List<MvRunnerJobInfo> getAllRunnerJobs() {
        var rs = ydb.sqlRead(sqlGetAllRunnerJobs, Params.empty()).getResultSet(0);
        List<MvRunnerJobInfo> jobs = new ArrayList<>();
        while (rs.next()) {
            jobs.add(parseRunnerJobInfo(rs));
        }
        return jobs;
    }

    public void deleteRunnerJob(String runnerId, String jobName) {
        ydb.sqlWrite(sqlDeleteRunnerJob, Params.of(
                "$runner_id", PrimitiveValue.newText(runnerId),
                "$job_name", PrimitiveValue.newText(jobName)
        ));
    }

    public void deleteRunnerJobs(String runnerId) {
        ydb.sqlWrite(sqlDeleteRunnerJobs, Params.of(
                "$runner_id", PrimitiveValue.newText(runnerId)
        ));
    }

    // MV_COMMANDS operations
    public void createCommand(MvCommand command) {
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
    }

    public List<MvCommand> getCommandsForRunner(String runnerId) {
        var rs = ydb.sqlRead(sqlGetCommandsForRunner, Params.of(
                "$runner_id", PrimitiveValue.newText(runnerId)
        )).getResultSet(0);
        List<MvCommand> commands = new ArrayList<>();
        while (rs.next()) {
            commands.add(parseCommandInfo(rs));
        }
        return commands;
    }

    public void updateCommandStatus(String runnerId, long commandNo, String status, String diag) {
        ydb.sqlWrite(sqlUpdateCommandStatus, Params.of(
                "$command_status", PrimitiveValue.newText(status),
                "$command_diag", diag != null
                        ? PrimitiveValue.newText(diag).makeOptional()
                        : PrimitiveType.Text.makeOptional().emptyValue(),
                "$runner_id", PrimitiveValue.newText(runnerId),
                "$command_no", PrimitiveValue.newUint64(commandNo)
        ));
    }

    private MvJobInfo parseJobInfo(ResultSetReader reader) {
        return new MvJobInfo(
                reader.getColumn("job_name").getText(),
                getJsonDocument(reader, "job_settings"),
                reader.getColumn("should_run").getBool()
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
}
