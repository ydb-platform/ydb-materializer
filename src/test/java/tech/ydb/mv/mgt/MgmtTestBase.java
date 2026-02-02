package tech.ydb.mv.mgt;

import java.util.Properties;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.YdbConnector;

/**
 *
 * @author zinal
 */
public abstract class MgmtTestBase extends AbstractIntegrationBase {

    private static final String CREATE_MGT_TABLES = """
        CREATE TABLE `test1/mv_jobs` (
            job_name Text NOT NULL,
            job_settings JsonDocument,
            should_run Bool,
            PRIMARY KEY(job_name)
        );

        CREATE TABLE `test1/mv_job_scans` (
            job_name Text NOT NULL,
            target_name Text NOT NULL,
            scan_settings JsonDocument,
            requested_at Timestamp,
            accepted_at Timestamp,
            runner_id Text,
            command_no Uint64,
            PRIMARY KEY(job_name, target_name)
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
            INDEX ix_job_name GLOBAL SYNC ON (job_name),
            PRIMARY KEY(runner_id, job_name)
        );

        CREATE TABLE `test1/mv_commands` (
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
        """;

    private static final String DROP_MGT_TABLES = """
        DROP TABLE `test1/mv_jobs`;
        DROP TABLE `test1/mv_job_scans`;
        DROP TABLE `test1/mv_runners`;
        DROP TABLE `test1/mv_runner_jobs`;
        DROP TABLE `test1/mv_commands`;
        """;

    protected static YdbConnector ydbConnector;
    protected MvJobDao jobDao = null;

    protected static void prepareMgtDb() {
        System.err.println("[AbstractMgtTest] Setting up management tables...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getMgtConfig(), "config.xml", null);
        ydbConnector = new YdbConnector(cfg);
        runDdl(ydbConnector, CREATE_MGT_TABLES);
    }

    protected static void clearMgtDb() {
        System.err.println("[AbstractMgtTest] Cleaning up management tables...");
        if (ydbConnector != null) {
            runDdl(ydbConnector, DROP_MGT_TABLES);
            ydbConnector.close();
            ydbConnector = null;
        }
    }

    protected static byte[] getMgtConfig() {
        Properties props = new Properties();
        props.setProperty("ydb.url", "grpc://localhost:2136/local");
        props.setProperty("ydb.auth.mode", "NONE");
        props.setProperty(MvBatchSettings.CONF_TABLE_JOBS, "test1/mv_jobs");
        props.setProperty(MvBatchSettings.CONF_TABLE_SCANS, "test1/mv_job_scans");
        props.setProperty(MvBatchSettings.CONF_TABLE_RUNNERS, "test1/mv_runners");
        props.setProperty(MvBatchSettings.CONF_TABLE_RUNNER_JOBS, "test1/mv_runner_jobs");
        props.setProperty(MvBatchSettings.CONF_TABLE_COMMANDS, "test1/mv_commands");

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
            props.storeToXML(baos, "Test props", java.nio.charset.StandardCharsets.UTF_8);
            return baos.toByteArray();
        } catch (java.io.IOException ix) {
            throw new RuntimeException(ix);
        }
    }

    protected void refreshBeforeRun() {
        // Clear all tables before each test
        runDdl(ydbConnector, "DELETE FROM `test1/mv_jobs`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_job_scans`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_runners`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_runner_jobs`");
        runDdl(ydbConnector, "DELETE FROM `test1/mv_commands`");

        jobDao = new MvJobDao(ydbConnector,
                new MvBatchSettings(ydbConnector.getConfig().getProperties()));
    }

}
