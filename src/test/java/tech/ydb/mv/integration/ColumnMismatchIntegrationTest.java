package tech.ydb.mv.integration;

import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.svc.MvService;

/**
 * Verifies that a materialized view whose output column names do not match the
 * destination table columns is rejected during metadata validation and that
 * handler startup fails with a clear error instead of an internal failure.
 */
public class ColumnMismatchIntegrationTest extends AbstractIntegrationBase {

    private static final String CREATE_TABLES = """
CREATE TABLE `colmatch_test/main`(
    id Int32, sub_ref Int32, data1 Text,
    PRIMARY KEY(id),
    INDEX ix_sub GLOBAL ON (sub_ref));

CREATE TABLE `colmatch_test/sub`(
    id Int32, main_ref Int32, data2 Text,
    PRIMARY KEY(id),
    INDEX ix_main GLOBAL ON (main_ref));

CREATE TABLE `colmatch_test/mv`(
    id_main Int32, id_sub Int32, data1 Text, data2 Text,
    PRIMARY KEY(id_main));

ALTER TABLE `colmatch_test/main` ADD CHANGEFEED `mv` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `colmatch_test/sub` ADD CHANGEFEED `mv` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
""";

    private static final String CDC_CONSUMERS = """
ALTER TOPIC `colmatch_test/main/mv` ADD CONSUMER c_colmatch;
ALTER TOPIC `colmatch_test/sub/mv` ADD CONSUMER c_colmatch;
""";

    private static final String UPSERT_CONFIG = """
UPSERT INTO `test1/statements` (module_id, statement_no, statement_text) VALUES
  ('', 1, @@CREATE ASYNC MATERIALIZED VIEW `colmatch_test/mv` AS
    SELECT main.id AS main_id, sub.id AS sub_id, main.data1 AS data1, sub.data2 AS data2
    FROM `colmatch_test/main` AS main
    LEFT JOIN `colmatch_test/sub` AS sub
      ON main.id = sub.main_ref;@@),

  ('colmatch_handler', 1, @@CREATE ASYNC HANDLER colmatch_handler CONSUMER c_colmatch
  PROCESS `colmatch_test/mv`,
  INPUT `colmatch_test/main` CHANGEFEED mv AS STREAM,
  INPUT `colmatch_test/sub` CHANGEFEED mv AS STREAM;@@);
""";

    private static final String DROP_TABLES = """
DROP TABLE `colmatch_test/main`;
DROP TABLE `colmatch_test/sub`;
DROP TABLE `colmatch_test/mv`;
""";

    @Override
    protected Properties getConfigProps() {
        Properties props = super.getConfigProps();
        props.setProperty(MvConfig.CONF_HANDLERS, "colmatch_handler");
        return props;
    }

    @BeforeEach
    public void init() {
        pause(5000L);
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, CREATE_TABLES_BASE);
            runDdl(conn, CREATE_TABLES);
            runDdl(conn, CDC_CONSUMERS);
            runDdl(conn, UPSERT_CONFIG);
        }
    }

    @AfterEach
    public void cleanup() {
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, DROP_TABLES);
            runDdl(conn, DROP_TABLES_BASE);
        }
    }

    @Test
    public void rejectsHandlerStartWhenOutputColumnsDoNotMatchDestination() {
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg, true)) {
            try (MvService svc = new MvService(conn)) {
                svc.applyDefaults(conn.getConfig().getProperties());

                Assertions.assertFalse(svc.getMetadata().isValid(),
                        "Metadata must be invalid when output column names do not match the destination table");
                Assertions.assertTrue(svc.getMetadata().getErrors().stream()
                        .anyMatch(MvIssue.UnknownOutputColumn.class::isInstance),
                        "Expected UnknownOutputColumn validation error");
                Assertions.assertTrue(svc.getMetadata().getErrors().stream()
                        .anyMatch(issue -> issue.getMessage().contains("main_id")),
                        "Expected validation error for mismatched column main_id");

                IllegalStateException error = Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> svc.startHandler("colmatch_handler"));
                Assertions.assertTrue(error.getMessage().contains("Refusing to start handler `colmatch_handler`"),
                        "Unexpected error message: " + error.getMessage());
                Assertions.assertTrue(error.getMessage().contains("Unknown output column"),
                        "Handler start must report the validation issue, got: " + error.getMessage());
                Assertions.assertFalse(svc.isRunning(),
                        "Handler must not be running after a failed start attempt");
            }
        }
    }
}
