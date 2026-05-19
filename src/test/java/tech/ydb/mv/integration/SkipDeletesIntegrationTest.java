package tech.ydb.mv.integration;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.table.query.Params;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.svc.MvService;

/**
 * Verifies SKIP_DELETES: UPSERTs from main and inner-joined fact tables are
 * applied, while DELETE events from both are ignored.
 *
 * colima start --arch aarch64 --vm-type=vz --vz-rosetta (or) colima start
 * --arch amd64
 *
 * while mvn test -Dtest=SkipDeletesIntegrationTest; do sleep 0.5s; done
 *
 * @author zinal
 */
public class SkipDeletesIntegrationTest extends AbstractIntegrationBase {

    public static final String CREATE_TABLES_SKIPDEL = """
CREATE TABLE `skipdel_test/main` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    val Text,
    PRIMARY KEY(id)
);

CREATE TABLE `skipdel_test/fact` (
    c1 Timestamp,
    c2 Int64,
    extra Int32,
    PRIMARY KEY(c1, c2)
);

CREATE TABLE `skipdel_test/mv` (
    id Text NOT NULL,
    val Text,
    extra Int32,
    PRIMARY KEY(id)
);

ALTER TABLE `skipdel_test/main` ADD CHANGEFEED `cf_main` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `skipdel_test/fact` ADD CHANGEFEED `cf_fact` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
""";

    public static final String DROP_TABLES_SKIPDEL = """
DROP TABLE `skipdel_test/main`;
DROP TABLE `skipdel_test/fact`;
DROP TABLE `skipdel_test/mv`;
""";

    public static final String CDC_CONSUMERS_SKIPDEL = """
ALTER TOPIC `skipdel_test/main/cf_main` ADD CONSUMER `skipdel_consumer`;
ALTER TOPIC `skipdel_test/fact/cf_fact` ADD CONSUMER `skipdel_consumer`;
""";

    public static final String UPSERT_CONFIG_SKIPDEL = """
UPSERT INTO `test1/statements` (module_id, statement_no, statement_text) VALUES
  ('', 1, @@CREATE ASYNC MATERIALIZED VIEW `skipdel_test/mv`
  OPTIONS SKIP_DELETES 'true'
  AS
  SELECT main.id AS id, main.val AS val, fact.extra AS extra
  FROM `skipdel_test/main` AS main
  INNER JOIN `skipdel_test/fact` AS fact
    ON main.c1=fact.c1 AND main.c2=fact.c2;@@),

  ('skipdel_handler', 1, @@CREATE ASYNC HANDLER skipdel_handler CONSUMER skipdel_consumer
  PROCESS `skipdel_test/mv`,
  INPUT `skipdel_test/main` CHANGEFEED cf_main AS STREAM,
  INPUT `skipdel_test/fact` CHANGEFEED cf_fact AS STREAM;@@);
""";

    public static final String SELECT_ALL_SKIPDEL = """
SELECT main.id AS id, main.val AS val, fact.extra AS extra
FROM `skipdel_test/main` AS main
INNER JOIN `skipdel_test/fact` AS fact
  ON main.c1=fact.c1 AND main.c2=fact.c2
""";

    private static final String TS1 = "Timestamp('2021-01-02T10:15:21Z')";
    private static final String TS2 = "Timestamp('2022-01-02T10:15:22Z')";

    public static final String WRITE_INITIAL = """
INSERT INTO `skipdel_test/main` (id,c1,c2,val) VALUES
 ('main-001'u, %s, 10001, 'alpha'u)
,('main-002'u, %s, 10002, 'beta'u)
;
INSERT INTO `skipdel_test/fact` (c1,c2,extra) VALUES
 (%s, 10001, 501)
,(%s, 10002, 502)
;
""".formatted(TS1, TS2, TS1, TS2);

    public static final String WRITE_UPDATES = """
UPSERT INTO `skipdel_test/main` (id,val) VALUES
 ('main-001'u, 'alpha-upd'u)
;
UPSERT INTO `skipdel_test/fact` (c1,c2,extra) VALUES
 (%s, 10001, 1501)
;
""".formatted(TS1);

    public static final String WRITE_DELETES = """
DELETE FROM `skipdel_test/main` WHERE id='main-001'u
;
DELETE FROM `skipdel_test/fact` WHERE c1=%s AND c2=10002
;
""".formatted(TS2);

    @Override
    protected Properties getConfigProps() {
        var props = super.getConfigProps();
        props.setProperty(MvConfig.CONF_HANDLERS, "skipdel_handler");
        return props;
    }

    @BeforeEach
    public void init() {
        pause(5000L);
        System.err.println("[SKIPDEL] Database setup...");
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, CREATE_TABLES_BASE);
            runDdl(conn, CREATE_TABLES_SKIPDEL);
            runDdl(conn, CDC_CONSUMERS_SKIPDEL);
            runDdl(conn, UPSERT_CONFIG_SKIPDEL);
        }
    }

    @AfterEach
    public void cleanup() {
        System.err.println("[SKIPDEL] Database cleanup...");
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, DROP_TABLES_BASE);
            runDdl(conn, DROP_TABLES_SKIPDEL);
        }
    }

    @Test
    public void skipDeletesIntegrationTest() {
        System.err.println("[SKIPDEL] Starting up...");
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg, true)) {
            try (MvService svc = new MvService(conn)) {
                svc.applyDefaults(conn.getConfig().getProperties());

                System.err.println("[SKIPDEL] Checking context...");
                svc.printIssues(System.out);
                Assertions.assertTrue(svc.getMetadata().isValid());
                var view = svc.getMetadata().getViews().get("skipdel_test/mv");
                Assertions.assertNotNull(view);
                Assertions.assertTrue(view.isSkipDeletes(),
                        "SKIP_DELETES option should be enabled on the view");

                System.err.println("[SKIPDEL] Starting the services...");
                svc.startDefaultHandlers();
                standardPause();

                System.err.println("[SKIPDEL] Writing initial data...");
                runDml(conn, WRITE_INITIAL);
                standardPause();
                System.err.println("[SKIPDEL] Checking MV after initial insert...");
                Assertions.assertEquals(0,
                        checkViewOutput(conn, "skipdel_test/mv", SELECT_ALL_SKIPDEL, false, "id"));

                System.err.println("[SKIPDEL] Applying updates on main and fact tables...");
                runDml(conn, WRITE_UPDATES);
                standardPause();
                System.err.println("[SKIPDEL] Checking MV after updates...");
                Assertions.assertEquals(0,
                        checkViewOutput(conn, "skipdel_test/mv", SELECT_ALL_SKIPDEL, false, "id"));
                assertMvRow(conn, "main-001", "alpha-upd", "1501");
                assertMvRow(conn, "main-002", "beta", "502");

                Map<String, Map<String, String>> snapshot = readMv(conn);
                Assertions.assertEquals(2, snapshot.size());

                System.err.println("[SKIPDEL] Deleting rows from main and fact tables...");
                runDml(conn, WRITE_DELETES);
                standardPause();

                System.err.println("[SKIPDEL] Checking MV still reflects pre-delete state...");
                Map<String, Map<String, String>> afterDeletes = readMv(conn);
                Assertions.assertEquals(snapshot, afterDeletes,
                        "DELETE events must be ignored when SKIP_DELETES is enabled");
                assertMvRow(conn, "main-001", "alpha-upd", "1501");
                assertMvRow(conn, "main-002", "beta", "502");

                System.err.println("[SKIPDEL] Source tables no longer join, MV must stay stale...");
                Assertions.assertNotEquals(0,
                        checkViewOutput(conn, "skipdel_test/mv", SELECT_ALL_SKIPDEL, false, "id"),
                        "MV should diverge from live join after source deletes");
            }
        }
    }

    private static Map<String, Map<String, String>> readMv(YdbConnector conn) {
        return convertResultSet(
                conn.sqlRead("SELECT * FROM `skipdel_test/mv`", Params.empty()).getResultSet(0),
                new String[]{"id"});
    }

    private static void assertMvRow(YdbConnector conn, String id,
            String expectedVal, String expectedExtra) {
        var rows = readMv(conn);
        var row = rows.get(id);
        Assertions.assertNotNull(row, "MV row missing for id=" + id);
        Assertions.assertEquals(expectedVal, row.get("val"));
        Assertions.assertEquals(expectedExtra, row.get("extra"));
    }

}
