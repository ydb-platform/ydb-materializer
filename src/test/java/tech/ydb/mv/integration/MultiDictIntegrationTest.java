package tech.ydb.mv.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.svc.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvSqlGen;

/**
 * Integration test for BATCH-style processing with multiple dictionaries.
 *
 * When both sub_table4 and sub_table5 (BATCH inputs) have changes, the
 * dictionary scan uses MvRowFilter with blocks for each dictionary. The block
 * offsets (startPos) must match the column order in the DictTrans SQL output,
 * which follows target source order. A bug caused wrong offsets when
 * dictChecks.keySet() iteration order differed from target source order,
 * leading to ActionKeysFilter rejecting all rows.
 *
 * @author zinal
 */
public class MultiDictIntegrationTest extends AbstractIntegrationBase {

    /**
     * Update BOTH dictionary tables (sub_table4 and sub_table5) so that when
     * the dictionary scan runs, MvChangesMultiDict has changes for both, and
     * toFilter builds a filter with multiple blocks.
     */
    private static final String WRITE_DICTS_0
            = """
UPSERT INTO `test1/sub_table4` (c15,c16) VALUES
 (101, 'sub_table4 Eins Updated 0'u)
,(103, 'sub_table4 Drei Updated 0'u);
UPSERT INTO `test1/sub_table5` (c21,c22) VALUES
 (202, 'sub_table5 Zwei Updated 0'u)
,(204, 'sub_table5 Vier Updated 0'u);
""";

    private static final String WRITE_DICTS_1
            = """
UPSERT INTO `test1/sub_table4` (c15,c16) VALUES
 (102, 'sub_table4 Zwei Updated 1'u)
,(104, 'sub_table4 Vier Updated 1'u);
""";

    private static final String WRITE_DICTS_2
            = """
UPSERT INTO `test1/sub_table5` (c21,c22) VALUES
 (201, 'sub_table5 Eins Updated 2'u)
,(203, 'sub_table5 Drei Updated 2'u);
""";

    @BeforeEach
    public void init() {
        prepareDb();
    }

    @AfterEach
    public void cleanup() {
        clearDb();
    }

    @Test
    public void batchMultiDictIntegrationTest() {
        System.err.println("[AAA] Starting up...");
        var cfg = MvConfig.fromBytes(getConfigBytes());
        try (YdbConnector conn = new YdbConnector(cfg, true)) {
            MvService wc = new MvService(conn);
            try {
                wc.applyDefaults(conn.getConfig().getProperties());

                System.err.println("[AAA] Checking context...");
                wc.printIssues(System.out);
                Assertions.assertTrue(wc.getMetadata().isValid());

                System.err.println("[AAA] Printing SQL...");
                wc.printBasicSql(System.out);

                System.err.println("[AAA] Generating SELECT ALL query...");
                MvViewExpr mainTarget = wc.getMetadata().getHandlers()
                        .get("handler1").getView("test1/mv1").getParts()
                        .values().iterator().next();
                String sqlQuery;
                try (MvSqlGen sg = new MvSqlGen(mainTarget)) {
                    sqlQuery = sg.makeSelectAll();
                }

                System.err.println("[AAA] Starting the services...");
                wc.startDefaultHandlers();
                wc.startDictionaryHandler();
                standardPause();

                System.err.println("[AAA] Checking the view output (should be empty)...");
                int diffCount = checkViewOutput(conn, sqlQuery, false);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Writing initial data...");
                runDml(conn, WRITE_INITIAL_DATA);
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery, false);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Clearing MV and doing full refresh...");
                clearMV(conn);
                refreshMV(wc);
                standardPause();
                standardPause();
                diffCount = checkViewOutput(conn, sqlQuery, false);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating BOTH dictionary tables...");
                runDml(conn, WRITE_DICTS_0);
                System.err.println("[AAA] Waiting for dictionary refresh (multi-dict scan)...");
                pause(10_000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery, true);
                Assertions.assertEquals(0, diffCount,
                        "View output mismatch after dictionary refresh with BOTH dicts");

                System.err.println("[AAA] Updating FIRST dictionary table...");
                runDml(conn, WRITE_DICTS_1);
                System.err.println("[AAA] Waiting for dictionary refresh (multi-dict scan)...");
                pause(15_000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery, false);
                Assertions.assertEquals(0, diffCount,
                        "View output mismatch after dictionary refresh with FIRST dicts");

                System.err.println("[AAA] Updating SECOND dictionary table...");
                runDml(conn, WRITE_DICTS_2);
                System.err.println("[AAA] Waiting for dictionary refresh (multi-dict scan)...");
                pause(15_000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery, true);
                Assertions.assertEquals(0, diffCount,
                        "View output mismatch after dictionary refresh with SECOND dicts");
            } finally {
                wc.shutdown();
            }
        }
    }

    private int checkViewOutput(YdbConnector conn, String sqlMain, boolean showNormal) {
        return checkViewOutput(conn, "test1/mv1", sqlMain, showNormal);
    }

    private void clearMV(YdbConnector conn) {
        conn.sqlWrite("DELETE FROM `test1/mv1`;", tech.ydb.table.query.Params.empty());
    }

    private void refreshMV(MvService wc) {
        wc.startScan("handler1", "test1/mv1");
    }
}
