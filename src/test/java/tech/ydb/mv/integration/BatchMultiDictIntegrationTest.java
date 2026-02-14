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
public class BatchMultiDictIntegrationTest extends AbstractIntegrationBase {

    /**
     * Update BOTH dictionary tables (sub_table4 and sub_table5) so that when
     * the dictionary scan runs, MvChangesMultiDict has changes for both, and
     * toFilter builds a filter with multiple blocks.
     */
    private static final String WRITE_BOTH_DICTS
            = """
UPSERT INTO `test1/sub_table4` (c15,c16) VALUES
 (101, 'Eins Updated'u)
,(103, 'Drei Updated'u);
UPSERT INTO `test1/sub_table5` (c21,c22) VALUES
 (102, 'Zwei Updated'u)
,(104, 'Vier Updated'u);
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
                int diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Writing initial data...");
                runDml(conn, WRITE_INITIAL_DATA);
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Clearing MV and doing full refresh...");
                clearMV(conn);
                refreshMV(wc);
                standardPause();
                standardPause();
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating BOTH dictionary tables...");
                runDml(conn, WRITE_BOTH_DICTS);
                standardPause();

                System.err.println("[AAA] Waiting for dictionary refresh (multi-dict scan)...");
                pause(20_000L);

                System.err.println("[AAA] Checking the view output after multi-dict refresh...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount,
                        "View output mismatch after dictionary refresh with multiple dicts");
            } finally {
                wc.shutdown();
            }
        }
    }

    private int checkViewOutput(YdbConnector conn, String sqlMain) {
        return checkViewOutput(conn, "test1/mv1", sqlMain);
    }

    private void clearMV(YdbConnector conn) {
        conn.sqlWrite("DELETE FROM `test1/mv1`;", tech.ydb.table.query.Params.empty());
    }

    private void refreshMV(MvService wc) {
        wc.startScan("handler1", "test1/mv1");
    }
}
