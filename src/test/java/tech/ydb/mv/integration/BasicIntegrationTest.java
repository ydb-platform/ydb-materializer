package tech.ydb.mv.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.table.query.Params;
import tech.ydb.topic.settings.DescribeConsumerSettings;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.svc.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvViewExpr;
import tech.ydb.mv.parser.MvSqlGen;

/**
 * colima start --arch aarch64 --vm-type=vz --vz-rosetta (or) colima start
 * --arch amd64
 *
 * while mvn test -Dtest=BasicIntegrationTest; do sleep 0.5s; done
 *
 * @author zinal
 */
public class BasicIntegrationTest extends AbstractIntegrationBase {

    private static final String WRITE_UP1
            = """
INSERT INTO `test1/main_table` (id,c1,c2,c3,c6,c15,c20) VALUES
 ('main-005'u, Timestamp('2021-01-02T10:15:21Z'), 10001, Decimal('10001.567',22,9), 7, 101, 'text message one'u)
;
UPSERT INTO `test1/sub_table1` (c1,c2,c8) VALUES
 (Timestamp('2021-01-02T10:15:21Z'), 10001, 1501)
,(Timestamp('2022-01-02T10:15:21Z'), 10002, 1502)
,(Timestamp('2023-01-02T10:15:21Z'), 10003, 1503)
,(Timestamp('2024-01-02T10:15:21Z'), 10004, 1504)
;
""";

    private static final String WRITE_UP2
            = """
DELETE FROM `test1/main_table` WHERE id='main-001'u
;
DELETE FROM `test1/sub_table2` WHERE c3=Decimal('10002.567',22,9) AND c4='val2'u
;
DELETE FROM `test1/sub_table2` WHERE c3=Decimal('10002.567',22,9) AND c4='val1'u
;
""";

    private static final String WRITE_UP3
            = """
UPSERT INTO `test1/sub_table3` (c5,c10) VALUES
  (1, 'One'u),
  (2, 'Two'u),
  (3, 'Three'u),
  (58, 'Welcome! News'u),
  (59, 'Adieu! News'u);
""";

    private static final String WRITE_UP4
            = """
UPSERT INTO `test1/sub_table4` (c15,c16) VALUES
 (101, 'Eins - Bis'u)
,(103, 'Drei - Bis'u)
;
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
    public void basicIntegrationTest() {
        // now the work
        System.err.println("[AAA] Starting up...");
        var cfg = MvConfig.fromBytes(getConfigBytes(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg, true)) {
            MvService wc = new MvService(conn);
            try {
                wc.applyDefaults(conn.getConfig().getProperties());

                System.err.println("[AAA] Checking context...");
                wc.printIssues(System.out);
                Assertions.assertTrue(wc.getMetadata().isValid());

                System.err.println("[AAA] Printing SQL...");
                wc.printSql(System.out);

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

                System.err.println("[AAA] Writing some input data...");
                runDml(conn, WRITE_INITIAL_DATA);
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating some rows...");
                runDml(conn, WRITE_UP1);
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating more rows...");
                runDml(conn, WRITE_UP2);
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Checking the topic consumer positions...");
                checkConsumerPositions(conn);
                System.err.println("[AAA] All done!");

                System.err.println("[AAA] Clearing MV...");
                clearMV(conn);
                System.err.println("[AAA] Starting the full refresh of MV...");
                refreshMV(wc);
                standardPause();
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Checking the dictionary history...");
                checkDictHist(conn);

                System.err.println("[AAA] Updating some dictionary rows...");
                runDml(conn, WRITE_UP3);
                standardPause();

                System.err.println("[AAA] Clearing MV...");
                clearMV(conn);
                System.err.println("[AAA] Starting the full refresh of MV...");
                refreshMV(wc);
                standardPause();
                standardPause();
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating more dictionary rows...");
                runDml(conn, WRITE_UP4);
                standardPause();

                System.err.println("[AAA] Checking the dictionary history again...");
                checkDictHist(conn);

                System.err.println("[AAA] Waiting for dictionary refresh...");
                pause(20_000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount);
            } finally {
                wc.shutdown();
            }
        }
    }

    private int checkViewOutput(YdbConnector conn, String sqlMain) {
        return checkViewOutput(conn, "test1/mv1", sqlMain);
    }

    private void checkConsumerPositions(YdbConnector conn) {
        String consumerName = "consumer1";
        checkConsumerPosition(conn, "test1/main_table", "cf0", consumerName, 6L);
        checkConsumerPosition(conn, "test1/sub_table1", "cf1", consumerName, 8L);
        checkConsumerPosition(conn, "test1/sub_table2", "cf2", consumerName, 9L);
        checkConsumerPosition(conn, "test1/sub_table3", "cf3", "dictionary", 2L);
        checkConsumerPosition(conn, "test1/sub_table4", "cf4", "dictionary", 4L);
    }

    private void checkConsumerPosition(YdbConnector conn, String tabName,
            String feed, String consumer, long expected) {
        var descMain = conn.getTopicClient().describeConsumer(
                conn.fullCdcTopicName(tabName, feed),
                consumer,
                DescribeConsumerSettings.newBuilder()
                        .withIncludeStats(true)
                        .build()).join().getValue();
        long sumMessages = 0L;
        for (var cpi : descMain.getPartitions()) {
            sumMessages += cpi.getConsumerStats().getCommittedOffset();
        }
        Assertions.assertEquals(expected, sumMessages);
    }

    private void clearMV(YdbConnector conn) {
        conn.sqlWrite("DELETE FROM `test1/mv1`;", Params.empty());
    }

    private void refreshMV(MvService wc) {
        wc.startScan("handler1", "test1/mv1");
    }

    private void checkDictHist(YdbConnector conn) {
        var rs = conn.sqlRead("SELECT diff_val, key_text, tv "
                + "FROM `test1/dict_hist` "
                + "WHERE src='test1/sub_table4'u"
                + "ORDER BY tv, key_text;",
                Params.empty()).getResultSet(0);
        System.out.println("--- dictionary comparison begin ---");
        while (rs.next()) {
            System.out.println("  DICT: " + rs.getColumn(1).getText()
                    + " at " + rs.getColumn(2).getTimestamp().toString()
                    + ": " + rs.getColumn(0).getValue().asOptional()
                            .get().asData().getJsonDocument());
        }
        System.out.println("--- dictionary comparison end ---");
    }

}
