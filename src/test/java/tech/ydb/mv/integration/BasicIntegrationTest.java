package tech.ydb.mv.integration;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tech.ydb.table.query.Params;
import tech.ydb.topic.settings.DescribeConsumerSettings;

import tech.ydb.mv.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvTarget;
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
INSERT INTO `test1/main_table` (id,c1,c2,c3,c6,c20) VALUES
 ('main-005'u, Timestamp('2021-01-02T10:15:21Z'), 10001, Decimal('10001.567',22,9), 7, 'text message one'u)
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
INSERT INTO `test1/sub_table3` (c5,c10) VALUES
  (1, 'One'u),
  (2, 'Two'u),
  (3, 'Three'u)
;
""";

    private static final String WRITE_UP3
            = """
UPSERT INTO `test1/sub_table3` (c5,c10) VALUES
 (58, 'Welcome! News'u)
,(59, 'Adieu! News'u)
;
""";


    @BeforeAll
    public static void init() {
        prepareDb();
    }

    @AfterAll
    public static void cleanup() {
        clearDb();
    }

    private void standardPause() {
        pause(2000L);
    }

    @Test
    public void basicIntegrationTest() {
        // now the work
        System.err.println("[AAA] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            MvService wc = new MvService(conn);
            try {
                wc.applyDefaults();

                System.err.println("[AAA] Checking context...");
                wc.printIssues();
                Assertions.assertTrue(wc.getMetadata().isValid());

                System.err.println("[AAA] Printing SQL...");
                wc.printSql();

                System.err.println("[AAA] Generating SELECT ALL query...");
                MvTarget mainTarget = wc.getMetadata().getHandlers()
                        .get("handler1").getTarget("test1/mv1");
                String sqlQuery;
                try (MvSqlGen sg = new MvSqlGen(mainTarget)) {
                    sqlQuery = sg.makeSelectAll();
                }

                System.err.println("[AAA] Starting the services...");
                wc.startHandlers();
                wc.startDictionaryHandler();
                standardPause();
                System.err.println("[AAA] Checking the view output (should be empty)...");
                int diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Writing some input data...");
                runDml(conn, WRITE_INITIAL);
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
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                if (diffCount > 0) {
                    System.out.println("********* dumping threads **********");
                    System.out.println(generateThreadDump());
                }
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Checking the dictionary history...");
                checkDictHist(conn);

                System.err.println("[AAA] Updating just dictionary rows...");
                runDml(conn, WRITE_UP3);
                standardPause();

                System.err.println("[AAA] Checking the dictionary history again...");
                checkDictHist(conn);
            } finally {
                wc.shutdown();
            }
        }
    }

    private int checkViewOutput(YdbConnector conn, String sqlMain) {
        String sqlMv = "SELECT * FROM `test1/mv1`";
        var left = convertResultSet(
                conn.sqlRead(sqlMain, Params.empty()).getResultSet(0), "id");
        var right = convertResultSet(
                conn.sqlRead(sqlMv, Params.empty()).getResultSet(0), "id");
        System.out.println("*** comparing rowsets, size1="
                + left.size() + ", size2=" + right.size());
        int diffCount = 0;
        for (var leftMe : left.entrySet()) {
            var rightVal = right.get(leftMe.getKey());
            if (rightVal == null) {
                System.out.println("  missing key: " + leftMe.getKey());
                ++diffCount;
                continue;
            }
            if (!leftMe.getValue().equals(rightVal)) {
                System.out.println("  unequal records: \n\t"
                        + leftMe.getValue() + "\n\t"
                        + rightVal);
                ++diffCount;
            }
        }
        for (var rightMe : right.entrySet()) {
            var leftVal = left.get(rightMe.getKey());
            if (leftVal == null) {
                System.out.println("  extra key: " + rightMe.getKey());
                ++diffCount;
            }
        }
        return diffCount;
    }

    private void checkConsumerPositions(YdbConnector conn) {
        String consumerName = "consumer1";
        checkConsumerPosition(conn, "test1/main_table", "cf1", consumerName, 6L);
        checkConsumerPosition(conn, "test1/sub_table1", "cf2", consumerName, 8L);
        checkConsumerPosition(conn, "test1/sub_table2", "cf3", consumerName, 9L);
        checkConsumerPosition(conn, "test1/sub_table3", "cf4", "dictionary", 5L);
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
        wc.startScan("handler1", "test1/mv1",
                new MvScanSettings(wc.getYdb().getConfig().getProperties()));
    }

    private void checkDictHist(YdbConnector conn) {
        var rs = conn.sqlRead("SELECT diff_val, key_text, tv "
                        + "FROM `test1/dict_hist` "
                + "WHERE src='test1/sub_table3'u"
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

    public static String generateThreadDump() {
        final StringBuilder dump = new StringBuilder();
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        for (ThreadInfo threadInfo : threadInfos) {
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

}
