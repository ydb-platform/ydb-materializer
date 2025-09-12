package tech.ydb.mv.integration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.mv.model.MvCoordinator;
import tech.ydb.mv.model.MvCoordinatorSettings;
import tech.ydb.mv.support.MvLocker;
import tech.ydb.query.QuerySession;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.topic.settings.DescribeConsumerSettings;
import tech.ydb.mv.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvScanSettings;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.parser.MvSqlGen;
import tech.ydb.mv.util.YdbConv;

import static tech.ydb.mv.util.YdbMisc.sleep;

/**
 * colima start --arch aarch64 --vm-type=vz --vz-rosetta (or) colima start
 * --arch amd64
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

    @RegisterExtension
    private static final YdbHelperExtension YDB = new YdbHelperExtension();

    private static String getConnectionUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(YDB.useTls() ? "grpcs://" : "grpc://");
        sb.append(YDB.endpoint());
        sb.append(YDB.database());
        return sb.toString();
    }

    private static byte[] getConfig() {
        Properties props = new Properties();
        props.setProperty("ydb.url", getConnectionUrl());
        props.setProperty("ydb.auth.mode", "NONE");
        props.setProperty("ydb.poolSize", "10");
        props.setProperty(MvConfig.CONF_INPUT_MODE, MvConfig.Input.TABLE.name());
        props.setProperty(MvConfig.CONF_INPUT_TABLE, "test1/statements");
        props.setProperty(MvConfig.CONF_HANDLERS, "handler1");
        props.setProperty(MvConfig.CONF_DEF_APPLY_THREADS, "1");
        props.setProperty(MvConfig.CONF_DEF_CDC_THREADS, "1");
        props.setProperty(MvConfig.CONF_SCAN_TABLE, "test1/scans_state");
        props.setProperty(MvConfig.CONF_DICT_TABLE, "test1/dict_hist");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            props.storeToXML(baos, "Test props", StandardCharsets.UTF_8);
            return baos.toByteArray();
        } catch (IOException ix) {
            throw new RuntimeException(ix);
        }
    }

    @Test
    public void basicIntegrationTest() {
        // have to wait a bit here for YDB startup
        pause(1000L);
        // now the work
        System.err.println("[AAA] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            fillDatabase(conn);
            System.err.println("[AAA] Preparation: completed.");
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
                pause(2000L);
                System.err.println("[AAA] Checking the view output (should be empty)...");
                int diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Writing some input data...");
                runDml(conn, WRITE_INITIAL);
                pause(2000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating some rows...");
                runDml(conn, WRITE_UP1);
                pause(2000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Updating more rows...");
                runDml(conn, WRITE_UP2);
                pause(2000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Checking the topic consumer positions...");
                checkConsumerPositions(conn);
                System.err.println("[AAA] All done!");

                System.err.println("[AAA] Clearing MV...");
                clearMV(conn);
                System.err.println("[AAA] Starting the full refresh of MV...");
                refreshMV(wc);
                pause(2000L);
                System.err.println("[AAA] Checking the view output...");
                diffCount = checkViewOutput(conn, sqlQuery);
                Assertions.assertEquals(0, diffCount);

                System.err.println("[AAA] Checking the dictionary history...");
                checkDictHist(conn);

                System.err.println("[AAA] Updating just dictionary rows...");
                runDml(conn, WRITE_UP3);
                pause(2000L);

                System.err.println("[AAA] Checking the dictionary history again...");
                checkDictHist(conn);
            } finally {
                wc.shutdown();
            }
        }
    }

    @Test
    public void mvCoordination() {
        pause(3000);

        final var concurrentQueue = new ConcurrentLinkedQueue<Integer>();
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "classpath:/config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            pause(10000);
            runDdl(conn,
                    """
                            CREATE TABLE mv_jobs (
                                job_name Text NOT NULL,
                                job_settings JsonDocument,
                                should_run Bool,
                                runner_id Text,
                                PRIMARY KEY(job_name)
                            );
                            
                            CREATE TABLE mv_commands (
                                runner_id Text NOT NULL,
                                command_no Uint64 NOT NULL,
                                created_at Timestamp,
                                command_type Text, -- START / STOP
                                job_name Text,
                                job_settings JsonDocument,
                                command_status Text, -- CREATED / TAKEN / SUCCESS / ERROR
                                command_diag Text,
                                PRIMARY KEY(runner_id, command_no)
                            );
                            """);
            runDdl(conn, "INSERT INTO mv_jobs(job_name, should_run) VALUES ('sys$coordinator', true)");
            var scheduler = Executors.newScheduledThreadPool(3);
            new MvCoordinator(new MvLocker(conn), scheduler, conn.getQueryRetryCtx(), new MvCoordinatorSettings(1),
                    "instance", () -> concurrentQueue.add(1)).start();

            pause(5000);
            Assertions.assertEquals(1, concurrentQueue.size());

            conn.getQueryRetryCtx().supplyResult(session -> session
                    .createQuery("UPDATE mv_jobs SET should_run = false WHERE 1 = 1", TxMode.NONE).execute()
            ).join().getStatus().expectSuccess();

            pause(5000);

            conn.getQueryRetryCtx().supplyResult(session -> session
                    .createQuery("UPDATE mv_jobs SET should_run = true WHERE 1 = 1", TxMode.NONE).execute()
            ).join().getStatus().expectSuccess();
            pause(10_000);

            Assertions.assertEquals(2, concurrentQueue.size());
        }
    }

    private void pause(long millis) {
        System.err.println("\t...Sleeping for " + millis + "...");
        sleep(millis);
    }

    public void fillDatabase(YdbConnector conn) {
        System.err.println("[AAA] Preparation: creating tables...");
        runDdl(conn, CREATE_TABLES);
        System.err.println("[AAA] Preparation: adding consumers...");
        runDdl(conn, CDC_CONSUMERS);
        System.err.println("[AAA] Preparation: adding config...");
        runDdl(conn, UPSERT_CONFIG);
    }

    private static CompletableFuture<Status> runSql(QuerySession qs, String sql, TxMode txMode) {
        return qs.createQuery(sql, txMode)
                .execute()
                .thenApply(res -> res.getStatus());
    }

    private static void runDdl(YdbConnector conn, String sql) {
        conn.getQueryRetryCtx()
                .supplyStatus(qs -> runSql(qs, sql, TxMode.NONE))
                .join()
                .expectSuccess();
    }

    private void runDml(YdbConnector conn, String sql) {
        conn.getQueryRetryCtx()
                .supplyStatus(qs -> runSql(qs, sql, TxMode.SERIALIZABLE_RW))
                .join()
                .expectSuccess();
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
        var rs = conn.sqlRead("SELECT full_val, key_text, tv "
                + "FROM `test1/dict_hist` ORDER BY tv, key_text;",
                Params.empty()).getResultSet(0);
        while (rs.next()) {
            System.out.println("  DICT: " + rs.getColumn(1).getText()
                    + " at " + rs.getColumn(2).getTimestamp().toString()
                    + ": " + rs.getColumn(0).getValue().asOptional()
                            .get().asData().getJsonDocument());
        }
    }

}
