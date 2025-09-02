package tech.ydb.mv.integration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.test.junit5.YdbHelperExtension;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.format.MvIssuePrinter;
import tech.ydb.mv.format.MvSqlPrinter;
import tech.ydb.query.QuerySession;

/**
 * colima start --arch aarch64 --vm-type=vz --vz-rosetta
 * (or) colima start --arch amd64
 *
 * @author zinal
 */
public class BasicIntegrationTest {

    private static final String CREATE_TABLES =
"""
CREATE TABLE `test1/statements` (
    statement_no Int32 NOT NULL,
    statement_text Text NOT NULL,
    PRIMARY KEY(statement_no)
);

CREATE TABLE `test1/main_table` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c20 Text,
    PRIMARY KEY(id),
    INDEX ix_c1_c2 GLOBAL ON (c1,c2),
    INDEX ix_c3 GLOBAL ON (c3)
);

CREATE TABLE `test1/sub_table1` (
    c1 Timestamp,
    c2 Int64,
    c8 Int32,
    PRIMARY KEY(c1, c2)
);

CREATE TABLE `test1/sub_table2` (
    c3 Decimal(22,9),
    c4 Text,
    c9 Date,
    PRIMARY KEY(c3, c4)
);

CREATE TABLE `test1/sub_table3` (
    c5 Int32 NOT NULL,
    c10 Text,
    PRIMARY KEY(c5)
);

CREATE TABLE `test1/mv1` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c8 Int32,
    c9 Date,
    c10 Text,
    c11 Text,
    c12 Int32,
    PRIMARY KEY(id),
    INDEX ix_c1 GLOBAL ON (c1)
);

ALTER TABLE `test1/main_table` ADD CHANGEFEED `cf1` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table1` ADD CHANGEFEED `cf2` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table2` ADD CHANGEFEED `cf3` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table3` ADD CHANGEFEED `cf4` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
""";

    private static final String CDC_CONSUMERS =
"""
ALTER TOPIC `test1/main_table/cf1` ADD CONSUMER `consumer1`;
ALTER TOPIC `test1/sub_table1/cf2` ADD CONSUMER `consumer1`;
ALTER TOPIC `test1/sub_table2/cf3` ADD CONSUMER `consumer1`;
ALTER TOPIC `test1/sub_table3/cf4` ADD CONSUMER `consumer1`;
""";

    private static final String UPSERT_DATA =
"""
UPSERT INTO `test1/statements` (statement_no,statement_text) VALUES
  (1, @@CREATE ASYNC MATERIALIZED VIEW `test1/mv1` AS
  SELECT main.id AS id, main.c1 AS c1, main . c2 AS c2, main . c3 AS c3,
         sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10,
         COMPUTE ON main #[ Substring(main.c20,3,5) ]# AS c11,
         COMPUTE #[ CAST(NULL AS Int32?) ]# AS c12
  FROM `test1/main_table` AS main
  INNER JOIN `test1/sub_table1` AS sub1
    ON main.c1=sub1.c1 AND main.c2=sub1.c2
  LEFT JOIN `test1/sub_table2` AS sub2
    ON main.c3=sub2.c3 AND 'val1'=sub2.c4
  INNER JOIN `test1/sub_table3` AS sub3
    ON sub3.c5=58
  WHERE COMPUTE ON main, sub2
  #[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u) ]#;@@),
  (2, @@CREATE ASYNC HANDLER handler1 CONSUMER consumer1 PROCESS `test1/mv1`,
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  INPUT `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  INPUT `test1/sub_table3` CHANGEFEED cf4 AS BATCH;@@);
""";

    @RegisterExtension
    private static final YdbHelperExtension YDB = new YdbHelperExtension();

    private static String getConnectionUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(YDB.useTls() ? "grpcs://" : "grpc://" );
        sb.append(YDB.endpoint());
        sb.append(YDB.database());
        return sb.toString();
    }

    private static byte[] getConfig() {
        Properties props = new Properties();
        props.setProperty("ydb.url", getConnectionUrl());
        props.setProperty("ydb.auth.mode", "NONE");
        props.setProperty(MvConfig.CONF_INPUT_MODE, MvConfig.Input.TABLE.name());
        props.setProperty(MvConfig.CONF_INPUT_TABLE, "test1/statements");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            props.storeToXML(baos, "Test props", StandardCharsets.UTF_8);
            return baos.toByteArray();
        } catch(IOException ix) {
            throw new RuntimeException(ix);
        }
    }

    @Test
    public void test1() {
        // has to wait a bit here
        try { Thread.sleep(5000L); } catch(InterruptedException ix) {}
        // now the work
        System.err.println("Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            fillDatabase(conn);
            System.err.println("Preparation: completed.");
            MvService wc = new MvService(conn);
            try {
                validateContext(wc);
                new MvSqlPrinter(wc.getContext()).write(System.out);
            } finally {
                wc.shutdown();
            }
        }
    }

    private void validateContext(MvService wc) {
        new MvIssuePrinter(wc.getContext()).write(System.out);
        Assertions.assertTrue(wc.getContext().isValid());
    }

    private void fillDatabase(YdbConnector conn) {
        System.err.println("Preparation: creating tables...");
        runScript(conn, CREATE_TABLES);
        System.err.println("Preparation: adding consumers...");
        runScript(conn, CDC_CONSUMERS);
        System.err.println("Preparation: adding data...");
        runScript(conn, UPSERT_DATA);
    }

    private void runScript(YdbConnector conn, String sql) {
        conn.getQueryRetryCtx()
                .supplyStatus(qs -> runDdl(qs, sql))
                .join()
                .expectSuccess();
    }

    private CompletableFuture<Status> runDdl(QuerySession qs, String sql) {
        return qs.createQuery(sql, TxMode.NONE)
                .execute()
                .thenApply(res -> res.getStatus());
    }

}
