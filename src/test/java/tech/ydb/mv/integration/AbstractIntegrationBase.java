package tech.ydb.mv.integration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.extension.RegisterExtension;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.data.YdbConv;
import tech.ydb.mv.support.YdbMisc;
import tech.ydb.query.QuerySession;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.test.junit5.YdbHelperExtension;

/**
 *
 * @author zinal
 */
public abstract class AbstractIntegrationBase {

    public static final String CREATE_TABLES
            = """
CREATE TABLE `test1/statements` (
    statement_no Int32 NOT NULL,
    statement_text Text NOT NULL,
    PRIMARY KEY(statement_no)
);

CREATE TABLE `test1/scans_state` (
   handler_name Text NOT NULL,
   table_name Text NOT NULL,
   updated_at Timestamp,
   key_position JsonDocument,
   PRIMARY KEY(handler_name, table_name)
);

CREATE TABLE `test1/dict_hist` (
   src Text NOT NULL,
   key_text Text NOT NULL,
   tv Timestamp NOT NULL,
   key_val JsonDocument,
   diff_val JsonDocument,
   PRIMARY KEY(src, key_text, tv)
);

CREATE TABLE `test1/main_table` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c6 Int32,
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
    c7 Text,
    c9 Date,
    main_id Text,
    PRIMARY KEY(c3, c4),
    INDEX ix_ref GLOBAL ON (main_id, c3, c4)
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
    c5 Text,
    c8 Int32,
    c9 Date,
    c10 Text,
    c11 Text,
    c12 Int32,
    PRIMARY KEY(id),
    INDEX ix_c1 GLOBAL ON (c1)
);

CREATE TABLE `test1/mv2` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c5 Text,
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
ALTER TABLE `test1/sub_table2` ADD CHANGEFEED `cf3` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
ALTER TABLE `test1/sub_table3` ADD CHANGEFEED `cf4` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
""";

    public static final String DROP_TABLES
            = """
DROP TABLE `test1/statements`;
DROP TABLE `test1/scans_state`;
DROP TABLE `test1/dict_hist`;
DROP TABLE `test1/main_table`;
DROP TABLE `test1/sub_table1`;
DROP TABLE `test1/sub_table2`;
DROP TABLE `test1/sub_table3`;
DROP TABLE `test1/mv1`;
DROP TABLE `test1/mv2`;
""";

    public static final String CDC_CONSUMERS1
            = """
ALTER TOPIC `test1/main_table/cf1` ADD CONSUMER `consumer1`;
ALTER TOPIC `test1/sub_table1/cf2` ADD CONSUMER `consumer1`;
ALTER TOPIC `test1/sub_table2/cf3` ADD CONSUMER `consumer1`;
ALTER TOPIC `test1/sub_table3/cf4` ADD CONSUMER `dictionary`;
""";

    public static final String CDC_CONSUMERS2
            = """
ALTER TOPIC `test1/main_table/cf1` ADD CONSUMER `consumer2`;
ALTER TOPIC `test1/sub_table1/cf2` ADD CONSUMER `consumer2`;
ALTER TOPIC `test1/sub_table2/cf3` ADD CONSUMER `consumer2`;
""";

    public static final String UPSERT_CONFIG
            = """
UPSERT INTO `test1/statements` (statement_no,statement_text) VALUES
  (1, @@CREATE ASYNC MATERIALIZED VIEW `test1/mv1` AS
  SELECT main.id AS id, main.c1 AS c1, main . c2 AS c2, main . c3 AS c3,
         sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10,
         #[ Unicode::Substring(main.c20,3,5) ]# AS c11,
         #[ CAST(999 AS Int32?) ]# AS c12, sub3.c5 AS c5
  FROM `test1/main_table` AS main
  INNER JOIN `test1/sub_table1` AS sub1
    ON main.c1=sub1.c1 AND main.c2=sub1.c2
  LEFT JOIN `test1/sub_table2` AS sub2
    ON main.c3=sub2.c3 AND 'val1'u=sub2.c4 AND main.id=sub2.main_id
  INNER JOIN `test1/sub_table3` AS sub3
    ON sub3.c5=58
  WHERE #[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u) ]#;@@),

  (2, @@CREATE ASYNC HANDLER handler1 CONSUMER consumer1 PROCESS `test1/mv1`,
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  INPUT `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  INPUT `test1/sub_table3` CHANGEFEED cf4 AS BATCH;@@),

  (3, @@CREATE ASYNC MATERIALIZED VIEW `test1/mv2` AS
    SELECT main.id AS id, main.c1 AS c1, main . c2 AS c2, main . c3 AS c3,
           sub1.c8 AS c8, sub2.c9 AS c9, sub3 . c10 AS c10,
           #[ Unicode::Substring(main.c20,3,5) ]# AS c11,
           #[ CAST(999 AS Int32?) ]# AS c12, sub3.c5 AS c5
    FROM `test1/main_table` AS main
    INNER JOIN `test1/sub_table1` AS sub1
      ON main.c1=sub1.c1 AND main.c2=sub1.c2
    LEFT JOIN `test1/sub_table2` AS sub2
      ON main.c3=sub2.c3 AND 'val1'u=sub2.c4 AND main.id=sub2.main_id
    INNER JOIN `test1/sub_table3` AS sub3
      ON sub3.c5=59
    WHERE #[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u) ]#;@@),

    (4, @@CREATE ASYNC HANDLER handler2 CONSUMER consumer2 PROCESS `test1/mv2`,
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  INPUT `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  INPUT `test1/sub_table3` CHANGEFEED cf4 AS BATCH;@@);
""";

    public static final String WRITE_INITIAL
            = """
INSERT INTO `test1/main_table` (id,c1,c2,c3,c6,c20) VALUES
 ('main-001'u, Timestamp('2021-01-02T10:15:21Z'), 10001, Decimal('10001.567',22,9), 7, 'text message one'u)
,('main-002'u, Timestamp('2022-01-02T10:15:21Z'), 10002, Decimal('10002.567',22,9), 7, 'text message two'u)
,('main-003'u, Timestamp('2023-01-02T10:15:21Z'), 10003, Decimal('10003.567',22,9), 7, 'text message three'u)
,('main-004'u, Timestamp('2024-01-02T10:15:21Z'), 10004, Decimal('10004.567',22,9), 7, 'text message four'u)
;
INSERT INTO `test1/sub_table1` (c1,c2,c8) VALUES
 (Timestamp('2021-01-02T10:15:21Z'), 10001, 501)
,(Timestamp('2022-01-02T10:15:21Z'), 10002, 502)
,(Timestamp('2023-01-02T10:15:21Z'), 10003, 503)
,(Timestamp('2024-01-02T10:15:21Z'), 10004, 504)
;
INSERT INTO `test1/sub_table2` (c3,c4,c7,c9,main_id) VALUES
 (Decimal('10001.567',22,9), 'val2'u, NULL,    Date('2020-07-10'), 'main-001'u)
,(Decimal('10002.567',22,9), 'val1'u, 'val2'u, Date('2020-07-11'), 'main-002'u)
,(Decimal('10003.567',22,9), 'val1'u, NULL,    Date('2020-07-12'), 'main-003'u)
,(Decimal('10004.567',22,9), 'val1'u, 'val2'u, Date('2020-07-13'), 'main-004'u)
,(Decimal('10002.567',22,9), 'val2'u, NULL,    Date('2020-07-14'), 'main-002'u)
,(Decimal('10003.567',22,9), 'val3'u, 'val2'u, Date('2020-07-15'), 'main-003'u)
,(Decimal('10004.567',22,9), 'val4'u, NULL,    Date('2020-07-16'), 'main-004'u)
;
INSERT INTO `test1/sub_table3` (c5,c10) VALUES
 (58, 'Welcome!'u)
,(59, 'Adieu!'u)
;
""";

    @RegisterExtension
    protected static final YdbHelperExtension YDB = new YdbHelperExtension();

    protected static String getConnectionUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(YDB.useTls() ? "grpcs://" : "grpc://");
        sb.append(YDB.endpoint());
        sb.append(YDB.database());
        return sb.toString();
    }

    protected static byte[] getConfig() {
        Properties props = new Properties();
        props.setProperty("ydb.url", getConnectionUrl());
        props.setProperty("ydb.auth.mode", "NONE");
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

    protected static void prepareDb() {
        // have to wait a bit here for YDB startup
        pause(5000L);
        // init database
        System.err.println("[AAA] Database setup...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            fillDatabase(conn);
        }
    }

    protected static void clearDb() {
        System.err.println("[AAA] Database cleanup...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, DROP_TABLES);
        }
    }

    protected static void pause(long millis) {
        System.err.println("\t...Sleeping for " + millis + "...");
        YdbMisc.sleep(millis);
    }

    protected static void fillDatabase(YdbConnector conn) {
        System.err.println("[AAA] Preparation: creating tables...");
        runDdl(conn, CREATE_TABLES);
        System.err.println("[AAA] Preparation: adding consumers...");
        runDdl(conn, CDC_CONSUMERS1);
        runDdl(conn, CDC_CONSUMERS2);
        System.err.println("[AAA] Preparation: adding config...");
        runDdl(conn, UPSERT_CONFIG);
    }

    protected static CompletableFuture<Status> runSql(QuerySession qs, String sql, TxMode txMode) {
        return qs.createQuery(sql, txMode)
                .execute()
                .thenApply(res -> res.getStatus());
    }

    protected static void runDdl(YdbConnector conn, String sql) {
        conn.getQueryRetryCtx()
                .supplyStatus(qs -> runSql(qs, sql, TxMode.NONE))
                .join()
                .expectSuccess();
    }

    protected static void runDml(YdbConnector conn, String sql) {
        conn.getQueryRetryCtx()
                .supplyStatus(qs -> runSql(qs, sql, TxMode.SERIALIZABLE_RW))
                .join()
                .expectSuccess();
    }


    protected static HashMap<String, HashMap<String, String>> convertResultSet(ResultSetReader rsr, String keyName) {
        int indexColumn = rsr.getColumnIndex(keyName);
        HashMap<String, HashMap<String, String>> output = new HashMap<>();
        while (rsr.next()) {
            String key = YdbConv.toPojo(rsr.getColumn(indexColumn).getValue()).toString();
            HashMap<String, String> value = new HashMap<>();
            for (int index = 0; index < rsr.getColumnCount(); ++index) {
                String name = rsr.getColumnName(index);
                Comparable<?> x = YdbConv.toPojo(rsr.getColumn(index).getValue());
                if (x!=null) {
                    value.put(name, x.toString());
                }
            }
            output.put(key, value);
        }
        return output;
    }
}
