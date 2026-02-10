package tech.ydb.mv.integration;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.table.result.ResultSetReader;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.data.YdbConv;
import tech.ydb.mv.svc.MvService;
import tech.ydb.table.query.Params;

/**
 * Integration test for deletes on a UNION ALL view with computed primary key.
 */
public class ComputedKeyDeleteIntegrationTest extends AbstractIntegrationBase {

    private static final String CREATE_TABLES_CK
            = """
            CREATE TABLE `test3/main1` (
                id Text NOT NULL,
                inner_id Int32,
                left_id Int32,
                dict_inner_id Int32,
                dict_left_id Int32,
                payload Text,
                PRIMARY KEY(id)
            );
            
            CREATE TABLE `test3/inter_inner1` (
                main_id Text NOT NULL,
                id Int32,
                value Text,
                PRIMARY KEY(main_id)
            );
            
            CREATE TABLE `test3/inter_left1` (
                main_id Text NOT NULL,
                id Int32,
                value Text,
                PRIMARY KEY(main_id)
            );
            
            CREATE TABLE `test3/dict_inner1` (
                id Int32 NOT NULL,
                name Text,
                PRIMARY KEY(id)
            );
            
            CREATE TABLE `test3/dict_left1` (
                id Int32 NOT NULL,
                name Text,
                PRIMARY KEY(id)
            );
            
            CREATE TABLE `test3/main2` (
                id Text NOT NULL,
                inner_id Int32,
                left_id Int32,
                dict_inner_id Int32,
                dict_left_id Int32,
                payload Text,
                PRIMARY KEY(id)
            );
            
            CREATE TABLE `test3/inter_inner2` (
                main_id Text NOT NULL,
                id Int32,
                value Text,
                PRIMARY KEY(main_id)
            );
            
            CREATE TABLE `test3/inter_left2` (
                main_id Text NOT NULL,
                id Int32,
                value Text,
                PRIMARY KEY(main_id)
            );
            
            CREATE TABLE `test3/dict_inner2` (
                id Int32 NOT NULL,
                name Text,
                PRIMARY KEY(id)
            );
            
            CREATE TABLE `test3/dict_left2` (
                id Int32 NOT NULL,
                name Text,
                PRIMARY KEY(id)
            );
            
            CREATE TABLE `test3/mv_complex` (
                kind Text NOT NULL,
                id Text NOT NULL,
                payload Text,
                inner_val Text,
                left_val Text,
                dict_inner_name Text,
                dict_left_name Text,
                PRIMARY KEY(kind, id)
            );
            
            ALTER TABLE `test3/main1` ADD CHANGEFEED `cf0` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
            ALTER TABLE `test3/inter_inner1` ADD CHANGEFEED `cf1` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            ALTER TABLE `test3/inter_left1` ADD CHANGEFEED `cf2` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            ALTER TABLE `test3/dict_inner1` ADD CHANGEFEED `cf3` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            ALTER TABLE `test3/dict_left1` ADD CHANGEFEED `cf4` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            
            ALTER TABLE `test3/main2` ADD CHANGEFEED `cf5` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
            ALTER TABLE `test3/inter_inner2` ADD CHANGEFEED `cf6` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            ALTER TABLE `test3/inter_left2` ADD CHANGEFEED `cf7` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            ALTER TABLE `test3/dict_inner2` ADD CHANGEFEED `cf8` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            ALTER TABLE `test3/dict_left2` ADD CHANGEFEED `cf9` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
            """;

    private static final String DROP_TABLES_CK
            = """
            DROP TABLE `test3/main1`;
            DROP TABLE `test3/inter_inner1`;
            DROP TABLE `test3/inter_left1`;
            DROP TABLE `test3/dict_inner1`;
            DROP TABLE `test3/dict_left1`;
            DROP TABLE `test3/main2`;
            DROP TABLE `test3/inter_inner2`;
            DROP TABLE `test3/inter_left2`;
            DROP TABLE `test3/dict_inner2`;
            DROP TABLE `test3/dict_left2`;
            DROP TABLE `test3/mv_complex`;
            """;

    private static final String CDC_CONSUMERS_CK
            = """
            ALTER TOPIC `test3/main1/cf0` ADD CONSUMER `consumer_ck`;
            ALTER TOPIC `test3/inter_inner1/cf1` ADD CONSUMER `consumer_ck`;
            ALTER TOPIC `test3/inter_left1/cf2` ADD CONSUMER `consumer_ck`;
            ALTER TOPIC `test3/dict_inner1/cf3` ADD CONSUMER `dictionary`;
            ALTER TOPIC `test3/dict_left1/cf4` ADD CONSUMER `dictionary`;
            
            ALTER TOPIC `test3/main2/cf5` ADD CONSUMER `consumer_ck`;
            ALTER TOPIC `test3/inter_inner2/cf6` ADD CONSUMER `consumer_ck`;
            ALTER TOPIC `test3/inter_left2/cf7` ADD CONSUMER `consumer_ck`;
            ALTER TOPIC `test3/dict_inner2/cf8` ADD CONSUMER `dictionary`;
            ALTER TOPIC `test3/dict_left2/cf9` ADD CONSUMER `dictionary`;
            """;

    private static final String UPSERT_CONFIG_CK
            = """
            UPSERT INTO `test1/statements` (statement_no,statement_text) VALUES
              (10, @@CREATE ASYNC MATERIALIZED VIEW `test3/mv_complex` AS
            (
            SELECT
                #[ 'first'u ]# AS kind,
                m.id AS id,
                m.payload AS payload,
                i.value AS inner_val,
                l.value AS left_val,
                di.name AS dict_inner_name,
                dl.name AS dict_left_name
            FROM `test3/main1` AS m
            INNER JOIN `test3/inter_inner1` AS i
              ON i.main_id=m.id
            LEFT JOIN `test3/inter_left1` AS l
              ON l.main_id=m.id
            INNER JOIN `test3/dict_inner1` AS di
              ON di.id=m.dict_inner_id
            LEFT JOIN `test3/dict_left1` AS dl
              ON dl.id=m.dict_left_id
            ) AS part1
            UNION ALL
            (
            SELECT
                #[ 'second'u ]# AS kind,
                m.id AS id,
                m.payload AS payload,
                i.value AS inner_val,
                l.value AS left_val,
                di.name AS dict_inner_name,
                dl.name AS dict_left_name
            FROM `test3/main2` AS m
            INNER JOIN `test3/inter_inner2` AS i
              ON i.main_id=m.id
            LEFT JOIN `test3/inter_left2` AS l
              ON l.main_id=m.id
            INNER JOIN `test3/dict_inner2` AS di
              ON di.id=m.dict_inner_id
            LEFT JOIN `test3/dict_left2` AS dl
              ON dl.id=m.dict_left_id
            ) AS part2;@@),
            
              (11, @@CREATE ASYNC HANDLER handler_ck CONSUMER consumer_ck
              PROCESS `test3/mv_complex`,
              INPUT `test3/main1` CHANGEFEED cf0 AS STREAM,
              INPUT `test3/inter_inner1` CHANGEFEED cf1 AS STREAM,
              INPUT `test3/inter_left1` CHANGEFEED cf2 AS STREAM,
              INPUT `test3/dict_inner1` CHANGEFEED cf3 AS BATCH,
              INPUT `test3/dict_left1` CHANGEFEED cf4 AS BATCH,
              INPUT `test3/main2` CHANGEFEED cf5 AS STREAM,
              INPUT `test3/inter_inner2` CHANGEFEED cf6 AS STREAM,
              INPUT `test3/inter_left2` CHANGEFEED cf7 AS STREAM,
              INPUT `test3/dict_inner2` CHANGEFEED cf8 AS BATCH,
              INPUT `test3/dict_left2` CHANGEFEED cf9 AS BATCH;@@);
            """;

    private static final String SELECT_ALL_CK = """
            (SELECT
                'first'u AS kind,
                m.id AS id,
                m.payload AS payload,
                i.value AS inner_val,
                l.value AS left_val,
                di.name AS dict_inner_name,
                dl.name AS dict_left_name
            FROM `test3/main1` AS m
            INNER JOIN `test3/inter_inner1` AS i
              ON i.main_id=m.id
            LEFT JOIN `test3/inter_left1` AS l
              ON l.main_id=m.id
            INNER JOIN `test3/dict_inner1` AS di
              ON di.id=m.dict_inner_id
            LEFT JOIN `test3/dict_left1` AS dl
              ON dl.id=m.dict_left_id)
            UNION ALL
            (SELECT
                'second'u AS kind,
                m.id AS id,
                m.payload AS payload,
                i.value AS inner_val,
                l.value AS left_val,
                di.name AS dict_inner_name,
                dl.name AS dict_left_name
            FROM `test3/main2` AS m
            INNER JOIN `test3/inter_inner2` AS i
              ON i.main_id=m.id
            LEFT JOIN `test3/inter_left2` AS l
              ON l.main_id=m.id
            INNER JOIN `test3/dict_inner2` AS di
              ON di.id=m.dict_inner_id
            LEFT JOIN `test3/dict_left2` AS dl
              ON dl.id=m.dict_left_id)
            """;

    private static final String WRITE_INITIAL_CK
            = """
            INSERT INTO `test3/dict_inner1` (id, name) VALUES
             (301, 'dict-inner-1'u),
             (302, 'dict-inner-2'u),
             (303, 'dict-inner-3'u),
             (304, 'dict-inner-4'u),
             (305, 'dict-inner-5'u);
            INSERT INTO `test3/dict_left1` (id, name) VALUES
             (401, 'dict-left-1'u),
             (402, 'dict-left-2'u),
             (403, 'dict-left-3'u),
             (404, 'dict-left-4'u),
             (405, 'dict-left-5'u);
            
            INSERT INTO `test3/main1` (id, inner_id, left_id, dict_inner_id, dict_left_id, payload) VALUES
             ('m1-a'u, 101, 201, 301, 401, 'payload-a'u),
             ('m1-b'u, 102, 202, 302, 402, 'payload-b'u),
             ('m1-c'u, 103, 203, 303, 403, 'payload-c'u),
             ('m1-d'u, 104, 204, 304, 404, 'payload-d'u),
             ('m1-e'u, 105, 205, 305, 405, 'payload-e'u);
            
            INSERT INTO `test3/inter_inner1` (main_id, id, value) VALUES
             ('m1-a'u, 101, 'inner-a'u),
             ('m1-b'u, 102, 'inner-b'u),
             ('m1-c'u, 103, 'inner-c'u),
             ('m1-d'u, 104, 'inner-d'u),
             ('m1-e'u, 105, 'inner-e'u);
            
            INSERT INTO `test3/inter_left1` (main_id, id, value) VALUES
             ('m1-a'u, 201, 'left-a'u),
             ('m1-b'u, 202, 'left-b'u),
             ('m1-c'u, 203, 'left-c'u),
             ('m1-d'u, 204, 'left-d'u),
             ('m1-e'u, 205, 'left-e'u);
            
            INSERT INTO `test3/dict_inner2` (id, name) VALUES
             (501, 'dict-inner-x'u),
             (502, 'dict-inner-y'u);
            INSERT INTO `test3/dict_left2` (id, name) VALUES
             (601, 'dict-left-x'u),
             (602, 'dict-left-y'u);
            
            INSERT INTO `test3/main2` (id, inner_id, left_id, dict_inner_id, dict_left_id, payload) VALUES
             ('m2-a'u, 401, 501, 501, 601, 'payload-x'u),
             ('m2-b'u, 402, 502, 502, 602, 'payload-y'u);
            
            INSERT INTO `test3/inter_inner2` (main_id, id, value) VALUES
             ('m2-a'u, 401, 'inner-x'u),
             ('m2-b'u, 402, 'inner-y'u);
            INSERT INTO `test3/inter_left2` (main_id, id, value) VALUES
             ('m2-a'u, 501, 'left-x'u),
             ('m2-b'u, 502, 'left-y'u);
            """;

    private static final String DELETE_MAIN_LEFTMOST
            = "DELETE FROM `test3/main1` WHERE id='m1-a'u;";

    private static final String DELETE_INTERMEDIATE_INNER
            = "DELETE FROM `test3/inter_inner1` WHERE main_id='m1-b'u;";

    private static final String DELETE_INTERMEDIATE_LEFT
            = "DELETE FROM `test3/inter_left1` WHERE main_id='m1-c'u;";

    private static final String DELETE_DICT_INNER
            = "DELETE FROM `test3/dict_inner1` WHERE id=304;";

    private static final String DELETE_DICT_LEFT
            = "DELETE FROM `test3/dict_left1` WHERE id=405;";

    @Override
    protected Properties getConfigProps() {
        var props = super.getConfigProps();
        props.setProperty(MvConfig.CONF_HANDLERS, "handler_ck");
        return props;
    }

    @BeforeEach
    public void init() {
        pause(10_000L);
        var cfg = MvConfig.fromBytes(getConfigBytes(), "config.xml");
        try (var conn = new YdbConnector(cfg, true)) {
            runDdl(conn, CREATE_TABLES_BASE);
            runDdl(conn, CREATE_TABLES_CK);
            runDdl(conn, CDC_CONSUMERS_CK);
            runDdl(conn, UPSERT_CONFIG_CK);
        }
    }

    @AfterEach
    public void cleanup() {
        var cfg = MvConfig.fromBytes(getConfigBytes(), "config.xml");
        try (var conn = new YdbConnector(cfg, true)) {
            runDdl(conn, DROP_TABLES_BASE);
            runDdl(conn, DROP_TABLES_CK);
        }
    }

    @Test
    public void deleteScenariosForComputedKey() {
        var cfg = MvConfig.fromBytes(getConfigBytes(), "config.xml");
        try (var conn = new YdbConnector(cfg, true)) {
            try (var svc = new MvService(conn)) {
                svc.applyDefaults(conn.getConfig().getProperties());
                System.err.println("[CK] Creating CDC streams...");
                svc.generateStreams(true, new PrintStream(new ByteArrayOutputStream()));
            }
            try (var svc = new MvService(conn)) {
                svc.applyDefaults(conn.getConfig().getProperties());

                System.err.println("[CK] Checking context...");
                svc.printIssues(System.out);
                Assertions.assertTrue(svc.getMetadata().isValid());

                System.err.println("[CK] Starting services...");
                svc.startDefaultHandlers();
                svc.startDictionaryHandler();
                standardPause();

                System.err.println("[CK] Writing initial data...");
                runDml(conn, WRITE_INITIAL_CK);
                standardPause();

                System.err.println("[CK] Verifying initial view output...");
                assertViewOutputEventually(conn, "initial", 0, 30_000L);

                // Delete from the leftmost (main) table.
                System.err.println("[CK] Deleting from main table...");
                runDml(conn, DELETE_MAIN_LEFTMOST);
                standardPause();
                assertViewOutputEventually(conn, "delete-main", 0, 30_000L);

                // Delete from an INNER-joined intermediate table.
                System.err.println("[CK] Deleting from inner intermediate table...");
                runDml(conn, DELETE_INTERMEDIATE_INNER);
                standardPause();
                assertViewOutputEventually(conn, "delete-inter-inner", 0, 30_000L);

                // Delete from a LEFT-joined intermediate table.
                System.err.println("[CK] Deleting from left intermediate table...");
                runDml(conn, DELETE_INTERMEDIATE_LEFT);
                standardPause();
                assertViewOutputEventually(conn, "delete-inter-left", 0, 30_000L);

                // Delete from a dictionary table joined with INNER.
                System.err.println("[CK] Deleting from inner dictionary table...");
                runDml(conn, DELETE_DICT_INNER);
                standardPause();
                standardPause();
                assertViewOutputEventually(conn, "delete-dict-inner", 0, 30_000L);

                // Delete from a dictionary table joined with LEFT.
                System.err.println("[CK] Deleting from left dictionary table...");
                runDml(conn, DELETE_DICT_LEFT);
                standardPause();
                standardPause();
                assertViewOutputEventually(conn, "delete-dict-left", 0, 30_000L);
            }
        }
    }

    private int checkViewOutputComposite(YdbConnector conn) {
        String viewName = "test3/mv_complex";
        var left = convertResultSetComposite(
                conn.sqlRead(SELECT_ALL_CK, Params.empty()).getResultSet(0),
                "kind", "id");
        var right = convertResultSetComposite(
                conn.sqlRead("SELECT * FROM `" + viewName + "`", Params.empty()).getResultSet(0),
                "kind", "id");
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

    private void assertViewOutputEventually(YdbConnector conn, String stage, int expected, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int diff = Integer.MAX_VALUE;
        while (System.currentTimeMillis() < deadline) {
            diff = checkViewOutputComposite(conn);
            if (diff == expected) {
                return;
            }
            pause(1000L);
        }
        System.out.println("[CK] Stage '" + stage + "' timed out, last diff=" + diff);
        // One more check to print detailed differences before failing.
        diff = checkViewOutputComposite(conn);
        Assertions.assertEquals(expected, diff);
    }

    private HashMap<String, HashMap<String, String>> convertResultSetComposite(
            ResultSetReader rsr, String key1, String key2) {
        int indexKey1 = rsr.getColumnIndex(key1);
        int indexKey2 = rsr.getColumnIndex(key2);
        HashMap<String, HashMap<String, String>> output = new HashMap<>();
        while (rsr.next()) {
            String k1 = YdbConv.toPojo(rsr.getColumn(indexKey1).getValue()).toString();
            String k2 = YdbConv.toPojo(rsr.getColumn(indexKey2).getValue()).toString();
            String key = k1 + "|" + k2;
            HashMap<String, String> value = new HashMap<>();
            for (int index = 0; index < rsr.getColumnCount(); ++index) {
                String name = rsr.getColumnName(index);
                Comparable<?> x = YdbConv.toPojo(rsr.getColumn(index).getValue());
                if (x != null) {
                    value.put(name, x.toString());
                }
            }
            output.put(key, value);
        }
        return output;
    }
}
