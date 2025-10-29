package tech.ydb.mv.integration;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.MvConfig;
import tech.ydb.mv.svc.MvService;
import tech.ydb.mv.YdbConnector;

/**
 * colima start --arch aarch64 --vm-type=vz --vz-rosetta (or) colima start
 * --arch amd64
 *
 * while mvn test -Dtest=UnionAllIntegrationTest; do sleep 0.5s; done
 *
 * @author zinal
 */
public class UnionAllIntegrationTest extends AbstractIntegrationBase {

    public static final String CREATE_TABLES3
            = """
CREATE TABLE `test2/main1` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c4 Int32,
    c5 Int32,
    c6 Text,
    PRIMARY KEY(id),
    INDEX ix_c4 GLOBAL ON (c4)
);

CREATE TABLE `test2/sub1` (
    c4 Int32 NOT NULL,
    c7 Text,
    PRIMARY KEY(c4)
);

CREATE TABLE `test2/main2` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c4 Int32,
    c5 Int32,
    c6 Text,
    PRIMARY KEY(id),
    INDEX ix_c4 GLOBAL ON (c4)
);

CREATE TABLE `test2/sub2` (
    c4 Int32 NOT NULL,
    c7 Text,
    PRIMARY KEY(c4)
);

CREATE TABLE `test2/mv1` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c4 Int32,
    c5 Int32,
    c6 Text,
    c7 Text,
    PRIMARY KEY(id),
    INDEX ix_c1 GLOBAL ON (c1),
    INDEX ix_c7 GLOBAL ON (c7)
);

ALTER TABLE `test2/main1` ADD CHANGEFEED `cf0` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test2/sub1` ADD CHANGEFEED `cf1` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test2/main2` ADD CHANGEFEED `cf2` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test2/sub2` ADD CHANGEFEED `cf3` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
""";

    public static final String DROP_TABLES3
            = """
DROP TABLE `test2/main1`;
DROP TABLE `test2/main2`;
DROP TABLE `test2/sub1`;
DROP TABLE `test2/sub2`;
DROP TABLE `test2/mv1`;
""";

    public static final String CDC_CONSUMERS3
            = """
ALTER TOPIC `test2/main1/cf0` ADD CONSUMER `consumer3`;
ALTER TOPIC `test2/sub1/cf1` ADD CONSUMER `consumer3`;
ALTER TOPIC `test2/main2/cf2` ADD CONSUMER `consumer3`;
ALTER TOPIC `test2/sub2/cf3` ADD CONSUMER `consumer3`;
""";

    public static final String UPSERT_CONFIG3
            = """
UPSERT INTO `test1/statements` (statement_no,statement_text) VALUES
  (1, @@CREATE ASYNC MATERIALIZED VIEW `test2/mv1` AS

(
SELECT
    m.id AS id,
    m.c1 AS c1,
    m.c2 AS c2,
    m.c3 AS c3,
    m.c4 AS c4,
    m.c5 AS c5,
    m.c6 AS c6,
    s.c7 AS c7
FROM `test2/main1` AS m
INNER JOIN `test2/sub1` AS s
  ON m.c4=s.c4
) AS part1
UNION ALL
(
SELECT
    m.id AS id,
    m.c1 AS c1,
    m.c2 AS c2,
    m.c3 AS c3,
    m.c4 AS c4,
    m.c5 AS c5,
    m.c6 AS c6,
    s.c7 AS c7
FROM `test2/main2` AS m
INNER JOIN `test2/sub2` AS s
  ON m.c4=s.c4
) AS part2;@@),

  (2, @@CREATE ASYNC HANDLER handler3 CONSUMER consumer3
  PROCESS `test2/mv1`,
  INPUT `test2/main1` CHANGEFEED cf0 AS STREAM,
  INPUT `test2/sub1` CHANGEFEED cf1 AS STREAM,
  INPUT `test2/main2` CHANGEFEED cf2 AS STREAM,
  INPUT `test2/sub2` CHANGEFEED cf3 AS STREAM;@@);
""";

    public static final String SELECT_ALL3 = """
(SELECT
    m.id AS id,
    m.c1 AS c1,
    m.c2 AS c2,
    m.c3 AS c3,
    m.c4 AS c4,
    m.c5 AS c5,
    m.c6 AS c6,
    s.c7 AS c7
FROM `test2/main1` AS m
INNER JOIN `test2/sub1` AS s
  ON m.c4=s.c4)
UNION ALL
(SELECT
    m.id AS id,
    m.c1 AS c1,
    m.c2 AS c2,
    m.c3 AS c3,
    m.c4 AS c4,
    m.c5 AS c5,
    m.c6 AS c6,
    s.c7 AS c7
FROM `test2/main2` AS m
INNER JOIN `test2/sub2` AS s
  ON m.c4=s.c4)
""";

    public static final String WRITE_INITIAL3_1
            = """
INSERT INTO `test2/main1` (id,c1,c2,c3,c4,c5,c6) VALUES
 ('main1-001'u, Timestamp('2021-01-02T10:15:21Z'), 10001, Decimal('10001.567',22,9), 101, 1, 'text message one'u)
,('main1-002'u, Timestamp('2022-01-02T10:15:22Z'), 10002, Decimal('10002.567',22,9), 102, 3, 'text message two'u)
,('main1-003'u, Timestamp('2023-01-02T10:15:23Z'), 10003, Decimal('10003.567',22,9), 103, 5, 'text message three'u)
,('main1-004'u, Timestamp('2024-01-02T10:15:24Z'), 10004, Decimal('10004.567',22,9), 104, 7, 'text message four'u)
;
INSERT INTO `test2/sub1` (c4, c7) VALUES
 (101, '101-aga'u)
,(102, '102-aga'u)
,(103, '103-aga'u)
,(104, '104-aga'u)
;
""";

    public static final String WRITE_INITIAL3_2
            = """
INSERT INTO `test2/main2` (id,c1,c2,c3,c4,c5,c6) VALUES
 ('main0-001'u, Timestamp('2021-01-02T11:15:21Z'), 20001, Decimal('20001.567',22,9), 201, 2, 'text message one'u)
,('main0-002'u, Timestamp('2022-01-02T11:15:22Z'), 20002, Decimal('20002.567',22,9), 202, 4, 'text message two'u)
,('main0-003'u, Timestamp('2023-01-02T11:15:23Z'), 20003, Decimal('20003.567',22,9), 203, 6, 'text message three'u)
,('main0-004'u, Timestamp('2024-01-02T11:15:24Z'), 20004, Decimal('20004.567',22,9), 204, 8, 'text message four'u)
;
INSERT INTO `test2/sub2` (c4, c7) VALUES
 (201, '201-hehe'u)
,(202, '202-hehe'u)
,(203, '203-hehe'u)
,(204, '204-hehe'u)
;
""";

    public static final String WRITE_UPDATE3
            = """
UPSERT INTO `test2/main1` (id,c4,c6) VALUES
 ('main1-002'u, 101, 'text message two-bis'u)
,('main1-003'u, 101, 'text message three-bis'u)
;
UPSERT INTO `test2/main2` (id,c4,c6) VALUES
 ('main0-001'u, 204, 'text message one-bis'u)
,('main0-004'u, 201, 'text message four-bis'u)
;
""";

    @Override
    protected Properties getConfigProps() {
        var props = super.getConfigProps();
        props.setProperty(MvConfig.CONF_HANDLERS, "handler3");
        return props;
    }

    @BeforeEach
    public void init() {
        // have to wait a bit here for YDB startup
        pause(5000L);
        // init database
        System.err.println("[UUU] Database setup...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfigBytes(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            System.err.println("[UUU] Preparation: creating tables...");
            runDdl(conn, CREATE_TABLES1);
            runDdl(conn, CREATE_TABLES3);
            System.err.println("[UUU] Preparation: adding consumers...");
            runDdl(conn, CDC_CONSUMERS3);
            System.err.println("[UUU] Preparation: adding config...");
            runDdl(conn, UPSERT_CONFIG3);
        }
    }

    @AfterEach
    public void cleanup() {
        System.err.println("[UUU] Database cleanup...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfigBytes(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, DROP_TABLES1);
            runDdl(conn, DROP_TABLES3);
        }
    }

    @Test
    public void unionAllIntegrationTest() {
        System.err.println("[UUU] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfigBytes(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            MvService svc = new MvService(conn);
            try {
                svc.applyDefaults(conn.getConfig().getProperties());

                System.err.println("[UUU] Checking context...");
                svc.printIssues(System.out);
                Assertions.assertTrue(svc.getMetadata().isValid());

                System.err.println("[UUU] Printing SQL...");
                svc.printSql(System.out);

                System.err.println("[UUU] Entering main test...");
                testLogic(svc);
            } finally {
                svc.shutdown();
            }
        }
    }

    private void testLogic(MvService svc) {
        System.err.println("[UUU] Starting the services...");
        svc.startDefaultHandlers();
        svc.startDictionaryHandler();
        standardPause();

        System.err.println("[UUU] Checking the view output (should be empty)...");
        int diffCount = checkViewOutput(svc);
        Assertions.assertEquals(0, diffCount);

        System.err.println("[UUU] Writing input data 1...");
        runDml(svc.getYdb(), WRITE_INITIAL3_1);
        standardPause();
        System.err.println("[UUU] Checking the view output...");
        diffCount = checkViewOutput(svc);
        Assertions.assertEquals(0, diffCount);

        System.err.println("[UUU] Writing input data 2...");
        runDml(svc.getYdb(), WRITE_INITIAL3_2);
        standardPause();
        System.err.println("[UUU] Checking the view output...");
        diffCount = checkViewOutput(svc);
        Assertions.assertEquals(0, diffCount);

        System.err.println("[UUU] Writing input data 3...");
        runDml(svc.getYdb(), WRITE_UPDATE3);
        standardPause();
        System.err.println("[UUU] Checking the view output...");
        diffCount = checkViewOutput(svc);
        Assertions.assertEquals(0, diffCount);
    }

    private int checkViewOutput(MvService svc) {
        return checkViewOutput(svc, "test2/mv1", SELECT_ALL3);
    }

}
