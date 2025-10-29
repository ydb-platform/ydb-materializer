package tech.ydb.mv.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.mv.AbstractIntegrationBase;
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

    @BeforeEach
    public void init() {
        // have to wait a bit here for YDB startup
        pause(5000L);
        // init database
        System.err.println("[UUU] Database setup...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
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
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            runDdl(conn, DROP_TABLES1);
            runDdl(conn, DROP_TABLES3);
        }
    }

    @Test
    public void unionAllIntegrationTest() {
        System.err.println("[UUU] Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            MvService wc = new MvService(conn);
            try {
                wc.applyDefaults(conn.getConfig().getProperties());

                System.err.println("[UUU] Checking context...");
                wc.printIssues(System.out);
                Assertions.assertTrue(wc.getMetadata().isValid());

                System.err.println("[UUU] Printing SQL...");
                wc.printSql(System.out);

                System.err.println("[UUU] Entering main test...");
                testLogic();
            } finally {
                wc.shutdown();
            }
        }
    }

    private void testLogic() {
    }

}
