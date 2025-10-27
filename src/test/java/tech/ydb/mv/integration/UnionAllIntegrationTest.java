package tech.ydb.mv.integration;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.table.query.Params;
import tech.ydb.topic.settings.DescribeConsumerSettings;

import tech.ydb.mv.AbstractIntegrationBase;
import tech.ydb.mv.svc.MvService;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.parser.MvSqlGen;

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
    c5 Text,
    c8 Int32,
    c9 Date,
    c10 Text,
    c11 Text,
    c12 Int32,
    c16 Text,
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

ALTER TABLE `test1/main_table` ADD CHANGEFEED `cf0` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table1` ADD CHANGEFEED `cf1` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table2` ADD CHANGEFEED `cf2` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
ALTER TABLE `test1/sub_table3` ADD CHANGEFEED `cf3` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
ALTER TABLE `test1/sub_table4` ADD CHANGEFEED `cf4` WITH (FORMAT = 'JSON', MODE = 'NEW_AND_OLD_IMAGES');
""";

}
