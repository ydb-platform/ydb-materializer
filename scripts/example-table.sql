-- An example of YDB table for MV definitions
CREATE TABLE `mv/statements` (
   statement_no Int32 NOT NULL,
   statement_text Text NOT NULL,
   PRIMARY KEY(statement_no);
);

-- An example schema for the source tables.
CREATE TABLE `test1/main_table` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c20 Text,
    PRIMARY KEY(id),
    INDEX ix_c1 GLOBAL ON (c1)
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

-- ****************************************

-- An example configuration "statement" for MV.
CREATE ASYNC MATERIALIZED VIEW `test1/mv1` AS
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
  #[ main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u) ]#;

CREATE ASYNC HANDLER h1
  PROCESS `test1/main_table` CHANGEFEED cf1 AS STREAM,
  PROCESS `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  PROCESS `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  PROCESS `test1/sub_table3` CHANGEFEED cf4 AS BATCH;
