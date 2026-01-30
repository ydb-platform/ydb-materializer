-- Minimal MV definition: single source table, single handler.
CREATE ASYNC MATERIALIZED VIEW `test1/mv_simple` AS
  SELECT main.id AS id, main.c1 AS c1
  FROM `test1/main_table` AS main;

CREATE ASYNC HANDLER h1 CONSUMER h1_consumer
  PROCESS `test1/mv_simple`
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM;
-- Minimal MV definition: single source table, single handler.
CREATE ASYNC MATERIALIZED VIEW `test1/mv_simple` AS
  SELECT main.id AS id, main.c1 AS c1
  FROM `test1/main_table` AS main;

CREATE ASYNC HANDLER h1 CONSUMER h1_consumer
  PROCESS `test1/mv_simple`
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM;
