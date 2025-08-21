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

CREATE ASYNC HANDLER h1 PROCESS `test1/mv1` CONSUMER h1_consumer
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  INPUT `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  INPUT `test1/sub_table3` CHANGEFEED cf4 AS BATCH;
